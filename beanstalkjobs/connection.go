package beanstalkjobs

import (
	"context"
	stderr "errors"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/beanstalkd/go-beanstalk"
	"github.com/cenkalti/backoff/v4"
	"github.com/roadrunner-server/errors"
	"go.uber.org/zap"
)

var errBeanstalkTimeout = stderr.New("beanstalk timeout")

type ConnPool struct {
	sync.RWMutex

	log *zap.Logger

	connTS atomic.Pointer[beanstalk.Conn]
	connT  atomic.Pointer[beanstalk.Conn]
	ts     atomic.Pointer[beanstalk.TubeSet]
	t      atomic.Pointer[beanstalk.Tube]

	network string
	address string
	tName   string
	tout    time.Duration
}

func NewConnPool(network, address, tName string, tout time.Duration, log *zap.Logger) (*ConnPool, error) {
	connT, err := beanstalk.DialTimeout(network, address, tout)
	if err != nil {
		return nil, err
	}

	connTS, err := beanstalk.DialTimeout(network, address, tout)
	if err != nil {
		return nil, err
	}

	tb := beanstalk.NewTube(connT, tName)
	ts := beanstalk.NewTubeSet(connTS, tName)

	cp := &ConnPool{
		log:     log,
		network: network,
		address: address,
		tName:   tName,
		tout:    tout,
	}

	cp.connTS.Store(connTS)
	cp.connT.Store(connT)
	cp.ts.Store(ts)
	cp.t.Store(tb)

	return cp, nil
}

// Put the payload
// TODO use the context ??
func (cp *ConnPool) Put(_ context.Context, body []byte, pri uint32, delay, ttr time.Duration) (uint64, error) {
	cp.RLock()
	defer cp.RUnlock()

	// TODO(rustatian): redial based on the token
	id, err := cp.t.Load().Put(body, pri, delay, ttr)
	if err != nil {
		// errN contains both, err and internal checkAndRedial error
		errN := cp.checkAndRedial(err)
		if errN != nil {
			return 0, errors.Errorf("err: %s\nerr redial: %s", err, errN)
		}

		return cp.t.Load().Put(body, pri, delay, ttr)
	}

	return id, nil
}

// Reserve reserves and returns a job from one of the tubes in t. If no
// job is available before time timeout has passed, Reserve returns a
// ConnError recording ErrTimeout.
//
// Typically, a client will reserve a job, perform some work, then delete
// the job with Conn.Delete.
func (cp *ConnPool) Reserve(reserveTimeout time.Duration) (uint64, []byte, error) {
	cp.RLock()
	defer cp.RUnlock()

	id, body, err := cp.ts.Load().Reserve(reserveTimeout)
	if err != nil {
		// errN contains both, err and internal checkAndRedial error
		errN := cp.checkAndRedial(err)
		if errN != nil {
			return 0, nil, stderr.Join(err, errN)
		}

		// retry Reserve only when we redialed
		return cp.ts.Load().Reserve(reserveTimeout)
	}

	return id, body, nil
}

func (cp *ConnPool) Delete(_ context.Context, id uint64) error {
	cp.RLock()
	defer cp.RUnlock()

	err := cp.connTS.Load().Delete(id)
	if err != nil {
		// errN contains both, err and internal checkAndRedial error
		errN := cp.checkAndRedial(err)
		if errN != nil {
			return errors.Errorf("err: %s\nerr redial: %s", err, errN)
		}

		// retry, Delete only when we redialed
		return cp.connTS.Load().Delete(id)
	}
	return nil
}

func (cp *ConnPool) Stats(_ context.Context) (map[string]string, error) {
	cp.RLock()
	defer cp.RUnlock()

	stat, err := cp.connTS.Load().Stats()
	if err != nil {
		errR := cp.checkAndRedial(err)
		if errR != nil {
			return nil, errors.Errorf("err: %s\nerr redial: %s", err, errR)
		}

		return cp.connTS.Load().Stats()
	}

	return stat, nil
}

// Stop and close the connections
func (cp *ConnPool) Stop() {
	cp.Lock()
	_ = cp.connTS.Load().Close()
	_ = cp.connT.Load().Close()
	cp.Unlock()
}

func (cp *ConnPool) redial() error {
	const op = errors.Op("connection_pool_redial")

	cp.Lock()
	// backoff here
	expb := backoff.NewExponentialBackOff()
	// TODO(rustatian) set via config
	expb.MaxElapsedTime = time.Minute

	operation := func() error {
		connT, err := beanstalk.DialTimeout(cp.network, cp.address, cp.tout)
		if err != nil {
			return err
		}
		if connT == nil {
			return errors.E(op, errors.Str("connectionT is nil"))
		}

		connTS, err := beanstalk.DialTimeout(cp.network, cp.address, cp.tout)
		if err != nil {
			return err
		}

		if connTS == nil {
			return errors.E(op, errors.Str("connectionTS is nil"))
		}

		t := beanstalk.NewTube(connT, cp.tName)
		cp.t.Swap(t)

		ts := beanstalk.NewTubeSet(connTS, cp.tName)
		cp.ts.Swap(ts)

		cp.connTS.Swap(connTS)
		cp.connT.Swap(connT)

		cp.log.Debug("beanstalk redial was successful")
		return nil
	}

	retryErr := backoff.Retry(operation, expb)
	if retryErr != nil {
		cp.Unlock()
		return retryErr
	}
	cp.Unlock()

	return nil
}

func (cp *ConnPool) checkAndRedial(err error) error {
	const op = errors.Op("connection_pool_check_redial")
	const EOF string = "EOF"
	var et beanstalk.ConnError
	var bErr *net.OpError

	if stderr.As(err, &et) {
		switch {
		case stderr.As(et.Err, &bErr):
			cp.log.Debug("beanstalk connection error, redialing", zap.Error(et))
			cp.RUnlock()
			errR := cp.redial()
			cp.RLock()
			// if redial failed - return
			if errR != nil {
				cp.log.Error("beanstalk redial failed", zap.Error(errR))
				return errors.E(op, errors.Errorf("%v:%v", bErr, errR))
			}

			cp.log.Debug("beanstalk redial was successful")
			// if redial was successful -> continue listening
			return nil
		case et.Err.Error() == EOF:
			cp.log.Debug("beanstalk connection error, redialing", zap.Error(et.Err))
			// if error is related to the broken connection - redial
			cp.RUnlock()
			errR := cp.redial()
			cp.RLock()
			// if redial failed - return
			if errR != nil {
				cp.log.Error("beanstalk redial failed", zap.Error(errR))
				return errors.E(op, errors.Errorf("%v:%v", err, errR))
			}

			cp.log.Debug("beanstalk redial was successful")
			// if redial was successful -> continue listening
			return nil
		case et.Op == "reserve-with-timeout" && (et.Err.Error() == "deadline soon" || et.Err.Error() == "timeout"):
			cp.log.Debug("connection deadline reached, continue listening", zap.Error(et))
			return errBeanstalkTimeout
		}
	}

	cp.log.Error("beanstalk connection error, unknown type of error", zap.Error(err))
	return err
}
