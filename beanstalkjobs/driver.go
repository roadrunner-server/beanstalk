package beanstalkjobs

import (
	"bytes"
	"context"
	"encoding/gob"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/roadrunner-server/api/v4/plugins/v4/jobs"
	"github.com/roadrunner-server/errors"
	jprop "go.opentelemetry.io/contrib/propagators/jaeger"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

const (
	pluginName string = "beanstalk"
	tracerName string = "jobs"
)

var _ jobs.Driver = (*Driver)(nil)

type Configurer interface {
	// UnmarshalKey takes a single key and unmarshals it into a Struct.
	UnmarshalKey(name string, out any) error
	// Has checks if a config section exists.
	Has(name string) bool
}

type Driver struct {
	log    *zap.Logger
	pq     jobs.Queue
	tracer *sdktrace.TracerProvider
	prop   propagation.TextMapPropagator

	pipeline  atomic.Pointer[jobs.Pipeline]
	listeners uint32

	// beanstalk
	pool           *ConnPool
	addr           string
	network        string
	reserveTimeout time.Duration
	reconnectCh    chan struct{}
	tout           time.Duration
	// tube name
	tName        string
	tubePriority *uint32
	priority     int64

	stopCh chan struct{}
}

func FromConfig(tracer *sdktrace.TracerProvider, configKey string, log *zap.Logger, cfg Configurer, pipe jobs.Pipeline, pq jobs.Queue) (*Driver, error) {
	const op = errors.Op("new_beanstalk_consumer")

	if tracer == nil {
		tracer = sdktrace.NewTracerProvider()
	}

	prop := propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}, jprop.Jaeger{})
	otel.SetTextMapPropagator(prop)

	// PARSE CONFIGURATION -------
	var conf config
	if !cfg.Has(configKey) {
		return nil, errors.E(op, errors.Errorf("no configuration by provided key: %s", configKey))
	}

	// if no global section
	if !cfg.Has(pluginName) {
		return nil, errors.E(op, errors.Str("no global beanstalk configuration, global configuration should contain beanstalk addrs and timeout"))
	}

	err := cfg.UnmarshalKey(configKey, &conf)
	if err != nil {
		return nil, errors.E(op, err)
	}

	err = cfg.UnmarshalKey(pluginName, &conf)
	if err != nil {
		return nil, errors.E(op, err)
	}

	conf.InitDefault()

	// PARSE CONFIGURATION -------

	dsn := strings.Split(conf.Addr, "://")
	if len(dsn) != 2 {
		return nil, errors.E(op, errors.Errorf("invalid socket DSN (tcp://127.0.0.1:11300, unix://beanstalk.sock), provided: %s", conf.Addr))
	}

	cPool, err := NewConnPool(dsn[0], dsn[1], conf.Tube, conf.Timeout, log)
	if err != nil {
		return nil, errors.E(op, err)
	}

	// initialize job Driver
	jc := &Driver{
		tracer:         tracer,
		prop:           prop,
		pq:             pq,
		log:            log,
		pool:           cPool,
		network:        dsn[0],
		addr:           dsn[1],
		tout:           conf.Timeout,
		tName:          conf.Tube,
		reserveTimeout: conf.ReserveTimeout,
		tubePriority:   conf.TubePriority,
		priority:       conf.PipePriority,

		// buffered with two because jobs root plugin can call Stop at the same time as Pause
		stopCh:      make(chan struct{}, 2),
		reconnectCh: make(chan struct{}, 2),
	}

	jc.pipeline.Store(&pipe)

	return jc, nil
}

func FromPipeline(tracer *sdktrace.TracerProvider, pipe jobs.Pipeline, log *zap.Logger, cfg Configurer, pq jobs.Queue) (*Driver, error) {
	const op = errors.Op("new_beanstalk_consumer")

	if tracer == nil {
		tracer = sdktrace.NewTracerProvider()
	}

	prop := propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}, jprop.Jaeger{})
	otel.SetTextMapPropagator(prop)

	// PARSE CONFIGURATION -------
	var conf config
	// if no global section
	if !cfg.Has(pluginName) {
		return nil, errors.E(op, errors.Str("no global beanstalk configuration, global configuration should contain beanstalk 'addrs' and timeout"))
	}

	err := cfg.UnmarshalKey(pluginName, &conf)
	if err != nil {
		return nil, errors.E(op, err)
	}

	conf.InitDefault()
	// PARSE CONFIGURATION -------

	dsn := strings.Split(conf.Addr, "://")
	if len(dsn) != 2 {
		return nil, errors.E(op, errors.Errorf("invalid socket DSN (tcp://127.0.0.1:11300, unix://beanstalk.sock), provided: %s", conf.Addr))
	}

	cPool, err := NewConnPool(dsn[0], dsn[1], pipe.String(tube, "default"), conf.Timeout, log)
	if err != nil {
		return nil, errors.E(op, err)
	}

	// initialize job Driver
	jc := &Driver{
		tracer:         tracer,
		prop:           prop,
		pq:             pq,
		log:            log,
		pool:           cPool,
		network:        dsn[0],
		addr:           dsn[1],
		tout:           conf.Timeout,
		tName:          pipe.String(tube, "default"),
		reserveTimeout: time.Second * time.Duration(pipe.Int(reserveTimeout, 5)),
		tubePriority:   toPtr(uint32(pipe.Int(tubePriority, 1))), //nolint:gosec
		priority:       pipe.Priority(),

		// buffered with two because jobs root plugin can call Stop at the same time as Pause
		stopCh:      make(chan struct{}, 2),
		reconnectCh: make(chan struct{}, 2),
	}

	jc.pipeline.Store(&pipe)

	return jc, nil
}

func (d *Driver) Push(ctx context.Context, jb jobs.Message) error {
	const op = errors.Op("beanstalk_push")
	// check if the pipeline registered

	ctx, span := trace.SpanFromContext(ctx).TracerProvider().Tracer(tracerName).Start(ctx, "beanstalk_push")
	defer span.End()

	// load atomic value
	pipe := *d.pipeline.Load()
	if pipe.Name() != jb.GroupID() {
		return errors.E(op, errors.Errorf("no such pipeline: %s, actual: %s", jb.GroupID(), pipe.Name()))
	}

	err := d.handleItem(ctx, fromJob(jb))
	if err != nil {
		return errors.E(op, err)
	}

	return nil
}

// State https://github.com/beanstalkd/beanstalkd/blob/master/doc/protocol.txt#L514
func (d *Driver) State(ctx context.Context) (*jobs.State, error) {
	const op = errors.Op("beanstalk_state")

	_, span := trace.SpanFromContext(ctx).TracerProvider().Tracer(tracerName).Start(ctx, "beanstalk_state")
	defer span.End()

	stat, err := d.pool.Stats(ctx)
	if err != nil {
		return nil, errors.E(op, err)
	}

	pipe := *d.pipeline.Load()

	out := &jobs.State{
		Priority: uint64(pipe.Priority()), //nolint:gosec
		Pipeline: pipe.Name(),
		Driver:   pipe.Driver(),
		Queue:    d.tName,
		Ready:    ready(atomic.LoadUint32(&d.listeners)),
	}

	// set stat, skip errors (replace with 0)
	// https://github.com/beanstalkd/beanstalkd/blob/master/doc/protocol.txt#L523
	if v, err := strconv.Atoi(stat["current-jobs-ready"]); err == nil {
		out.Active = int64(v)
	}

	// https://github.com/beanstalkd/beanstalkd/blob/master/doc/protocol.txt#L525
	if v, err := strconv.Atoi(stat["current-jobs-reserved"]); err == nil {
		// this is not an error, reserved in beanstalk behaves like an active jobs
		out.Reserved = int64(v)
	}

	// https://github.com/beanstalkd/beanstalkd/blob/master/doc/protocol.txt#L528
	if v, err := strconv.Atoi(stat["current-jobs-delayed"]); err == nil {
		out.Delayed = int64(v)
	}

	return out, nil
}

func (d *Driver) Run(ctx context.Context, p jobs.Pipeline) error {
	const op = errors.Op("beanstalk_run")
	start := time.Now().UTC()

	_, span := trace.SpanFromContext(ctx).TracerProvider().Tracer(tracerName).Start(ctx, "beanstalk_run")
	defer span.End()

	// load atomic value
	// check if the pipeline registered
	pipe := *d.pipeline.Load()
	if pipe.Name() != p.Name() {
		return errors.E(op, errors.Errorf("no such pipeline: %s, actual: %s", p.Name(), pipe.Name()))
	}

	atomic.AddUint32(&d.listeners, 1)

	go d.listen()

	d.log.Debug("pipeline was started", zap.String("driver", pipe.Driver()), zap.String("pipeline", pipe.Name()), zap.Time("start", start), zap.Int64("elapsed", time.Since(start).Milliseconds()))
	return nil
}

func (d *Driver) Stop(ctx context.Context) error {
	start := time.Now().UTC()
	pipe := *d.pipeline.Load()

	_, span := trace.SpanFromContext(ctx).TracerProvider().Tracer(tracerName).Start(ctx, "beanstalk_stop")
	defer span.End()

	if atomic.LoadUint32(&d.listeners) == 1 {
		d.stopCh <- struct{}{}
	}

	// release associated resources
	d.pool.Stop()

	// remove all pending JOBS associated with the pipeline
	_ = d.pq.Remove(pipe.Name())

	d.log.Debug("pipeline was stopped", zap.String("driver", pipe.Driver()), zap.String("pipeline", pipe.Name()), zap.Time("start", start), zap.Int64("elapsed", time.Since(start).Milliseconds()))
	return nil
}

func (d *Driver) Pause(ctx context.Context, p string) error {
	start := time.Now().UTC()

	_, span := trace.SpanFromContext(ctx).TracerProvider().Tracer(tracerName).Start(ctx, "beanstalk_pause")
	defer span.End()

	// load atomic value
	pipe := *d.pipeline.Load()
	if pipe.Name() != p {
		return errors.Errorf("no such pipeline: %s", pipe.Name())
	}

	l := atomic.LoadUint32(&d.listeners)
	// no active listeners
	if l == 0 {
		return errors.Str("no active listeners, nothing to pause")
	}

	atomic.AddUint32(&d.listeners, ^uint32(0))

	d.stopCh <- struct{}{}
	d.log.Debug("pipeline was paused", zap.String("driver", pipe.Driver()), zap.String("pipeline", pipe.Name()), zap.Time("start", start), zap.Int64("elapsed", time.Since(start).Milliseconds()))

	return nil
}

func (d *Driver) Resume(ctx context.Context, p string) error {
	start := time.Now().UTC()

	_, span := trace.SpanFromContext(ctx).TracerProvider().Tracer(tracerName).Start(ctx, "beanstalk_resume")
	defer span.End()

	// load atomic value
	pipe := *d.pipeline.Load()
	if pipe.Name() != p {
		return errors.Errorf("no such pipeline: %s", pipe.Name())
	}

	l := atomic.LoadUint32(&d.listeners)
	// no active listeners
	if l == 1 {
		return errors.Str("sqs listener already in the active state")
	}

	// start listener
	go d.listen()

	// increase num of listeners
	atomic.AddUint32(&d.listeners, 1)
	d.log.Debug("pipeline was resumed", zap.String("driver", pipe.Driver()), zap.String("pipeline", pipe.Name()), zap.Time("start", start), zap.Int64("elapsed", time.Since(start).Milliseconds()))

	return nil
}

func (d *Driver) handleItem(ctx context.Context, item *Item) error {
	const op = errors.Op("beanstalk_handle_item")

	d.prop.Inject(ctx, propagation.HeaderCarrier(item.headers))

	bb := new(bytes.Buffer)
	bb.Grow(64)
	err := gob.NewEncoder(bb).Encode(item)
	if err != nil {
		return errors.E(op, err)
	}

	body := make([]byte, bb.Len())
	copy(body, bb.Bytes())
	bb.Reset()
	bb = nil

	// https://github.com/beanstalkd/beanstalkd/blob/master/doc/protocol.txt#L458
	// <pri> is an integer < 2**32. Jobs with smaller priority values will be
	// scheduled before jobs with larger priorities. The most urgent priority is 0;
	// the least urgent priority is 4,294,967,295.
	//
	// <delay> is an integer number of seconds to wait before putting the job in
	// the ready queue. The job will be in the "delayed" state during this time.
	// Maximum delay is 2**32-1.
	//
	// <ttr> -- time to run -- is an integer number of seconds to allow a worker
	// to run this job. This time is counted from the moment a worker reserves
	// this job. If the worker does not delete, release, or bury the job within
	// <ttr> seconds, the job will time out and the server will release the job.
	//	The minimum ttr is 1. If the client sends 0, the server will silently
	// increase the ttr to 1. Maximum ttr is 2**32-1.
	id, err := d.pool.Put(ctx, body, *d.tubePriority, item.Options.DelayDuration(), d.tout)
	if err != nil {
		errD := d.pool.Delete(ctx, id)
		if errD != nil {
			return errors.E(op, errors.Errorf("%s:%s", err.Error(), errD.Error()))
		}
		return errors.E(op, err)
	}

	return nil
}

func ready(r uint32) bool {
	return r > 0
}
