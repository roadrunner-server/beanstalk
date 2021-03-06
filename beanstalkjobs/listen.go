package beanstalkjobs

import (
	"context"
	stderr "errors"

	"github.com/beanstalkd/go-beanstalk"
	"go.uber.org/zap"
)

func (c *Consumer) listen() {
	for {
		select {
		case <-c.stopCh:
			c.log.Debug("beanstalk listener stopped")
			return
		default:
			id, body, err := c.pool.Reserve(c.reserveTimeout)
			if err != nil {
				// error isn't wrapped
				if errB, ok := err.(beanstalk.ConnError); ok { //nolint:errorlint
					if stderr.Is(errB.Err, beanstalk.ErrTimeout) {
						c.log.Info("beanstalk reserve timeout", zap.Error(errB))
						continue
					}
				}

				// in case of other error - continue
				c.log.Warn("beanstalk reserve", zap.Error(err))
				continue
			}

			item := &Item{}
			err = c.unpack(id, body, item)
			if err != nil {
				c.log.Error("beanstalk unpack item", zap.Error(err))
				errDel := c.pool.Delete(context.Background(), id)
				if errDel != nil {
					c.log.Error("delete item", zap.Error(errDel), zap.Uint64("id", id))
				}
				continue
			}

			if item.Options.AutoAck {
				c.log.Debug("auto_ack option enabled", zap.Uint64("id", id))
				errDel := c.pool.Delete(context.Background(), id)
				if errDel != nil {
					c.log.Error("delete item", zap.Error(errDel), zap.Uint64("id", id))
				}
			}

			// insert job into the priority queue
			c.pq.Insert(item)
		}
	}
}
