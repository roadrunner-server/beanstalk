package beanstalkjobs

import (
	"context"
	stderr "errors"

	"github.com/beanstalkd/go-beanstalk"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	"go.uber.org/zap"
)

func (d *Driver) listen() {
	for {
		select {
		case <-d.stopCh:
			d.log.Debug("beanstalk listener stopped")
			return
		default:
			id, body, err := d.pool.Reserve(d.reserveTimeout)
			if err != nil {
				// error isn't wrapped
				if errB, ok := err.(beanstalk.ConnError); ok { //nolint:errorlint
					if stderr.Is(errB.Err, beanstalk.ErrTimeout) {
						d.log.Info("beanstalk reserve timeout", zap.Error(errB))
						continue
					}
				}

				// in case of other error - continue
				d.log.Warn("beanstalk reserve", zap.Error(err))
				continue
			}

			item := &Item{}
			err = d.unpack(id, body, item)
			ctx := otel.GetTextMapPropagator().Extract(context.Background(), propagation.HeaderCarrier(item.Headers))
			ctx, span := d.tracer.Tracer(tracerName).Start(ctx, "beanstalk_listener")

			if err != nil {
				span.SetAttributes(attribute.KeyValue{
					Key:   "error",
					Value: attribute.StringValue(err.Error()),
				})
				d.log.Error("beanstalk unpack item", zap.Error(err))
				errDel := d.pool.Delete(context.Background(), id)
				if errDel != nil {
					d.log.Error("delete item", zap.Error(errDel), zap.Uint64("id", id))
				}

				span.End()
				continue
			}

			if item.Options.AutoAck {
				d.log.Debug("auto_ack option enabled", zap.Uint64("id", id))
				errDel := d.pool.Delete(context.Background(), id)
				if errDel != nil {
					span.SetAttributes(attribute.KeyValue{
						Key:   "error",
						Value: attribute.StringValue(err.Error()),
					})
					d.log.Error("delete item", zap.Error(errDel), zap.Uint64("id", id))
				}
			}

			d.prop.Inject(ctx, propagation.HeaderCarrier(item.Headers))
			// insert job into the priority queue
			d.pq.Insert(item)
			span.End()
		}
	}
}
