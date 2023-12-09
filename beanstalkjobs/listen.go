package beanstalkjobs

import (
	"context"
	stderr "errors"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.uber.org/zap"
)

func (d *Driver) listen() {
	for {
		select {
		case <-d.stopCh:
			d.log.Debug("beanstalk listener stopped")
			// remove all items associated with the pipeline
			_ = d.pq.Remove((*d.pipeline.Load()).Name())
			return
		default:
			id, body, err := d.pool.Reserve(d.reserveTimeout)
			if err != nil {
				// this is not an error, but just a wait timeout for the new job
				if stderr.Is(err, errBeanstalkTimeout) {
					continue
				}

				// in case of another error log and continue
				d.log.Error("beanstalk listen", zap.Error(err))
				continue
			}

			item := &Item{}
			d.unpack(id, body, item)

			ctx := otel.GetTextMapPropagator().Extract(context.Background(), propagation.HeaderCarrier(item.headers))
			ctx, span := d.tracer.Tracer(tracerName).Start(ctx, "beanstalk_listener")

			if item.Options.AutoAck {
				d.log.Debug("auto_ack option enabled", zap.Uint64("id", id))
				errDel := d.pool.Delete(context.Background(), id)
				if errDel != nil {
					span.RecordError(errDel)
					d.log.Error("delete item", zap.Error(errDel), zap.Uint64("id", id))
				}
			}

			// check the header before injecting OTEL headers
			if item.headers == nil {
				item.headers = make(map[string][]string, 2)
			}

			d.prop.Inject(ctx, propagation.HeaderCarrier(item.headers))
			// insert job into the priority queue
			d.pq.Insert(item)
			span.End()
		}
	}
}
