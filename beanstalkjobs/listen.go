package beanstalkjobs

import (
	"context"
	stderr "errors"

	"go.opentelemetry.io/otel/propagation"
)

func (d *Driver) listen(ctx context.Context) {
	baseCtx := context.WithoutCancel(ctx)
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

				// in case of another error, log and continue
				d.log.Error("beanstalk listen", "error", err)
				continue
			}

			item := &Item{}
			d.unpack(id, body, item)

			if item.Hdrs == nil {
				item.Hdrs = make(map[string][]string, 2)
			}

			itemCtx := d.prop.Extract(baseCtx, propagation.HeaderCarrier(item.Hdrs))
			itemCtx, span := d.tracer.Tracer(tracerName).Start(itemCtx, "beanstalk_listener")

			if item.Options.AutoAck {
				d.log.Debug("auto_ack option enabled", "id", id)
				errDel := d.pool.Delete(itemCtx, id)
				if errDel != nil {
					span.RecordError(errDel)
					d.log.Error("delete item", "error", errDel, "id", id)
				}
				span.End()
				// job already ack'd; do not insert into the queue to avoid double-processing
				continue
			}

			d.prop.Inject(itemCtx, propagation.HeaderCarrier(item.Hdrs))
			// insert a job into the priority queue
			d.pq.Insert(item)
			span.End()
		}
	}
}
