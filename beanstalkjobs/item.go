package beanstalkjobs

import (
	"bytes"
	"context"
	"encoding/gob"
	"maps"
	"sync/atomic"
	"time"

	"github.com/beanstalkd/go-beanstalk"
	"github.com/goccy/go-json"
	"github.com/google/uuid"
	"github.com/roadrunner-server/api/v4/plugins/v4/jobs"
	"go.uber.org/zap"
)

var _ jobs.Job = (*Item)(nil)

const (
	auto string = "deduced_by_rr"
)

type Item struct {
	// Job contains the pluginName of job broker (usually PHP class).
	Job string `json:"job"`
	// Ident is a unique identifier of the job, should be provided from outside
	Ident string `json:"id"`
	// Payload is string data (usually JSON) passed to Job broker.
	Payload []byte `json:"payload"`
	// Headers with key-values pairs
	headers map[string][]string
	// Options contain a set of PipelineOptions specific to job execution. Can be empty.
	Options *Options `json:"options,omitempty"`
}

// Options carry information about how to handle a given job.
type Options struct {
	// Priority is job priority, default - 10
	// pointer to distinguish 0 as a priority and nil as a priority not set
	Priority int64 `json:"priority"`
	// Pipeline manually specified pipeline.
	Pipeline string `json:"pipeline,omitempty"`
	// Delay defines time duration to delay execution for. Defaults to none.
	Delay int `json:"delay,omitempty"`
	// AutoAck option
	AutoAck bool `json:"auto_ack"`
	// Beanstalk Tube
	Queue string `json:"queue,omitempty"`

	// Private ================
	id        uint64
	conn      atomic.Pointer[beanstalk.Conn]
	requeueFn func(context.Context, *Item) error
}

// DelayDuration returns delay duration in the form of time.Duration.
func (o *Options) DelayDuration() time.Duration {
	return time.Second * time.Duration(o.Delay)
}

func (i *Item) ID() string {
	return i.Ident
}

func (i *Item) Priority() int64 {
	return i.Options.Priority
}

func (i *Item) GroupID() string {
	return i.Options.Pipeline
}

// Body packs job payload into binary payload.
func (i *Item) Body() []byte {
	return i.Payload
}

func (i *Item) Headers() map[string][]string {
	return i.headers
}

// Context packs job context (job, id) into binary payload.
// Not used in the sqs, MessageAttributes used instead
func (i *Item) Context() ([]byte, error) {
	ctx, err := json.Marshal(
		struct {
			ID       string              `json:"id"`
			Job      string              `json:"job"`
			Driver   string              `json:"driver"`
			Headers  map[string][]string `json:"headers"`
			Queue    string              `json:"queue,omitempty"`
			Pipeline string              `json:"pipeline"`
		}{
			ID:       i.Ident,
			Job:      i.Job,
			Driver:   pluginName,
			Headers:  i.headers,
			Queue:    i.Options.Queue,
			Pipeline: i.Options.Pipeline,
		},
	)

	if err != nil {
		return nil, err
	}

	return ctx, nil
}

func (i *Item) Ack() error {
	if i.Options.AutoAck {
		return nil
	}
	return i.Options.conn.Load().Delete(i.Options.id)
}

func (i *Item) NackWithOptions(redeliver bool, delay int) error {
	if i.Options.AutoAck {
		return nil
	}

	if redeliver {
		i.Options.Delay = delay
		err := i.Options.requeueFn(context.Background(), i)
		if err != nil {
			return err
		}

		return nil
	}

	return i.Options.conn.Load().Delete(i.Options.id)
}

func (i *Item) Nack() error {
	if i.Options.AutoAck {
		return nil
	}

	return i.Options.conn.Load().Delete(i.Options.id)
}

func (i *Item) Requeue(headers map[string][]string, delay int) error {
	// overwrite the delay
	i.Options.Delay = delay
	maps.Copy(i.headers, headers)

	err := i.Options.requeueFn(context.Background(), i)
	if err != nil {
		return err
	}

	// delete an old job
	err = i.Options.conn.Load().Delete(i.Options.id)
	if err != nil {
		return err
	}

	return nil
}

func (i *Item) Respond(_ []byte, _ string) error {
	return nil
}

func fromJob(job jobs.Message) *Item {
	return &Item{
		Job:     job.Name(),
		Ident:   job.ID(),
		Payload: job.Payload(),
		headers: job.Headers(),
		Options: &Options{
			AutoAck:  job.AutoAck(),
			Priority: job.Priority(),
			Pipeline: job.GroupID(),
			Delay:    int(job.Delay()),
		},
	}
}

func (d *Driver) unpack(id uint64, data []byte, out *Item) {
	// try to decode the item
	err := gob.NewDecoder(bytes.NewBuffer(data)).Decode(out)
	// if not - fill the item with default values (or values we already have)
	if err != nil {
		d.log.Debug("failed to unpack the item", zap.Error(err))

		*out = Item{
			Job:     auto,
			Ident:   uuid.NewString(),
			Payload: data,
			headers: make(map[string][]string, 2),
			Options: &Options{
				Priority:  (*d.pipeline.Load()).Priority(),
				Pipeline:  (*d.pipeline.Load()).Name(),
				Queue:     d.tName,
				id:        id,
				requeueFn: d.handleItem,
			},
		}

		out.Options.conn.Store(d.pool.connTS.Load())
		return
	}

	if out.Options == nil {
		out.Options = &Options{}
	}

	if out.Options.Priority == 0 {
		out.Options.Priority = d.priority
	}

	out.Options.Queue = d.tName
	out.Options.conn.Store(d.pool.connTS.Load())
	out.Options.id = id
	out.Options.requeueFn = d.handleItem
}
