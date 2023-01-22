package beanstalkjobs

import (
	"bytes"
	"context"
	"encoding/gob"
	"sync/atomic"
	"time"

	"github.com/beanstalkd/go-beanstalk"
	"github.com/goccy/go-json"
	"github.com/google/uuid"
	"github.com/roadrunner-server/api/v4/plugins/v1/jobs"
	"github.com/roadrunner-server/sdk/v4/utils"
	"go.uber.org/zap"
)

const (
	auto string = "deduced_by_rr"
)

type Item struct {
	// Job contains pluginName of job broker (usually PHP class).
	Job string `json:"job"`

	// Ident is unique identifier of the job, should be provided from outside
	Ident string `json:"id"`

	// Payload is string data (usually JSON) passed to Job broker.
	Payload string `json:"payload"`

	// Headers with key-values pairs
	Headers map[string][]string `json:"headers"`

	// Options contains set of PipelineOptions specific to job execution. Can be empty.
	Options *Options `json:"options,omitempty"`
}

// Options carry information about how to handle given job.
type Options struct {
	// Priority is job priority, default - 10
	// pointer to distinguish 0 as a priority and nil as priority not set
	Priority int64 `json:"priority"`

	// Pipeline manually specified pipeline.
	Pipeline string `json:"pipeline,omitempty"`

	// Delay defines time duration to delay execution for. Defaults to none.
	Delay int64 `json:"delay,omitempty"`

	// AutoAck option
	AutoAck bool `json:"auto_ack"`

	// Private ================
	id        uint64
	conn      atomic.Pointer[beanstalk.Conn]
	requeueFn func(context.Context, *Item) error
}

// DelayDuration returns delay duration in a form of time.Duration.
func (o *Options) DelayDuration() time.Duration {
	return time.Second * time.Duration(o.Delay)
}

func (i *Item) ID() string {
	return i.Ident
}

func (i *Item) Priority() int64 {
	return i.Options.Priority
}

// Body packs job payload into binary payload.
func (i *Item) Body() []byte {
	return utils.AsBytes(i.Payload)
}

// Context packs job context (job, id) into binary payload.
// Not used in the sqs, MessageAttributes used instead
func (i *Item) Context() ([]byte, error) {
	ctx, err := json.Marshal(
		struct {
			ID       string              `json:"id"`
			Job      string              `json:"job"`
			Headers  map[string][]string `json:"headers"`
			Pipeline string              `json:"pipeline"`
		}{ID: i.Ident, Job: i.Job, Headers: i.Headers, Pipeline: i.Options.Pipeline},
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

func (i *Item) Nack() error {
	if i.Options.AutoAck {
		return nil
	}
	return i.Options.conn.Load().Delete(i.Options.id)
}

func (i *Item) Requeue(headers map[string][]string, delay int64) error {
	// overwrite the delay
	i.Options.Delay = delay
	i.Headers = headers

	err := i.Options.requeueFn(context.Background(), i)
	if err != nil {
		return err
	}

	// delete old job
	err = i.Options.conn.Load().Delete(i.Options.id)
	if err != nil {
		return err
	}

	return nil
}

func (i *Item) Respond(_ []byte, _ string) error {
	return nil
}

func fromJob(job jobs.Job) *Item {
	return &Item{
		Job:     job.Name(),
		Ident:   job.ID(),
		Payload: job.Payload(),
		Headers: job.Headers(),
		Options: &Options{
			AutoAck:  job.AutoAck(),
			Priority: job.Priority(),
			Pipeline: job.Pipeline(),
			Delay:    job.Delay(),
		},
	}
}

func (d *Driver) unpack(id uint64, data []byte, out *Item) error {
	err := gob.NewDecoder(bytes.NewBuffer(data)).Decode(out)
	if err != nil {
		if d.consumeAll {
			uid := uuid.NewString()
			d.log.Debug("get raw payload", zap.String("assigned ID", uid))

			if isJSONEncoded(data) != nil {
				data, err = json.Marshal(data)
				if err != nil {
					return err
				}
			}

			*out = Item{
				Job:     auto,
				Ident:   uid,
				Payload: utils.AsString(data),
				Headers: nil,
				Options: &Options{
					Priority:  10,
					Pipeline:  auto,
					Delay:     0,
					AutoAck:   false,
					id:        id,
					requeueFn: d.handleItem,
				},
			}

			out.Options.conn.Store(d.pool.connTS.Load())

			return nil
		}
		return err
	}

	if out.Options.Priority == 0 {
		out.Options.Priority = d.priority
	}

	out.Options.conn.Store(d.pool.connTS.Load())
	out.Options.id = id
	out.Options.requeueFn = d.handleItem

	return nil
}

func isJSONEncoded(data []byte) error {
	var a any
	return json.Unmarshal(data, &a)
}
