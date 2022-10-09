package beanstalk

import (
	"github.com/roadrunner-server/beanstalk/v3/beanstalkjobs"
	"github.com/roadrunner-server/sdk/v3/plugins/jobs"
	"github.com/roadrunner-server/sdk/v3/plugins/jobs/pipeline"
	priorityqueue "github.com/roadrunner-server/sdk/v3/priority_queue"
	"go.uber.org/zap"
)

const (
	pluginName string = "beanstalk"
)

type Configurer interface {
	// UnmarshalKey takes a single key and unmarshals it into a Struct.
	UnmarshalKey(name string, out any) error

	// Has checks if config section exists.
	Has(name string) bool
}

type Plugin struct {
	log *zap.Logger
	cfg Configurer
}

func (p *Plugin) Init(log *zap.Logger, cfg Configurer) error {
	p.log = new(zap.Logger)
	*p.log = *log
	p.cfg = cfg
	return nil
}

func (p *Plugin) Name() string {
	return pluginName
}

func (p *Plugin) ConsumerFromConfig(configKey string, pq priorityqueue.Queue) (jobs.Consumer, error) {
	return beanstalkjobs.NewBeanstalkConsumer(configKey, p.log, p.cfg, pq)
}

func (p *Plugin) ConsumerFromPipeline(pipe *pipeline.Pipeline, pq priorityqueue.Queue) (jobs.Consumer, error) {
	return beanstalkjobs.FromPipeline(pipe, p.log, p.cfg, pq)
}
