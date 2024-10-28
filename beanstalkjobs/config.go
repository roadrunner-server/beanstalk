package beanstalkjobs

import (
	"time"
)

const (
	tubePriority   string = "tube_priority"
	tube           string = "tube"
	reserveTimeout string = "reserve_timeout"
)

type config struct {
	// global
	Addr    string        `mapstructure:"addr"`
	Timeout time.Duration `mapstructure:"timeout"`
	// local
	PipePriority   int64         `mapstructure:"priority"`
	TubePriority   *uint32       `mapstructure:"tube_priority"`
	Tube           string        `mapstructure:"tube"`
	ReserveTimeout time.Duration `mapstructure:"reserve_timeout"`
}

func (c *config) InitDefault() {
	if c.Tube == "" {
		c.Tube = "default"
	}

	if c.ReserveTimeout == 0 {
		c.ReserveTimeout = time.Second * 1
	}

	if c.TubePriority == nil {
		c.TubePriority = toPtr(uint32(0))
	}

	if c.PipePriority == 0 {
		c.PipePriority = 10
	}

	if c.Addr == "" {
		c.Addr = "tcp://127.0.0.1:11300"
	}

	if c.Timeout == 0 {
		c.Timeout = time.Second * 30
	}
}

func toPtr[T any](v T) *T {
	return &v
}
