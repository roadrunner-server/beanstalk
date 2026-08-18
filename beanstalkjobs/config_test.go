package beanstalkjobs

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestConfigDefaults(t *testing.T) {
	c := &config{}
	c.InitDefault()

	require.Equal(t, "default", c.Tube)
	require.Equal(t, time.Second, c.ReserveTimeout)
	require.Equal(t, int64(10), c.PipePriority)
	require.Equal(t, "tcp://127.0.0.1:11300", c.Addr)
	require.Equal(t, time.Second*30, c.Timeout)
	require.NotNil(t, c.TubePriority)
	require.Zero(t, *c.TubePriority)
}

func TestConfigKeepsExplicitValues(t *testing.T) {
	pri := uint32(7)
	c := &config{
		Addr:           "tcp://beanstalkd:11300",
		Timeout:        time.Second * 5,
		PipePriority:   3,
		TubePriority:   &pri,
		Tube:           "worker",
		ReserveTimeout: time.Second * 15,
	}
	c.InitDefault()

	require.Equal(t, "tcp://beanstalkd:11300", c.Addr)
	require.Equal(t, time.Second*5, c.Timeout)
	require.Equal(t, int64(3), c.PipePriority)
	require.Equal(t, uint32(7), *c.TubePriority)
	require.Equal(t, "worker", c.Tube)
	require.Equal(t, time.Second*15, c.ReserveTimeout)
}

// TestConfigZeroTubePriority covers the pointer the config uses to tell an
// explicit priority of zero, the most urgent one beanstalkd takes, from an
// option the pipeline never set.
func TestConfigZeroTubePriority(t *testing.T) {
	pri := uint32(0)
	c := &config{TubePriority: &pri}
	c.InitDefault()

	require.Same(t, &pri, c.TubePriority)
}
