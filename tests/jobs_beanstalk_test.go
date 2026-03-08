package tests

import (
	"context"
	"log/slog"
	"net"
	"net/rpc"
	"os"
	"os/signal"
	"slices"
	"sync"
	"syscall"
	"testing"
	"time"

	"tests/helpers"
	mocklogger "tests/mock"

	"github.com/beanstalkd/go-beanstalk"
	"github.com/google/uuid"
	jobsProto "github.com/roadrunner-server/api/v4/build/jobs/v1"
	jobState "github.com/roadrunner-server/api/v4/plugins/v1/jobs"
	beanstalkPlugin "github.com/roadrunner-server/beanstalk/v5"
	"github.com/roadrunner-server/config/v5"
	"github.com/roadrunner-server/endure/v2"
	goridgeRpc "github.com/roadrunner-server/goridge/v3/pkg/rpc"
	"github.com/roadrunner-server/informer/v5"
	"github.com/roadrunner-server/jobs/v5"
	"github.com/roadrunner-server/logger/v5"
	"github.com/roadrunner-server/resetter/v5"
	rpcPlugin "github.com/roadrunner-server/rpc/v5"
	"github.com/roadrunner-server/server/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.uber.org/zap"
)

func TestBeanstalkInit(t *testing.T) {
	cont := endure.New(slog.LevelDebug, endure.GracefulShutdownTimeout(time.Second*2))

	cfg := &config.Plugin{
		Version: "v2025.1.8",
		Path:    "configs/.rr-beanstalk-init.yaml",
	}

	l, oLogger := mocklogger.ZapTestLogger(zap.DebugLevel)
	err := cont.RegisterAll(
		l,
		cfg,
		&server.Plugin{},
		&rpcPlugin.Plugin{},
		&jobs.Plugin{},
		&resetter.Plugin{},
		&informer.Plugin{},
		&beanstalkPlugin.Plugin{},
	)
	assert.NoError(t, err)

	err = cont.Init()
	if err != nil {
		t.Fatal(err)
	}

	ch, err := cont.Serve()
	if err != nil {
		t.Fatal(err)
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	wg := &sync.WaitGroup{}

	stopCh := make(chan struct{}, 1)

	wg.Go(func() {
		for {
			select {
			case e := <-ch:
				assert.Fail(t, "error", e.Error.Error())
				err = cont.Stop()
				if err != nil {
					assert.FailNow(t, "error", err.Error())
				}
			case <-sig:
				err = cont.Stop()
				if err != nil {
					assert.FailNow(t, "error", err.Error())
				}
				return
			case <-stopCh:
				// timeout
				err = cont.Stop()
				if err != nil {
					assert.FailNow(t, "error", err.Error())
				}
				return
			}
		}
	})

	time.Sleep(time.Second * 3)

	t.Run("PushPipeline", helpers.PushToPipe("test-1", false, "127.0.0.1:7002"))
	t.Run("PushPipeline", helpers.PushToPipe("test-2", false, "127.0.0.1:7002"))

	time.Sleep(time.Second)

	t.Run("DestroyPipeline", helpers.DestroyPipelines("127.0.0.1:7002", "test-1", "test-2"))

	stopCh <- struct{}{}
	wg.Wait()

	require.Equal(t, 2, oLogger.FilterMessageSnippet("pipeline was started").Len())
	require.Equal(t, 2, oLogger.FilterMessageSnippet("pipeline was stopped").Len())
	require.Equal(t, 2, oLogger.FilterMessageSnippet("beanstalk listener stopped").Len())
}

func TestBeanstalkInitPQ(t *testing.T) {
	cont := endure.New(slog.LevelDebug, endure.GracefulShutdownTimeout(time.Second*60))

	cfg := &config.Plugin{
		Version: "v2025.1.8",
		Path:    "configs/.rr-beanstalk-pq.yaml",
	}

	l, oLogger := mocklogger.ZapTestLogger(zap.DebugLevel)
	err := cont.RegisterAll(
		cfg,
		&server.Plugin{},
		&rpcPlugin.Plugin{},
		l,
		&jobs.Plugin{},
		&resetter.Plugin{},
		&informer.Plugin{},
		&beanstalkPlugin.Plugin{},
	)
	assert.NoError(t, err)

	err = cont.Init()
	if err != nil {
		t.Fatal(err)
	}

	ch, err := cont.Serve()
	if err != nil {
		t.Fatal(err)
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	wg := &sync.WaitGroup{}

	stopCh := make(chan struct{}, 1)

	wg.Go(func() {
		for {
			select {
			case e := <-ch:
				assert.Fail(t, "error", e.Error.Error())
				err = cont.Stop()
				if err != nil {
					assert.FailNow(t, "error", err.Error())
				}
			case <-sig:
				err = cont.Stop()
				if err != nil {
					assert.FailNow(t, "error", err.Error())
				}
				return
			case <-stopCh:
				// timeout
				err = cont.Stop()
				if err != nil {
					assert.FailNow(t, "error", err.Error())
				}
				return
			}
		}
	})

	time.Sleep(time.Second * 3)

	for range 100 {
		t.Run("PushPipeline", helpers.PushToPipe("test-1-pq", false, "127.0.0.1:6601"))
		t.Run("PushPipeline", helpers.PushToPipe("test-2-pq", false, "127.0.0.1:6601"))
	}

	time.Sleep(time.Second)

	t.Run("DestroyPipeline", helpers.DestroyPipelines("127.0.0.1:6601", "test-1-pq", "test-2-pq"))

	stopCh <- struct{}{}
	wg.Wait()

	require.Equal(t, 2, oLogger.FilterMessageSnippet("pipeline was started").Len())
	require.Equal(t, 2, oLogger.FilterMessageSnippet("pipeline was stopped").Len())
	require.Equal(t, 200, oLogger.FilterMessageSnippet("job was pushed successfully").Len())
	require.Equal(t, 4, oLogger.FilterMessageSnippet("job processing was started").Len())
	require.Equal(t, 2, oLogger.FilterMessageSnippet("beanstalk listener stopped").Len())
}

func TestBeanstalkInitAutoAck(t *testing.T) {
	cont := endure.New(slog.LevelDebug, endure.GracefulShutdownTimeout(time.Second*60))

	cfg := &config.Plugin{
		Version: "v2025.1.8",
		Path:    "configs/.rr-beanstalk-init.yaml",
	}

	l, oLogger := mocklogger.ZapTestLogger(zap.DebugLevel)
	err := cont.RegisterAll(
		cfg,
		&server.Plugin{},
		&rpcPlugin.Plugin{},
		l,
		&jobs.Plugin{},
		&resetter.Plugin{},
		&informer.Plugin{},
		&beanstalkPlugin.Plugin{},
	)
	assert.NoError(t, err)

	err = cont.Init()
	if err != nil {
		t.Fatal(err)
	}

	ch, err := cont.Serve()
	if err != nil {
		t.Fatal(err)
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	wg := &sync.WaitGroup{}

	stopCh := make(chan struct{}, 1)

	wg.Go(func() {
		for {
			select {
			case e := <-ch:
				assert.Fail(t, "error", e.Error.Error())
				err = cont.Stop()
				if err != nil {
					assert.FailNow(t, "error", err.Error())
				}
			case <-sig:
				err = cont.Stop()
				if err != nil {
					assert.FailNow(t, "error", err.Error())
				}
				return
			case <-stopCh:
				// timeout
				err = cont.Stop()
				if err != nil {
					assert.FailNow(t, "error", err.Error())
				}
				return
			}
		}
	})

	time.Sleep(time.Second * 3)

	t.Run("PushPipeline", helpers.PushToPipe("test-1", true, "127.0.0.1:7002"))
	t.Run("PushPipeline", helpers.PushToPipe("test-2", true, "127.0.0.1:7002"))

	time.Sleep(time.Second)
	t.Run("DestroyPipeline", helpers.DestroyPipelines("127.0.0.1:7002", "test-1", "test-2"))

	stopCh <- struct{}{}
	wg.Wait()

	require.Equal(t, 2, oLogger.FilterMessageSnippet("auto_ack option enabled").Len())
	require.Equal(t, 2, oLogger.FilterMessageSnippet("pipeline was started").Len())
	require.Equal(t, 2, oLogger.FilterMessageSnippet("pipeline was stopped").Len())
	require.Equal(t, 2, oLogger.FilterMessageSnippet("beanstalk listener stopped").Len())
}

func TestBeanstalkInitV27(t *testing.T) {
	cont := endure.New(slog.LevelDebug, endure.GracefulShutdownTimeout(time.Second*60))

	cfg := &config.Plugin{
		Version: "v2025.1.8",
		Path:    "configs/.rr-beanstalk-init-v27.yaml",
	}

	l, oLogger := mocklogger.ZapTestLogger(zap.DebugLevel)
	err := cont.RegisterAll(
		cfg,
		&server.Plugin{},
		&rpcPlugin.Plugin{},
		l,
		&jobs.Plugin{},
		&resetter.Plugin{},
		&informer.Plugin{},
		&beanstalkPlugin.Plugin{},
	)
	assert.NoError(t, err)

	err = cont.Init()
	if err != nil {
		t.Fatal(err)
	}

	ch, err := cont.Serve()
	if err != nil {
		t.Fatal(err)
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	wg := &sync.WaitGroup{}

	stopCh := make(chan struct{}, 1)

	wg.Go(func() {
		for {
			select {
			case e := <-ch:
				assert.Fail(t, "error", e.Error.Error())
				err = cont.Stop()
				if err != nil {
					assert.FailNow(t, "error", err.Error())
				}
			case <-sig:
				err = cont.Stop()
				if err != nil {
					assert.FailNow(t, "error", err.Error())
				}
				return
			case <-stopCh:
				// timeout
				err = cont.Stop()
				if err != nil {
					assert.FailNow(t, "error", err.Error())
				}
				return
			}
		}
	})

	time.Sleep(time.Second * 3)

	t.Run("PushPipeline", helpers.PushToPipe("test-1", false, "127.0.0.1:7004"))
	t.Run("PushPipeline", helpers.PushToPipe("test-2", false, "127.0.0.1:7004"))

	time.Sleep(time.Second)
	t.Run("DestroyPipeline", helpers.DestroyPipelines("127.0.0.1:7004", "test-1", "test-2"))

	stopCh <- struct{}{}
	wg.Wait()

	require.Equal(t, 2, oLogger.FilterMessageSnippet("pipeline was started").Len())
	require.Equal(t, 2, oLogger.FilterMessageSnippet("pipeline was stopped").Len())
	require.Equal(t, 2, oLogger.FilterMessageSnippet("beanstalk listener stopped").Len())
}

func TestBeanstalkStats(t *testing.T) {
	cont := endure.New(slog.LevelDebug, endure.GracefulShutdownTimeout(time.Second*60))

	cfg := &config.Plugin{
		Version: "v2025.1.8",
		Path:    "configs/.rr-beanstalk-declare.yaml",
	}

	err := cont.RegisterAll(
		cfg,
		&server.Plugin{},
		&rpcPlugin.Plugin{},
		&logger.Plugin{},
		&jobs.Plugin{},
		&resetter.Plugin{},
		&informer.Plugin{},
		&beanstalkPlugin.Plugin{},
	)
	assert.NoError(t, err)

	err = cont.Init()
	if err != nil {
		t.Fatal(err)
	}

	ch, err := cont.Serve()
	if err != nil {
		t.Fatal(err)
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	wg := &sync.WaitGroup{}

	stopCh := make(chan struct{}, 1)

	wg.Go(func() {
		for {
			select {
			case e := <-ch:
				assert.Fail(t, "error", e.Error.Error())
				err = cont.Stop()
				if err != nil {
					assert.FailNow(t, "error", err.Error())
				}
			case <-sig:
				err = cont.Stop()
				if err != nil {
					assert.FailNow(t, "error", err.Error())
				}
				return
			case <-stopCh:
				// timeout
				err = cont.Stop()
				if err != nil {
					assert.FailNow(t, "error", err.Error())
				}
				return
			}
		}
	})

	time.Sleep(time.Second * 3)

	t.Run("DeclarePipeline", declareBeanstalkPipe("127.0.0.1:7001"))
	t.Run("ConsumePipeline", helpers.ResumePipes("127.0.0.1:7001", "test-3"))
	t.Run("PushPipeline", helpers.PushToPipe("test-3", false, "127.0.0.1:7001"))
	time.Sleep(time.Second * 2)
	t.Run("PausePipeline", helpers.PausePipelines("127.0.0.1:7001", "test-3"))
	time.Sleep(time.Second * 3)
	t.Run("PushPipelineDelayed", helpers.PushToPipeDelayed("127.0.0.1:7001", "test-3", 8))
	t.Run("PushPipeline", helpers.PushToPipe("test-3", false, "127.0.0.1:7001"))
	time.Sleep(time.Second)

	out := &jobState.State{}
	t.Run("Stats", helpers.Stats("127.0.0.1:7001", out))

	assert.Equal(t, out.Pipeline, "test-3")
	assert.Equal(t, out.Driver, "beanstalk")
	assert.NotEmpty(t, out.Queue)

	out = &jobState.State{}
	t.Run("Stats", helpers.Stats("127.0.0.1:7001", out))

	assert.Equal(t, int64(201), out.Active)
	assert.Equal(t, int64(1), out.Delayed)
	assert.Equal(t, int64(0), out.Reserved)
	assert.Equal(t, uint64(3), out.Priority)

	time.Sleep(time.Second)
	t.Run("ResumePipeline", helpers.ResumePipes("127.0.0.1:7001", "test-3"))
	time.Sleep(time.Second * 15)

	out = &jobState.State{}
	t.Run("Stats", helpers.Stats("127.0.0.1:7001", out))

	assert.Equal(t, out.Pipeline, "test-3")
	assert.Equal(t, out.Driver, "beanstalk")
	assert.NotEmpty(t, out.Queue)

	assert.Equal(t, int64(200), out.Active)
	assert.Equal(t, int64(0), out.Delayed)
	assert.Equal(t, int64(0), out.Reserved)
	assert.Equal(t, uint64(3), out.Priority)

	time.Sleep(time.Second)
	t.Run("DestroyPipeline", helpers.DestroyPipelines("127.0.0.1:7001", "test-3"))

	time.Sleep(time.Second)
	stopCh <- struct{}{}
	wg.Wait()
}

func TestBeanstalkDeclare(t *testing.T) {
	cont := endure.New(slog.LevelDebug, endure.GracefulShutdownTimeout(time.Second*60))

	cfg := &config.Plugin{
		Version: "v2025.1.8",
		Path:    "configs/.rr-beanstalk-declare.yaml",
	}

	err := cont.RegisterAll(
		cfg,
		&server.Plugin{},
		&rpcPlugin.Plugin{},
		&logger.Plugin{},
		&jobs.Plugin{},
		&resetter.Plugin{},
		&informer.Plugin{},
		&beanstalkPlugin.Plugin{},
	)
	assert.NoError(t, err)

	err = cont.Init()
	if err != nil {
		t.Fatal(err)
	}

	ch, err := cont.Serve()
	if err != nil {
		t.Fatal(err)
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	wg := &sync.WaitGroup{}

	stopCh := make(chan struct{}, 1)

	wg.Go(func() {
		for {
			select {
			case e := <-ch:
				assert.Fail(t, "error", e.Error.Error())
				err = cont.Stop()
				if err != nil {
					assert.FailNow(t, "error", err.Error())
				}
			case <-sig:
				err = cont.Stop()
				if err != nil {
					assert.FailNow(t, "error", err.Error())
				}
				return
			case <-stopCh:
				// timeout
				err = cont.Stop()
				if err != nil {
					assert.FailNow(t, "error", err.Error())
				}
				return
			}
		}
	})

	time.Sleep(time.Second * 3)

	t.Run("DeclareBeanstalkPipeline", declareBeanstalkPipe("127.0.0.1:7001"))
	t.Run("ConsumeBeanstalkPipeline", helpers.ResumePipes("127.0.0.1:7001", "test-3"))
	t.Run("PushBeanstalkPipeline", helpers.PushToPipe("test-3", false, "127.0.0.1:7001"))
	t.Run("PauseBeanstalkPipeline", helpers.PausePipelines("127.0.0.1:7001", "test-3"))
	time.Sleep(time.Second * 5)
	t.Run("DestroyBeanstalkPipeline", helpers.DestroyPipelines("127.0.0.1:7001", "test-3"))

	time.Sleep(time.Second * 5)
	stopCh <- struct{}{}
	wg.Wait()
}

func TestBeanstalkJobsError(t *testing.T) {
	cont := endure.New(slog.LevelDebug, endure.GracefulShutdownTimeout(time.Second*60))

	cfg := &config.Plugin{
		Version: "v2025.1.8",
		Path:    "configs/.rr-beanstalk-jobs-err.yaml",
	}

	err := cont.RegisterAll(
		cfg,
		&server.Plugin{},
		&rpcPlugin.Plugin{},
		&logger.Plugin{},
		&jobs.Plugin{},
		&resetter.Plugin{},
		&informer.Plugin{},
		&beanstalkPlugin.Plugin{},
	)
	assert.NoError(t, err)

	err = cont.Init()
	if err != nil {
		t.Fatal(err)
	}

	ch, err := cont.Serve()
	if err != nil {
		t.Fatal(err)
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	wg := &sync.WaitGroup{}

	stopCh := make(chan struct{}, 1)

	wg.Go(func() {
		for {
			select {
			case e := <-ch:
				assert.Fail(t, "error", e.Error.Error())
				err = cont.Stop()
				if err != nil {
					assert.FailNow(t, "error", err.Error())
				}
			case <-sig:
				err = cont.Stop()
				if err != nil {
					assert.FailNow(t, "error", err.Error())
				}
				return
			case <-stopCh:
				// timeout
				err = cont.Stop()
				if err != nil {
					assert.FailNow(t, "error", err.Error())
				}
				return
			}
		}
	})

	time.Sleep(time.Second * 3)

	t.Run("DeclareBeanstalkPipeline", declareBeanstalkPipe("127.0.0.1:7005"))
	t.Run("ConsumeBeanstalkPipeline", helpers.ResumePipes("127.0.0.1:7005", "test-3"))
	t.Run("PushBeanstalkPipeline", helpers.PushToPipe("test-3", false, "127.0.0.1:7005"))
	time.Sleep(time.Second * 25)
	t.Run("PauseBeanstalkPipeline", helpers.PausePipelines("127.0.0.1:7005", "test-3"))
	time.Sleep(time.Second)
	t.Run("DestroyBeanstalkPipeline", helpers.DestroyPipelines("127.0.0.1:7005", "test-3"))

	time.Sleep(time.Second * 5)
	stopCh <- struct{}{}
	wg.Wait()
}

func TestBeanstalkNoGlobalSection(t *testing.T) {
	cont := endure.New(slog.LevelDebug, endure.GracefulShutdownTimeout(time.Second*60))

	cfg := &config.Plugin{
		Version: "v2025.1.8",
		Path:    "configs/.rr-no-global.yaml",
	}

	err := cont.RegisterAll(
		cfg,
		&server.Plugin{},
		&rpcPlugin.Plugin{},
		&logger.Plugin{},
		&jobs.Plugin{},
		&resetter.Plugin{},
		&informer.Plugin{},
		&beanstalkPlugin.Plugin{},
	)
	assert.NoError(t, err)

	err = cont.Init()
	if err != nil {
		t.Fatal(err)
	}

	_, err = cont.Serve()
	require.NoError(t, err)
}

func TestBeanstalkRaw(t *testing.T) {
	cont := endure.New(slog.LevelDebug, endure.GracefulShutdownTimeout(time.Second*60))

	cfg := &config.Plugin{
		Version: "v2025.1.8",
		Path:    "configs/.rr-beanstalk-raw.yaml",
	}

	l, oLogger := mocklogger.ZapTestLogger(zap.DebugLevel)
	err := cont.RegisterAll(
		cfg,
		&server.Plugin{},
		&rpcPlugin.Plugin{},
		l,
		&jobs.Plugin{},
		&resetter.Plugin{},
		&informer.Plugin{},
		&beanstalkPlugin.Plugin{},
	)
	assert.NoError(t, err)

	err = cont.Init()
	if err != nil {
		t.Fatal(err)
	}

	ch, err := cont.Serve()
	if err != nil {
		t.Fatal(err)
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	wg := &sync.WaitGroup{}

	stopCh := make(chan struct{}, 1)

	wg.Go(func() {
		for {
			select {
			case e := <-ch:
				assert.Fail(t, "error", e.Error.Error())
				err = cont.Stop()
				if err != nil {
					assert.FailNow(t, "error", err.Error())
				}
			case <-sig:
				err = cont.Stop()
				if err != nil {
					assert.FailNow(t, "error", err.Error())
				}
				return
			case <-stopCh:
				// timeout
				err = cont.Stop()
				if err != nil {
					assert.FailNow(t, "error", err.Error())
				}
				return
			}
		}
	})

	time.Sleep(time.Second * 3)

	connT, err := beanstalk.DialTimeout("tcp", "127.0.0.1:11300", time.Second*10)
	require.NoError(t, err)

	tb := beanstalk.NewTube(connT, "default-raw")

	_, err = tb.Put([]byte("fooobarrbazzz"), 1, 0, 0)
	require.NoError(t, err)

	time.Sleep(time.Second * 5)
	t.Run("DestroyPipeline", helpers.DestroyPipelines("127.0.0.1:7006", "test-raw"))

	stopCh <- struct{}{}
	wg.Wait()
	time.Sleep(time.Second)

	assert.Equal(t, 1, oLogger.FilterMessageSnippet("pipeline was started").Len())
	assert.Equal(t, 1, oLogger.FilterMessageSnippet("pipeline was stopped").Len())
	assert.Equal(t, 1, oLogger.FilterMessageSnippet("job processing was started").Len())
	assert.Equal(t, 1, oLogger.FilterMessageSnippet("job was processed successfully").Len())

	t.Cleanup(func() {
		_ = connT.Close()
	})
}

func TestBeanstalkInitV27BadResp(t *testing.T) {
	cont := endure.New(slog.LevelDebug, endure.GracefulShutdownTimeout(time.Second*60))

	cfg := &config.Plugin{
		Version: "v2025.1.8",
		Path:    "configs/.rr-beanstalk-init-br.yaml",
	}

	l, oLogger := mocklogger.ZapTestLogger(zap.DebugLevel)
	err := cont.RegisterAll(
		cfg,
		&server.Plugin{},
		&rpcPlugin.Plugin{},
		l,
		&jobs.Plugin{},
		&resetter.Plugin{},
		&informer.Plugin{},
		&beanstalkPlugin.Plugin{},
	)
	assert.NoError(t, err)

	err = cont.Init()
	if err != nil {
		t.Fatal(err)
	}

	ch, err := cont.Serve()
	if err != nil {
		t.Fatal(err)
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	wg := &sync.WaitGroup{}

	stopCh := make(chan struct{}, 1)

	wg.Go(func() {
		for {
			select {
			case e := <-ch:
				assert.Fail(t, "error", e.Error.Error())
				err = cont.Stop()
				if err != nil {
					assert.FailNow(t, "error", err.Error())
				}
			case <-sig:
				err = cont.Stop()
				if err != nil {
					assert.FailNow(t, "error", err.Error())
				}
				return
			case <-stopCh:
				// timeout
				err = cont.Stop()
				if err != nil {
					assert.FailNow(t, "error", err.Error())
				}
				return
			}
		}
	})

	time.Sleep(time.Second * 3)

	t.Run("PushPipeline", helpers.PushToPipe("test-init-br-1", false, "127.0.0.1:7003"))
	t.Run("PushPipeline", helpers.PushToPipe("test-init-br-2", false, "127.0.0.1:7003"))

	time.Sleep(time.Second)
	t.Run("DestroyPipeline", helpers.DestroyPipelines("127.0.0.1:7003", "test-init-br-1", "test-init-br-2"))

	stopCh <- struct{}{}
	wg.Wait()

	require.Equal(t, 2, oLogger.FilterMessageSnippet("pipeline was started").Len())
	require.Equal(t, 2, oLogger.FilterMessageSnippet("pipeline was stopped").Len())
	require.Equal(t, 2, oLogger.FilterMessageSnippet("response handler error").Len())
	require.Equal(t, 2, oLogger.FilterMessageSnippet("beanstalk listener stopped").Len())
}

type inMemoryTracer struct {
	tp  *sdktrace.TracerProvider
	exp *tracetest.InMemoryExporter
}

func newInMemoryTracer(t *testing.T) *inMemoryTracer {
	t.Helper()
	exp := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exp))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	return &inMemoryTracer{tp: tp, exp: exp}
}

func (m *inMemoryTracer) Init() error                      { return nil }
func (m *inMemoryTracer) Name() string                     { return "inMemoryTracer" }
func (m *inMemoryTracer) Tracer() *sdktrace.TracerProvider { return m.tp }

func TestBeanstalkOTEL(t *testing.T) {
	cont := endure.New(slog.LevelDebug, endure.GracefulShutdownTimeout(time.Second*60))

	cfg := &config.Plugin{
		Version: "v2025.1.8",
		Path:    "configs/.rr-beanstalk-otel.yaml",
	}

	tracer := newInMemoryTracer(t)
	l, oLogger := mocklogger.ZapTestLogger(zap.DebugLevel)
	err := cont.RegisterAll(
		cfg,
		&server.Plugin{},
		&rpcPlugin.Plugin{},
		l,
		tracer,
		&jobs.Plugin{},
		&resetter.Plugin{},
		&informer.Plugin{},
		&beanstalkPlugin.Plugin{},
	)
	assert.NoError(t, err)

	err = cont.Init()
	if err != nil {
		t.Fatal(err)
	}

	ch, err := cont.Serve()
	if err != nil {
		t.Fatal(err)
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	wg := &sync.WaitGroup{}

	stopCh := make(chan struct{}, 1)

	wg.Go(func() {
		for {
			select {
			case e := <-ch:
				assert.Fail(t, "error", e.Error.Error())
				err = cont.Stop()
				if err != nil {
					assert.FailNow(t, "error", err.Error())
				}
			case <-sig:
				err = cont.Stop()
				if err != nil {
					assert.FailNow(t, "error", err.Error())
				}
				return
			case <-stopCh:
				// timeout
				err = cont.Stop()
				if err != nil {
					assert.FailNow(t, "error", err.Error())
				}
				return
			}
		}
	})

	time.Sleep(time.Second * 3)
	t.Run("PushPipeline", helpers.PushToPipe("test-1", false, "127.0.0.1:7002"))
	time.Sleep(time.Second * 2)

	t.Run("DestroyPipeline", helpers.DestroyPipelines("127.0.0.1:7002", "test-1"))

	stopCh <- struct{}{}
	wg.Wait()

	spanMap := make(map[string]struct{})
	for _, s := range tracer.exp.GetSpans() {
		spanMap[s.Name] = struct{}{}
	}

	var spans []string
	for name := range spanMap {
		spans = append(spans, name)
	}
	slices.Sort(spans)

	expected := []string{
		"beanstalk_listener",
		"beanstalk_push",
		"beanstalk_stop",
		"destroy_pipeline",
		"jobs_listener",
		"push",
	}
	assert.Equal(t, expected, spans)

	assert.Equal(t, 1, oLogger.FilterMessageSnippet("pipeline was started").Len())
	assert.Equal(t, 1, oLogger.FilterMessageSnippet("pipeline was stopped").Len())
	assert.Equal(t, 1, oLogger.FilterMessageSnippet("beanstalk listener stopped").Len())
}

func declareBeanstalkPipe(address string) func(t *testing.T) {
	return func(t *testing.T) {
		conn, err := (&net.Dialer{}).DialContext(context.Background(), "tcp", address)
		require.NoError(t, err)
		client := rpc.NewClientWithCodec(goridgeRpc.NewClientCodec(conn))

		pipe := &jobsProto.DeclareRequest{Pipeline: map[string]string{
			"driver":          "beanstalk",
			"name":            "test-3",
			"tube":            uuid.NewString(),
			"reserve_timeout": "60s",
			"priority":        "3",
			"tube_priority":   "10",
		}}

		er := &jobsProto.Empty{}
		err = client.Call("jobs.Declare", pipe, er)
		require.NoError(t, err)
	}
}
