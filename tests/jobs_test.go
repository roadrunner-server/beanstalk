package tests

import (
	"context"
	"log/slog"
	"slices"
	"testing"

	"tests/helpers"

	jobState "github.com/roadrunner-server/api-plugins/v6/jobs"
	beanstalkPlugin "github.com/roadrunner-server/beanstalk/v6"
	"github.com/roadrunner-server/informer/v6"
	"github.com/roadrunner-server/jobs/v6"
	"github.com/roadrunner-server/resetter/v6"
	rpcPlugin "github.com/roadrunner-server/rpc/v6"
	"github.com/roadrunner-server/server/v6"
	"github.com/stretchr/testify/require"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

const (
	initAddr     = "127.0.0.1:7002"
	declareAddr  = "127.0.0.1:7001"
	pqAddr       = "127.0.0.1:6601"
	errAddr      = "127.0.0.1:7005"
	badRespAddr  = "127.0.0.1:7003"
	rawAddr      = "127.0.0.1:7006"
	noGlobalAddr = "127.0.0.1:7008"
	// declared is the pipeline the declare configs create over rpc.
	declared = "test-3"
)

func jobsPlugins() []any {
	return []any{
		&server.Plugin{},
		&rpcPlugin.Plugin{},
		&jobs.Plugin{},
		&resetter.Plugin{},
		&informer.Plugin{},
		&beanstalkPlugin.Plugin{},
	}
}

// boot starts the container with the observed logger and waits for the rpc
// listener, which is the readiness signal the fixed sleeps used to stand in for.
func boot(t *testing.T, cfgPath string, addr string, opts ...helpers.Option) (*helpers.RR, func()) {
	t.Helper()

	return helpers.Start(t, cfgPath, jobsPlugins(),
		append([]helpers.Option{
			helpers.WithObservedLogger(),
			helpers.WithTCPProbe(addr),
		}, opts...)...)
}

// TestBoots covers the config-declared pipelines: both come up at startup and
// both tear their listener down on destroy.
func TestBoots(t *testing.T) {
	rr, _ := boot(t, "configs/.rr-beanstalk-init.yaml", initAddr)

	rr.RequireLogCount(t, "pipeline was started", 2)

	helpers.DestroyPipelines(initAddr, "test-1", "test-2")(t)

	rr.RequireLogCount(t, "pipeline was stopped", 2)
	rr.RequireLogCount(t, "beanstalk listener stopped", 2)
}

// TestPushAndProcess follows two jobs from the rpc call to the worker ack.
func TestPushAndProcess(t *testing.T) {
	rr, _ := boot(t, "configs/.rr-beanstalk-init.yaml", initAddr)

	helpers.PushToPipe("test-1", false, initAddr)(t)
	helpers.PushToPipe("test-2", false, initAddr)(t)

	rr.WaitLog(t, "job was processed successfully", 2)

	helpers.DestroyPipelines(initAddr, "test-1", "test-2")(t)

	rr.RequireLogCount(t, "job was pushed successfully", 2)
	rr.RequireLogCount(t, "job was processed successfully", 2)
}

// TestAutoAck checks the listener deletes the job itself, before the worker ever
// sees it, when the job carries the auto ack option.
func TestAutoAck(t *testing.T) {
	rr, _ := boot(t, "configs/.rr-beanstalk-init.yaml", initAddr)

	helpers.PushToPipe("test-1", true, initAddr)(t)
	helpers.PushToPipe("test-2", true, initAddr)(t)

	rr.WaitLog(t, "job was processed successfully", 2)

	helpers.DestroyPipelines(initAddr, "test-1", "test-2")(t)

	rr.RequireLogCount(t, "auto_ack option enabled", 2)
}

// TestPriorityQueueBacklog pushes far more jobs than the two slow workers can
// take, so most of them sit in the priority queue until the pipelines are
// destroyed under them.
func TestPriorityQueueBacklog(t *testing.T) {
	const rounds = 100

	rr, _ := boot(t, "configs/.rr-beanstalk-pq.yaml", pqAddr)

	for range rounds {
		helpers.PushToPipe("test-1-pq", false, pqAddr)(t)
		helpers.PushToPipe("test-2-pq", false, pqAddr)(t)
	}

	rr.RequireLogCount(t, "job was pushed successfully", 2*rounds)

	// both workers have to be busy before the destroy, otherwise the backlog
	// would never form
	rr.WaitLog(t, "job processing was started", 2)

	helpers.DestroyPipelines(pqAddr, "test-1-pq", "test-2-pq")(t)

	rr.RequireLogCount(t, "pipeline was started", 2)
	rr.RequireLogCount(t, "pipeline was stopped", 2)
	rr.RequireLogCount(t, "beanstalk listener stopped", 2)
}

// TestDeclareAndConsume declares a pipeline over rpc, runs a job through it and
// pauses it again. The old test made the same calls and asserted nothing.
func TestDeclareAndConsume(t *testing.T) {
	rr, _ := boot(t, "configs/.rr-beanstalk-declare.yaml", declareAddr)

	helpers.DeclarePipe(declareAddr, declared, "tube-declare")(t)
	helpers.ResumePipes(declareAddr, declared)(t)
	rr.WaitLog(t, "pipeline was resumed", 1)

	helpers.PushToPipe(declared, false, declareAddr)(t)
	rr.WaitLog(t, "job was processed successfully", 1)

	helpers.PausePipelines(declareAddr, declared)(t)
	rr.WaitLog(t, "pipeline was paused", 1)

	helpers.DestroyPipelines(declareAddr, declared)(t)

	rr.RequireLogCount(t, "job was pushed successfully", 1)
	rr.RequireLogCount(t, "job was processed successfully", 1)
	rr.RequireLogCount(t, "pipeline was stopped", 1)
}

// TestPauseStopsConsuming checks a paused pipeline still accepts pushes but
// leaves them on the tube until it is resumed.
//
// Pause only signals the listener, which notices once its pending reserve
// returns, so the push waits for the listener to report itself stopped rather
// than for the pause call.
func TestPauseStopsConsuming(t *testing.T) {
	rr, _ := boot(t, "configs/.rr-beanstalk-declare.yaml", declareAddr)

	helpers.DeclarePipe(declareAddr, declared, "tube-pause")(t)
	helpers.ResumePipes(declareAddr, declared)(t)
	rr.WaitLog(t, "pipeline was resumed", 1)

	helpers.PausePipelines(declareAddr, declared)(t)
	rr.WaitLog(t, "beanstalk listener stopped", 1)

	helpers.PushToPipe(declared, false, declareAddr)(t)
	rr.WaitLog(t, "job was pushed successfully", 1)
	rr.NeverLog(t, "job was processed successfully")

	helpers.ResumePipes(declareAddr, declared)(t)
	rr.WaitLog(t, "job was processed successfully", 1)

	helpers.DestroyPipelines(declareAddr, declared)(t)
}

// TestStatsReportTubeCounters covers the state report. The counters have to
// describe the pipeline's own tube: the old test declared a fresh tube and then
// asserted on 200 jobs another test had left on the server.
func TestStatsReportTubeCounters(t *testing.T) {
	const queued = 5

	boot(t, "configs/.rr-beanstalk-declare.yaml", declareAddr)

	helpers.DeclarePipe(declareAddr, declared, "tube-stats")(t)

	// nothing has touched the tube yet, so beanstalkd does not know it
	empty := helpers.StatsFor(t, declareAddr, declared)
	require.Equal(t, "beanstalk", empty.Driver)
	require.Equal(t, "tube-stats", empty.Queue)
	require.Equal(t, uint64(3), empty.Priority)
	require.False(t, empty.Ready)
	require.Zero(t, empty.Active)

	for range queued {
		helpers.PushToPipe(declared, false, declareAddr)(t)
	}
	helpers.PushToPipeDelayed(declareAddr, declared, 5)(t)

	// still paused, so everything stays on the tube
	queuedState := helpers.WaitStats(t, declareAddr, declared, func(s *jobState.State) bool {
		return s.Active == queued && s.Delayed == 1
	})

	require.Zero(t, queuedState.Reserved)
	require.False(t, queuedState.Ready)

	helpers.ResumePipes(declareAddr, declared)(t)

	drained := helpers.WaitStats(t, declareAddr, declared, func(s *jobState.State) bool {
		return s.Active == 0 && s.Delayed == 0 && s.Reserved == 0
	})

	require.True(t, drained.Ready)
	require.Equal(t, "tube-stats", drained.Queue)

	helpers.DestroyPipelines(declareAddr, declared)(t)
}

// TestRequeueRetriesUntilComplete covers the worker that fails a job with a
// growing attempts header and only completes it on the fourth delivery. The old
// test slept out the three five second delays and asserted nothing.
func TestRequeueRetriesUntilComplete(t *testing.T) {
	rr, _ := boot(t, "configs/.rr-beanstalk-jobs-err.yaml", errAddr)

	helpers.DeclarePipe(errAddr, declared, "tube-err")(t)
	helpers.ResumePipes(errAddr, declared)(t)
	helpers.PushToPipe(declared, false, errAddr)(t)

	rr.WaitLog(t, "job was processed successfully", 1)

	helpers.PausePipelines(errAddr, declared)(t)
	helpers.DestroyPipelines(errAddr, declared)(t)

	// one original delivery plus the three the worker requeued
	rr.RequireLogCount(t, "job processing was started", 4)
	rr.RequireLogCount(t, "job was pushed successfully", 1)
	rr.RequireLogCount(t, "job was processed successfully", 1)
	rr.RequireLogCount(t, "pipeline was stopped", 1)
}

// TestRawJob covers a job put on the tube by something other than RoadRunner.
// The body is not an encoded item, so the driver has to wrap it rather than
// drop it.
func TestRawJob(t *testing.T) {
	rr, _ := boot(t, "configs/.rr-beanstalk-raw.yaml", rawAddr)

	helpers.PutRaw(t, "default-raw", []byte("fooobarrbazzz"))

	rr.WaitLog(t, "job was processed successfully", 1)

	helpers.DestroyPipelines(rawAddr, "test-raw")(t)

	rr.RequireLogCount(t, "failed to unpack the item", 1)
	rr.RequireLogCount(t, "pipeline was started", 1)
	rr.RequireLogCount(t, "pipeline was stopped", 1)
	rr.RequireLogCount(t, "job processing was started", 1)
	rr.RequireLogCount(t, "job was processed successfully", 1)
}

// TestBadResponseIsReported covers a worker answering with a payload the jobs
// response handler cannot parse.
func TestBadResponseIsReported(t *testing.T) {
	rr, _ := boot(t, "configs/.rr-beanstalk-init-br.yaml", badRespAddr)

	helpers.PushToPipe("test-init-br-1", false, badRespAddr)(t)
	helpers.PushToPipe("test-init-br-2", false, badRespAddr)(t)

	rr.WaitLog(t, "response handler error", 2)

	helpers.DestroyPipelines(badRespAddr, "test-init-br-1", "test-init-br-2")(t)

	rr.RequireLogCount(t, "response handler error", 2)
	rr.RequireLogCount(t, "pipeline was started", 2)
	rr.RequireLogCount(t, "pipeline was stopped", 2)
}

// TestNoGlobalSection covers a config with pipelines but no beanstalk section.
// The plugin disables itself and the container still serves.
func TestNoGlobalSection(t *testing.T) {
	boot(t, "configs/.rr-no-global.yaml", noGlobalAddr, helpers.WithLogLevel(slog.LevelError))
}

// TestOTELSpans checks the spans the driver emits and that a listener span
// continues the trace the push started.
func TestOTELSpans(t *testing.T) {
	tracer := newInMemoryTracer(t)

	rr, _ := boot(t, "configs/.rr-beanstalk-otel.yaml", initAddr, helpers.WithPlugin(tracer))

	helpers.PushToPipe("test-1", false, initAddr)(t)
	helpers.PushToPipe("test-1", false, initAddr)(t)

	rr.WaitLog(t, "job was processed successfully", 2)

	helpers.DestroyPipelines(initAddr, "test-1")(t)

	rr.RequireLogCount(t, "pipeline was started", 1)
	rr.RequireLogCount(t, "pipeline was stopped", 1)
	rr.RequireLogCount(t, "beanstalk listener stopped", 1)

	spanNames := make(map[string]struct{})
	listenerSpanIDs := make(map[trace.SpanID]struct{})
	for _, s := range tracer.exp.GetSpans() {
		spanNames[s.Name] = struct{}{}
		if s.Name == "beanstalk_listener" {
			listenerSpanIDs[s.SpanContext.SpanID()] = struct{}{}
		}
	}

	names := make([]string, 0, len(spanNames))
	for name := range spanNames {
		names = append(names, name)
	}
	slices.Sort(names)

	require.Equal(t, []string{
		"beanstalk_listener",
		"beanstalk_push",
		"beanstalk_stop",
		"destroy_pipeline",
		"jobs_listener",
		"push",
	}, names)

	// the listener span continues the push trace, it is never chained to another
	// listener span
	for _, s := range tracer.exp.GetSpans() {
		if s.Name != "beanstalk_listener" {
			continue
		}

		require.True(t, s.Parent.IsValid(), "beanstalk_listener span has no parent from the push trace")
		_, parentIsListener := listenerSpanIDs[s.Parent.SpanID()]
		require.False(t, parentIsListener, "beanstalk_listener span is chained to another listener span")
	}
}

// inMemoryTracer stands in for the otel plugin, keeping the spans in process.
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

func (*inMemoryTracer) Init() error                        { return nil }
func (*inMemoryTracer) Name() string                       { return "inMemoryTracer" }
func (m *inMemoryTracer) Tracer() *sdktrace.TracerProvider { return m.tp }
