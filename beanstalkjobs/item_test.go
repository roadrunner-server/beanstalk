package beanstalkjobs

import (
	"bytes"
	"context"
	"encoding/gob"
	"encoding/json"
	"log/slog"
	"testing"
	"time"

	"github.com/roadrunner-server/api-plugins/v6/jobs"
	"github.com/stretchr/testify/require"
)

// testPipeline is the jobs.Pipeline the jobs plugin hands to the driver.
type testPipeline struct {
	name     string
	driver   string
	priority int64
}

func (p *testPipeline) Name() string                      { return p.name }
func (p *testPipeline) Driver() string                    { return p.driver }
func (p *testPipeline) Priority() int64                   { return p.priority }
func (*testPipeline) With(string, any)                    {}
func (*testPipeline) Has(string) bool                     { return false }
func (*testPipeline) String(_ string, d string) string    { return d }
func (*testPipeline) Int(_ string, d int) int             { return d }
func (*testPipeline) Bool(_ string, d bool) bool          { return d }
func (*testPipeline) Map(string, map[string]string) error { return nil }
func (*testPipeline) Get(string) any                      { return nil }

var _ jobs.Pipeline = (*testPipeline)(nil)

// testMessage is the jobs.Message the jobs plugin hands to Push.
type testMessage struct {
	name     string
	id       string
	payload  []byte
	headers  map[string][]string
	priority int64
	groupID  string
	delay    int64
	autoAck  bool
}

func (m *testMessage) ID() string                   { return m.id }
func (m *testMessage) GroupID() string              { return m.groupID }
func (m *testMessage) Priority() int64              { return m.priority }
func (m *testMessage) Name() string                 { return m.name }
func (m *testMessage) Payload() []byte              { return m.payload }
func (m *testMessage) Delay() int64                 { return m.delay }
func (m *testMessage) AutoAck() bool                { return m.autoAck }
func (m *testMessage) Headers() map[string][]string { return m.headers }
func (m *testMessage) UpdatePriority(p int64)       { m.priority = p }
func (*testMessage) Offset() int64                  { return 0 }
func (*testMessage) Partition() int32               { return 0 }
func (*testMessage) Topic() string                  { return "" }
func (*testMessage) Metadata() string               { return "" }

var _ jobs.Message = (*testMessage)(nil)

// recorder is a slog handler keeping the rendered messages.
type recorder struct {
	records []string
}

func (*recorder) Enabled(context.Context, slog.Level) bool { return true }

func (r *recorder) Handle(_ context.Context, rec slog.Record) error {
	r.records = append(r.records, rec.Message)
	return nil
}

func (r *recorder) WithAttrs([]slog.Attr) slog.Handler { return r }
func (r *recorder) WithGroup(string) slog.Handler      { return r }

// newTestDriver builds a driver with everything unpack touches and nothing else,
// so the body decoding can be covered without a beanstalkd.
func newTestDriver(priority int64) (*Driver, *recorder) {
	rec := &recorder{}
	d := &Driver{
		log:      slog.New(rec),
		pool:     &ConnPool{},
		tName:    "default-1",
		priority: priority,
	}

	var pipe jobs.Pipeline = &testPipeline{name: "test-1", driver: pluginName, priority: priority}
	d.pipeline.Store(&pipe)

	return d, rec
}

func TestFromJob(t *testing.T) {
	item := fromJob(&testMessage{
		name:     "some/php/namespace",
		id:       "job-id",
		payload:  []byte(`{"hello":"world"}`),
		headers:  map[string][]string{"test": {"test2"}},
		priority: 3,
		groupID:  "test-1",
		delay:    5,
		autoAck:  true,
	})

	require.Equal(t, "job-id", item.ID())
	require.Equal(t, "test-1", item.GroupID())
	require.Equal(t, int64(3), item.Priority())
	require.Equal(t, []byte(`{"hello":"world"}`), item.Body())
	require.Equal(t, map[string][]string{"test": {"test2"}}, item.Headers())
	require.Equal(t, 5, item.Options.Delay)
	require.True(t, item.Options.AutoAck)
}

func TestDelayDuration(t *testing.T) {
	require.Equal(t, 5*time.Second, (&Options{Delay: 5}).DelayDuration())
	require.Zero(t, (&Options{}).DelayDuration())
}

func TestItemContext(t *testing.T) {
	item := &Item{
		Job:     "some/php/namespace",
		Ident:   "job-id",
		Hdrs:    map[string][]string{"test": {"test2"}},
		Options: &Options{Pipeline: "test-1", Queue: "default-1"},
	}

	data, err := item.Context()
	require.NoError(t, err)

	var got map[string]any
	require.NoError(t, json.Unmarshal(data, &got))
	require.Equal(t, "job-id", got["id"])
	require.Equal(t, "beanstalk", got["driver"])
	require.Equal(t, "default-1", got["queue"])
	require.Equal(t, "test-1", got["pipeline"])
}

// TestUnpackRoundTrip covers a body this driver produced. beanstalkd carries no
// metadata of its own, so the whole item is gob encoded into the payload.
func TestUnpackRoundTrip(t *testing.T) {
	d, _ := newTestDriver(11)

	buf := &bytes.Buffer{}
	require.NoError(t, gob.NewEncoder(buf).Encode(&Item{
		Job:     "some/php/namespace",
		Ident:   "job-id",
		Payload: []byte(`{"hello":"world"}`),
		Hdrs:    map[string][]string{"test": {"test2"}},
		Options: &Options{Priority: 3, Pipeline: "test-1"},
	}))

	out := &Item{}
	d.unpack(42, buf.Bytes(), out)

	require.Equal(t, "job-id", out.ID())
	require.Equal(t, int64(3), out.Priority())
	require.Equal(t, map[string][]string{"test": {"test2"}}, out.Headers())
	require.Equal(t, "default-1", out.Options.Queue)
	require.Equal(t, uint64(42), out.Options.id)
}

// TestUnpackInheritsPipelinePriority covers a body carrying no priority of its
// own, which has to fall back to the pipeline default rather than zero.
func TestUnpackInheritsPipelinePriority(t *testing.T) {
	d, _ := newTestDriver(11)

	buf := &bytes.Buffer{}
	require.NoError(t, gob.NewEncoder(buf).Encode(&Item{Ident: "job-id", Options: &Options{}}))

	out := &Item{}
	d.unpack(1, buf.Bytes(), out)

	require.Equal(t, int64(11), out.Priority())
}

// TestUnpackSyntheticItem covers a job put on the tube by something other than
// RoadRunner. The raw bytes become the payload instead of being dropped.
func TestUnpackSyntheticItem(t *testing.T) {
	d, rec := newTestDriver(11)

	body := []byte("fooobarrbazzz")
	out := &Item{}
	d.unpack(7, body, out)

	require.Equal(t, auto, out.Job)
	require.NotEmpty(t, out.ID())
	require.Equal(t, body, out.Body())
	require.Equal(t, int64(11), out.Priority())
	require.Equal(t, "test-1", out.Options.Pipeline)
	require.Equal(t, "default-1", out.Options.Queue)
	require.Equal(t, uint64(7), out.Options.id)
	require.NotNil(t, out.Headers())
	require.Contains(t, rec.records, "failed to unpack the item")
}

// TestAutoAckItemSkipsBroker checks the worker reply is a no-op once the
// listener deleted the job, so none of these reach the nil connection.
func TestAutoAckItemSkipsBroker(t *testing.T) {
	newItem := func() *Item { return &Item{Options: &Options{AutoAck: true}} }

	require.NoError(t, newItem().Ack())
	require.NoError(t, newItem().Nack())
	require.NoError(t, newItem().NackWithOptions(true, 0))
	require.NoError(t, newItem().NackWithOptions(false, 0))
}
