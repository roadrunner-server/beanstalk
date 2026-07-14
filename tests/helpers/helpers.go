package helpers

import (
	"bytes"
	"net"
	"net/http"
	"net/rpc"
	"slices"
	"testing"
	"time"

	"github.com/google/uuid"
	jobsProto "github.com/roadrunner-server/api-go/v6/jobs/v2"
	jobState "github.com/roadrunner-server/api-plugins/v6/jobs"
	goridgeRpc "github.com/roadrunner-server/goridge/v4/pkg/rpc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/emptypb"
)

const (
	push     string = "jobs.Push"
	pause    string = "jobs.Pause"
	destroy  string = "jobs.Destroy"
	resume   string = "jobs.Resume"
	declare  string = "jobs.Declare"
	getStats string = "jobs.GetStats"
)

// NewJobsClient dials the RoadRunner RPC endpoint and returns a net/rpc client
// speaking the goridge frame codec (the same transport the PHP SDK uses).
func NewJobsClient(t *testing.T, address string) *rpc.Client {
	t.Helper()
	conn, err := (&net.Dialer{}).DialContext(t.Context(), "tcp", address)
	require.NoError(t, err)
	client := rpc.NewClientWithCodec(goridgeRpc.NewClientCodec(conn))
	t.Cleanup(func() { _ = client.Close() })
	return client
}

func ResumePipes(address string, pipes ...string) func(t *testing.T) {
	return func(t *testing.T) {
		client := NewJobsClient(t, address)
		err := client.Call(resume, &jobsProto.Pipelines{Pipelines: slices.Clone(pipes)}, &jobsProto.JobsHandlerResponse{})
		require.NoError(t, err)
	}
}

func PushToPipe(pipeline string, autoAck bool, address string) func(t *testing.T) {
	return func(t *testing.T) {
		client := NewJobsClient(t, address)
		err := client.Call(push, &jobsProto.PushRequest{Job: createDummyJob(pipeline, autoAck)}, &jobsProto.JobsHandlerResponse{})
		require.NoError(t, err)
	}
}

func PushToPipeDelayed(address string, pipeline string, delay int64) func(t *testing.T) {
	return func(t *testing.T) {
		client := NewJobsClient(t, address)
		req := &jobsProto.PushRequest{Job: &jobsProto.Job{
			Job:     "some/php/namespace",
			Id:      uuid.NewString(),
			Payload: []byte(`{"hello":"world"}`),
			Headers: map[string]*jobsProto.JobHeaderValue{"test": {Values: []string{"test2"}}},
			Options: &jobsProto.Options{
				Priority: 1,
				Pipeline: pipeline,
				Delay:    delay,
			},
		}}
		err := client.Call(push, req, &jobsProto.JobsHandlerResponse{})
		assert.NoError(t, err)
	}
}

func createDummyJob(pipeline string, autoAck bool) *jobsProto.Job {
	return &jobsProto.Job{
		Job:     "some/php/namespace",
		Id:      uuid.NewString(),
		Payload: []byte(`{"hello":"world"}`),
		Headers: map[string]*jobsProto.JobHeaderValue{"test": {Values: []string{"test2"}}},
		Options: &jobsProto.Options{
			AutoAck:  autoAck,
			Priority: 1,
			Pipeline: pipeline,
			Topic:    pipeline,
		},
	}
}

func PausePipelines(address string, pipes ...string) func(t *testing.T) {
	return func(t *testing.T) {
		client := NewJobsClient(t, address)
		err := client.Call(pause, &jobsProto.Pipelines{Pipelines: slices.Clone(pipes)}, &jobsProto.JobsHandlerResponse{})
		require.NoError(t, err)
	}
}

func DestroyPipelines(address string, pipes ...string) func(t *testing.T) {
	return func(t *testing.T) {
		client := NewJobsClient(t, address)
		req := &jobsProto.Pipelines{Pipelines: slices.Clone(pipes)}

		// Retry the destroy 10× with 1s gaps; if all attempts fail, return
		// without asserting. Some negative tests intentionally destroy
		// non-existent pipelines and rely on this silent-after-retry pattern.
		for range 10 {
			err := client.Call(destroy, req, &jobsProto.Pipelines{})
			if err == nil {
				return
			}
			time.Sleep(time.Second)
		}
	}
}

func Stats(address string, state *jobState.State) func(t *testing.T) {
	return func(t *testing.T) {
		client := NewJobsClient(t, address)

		resp := &jobsProto.Stats{}
		err := client.Call(getStats, &emptypb.Empty{}, resp)
		require.NoError(t, err)
		require.NotEmpty(t, resp.GetStats())

		st := resp.GetStats()[0]
		state.Queue = st.GetQueue()
		state.Pipeline = st.GetPipeline()
		state.Driver = st.GetDriver()
		state.Active = st.GetActive()
		state.Delayed = st.GetDelayed()
		state.Reserved = st.GetReserved()
		state.Ready = st.GetReady()
		state.Priority = st.GetPriority()
	}
}

func EnableProxy(name string, t *testing.T) {
	buf := new(bytes.Buffer)
	buf.WriteString(`{"enabled":true}`)

	resp, err := http.Post("http://127.0.0.1:8474/proxies/"+name, "application/json", buf) //nolint:noctx
	require.NoError(t, err)
	require.Equal(t, 200, resp.StatusCode)
	if resp.Body != nil {
		_ = resp.Body.Close()
	}
}

func DisableProxy(name string, t *testing.T) {
	buf := new(bytes.Buffer)
	buf.WriteString(`{"enabled":false}`)

	resp, err := http.Post("http://127.0.0.1:8474/proxies/"+name, "application/json", buf) //nolint:noctx
	require.NoError(t, err)
	require.Equal(t, 200, resp.StatusCode)
	if resp.Body != nil {
		_ = resp.Body.Close()
	}
}

func DeleteProxy(name string, t *testing.T) {
	client := &http.Client{}

	req, err := http.NewRequest(http.MethodDelete, "http://127.0.0.1:8474/proxies/"+name, nil) //nolint:noctx
	require.NoError(t, err)

	resp, err := client.Do(req)
	require.NoError(t, err)
	require.Equal(t, 204, resp.StatusCode)
	if resp.Body != nil {
		_ = resp.Body.Close()
	}
}
