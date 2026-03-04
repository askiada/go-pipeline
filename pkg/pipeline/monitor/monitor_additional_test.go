//nolint:testpackage // uses internal monitor helpers and types.
package monitor

import (
	"bytes"
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/askiada/go-pipeline/v2/pkg/pipeline"
	"github.com/askiada/go-pipeline/v2/pkg/pipeline/model"
)

type flushRecorder struct {
	header http.Header
	status int
	buf    bytes.Buffer
}

func (fr *flushRecorder) Header() http.Header {
	if fr.header == nil {
		fr.header = make(http.Header)
	}

	return fr.header
}

func (fr *flushRecorder) Write(data []byte) (int, error) {
	return fr.buf.Write(data)
}

func (fr *flushRecorder) WriteHeader(statusCode int) {
	fr.status = statusCode
}

func (fr *flushRecorder) Flush() {}

type nonFlusherRecorder struct {
	header http.Header
	status int
	buf    bytes.Buffer
}

func (nr *nonFlusherRecorder) Header() http.Header {
	if nr.header == nil {
		nr.header = make(http.Header)
	}

	return nr.header
}

func (nr *nonFlusherRecorder) Write(data []byte) (int, error) {
	return nr.buf.Write(data)
}

func (nr *nonFlusherRecorder) WriteHeader(statusCode int) {
	nr.status = statusCode
}

func lineMeasurement(line string) string {
	head := strings.SplitN(line, " ", 2)[0]

	return strings.SplitN(head, ",", 2)[0]
}

func hasMeasurement(lines []string, measurement string) bool {
	for _, line := range lines {
		if lineMeasurement(line) == measurement {
			return true
		}
	}

	return false
}

func TestMonitorPrepareHooksEmitMetaAndLinks(t *testing.T) {
	t.Parallel()

	emitter := &stubEmitter{}
	opt := PipelineMonitor(&Config{customEmitter: emitter})
	require.NoError(t, opt.New())

	parent := &pipeline.StepInfo{
		Name:       "parent",
		Type:       model.NormalStepType,
		Concurrent: 2,
		BufferSize: 4,
	}
	step := &pipeline.StepInfo{
		Name:       "child",
		Type:       model.NormalStepType,
		Concurrent: 1,
		BufferSize: 2,
	}
	merger := &pipeline.StepInfo{Name: "merge", Type: model.NormalStepType}

	require.NoError(t, opt.PrepareStep(parent, step))
	require.NoError(t, opt.PrepareSplitter(parent, step))
	require.NoError(t, opt.PrepareMerger([]*pipeline.StepInfo{parent, step}, merger))
	require.NoError(t, opt.PrepareSink(parent, step))

	lines := emitter.Lines()
	require.True(t, hasMeasurement(lines, "pipeline_step"))
	require.True(t, hasMeasurement(lines, "pipeline_link"))
}

func TestMonitorOutputHooksNoop(t *testing.T) {
	t.Parallel()

	opt := PipelineMonitor(&Config{customEmitter: &stubEmitter{}})
	require.NoError(t, opt.New())

	parent := &pipeline.StepInfo{Name: "parent"}
	step := &pipeline.StepInfo{Name: "step"}

	require.NoError(t, opt.OnStepOutput(parent, step))
	require.NoError(t, opt.OnSplitterOutput(parent, step))
	require.NoError(t, opt.OnMergerOutput(parent, step))
	require.NoError(t, opt.OnSinkOutput(parent, step))
	require.NoError(t, opt.AfterSink(step))
}

func TestMonitorOutputMetricsEmitters(t *testing.T) {
	t.Parallel()

	emitter := &stubEmitter{}
	opt := PipelineMonitor(&Config{customEmitter: emitter})
	require.NoError(t, opt.New())

	parent := &pipeline.StepInfo{Name: "parent", Type: model.NormalStepType}
	step := &pipeline.StepInfo{Name: "step", Type: model.NormalStepType}

	require.NoError(t, opt.OnSplitterOutputMetrics(parent, step, 4*time.Millisecond, 2*time.Millisecond))
	require.NoError(t, opt.OnMergerOutputMetrics(parent, step, 3*time.Millisecond))
	require.NoError(t, opt.OnSinkOutputMetrics(parent, step, 5*time.Millisecond, 1*time.Millisecond))

	lines := emitter.Lines()
	require.True(t, hasMeasurement(lines, "splitter_output"))
	require.True(t, hasMeasurement(lines, "merger_output"))
	require.True(t, hasMeasurement(lines, "sink_output"))
}

func TestMonitorMergeTagsSkipsEmpty(t *testing.T) {
	t.Parallel()

	emitter := &stubEmitter{}
	opt := PipelineMonitor(&Config{
		RunID:         "run-1",
		RunName:       "nightly",
		Origin:        "origin-1",
		PipelineName:  "pipe-1",
		customEmitter: emitter,
	})
	require.NoError(t, opt.New())

	merged := opt.mergeTags(map[string]string{
		"extra": "value",
		"empty": "",
	})

	require.Equal(t, "run-1", merged["run_id"])
	require.Equal(t, "nightly", merged["run_name"])
	require.Equal(t, "origin-1", merged["origin"])
	require.Equal(t, "pipe-1", merged["pipeline_name"])
	require.Equal(t, "value", merged["extra"])
	_, ok := merged["empty"]
	require.False(t, ok)
}

func TestBuildLineEscapesFieldStrings(t *testing.T) {
	t.Parallel()

	fields := map[string]any{
		"count":  int64(1),
		"active": true,
		"note":   "line \"one\" \\ two",
	}
	line := buildLine(
		"metric name",
		[]string{tagPair("base", "value")},
		map[string]string{"tag": "space value"},
		fields,
		time.Unix(0, 0),
	)

	require.Contains(t, line, "metric\\ name")
	require.Contains(t, line, "tag=space\\ value")
	require.Contains(t, line, "note=\"line \\\"one\\\" \\\\ two\"")

	require.Empty(t, buildLine("", nil, nil, fields, time.Unix(0, 0)))
	require.Empty(t, buildLine("metric", nil, nil, nil, time.Unix(0, 0)))
}

func TestTelegrafEmitterLifecycle(t *testing.T) {
	t.Parallel()

	listener, err := (&net.ListenConfig{}).ListenPacket(context.Background(), "udp", "127.0.0.1:0")
	if err != nil {
		t.Skipf("udp listener unavailable: %v", err)
	}
	defer listener.Close()

	emitter, err := newTelegrafEmitter("udp", listener.LocalAddr().String(), 1)
	require.NoError(t, err)

	emitter.Emit("hello")

	buf := make([]byte, 64)

	require.NoError(t, listener.SetReadDeadline(time.Now().Add(200*time.Millisecond)))

	n, _, err := listener.ReadFrom(buf)
	require.NoError(t, err)
	require.Contains(t, string(buf[:n]), "hello")

	require.NoError(t, emitter.Close())
	require.NoError(t, emitter.Err())
}

func TestTelegrafEmitterSetErrOnce(t *testing.T) {
	t.Parallel()

	emitter := &telegrafEmitter{}

	testErr := errors.New("boom")

	emitter.setErr(nil)
	emitter.setErr(testErr)
	emitter.setErr(errors.New("other"))

	require.ErrorIs(t, emitter.Err(), testErr)
}

func TestMonitorServeEventsStreams(t *testing.T) {
	t.Parallel()

	emitter := &stubEmitter{}
	opt := PipelineMonitor(&Config{
		EnableUI:      true,
		RunID:         "run-1",
		RunName:       "nightly",
		Origin:        "origin-1",
		customEmitter: emitter,
	})
	require.NoError(t, opt.New())

	opt.ui.markRunStarted(time.Unix(0, 0))
	opt.ui.recordEvent(
		"pipeline_step",
		map[string]string{"step_name": "step"},
		map[string]any{"count": int64(1)},
		time.Unix(0, 0),
	)
	opt.ui.recordEvent(
		"step_output",
		map[string]string{"step_name": "step"},
		map[string]any{"count": int64(1)},
		time.Unix(0, 0),
	)

	hub := newUIHub(1)
	opt.ui.setServer(hub, &http.Server{}, "127.0.0.1:0")

	recorder := &flushRecorder{}
	req := httptest.NewRequest(http.MethodGet, "http://example/events", nil)
	ctx, cancel := context.WithCancel(req.Context())
	req = req.WithContext(ctx)

	done := make(chan struct{})

	go func() {
		opt.serveEvents(recorder, req)
		close(done)
	}()

	cancel()

	select {
	case <-done:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("timed out waiting for events stream to close")
	}

	body := recorder.buf.String()
	require.Contains(t, body, "\"measurement\":\"monitor_status\"")
	require.Contains(t, body, "\"measurement\":\"pipeline_step\"")
	require.Contains(t, body, "\"measurement\":\"monitor_snapshot\"")
}

func TestMonitorServeEventsNoHubOrFlusher(t *testing.T) {
	t.Parallel()

	recorder := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "http://example/events", nil)

	var pm *pipelineMonitor
	pm.serveEvents(recorder, req)
	require.Equal(t, http.StatusNotFound, recorder.Code)

	emitter := &stubEmitter{}
	opt := PipelineMonitor(&Config{EnableUI: true, customEmitter: emitter})
	require.NoError(t, opt.New())

	opt.ui.setServer(newUIHub(1), &http.Server{}, "127.0.0.1:0")

	nonFlusher := &nonFlusherRecorder{}
	opt.serveEvents(nonFlusher, req)
	require.Equal(t, http.StatusInternalServerError, nonFlusher.status)
}

func TestMonitorUIErrorsAndAddr(t *testing.T) {
	t.Parallel()

	var pm *pipelineMonitor
	require.Empty(t, pm.UIAddr())
	require.NoError(t, pm.uiError())

	emitter := &stubEmitter{}
	opt := PipelineMonitor(&Config{customEmitter: emitter})
	require.NoError(t, opt.New())

	testErr := errors.New("ui failed")
	opt.setUIErr(testErr)
	require.ErrorIs(t, opt.uiError(), testErr)
}

func TestUIHubUnsubscribeClosesStream(t *testing.T) {
	t.Parallel()

	hub := newUIHub(1)
	stream := hub.subscribe()

	hub.unsubscribe(stream)

	_, ok := <-stream
	require.False(t, ok)

	hub.unsubscribe(nil)
}

func TestUIStateMetaSnapshotAndErr(t *testing.T) {
	t.Parallel()

	state := &uiState{}

	state.recordEvent(
		"pipeline_step",
		map[string]string{"step_name": "step"},
		map[string]any{"count": int64(1)},
		time.Unix(0, 0),
	)

	meta := state.metaSnapshot()
	require.Len(t, meta, 1)

	meta[0].Measurement = "modified"
	metaCheck := state.metaSnapshot()
	require.Equal(t, "pipeline_step", metaCheck[0].Measurement)

	require.Nil(t, state.hubValue())

	err := errors.New("boom")
	state.setErr(err)
	state.setErr(errors.New("other"))
	require.ErrorIs(t, state.errValue(), err)
}
