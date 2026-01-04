//nolint:testpackage // exercises internal UI helpers directly.
package monitor

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestMonitorServeEventsClosedStream(t *testing.T) {
	t.Parallel()

	opt := PipelineMonitor(&Config{EnableUI: true, customEmitter: &stubEmitter{}})
	require.NoError(t, opt.New())

	hub := newUIHub(1)
	hub.close()
	opt.ui.setServer(hub, &http.Server{}, "127.0.0.1:0")

	recorder := &flushRecorder{}
	req := httptest.NewRequest(http.MethodGet, "http://example/events", nil)

	opt.serveEvents(recorder, req)
	require.Contains(t, recorder.buf.String(), "\"measurement\":\"monitor_status\"")
}

func TestWriteSnapshotEventWhenDisabled(t *testing.T) {
	t.Parallel()

	opt := PipelineMonitor(&Config{EnableUI: false, customEmitter: &stubEmitter{}})
	require.NoError(t, opt.New())

	recorder := &flushRecorder{}
	opt.writeSnapshotEvent(recorder)
	require.Empty(t, recorder.buf.String())
}

func TestWriteEventMarshalError(t *testing.T) {
	t.Parallel()

	opt := PipelineMonitor(&Config{customEmitter: &stubEmitter{}})
	require.NoError(t, opt.New())

	recorder := &flushRecorder{}
	opt.writeEvent(recorder, monitorEvent{
		Measurement: "bad",
		Fields: map[string]any{
			"bad": func() {},
		},
	})

	require.Empty(t, recorder.buf.String())
}

func TestDrainEventsWritesBuffered(t *testing.T) {
	t.Parallel()

	opt := PipelineMonitor(&Config{customEmitter: &stubEmitter{}})
	require.NoError(t, opt.New())

	stream := make(chan monitorEvent, 1)
	stream <- monitorEvent{Measurement: "event"}

	close(stream)

	recorder := &flushRecorder{}
	opt.drainEvents(recorder, stream, recorder)

	require.Contains(t, recorder.buf.String(), "\"measurement\":\"event\"")
}

func TestMetaSnapshotAndSnapshotNil(t *testing.T) {
	t.Parallel()

	var pm *pipelineMonitor
	require.Nil(t, pm.metaSnapshot())
	require.Nil(t, pm.uiSnapshotEvent())
}

func TestUIStateStartOnceAndRecordEventPublish(t *testing.T) {
	t.Parallel()

	state := &uiState{}
	calls := 0

	state.startOnce(func() { calls++ })
	state.startOnce(func() { calls++ })
	require.Equal(t, 1, calls)

	hub := newUIHub(1)
	state.setServer(hub, &http.Server{}, "addr")

	event, outHub, publish := state.recordEvent(
		"step_output",
		map[string]string{"step_name": "step"},
		nil,
		time.Unix(0, 0),
	)

	require.True(t, publish)
	require.Equal(t, hub, outHub)
	require.Equal(t, int64(1), event.Fields["seq"])
}

func TestUIStateAccessorsAndErrors(t *testing.T) {
	t.Parallel()

	state := &uiState{}
	state.setErr(nil)
	require.NoError(t, state.errValue())

	hub := newUIHub(1)
	server := &http.Server{}
	state.setServer(hub, server, "addr")
	require.Equal(t, hub, state.hubValue())
	require.Equal(t, "addr", state.addrValue())

	state.clearServer()
	require.Nil(t, state.hubValue())
	require.Equal(t, "addr", state.addrValue())
}

func TestUIStateMarkRunStartedKeepsFirst(t *testing.T) {
	t.Parallel()

	state := &uiState{}
	start := time.Unix(1, 0)
	state.markRunStarted(start)
	state.markRunStarted(time.Unix(2, 0))

	_, _, runStarted := state.snapshot()
	require.Equal(t, start, runStarted)
}

func TestCopyFieldsEmpty(t *testing.T) {
	t.Parallel()

	require.Nil(t, copyFields(nil))
	require.Nil(t, copyFields(map[string]any{}))
}

func TestUIHubClosedBehaviors(t *testing.T) {
	t.Parallel()

	hub := newUIHub(0)
	stream := hub.subscribe()
	require.Equal(t, defaultBufferSize, cap(stream))

	hub.close()
	hub.publish(monitorEvent{Measurement: "event"})

	stream = hub.subscribe()
	_, ok := <-stream
	require.False(t, ok)
}

func TestUIHubPublishPrioritySendsWhenIdle(t *testing.T) {
	t.Parallel()

	hub := newUIHub(1)
	stream := hub.subscribe()

	hub.publishPriority(monitorEvent{Measurement: "snapshot"})

	select {
	case event := <-stream:
		require.Equal(t, "snapshot", event.Measurement)
	case <-time.After(50 * time.Millisecond):
		t.Fatal("timed out waiting for priority event")
	}

	hub.close()
	hub.publishPriority(monitorEvent{Measurement: "ignored"})
}

func TestUITotalsHelpers(t *testing.T) {
	t.Parallel()

	totals := &uiTotals{}
	totals.applyOutput(nil, map[string]any{"count": int64(1)})
	require.Nil(t, totals.outputs)

	require.Nil(t, totals.applyCount(nil, map[string]any{"count": int64(1)}, nil))
	require.Empty(t, tagStepName(nil))
}

func TestFieldInt64CoversTypes(t *testing.T) {
	t.Parallel()

	fields := map[string]any{
		"i64":  int64(2),
		"i":    int(3),
		"f32":  float32(4),
		"f64":  float64(5),
		"num":  json.Number("6"),
		"bad":  json.Number("x"),
		"nil":  nil,
		"bool": true,
	}

	require.Equal(t, int64(2), fieldInt64(fields, "i64"))
	require.Equal(t, int64(3), fieldInt64(fields, "i"))
	require.Equal(t, int64(4), fieldInt64(fields, "f32"))
	require.Equal(t, int64(5), fieldInt64(fields, "f64"))
	require.Equal(t, int64(6), fieldInt64(fields, "num"))
	require.Equal(t, int64(0), fieldInt64(fields, "bad"))
	require.Equal(t, int64(0), fieldInt64(fields, "nil"))
	require.Equal(t, int64(0), fieldInt64(fields, "bool"))
	require.Equal(t, int64(0), fieldInt64(fields, "missing"))
}
