//nolint:testpackage // uses internal test hooks for the custom emitter.
package monitor

import (
	"context"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/askiada/go-pipeline/v2/pkg/pipeline/model"
)

type stubEmitter struct {
	mu    sync.Mutex
	lines []string
}

func (se *stubEmitter) Emit(line string) {
	if line == "" {
		return
	}

	se.mu.Lock()
	defer se.mu.Unlock()

	se.lines = append(se.lines, line)
}

func (se *stubEmitter) Close() error {
	return nil
}

func (se *stubEmitter) Err() error {
	return nil
}

func (se *stubEmitter) Lines() []string {
	se.mu.Lock()
	defer se.mu.Unlock()

	out := make([]string, len(se.lines))
	copy(out, se.lines)

	return out
}

func TestMonitorEmitsRunTags(t *testing.T) {
	t.Parallel()

	emitter := &stubEmitter{}
	cfg := Config{
		RunID:         "run-1",
		RunName:       "nightly",
		Origin:        "container-1",
		PipelineName:  "pipeline-a",
		customEmitter: emitter,
	}

	opt := model.PipelineOption(PipelineMonitor(&cfg))
	require.NoError(t, opt.New())
	metricsOpt, ok := opt.(model.PipelineMetricsOption)
	require.True(t, ok)

	parent := &model.StepInfo{Name: "root", Type: model.NormalStepType}
	step := &model.StepInfo{Name: "step-1", Type: model.NormalStepType}

	require.NoError(t, metricsOpt.OnStepOutputMetrics(parent, step, 12*time.Millisecond, 5*time.Millisecond))

	lines := emitter.Lines()
	require.Len(t, lines, 1)

	line := lines[0]
	require.Contains(t, line, "run_id=run-1")
	require.Contains(t, line, "run_name=nightly")
	require.Contains(t, line, "origin=container-1")
	require.Contains(t, line, "pipeline_name=pipeline-a")
	require.Contains(t, line, "step_name=step-1")
	require.Contains(t, line, "parent_step=root")
	require.Contains(t, line, "duration_ms=5i")
	require.Contains(t, line, "transport_ms=12i")
}

func TestMonitorSkipsDryRun(t *testing.T) {
	t.Parallel()

	emitter := &stubEmitter{}
	cfg := Config{
		RunID:         "run-2",
		RunName:       "dry-run",
		Origin:        "container-2",
		customEmitter: emitter,
	}

	opt := model.PipelineOption(PipelineMonitor(&cfg))
	require.NoError(t, opt.New())

	runAware, ok := opt.(model.RunOptionAware)
	require.True(t, ok)
	runAware.SetRunOptions(model.RunOptions{DryRun: true})

	parent := &model.StepInfo{Name: "root", Type: model.NormalStepType}
	step := &model.StepInfo{Name: "step-1", Type: model.NormalStepType}

	metricsOpt, ok := opt.(model.PipelineMetricsOption)
	require.True(t, ok)
	require.NoError(t, metricsOpt.OnStepOutputMetrics(parent, step, 2*time.Millisecond, 1*time.Millisecond))

	lines := emitter.Lines()
	require.Empty(t, lines)
}

func TestMonitorUIStartsOnRun(t *testing.T) {
	t.Parallel()

	emitter := &stubEmitter{}
	cfg := Config{
		BindAddr:      "127.0.0.1:0",
		EnableUI:      true,
		customEmitter: emitter,
	}

	opt := PipelineMonitor(&cfg)
	require.NoError(t, opt.New())

	opt.SetRunOptions(model.RunOptions{})

	addr := opt.UIAddr()
	if addr == "" {
		err := opt.uiError()
		if err != nil {
			t.Skipf("monitor ui unavailable: %v", err)
		}
	}

	require.NotEmpty(t, addr)

	require.Eventually(t, func() bool {
		req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "http://"+addr+"/", nil)
		if err != nil {
			return false
		}

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return false
		}

		defer resp.Body.Close()

		return resp.StatusCode == http.StatusOK
	}, 500*time.Millisecond, 25*time.Millisecond)

	require.NoError(t, opt.Finish())
}

func TestMonitorUISkipsDryRun(t *testing.T) {
	t.Parallel()

	emitter := &stubEmitter{}
	cfg := Config{
		BindAddr:      "127.0.0.1:0",
		EnableUI:      true,
		customEmitter: emitter,
	}

	opt := PipelineMonitor(&cfg)
	require.NoError(t, opt.New())

	opt.SetRunOptions(model.RunOptions{DryRun: true})
	require.Empty(t, opt.UIAddr())

	require.NoError(t, opt.Finish())
}

func TestMonitorUISnapshotTotals(t *testing.T) {
	t.Parallel()

	emitter := &stubEmitter{}
	cfg := Config{
		EnableUI:      true,
		customEmitter: emitter,
	}

	opt := PipelineMonitor(&cfg)
	require.NoError(t, opt.New())

	metricsOpt, ok := any(opt).(model.PipelineMetricsOption)
	require.True(t, ok)

	stepA := &model.StepInfo{Name: "step-a", Type: model.NormalStepType}
	stepB := &model.StepInfo{Name: "step-b", Type: model.NormalStepType}

	require.NoError(t, metricsOpt.OnStepOutputMetrics(stepA, stepA, 30*time.Millisecond, 20*time.Millisecond))
	require.NoError(t, metricsOpt.OnStepOutputMetrics(stepA, stepA, 10*time.Millisecond, 5*time.Millisecond))
	require.NoError(t, metricsOpt.OnStepOutputMetrics(stepA, stepB, 12*time.Millisecond, 7*time.Millisecond))

	require.NoError(t, opt.OnStepDrop(stepA, model.StepDropBufferFull))
	require.NoError(t, opt.OnStepRetry(stepA, stepA, 1, 15*time.Millisecond))
	require.NoError(t, opt.OnStepErrorRoute(stepB))

	require.NoError(t, metricsOpt.AfterSinkMetrics(stepB, 1500*time.Millisecond))

	snapshot := opt.uiSnapshotEvent()
	require.NotNil(t, snapshot)

	outputs, ok := snapshot.Fields["outputs"].(map[string]uiOutputTotals)
	require.True(t, ok)

	require.Equal(t, uiOutputTotals{Count: 2, DurationMs: 25, TransportMs: 40}, outputs["step-a"])
	require.Equal(t, uiOutputTotals{Count: 1, DurationMs: 7, TransportMs: 12}, outputs["step-b"])

	drops, ok := snapshot.Fields["drops"].(map[string]int64)
	require.True(t, ok)
	require.Equal(t, int64(1), drops["step-a"])

	retries, ok := snapshot.Fields["retries"].(map[string]int64)
	require.True(t, ok)
	require.Equal(t, int64(1), retries["step-a"])

	errorRoutes, ok := snapshot.Fields["error_routes"].(map[string]int64)
	require.True(t, ok)
	require.Equal(t, int64(1), errorRoutes["step-b"])

	runTotal, ok := snapshot.Fields["run_total_ms"].(int64)
	require.True(t, ok)
	require.Equal(t, int64(1500), runTotal)

	seq, ok := snapshot.Fields["snapshot_seq"].(int64)
	require.True(t, ok)
	require.Equal(t, int64(7), seq)

	require.NoError(t, opt.Finish())
}

func TestUIHubPublishPriorityReplacesBufferedEvent(t *testing.T) {
	t.Parallel()

	hub := newUIHub(1)
	stream := hub.subscribe()

	hub.publish(monitorEvent{Measurement: "step_output"})
	hub.publishPriority(monitorEvent{Measurement: "monitor_snapshot"})

	select {
	case event := <-stream:
		require.Equal(t, "monitor_snapshot", event.Measurement)
	case <-time.After(50 * time.Millisecond):
		t.Fatal("timed out waiting for snapshot event")
	}
}
