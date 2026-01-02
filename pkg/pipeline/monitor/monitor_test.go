//nolint:testpackage // uses internal test hooks for the custom emitter.
package monitor

import (
	"context"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/askiada/go-pipeline/pkg/pipeline/model"
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
