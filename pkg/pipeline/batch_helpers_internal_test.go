package pipeline

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/askiada/go-pipeline/v2/pkg/pipeline/model"
)

type batchOutputObserver struct {
	PipelineDefaults

	calls int
	err   error
}

func (o *batchOutputObserver) OnStepOutput(_, _ *StepInfo) error {
	o.calls++

	return o.err
}

type batchMetricsObserver struct {
	calls   int
	wait    time.Duration
	compute time.Duration
	err     error
}

func (o *batchMetricsObserver) OnStepOutputMetrics(_, _ *StepInfo, wait, compute time.Duration) error {
	o.calls++
	o.wait = wait
	o.compute = compute

	return o.err
}

func (o *batchMetricsObserver) OnSplitterOutputMetrics(_, _ *StepInfo, _, _ time.Duration) error {
	return nil
}

func (o *batchMetricsObserver) OnMergerOutputMetrics(_, _ *StepInfo, _ time.Duration) error {
	return nil
}

func (o *batchMetricsObserver) OnSinkOutputMetrics(_, _ *StepInfo, _, _ time.Duration) error {
	return nil
}

func (o *batchMetricsObserver) AfterSinkMetrics(_ *StepInfo, _ time.Duration) error {
	return nil
}

func TestResetBatchTimer(t *testing.T) {
	t.Parallel()

	timer := time.NewTimer(time.Hour)
	defer timer.Stop()

	kept, ch := resetBatchTimer(timer, 0)
	assert.Equal(t, timer, kept)
	assert.Nil(t, ch)

	created, createdCh := resetBatchTimer(nil, 5*time.Millisecond)
	require.NotNil(t, created)
	require.NotNil(t, createdCh)
	stopBatchTimer(created)

	reset, resetCh := resetBatchTimer(created, 5*time.Millisecond)
	assert.Equal(t, created, reset)
	assert.Equal(t, createdCh, resetCh)
	stopBatchTimer(reset)
}

func TestBatchTrackerLifecycle(t *testing.T) {
	t.Parallel()

	tracker := newBatchTracker(2, 10*time.Millisecond)
	tracker.startBatch()
	started := tracker.batchStart
	require.False(t, started.IsZero())
	require.NotNil(t, tracker.timerC)

	tracker.startBatch()
	assert.True(t, started.Equal(tracker.batchStart))

	tracker.batchStart = time.Now().Add(-20 * time.Millisecond)
	assert.True(t, tracker.shouldFlushForTime())

	tracker.recordWait(5 * time.Millisecond)
	tracker.recordItem()
	tracker.recordItem()
	assert.True(t, tracker.shouldFlushForSize())

	count, wait := tracker.snapshotAndReset()
	assert.Equal(t, 2, count)
	assert.Equal(t, 5*time.Millisecond, wait)
	assert.True(t, tracker.batchStart.IsZero())
	assert.Nil(t, tracker.timerC)
}

func TestReportBatchOutputWithoutMetrics(t *testing.T) {
	t.Parallel()

	input := &Step[int]{Details: &StepInfo{Name: "input"}}
	output := &Step[[]int]{Details: &StepInfo{Name: "output"}}
	observer := &batchOutputObserver{}

	cfg := hookConfig{
		opts:          []model.PipelineOption{observer},
		outputMetrics: false,
	}

	require.NoError(t, reportBatchOutput(cfg, input, output, 1, 0, 0))
	assert.Equal(t, 1, observer.calls)
}

func TestReportBatchOutputWithMetrics(t *testing.T) {
	t.Parallel()

	input := &Step[int]{Details: &StepInfo{Name: "input"}}
	output := &Step[[]int]{Details: &StepInfo{Name: "output"}}
	observer := &batchOutputObserver{}
	metrics := &batchMetricsObserver{}

	cfg := hookConfig{
		opts:          []model.PipelineOption{observer},
		metricsOpts:   []model.PipelineMetricsOption{metrics},
		outputMetrics: true,
	}

	require.NoError(t, reportBatchOutput(cfg, input, output, 2, 6*time.Millisecond, 10*time.Millisecond))
	assert.Equal(t, 1, observer.calls)
	assert.Equal(t, 1, metrics.calls)
	assert.Equal(t, 3*time.Millisecond, metrics.wait)
	assert.Equal(t, 5*time.Millisecond, metrics.compute)
}

func TestReportBatchOutputWithMetricsZeroCount(t *testing.T) {
	t.Parallel()

	input := &Step[int]{Details: &StepInfo{Name: "input"}}
	output := &Step[[]int]{Details: &StepInfo{Name: "output"}}
	metrics := &batchMetricsObserver{}

	cfg := hookConfig{
		metricsOpts:   []model.PipelineMetricsOption{metrics},
		outputMetrics: true,
	}

	require.NoError(t, reportBatchOutput(cfg, input, output, 0, 10*time.Millisecond, 20*time.Millisecond))
	assert.Equal(t, 1, metrics.calls)
	assert.Equal(t, time.Duration(0), metrics.wait)
	assert.Equal(t, time.Duration(0), metrics.compute)
}

func TestReportBatchOutputErrors(t *testing.T) {
	t.Parallel()

	input := &Step[int]{Details: &StepInfo{Name: "input"}}
	output := &Step[[]int]{Details: &StepInfo{Name: "output"}}
	observer := &batchOutputObserver{err: errors.New("observer failed")}

	cfg := hookConfig{
		opts:          []model.PipelineOption{observer},
		outputMetrics: false,
	}

	err := reportBatchOutput(cfg, input, output, 1, 0, 0)
	require.Error(t, err)

	metrics := &batchMetricsObserver{err: errors.New("metrics failed")}
	cfg = hookConfig{
		metricsOpts:   []model.PipelineMetricsOption{metrics},
		outputMetrics: true,
	}
	err = reportBatchOutput(cfg, input, output, 1, time.Millisecond, time.Millisecond)
	require.Error(t, err)
}
