package pipeline

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/askiada/go-pipeline/v2/pkg/pipeline/model"
)

type stepOutputErrObserver struct {
	PipelineDefaults

	err error
}

func (o stepOutputErrObserver) OnStepOutput(_, _ *StepInfo) error {
	return o.err
}

type stepOutputMetricsErrObserver struct {
	PipelineDefaults

	err error
}

func (o stepOutputMetricsErrObserver) OnStepOutputMetrics(_, _ *StepInfo, _, _ time.Duration) error {
	return o.err
}

func (stepOutputMetricsErrObserver) OnSplitterOutputMetrics(_, _ *StepInfo, _, _ time.Duration) error {
	return nil
}

func (stepOutputMetricsErrObserver) OnMergerOutputMetrics(_, _ *StepInfo, _ time.Duration) error {
	return nil
}

func (stepOutputMetricsErrObserver) OnSinkOutputMetrics(_, _ *StepInfo, _, _ time.Duration) error {
	return nil
}

func (stepOutputMetricsErrObserver) AfterSinkMetrics(_ *StepInfo, _ time.Duration) error {
	return nil
}

func TestAcquireStepInputContextCanceled(t *testing.T) {
	t.Parallel()

	limiter := newInFlightLimiter(1)
	require.NotNil(t, limiter)
	require.NoError(t, limiter.acquire(context.Background()))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, ok, release, _, err := acquireStepInput(ctx, 1, limiter, make(chan int), false)
	require.Error(t, err)
	require.ErrorIs(t, err, context.Canceled)
	require.False(t, ok)
	require.Nil(t, release)

	limiter.release()
}

func TestReceiveStepInputContextCanceled(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, ok, _, err := receiveStepInput(ctx, 1, make(chan int), false)
	require.Error(t, err)
	require.ErrorIs(t, err, context.Canceled)
	require.False(t, ok)
}

func TestWaitRateLimitContextCanceled(t *testing.T) {
	t.Parallel()

	limiter := newRateLimiter(&model.RateLimitPolicy{Every: time.Hour, Burst: 1})
	require.NotNil(t, limiter)
	_ = limiter.takeToken()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := waitRateLimit(ctx, 1, limiter)
	require.Error(t, err)
	require.ErrorIs(t, err, context.Canceled)
}

func TestRunSequentialStepLoopRateLimitError(t *testing.T) {
	limiter := newRateLimiter(&model.RateLimitPolicy{Every: time.Second, Burst: 1})
	require.NotNil(t, limiter)
	_ = limiter.takeToken()

	input := &Step[int]{Output: make(chan int, 1)}
	input.Output <- 1

	close(input.Output)

	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		time.Sleep(5 * time.Millisecond)
		cancel()
	}()

	err := runSequentialStepLoop(ctx, 1, input, limiter, nil, false, func(context.Context, int, time.Duration, func()) error {
		return nil
	})
	require.Error(t, err)
	require.ErrorIs(t, err, context.Canceled)
}

func TestRunSequentialStepLoopProcessError(t *testing.T) {
	t.Parallel()

	input := &Step[int]{Output: make(chan int, 1)}
	input.Output <- 1

	close(input.Output)

	expectedErr := errors.New("process failed")
	err := runSequentialStepLoop(context.Background(), 1, input, nil, nil, false, func(context.Context, int, time.Duration, func()) error {
		return expectedErr
	})
	require.ErrorIs(t, err, expectedErr)
}

func TestSendStepOutputHookError(t *testing.T) {
	t.Parallel()

	input := &Step[int]{Details: &StepInfo{Name: "input"}}
	output := &Step[int]{Details: &StepInfo{Name: "output"}, Output: make(chan int, 1)}

	hookErr := errors.New("hook failed")
	cfg := hookConfig{opts: []model.PipelineOption{stepOutputErrObserver{err: hookErr}}}

	_, err := sendStepOutput(context.Background(), 1, input, output, 1, 0, 0, nil, nil, cfg)
	require.ErrorIs(t, err, hookErr)
}

func TestSendStepOutputMetricsError(t *testing.T) {
	t.Parallel()

	input := &Step[int]{Details: &StepInfo{Name: "input"}}
	output := &Step[int]{Details: &StepInfo{Name: "output"}, Output: make(chan int, 1)}

	metricErr := errors.New("metric failed")
	cfg := hookConfig{
		outputMetrics: true,
		metricsOpts:   []model.PipelineMetricsOption{stepOutputMetricsErrObserver{err: metricErr}},
	}

	_, err := sendStepOutput(context.Background(), 1, input, output, 1, 0, 0, nil, nil, cfg)
	require.ErrorIs(t, err, metricErr)
}

func TestSendOneToManyOutputsNoValues(t *testing.T) {
	t.Parallel()

	input := &Step[int]{Details: &StepInfo{Name: "input"}}
	output := &Step[int]{Details: &StepInfo{Name: "output"}, Output: make(chan int, 1)}

	dropped, err := sendOneToManyOutputs(context.Background(), 1, input, output, nil, 0, 0, nil, nil, hookConfig{})
	require.NoError(t, err)
	require.False(t, dropped)
}

func TestSendOneToManyOutputsHookError(t *testing.T) {
	t.Parallel()

	input := &Step[int]{Details: &StepInfo{Name: "input"}}
	output := &Step[int]{Details: &StepInfo{Name: "output"}, Output: make(chan int, 1)}

	hookErr := errors.New("hook failed")
	cfg := hookConfig{opts: []model.PipelineOption{stepOutputErrObserver{err: hookErr}}}

	_, err := sendOneToManyOutputs(context.Background(), 1, input, output, []int{1}, 0, 0, nil, nil, cfg)
	require.ErrorIs(t, err, hookErr)
}

func TestSendOneToManyOutputsMetricsError(t *testing.T) {
	t.Parallel()

	input := &Step[int]{Details: &StepInfo{Name: "input"}}
	output := &Step[int]{Details: &StepInfo{Name: "output"}, Output: make(chan int, 1)}

	metricErr := errors.New("metric failed")
	cfg := hookConfig{
		outputMetrics: true,
		metricsOpts:   []model.PipelineMetricsOption{stepOutputMetricsErrObserver{err: metricErr}},
	}

	_, err := sendOneToManyOutputs(context.Background(), 1, input, output, []int{1}, 0, 0, nil, nil, cfg)
	require.ErrorIs(t, err, metricErr)
}
