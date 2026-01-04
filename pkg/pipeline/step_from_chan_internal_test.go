package pipeline

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/askiada/go-pipeline/v2/pkg/pipeline/model"
)

type stepOutputObserver struct {
	PipelineDefaults

	err   error
	calls int
}

func (o *stepOutputObserver) OnStepOutput(_, _ *StepInfo) error {
	o.calls++

	return o.err
}

type stepOutputMetricsObserver struct {
	PipelineDefaults

	err error
}

func (o stepOutputMetricsObserver) OnStepOutputMetrics(_, _ *StepInfo, _, _ time.Duration) error {
	return o.err
}

func (o stepOutputMetricsObserver) OnSplitterOutputMetrics(_, _ *StepInfo, _, _ time.Duration) error {
	return nil
}

func (o stepOutputMetricsObserver) OnMergerOutputMetrics(_, _ *StepInfo, _ time.Duration) error {
	return nil
}

func (o stepOutputMetricsObserver) OnSinkOutputMetrics(_, _ *StepInfo, _, _ time.Duration) error {
	return nil
}

func (o stepOutputMetricsObserver) AfterSinkMetrics(_ *StepInfo, _ time.Duration) error {
	return nil
}

func TestRunStepFromChanDefaultsConcurrency(t *testing.T) {
	t.Parallel()

	input := &Step[int]{Details: &StepInfo{Name: "input"}, Output: make(chan int, 1)}
	input.Output <- 1

	close(input.Output)

	output := &Step[int]{Details: &StepInfo{Name: "output", Concurrent: 0}, Output: make(chan int, 1)}

	stepFn := func(ctx context.Context, in <-chan int, out chan int) error {
		for v := range in {
			out <- v
		}

		return nil
	}

	require.NoError(t, runStepFromChan(context.Background(), input, output, stepFn, hookConfig{}))
	require.Equal(t, 1, <-output.Output)
}

func TestSequentialStepFromChanSkipsHooksWhenNoItems(t *testing.T) {
	t.Parallel()

	input := &Step[int]{Details: &StepInfo{Name: "input"}, Output: make(chan int)}
	close(input.Output)

	output := &Step[int]{Details: &StepInfo{Name: "output"}, Output: make(chan int)}

	observer := &stepOutputObserver{}
	cfg := hookConfig{opts: []model.PipelineOption{observer}}

	stepFn := func(ctx context.Context, in <-chan int, out chan int) error {
		for range in {
		}

		return nil
	}

	require.NoError(t, sequentialStepFromChanFn(context.Background(), 1, input, output, stepFn, cfg))
	require.Equal(t, 0, observer.calls)
}

func TestSequentialStepFromChanReportsHookErrors(t *testing.T) {
	t.Parallel()

	input := &Step[int]{Details: &StepInfo{Name: "input"}, Output: make(chan int, 1)}
	input.Output <- 1

	close(input.Output)

	output := &Step[int]{Details: &StepInfo{Name: "output"}, Output: make(chan int, 1)}

	observer := &stepOutputObserver{err: errors.New("hook failed")}
	cfg := hookConfig{opts: []model.PipelineOption{observer}}

	stepFn := func(ctx context.Context, in <-chan int, out chan int) error {
		for v := range in {
			out <- v
		}

		return nil
	}

	err := sequentialStepFromChanFn(context.Background(), 1, input, output, stepFn, cfg)
	require.ErrorIs(t, err, observer.err)
}

func TestSequentialStepFromChanMetricsError(t *testing.T) {
	t.Parallel()

	input := &Step[int]{Details: &StepInfo{Name: "input"}, Output: make(chan int, 1)}
	input.Output <- 1

	close(input.Output)

	output := &Step[int]{Details: &StepInfo{Name: "output"}, Output: make(chan int, 1)}

	metricsErr := errors.New("metrics failed")
	cfg := hookConfig{
		outputMetrics: true,
		metricsOpts:   []model.PipelineMetricsOption{stepOutputMetricsObserver{err: metricsErr}},
	}

	stepFn := func(ctx context.Context, in <-chan int, out chan int) error {
		for v := range in {
			out <- v
		}

		return nil
	}

	err := sequentialStepFromChanFn(context.Background(), 1, input, output, stepFn, cfg)
	require.ErrorIs(t, err, metricsErr)
}

func TestSequentialStepFromChanStepError(t *testing.T) {
	t.Parallel()

	input := &Step[int]{Details: &StepInfo{Name: "input"}, Output: make(chan int)}
	output := &Step[int]{Details: &StepInfo{Name: "output"}, Output: make(chan int)}

	stepFnErr := errors.New("step failed")
	stepFn := func(context.Context, <-chan int, chan int) error {
		return stepFnErr
	}

	err := sequentialStepFromChanFn(context.Background(), 1, input, output, stepFn, hookConfig{})
	require.ErrorIs(t, err, stepFnErr)
}

func TestConcurrentStepFromChanPropagatesError(t *testing.T) {
	t.Parallel()

	input := &Step[int]{Details: &StepInfo{Name: "input"}, Output: make(chan int)}
	output := &Step[int]{Details: &StepInfo{Name: "output", Concurrent: 2}, Output: make(chan int)}

	stepFn := func(context.Context, <-chan int, chan int) error {
		return errors.New("step failed")
	}

	err := concurrentStepFromChanFn(context.Background(), input, output, stepFn, hookConfig{})
	require.Error(t, err)
}

func TestRunStepFromChanValidatesOptions(t *testing.T) {
	t.Parallel()

	input := &Step[int]{Details: &StepInfo{Name: "input"}}
	output := &Step[int]{Details: &StepInfo{Name: "output"}, RetryPolicy: &model.RetryPolicy{MaxAttempts: 2}}

	err := runStepFromChan(context.Background(), input, output, nil, hookConfig{})
	require.ErrorIs(t, err, ErrRetryUnsupported)
}

func TestValidateFromChanOptionsNil(t *testing.T) {
	t.Parallel()

	require.NoError(t, validateFromChanOptions[int](nil))
}
