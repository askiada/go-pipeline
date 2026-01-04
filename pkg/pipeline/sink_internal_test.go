package pipeline

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/askiada/go-pipeline/v2/pkg/pipeline/model"
)

type sinkOutputObserver struct {
	PipelineDefaults

	err   error
	calls int
}

func (o *sinkOutputObserver) OnSinkOutput(_, _ *StepInfo) error {
	o.calls++

	return o.err
}

type sinkMetricsObserver struct {
	PipelineDefaults

	err error
}

func (o sinkMetricsObserver) OnStepOutputMetrics(_, _ *StepInfo, _, _ time.Duration) error {
	return nil
}

func (o sinkMetricsObserver) OnSplitterOutputMetrics(_, _ *StepInfo, _, _ time.Duration) error {
	return nil
}

func (o sinkMetricsObserver) OnMergerOutputMetrics(_, _ *StepInfo, _ time.Duration) error {
	return nil
}

func (o sinkMetricsObserver) OnSinkOutputMetrics(_, _ *StepInfo, _, _ time.Duration) error {
	return o.err
}

func (o sinkMetricsObserver) AfterSinkMetrics(_ *StepInfo, _ time.Duration) error {
	return nil
}

type sinkPrepareObserver struct {
	PipelineDefaults

	err error
}

func (o sinkPrepareObserver) PrepareSink(_, _ *StepInfo) error {
	return o.err
}

func TestValidateSinkFromChanOptionsNil(t *testing.T) {
	t.Parallel()

	require.NoError(t, validateSinkFromChanOptions[int](nil))
}

func TestSequentialSinkFromChanNoItemsSkipsHooks(t *testing.T) {
	t.Parallel()

	input := &Step[int]{Details: &StepInfo{Name: "input"}, Output: make(chan int)}
	close(input.Output)

	output := &Step[int]{Details: &StepInfo{Name: "output"}}

	observer := &sinkOutputObserver{}
	cfg := hookConfig{opts: []model.PipelineOption{observer}}

	stepFn := func(ctx context.Context, input <-chan int) error {
		for range input {
		}

		return nil
	}

	require.NoError(t, sequentialSinkFromChanFn(context.Background(), 1, input, output, stepFn, cfg))
	require.Equal(t, 0, observer.calls)
}

func TestSequentialSinkFromChanReportsHookErrors(t *testing.T) {
	t.Parallel()

	input := &Step[int]{Details: &StepInfo{Name: "input"}, Output: make(chan int, 1)}
	input.Output <- 1

	close(input.Output)

	output := &Step[int]{Details: &StepInfo{Name: "output"}}

	hookErr := errors.New("hook failed")
	observer := &sinkOutputObserver{err: hookErr}
	cfg := hookConfig{opts: []model.PipelineOption{observer}}

	stepFn := func(ctx context.Context, input <-chan int) error {
		for range input {
		}

		return nil
	}

	err := sequentialSinkFromChanFn(context.Background(), 1, input, output, stepFn, cfg)
	require.ErrorIs(t, err, hookErr)
}

func TestSequentialSinkFromChanMetricsError(t *testing.T) {
	t.Parallel()

	input := &Step[int]{Details: &StepInfo{Name: "input"}, Output: make(chan int, 1)}
	input.Output <- 1

	close(input.Output)

	output := &Step[int]{Details: &StepInfo{Name: "output"}}

	metricErr := errors.New("metric failed")
	cfg := hookConfig{
		outputMetrics: true,
		metricsOpts:   []model.PipelineMetricsOption{sinkMetricsObserver{err: metricErr}},
	}

	stepFn := func(ctx context.Context, input <-chan int) error {
		for range input {
		}

		return nil
	}

	err := sequentialSinkFromChanFn(context.Background(), 1, input, output, stepFn, cfg)
	require.ErrorIs(t, err, metricErr)
}

func TestSequentialSinkFromChanStepError(t *testing.T) {
	t.Parallel()

	input := &Step[int]{Details: &StepInfo{Name: "input"}, Output: make(chan int)}
	close(input.Output)

	output := &Step[int]{Details: &StepInfo{Name: "output"}}

	stepErr := errors.New("sink failed")
	stepFn := func(ctx context.Context, input <-chan int) error {
		for range input {
		}

		return stepErr
	}

	err := sequentialSinkFromChanFn(context.Background(), 1, input, output, stepFn, hookConfig{})
	require.ErrorIs(t, err, stepErr)
}

func TestConcurrentSinkFromChanPropagatesError(t *testing.T) {
	t.Parallel()

	input := &Step[int]{Details: &StepInfo{Name: "input"}, Output: make(chan int)}
	close(input.Output)

	output := &Step[int]{Details: &StepInfo{Name: "output", Concurrent: 2}}

	stepFn := func(ctx context.Context, input <-chan int) error {
		for range input {
		}

		return errors.New("sink failed")
	}

	err := concurrentSinkFromChanFn(context.Background(), input, output, stepFn, hookConfig{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "unable to wait for all go routines")
}

func TestRunSinkFromChanDefaultsConcurrency(t *testing.T) {
	t.Parallel()

	input := &Step[int]{Details: &StepInfo{Name: "input"}, Output: make(chan int, 1)}
	input.Output <- 1

	close(input.Output)

	output := &Step[int]{Details: &StepInfo{Name: "output"}}

	stepFn := func(ctx context.Context, input <-chan int) error {
		for range input {
		}

		return nil
	}

	require.NoError(t, runSinkFromChan(context.Background(), input, output, stepFn, hookConfig{}))
	require.Equal(t, 1, output.Details.Concurrent)
}

func TestRunSinkRejectsDropOutputOptions(t *testing.T) {
	t.Parallel()

	input := &Step[int]{Details: &StepInfo{Name: "input"}}
	output := &Step[int]{Details: &StepInfo{Name: "output"}, DropOnOutputFull: true}

	err := runSink(context.Background(), input, output, func(context.Context, int) error {
		return nil
	}, hookConfig{})
	require.ErrorIs(t, err, ErrDropOutputUnsupported)
}

func TestSinkPrepareError(t *testing.T) {
	t.Parallel()

	expectedErr := errors.New("prepare failed")
	pipe, err := New(sinkPrepareObserver{err: expectedErr})
	require.NoError(t, err)

	input := &Step[int]{Details: &StepInfo{Name: "input"}}
	sink := Sink(pipe, "sink", input, func(context.Context, int) error {
		return nil
	})
	require.Nil(t, sink)
	require.ErrorIs(t, pipe.Err(), expectedErr)
}
