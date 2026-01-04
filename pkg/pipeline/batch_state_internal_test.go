package pipeline

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/askiada/go-pipeline/v2/pkg/pipeline/model"
)

func TestNewBatchStateSetsSendTimer(t *testing.T) {
	t.Parallel()

	input := &Step[int]{Details: &StepInfo{Name: "input"}}
	output := &Step[[]int]{Details: &StepInfo{Name: "batch"}}
	output.DropOnOutputTimeout = 5 * time.Millisecond

	state := newBatchState(1, input, output, &model.BatchPolicy{MaxSize: 1}, hookConfig{})
	require.NotNil(t, state.sendTimer)
	stopBatchTimer(state.sendTimer)
}

func TestValidateBatchOptionsNilAndRetry(t *testing.T) {
	t.Parallel()

	require.NoError(t, validateBatchOptions[int](nil))

	step := &Step[int]{RetryPolicy: &model.RetryPolicy{}}
	require.ErrorIs(t, validateBatchOptions(step), ErrRetryUnsupported)
}

func TestStopBatchTimerDrains(t *testing.T) {
	t.Parallel()

	timer := time.NewTimer(5 * time.Millisecond)
	time.Sleep(10 * time.Millisecond)

	stopBatchTimer(timer)
}

func TestBatchStateFlushEmptyClearsTimer(t *testing.T) {
	t.Parallel()

	input := &Step[int]{Details: &StepInfo{Name: "input"}}
	output := &Step[[]int]{Details: &StepInfo{Name: "batch"}}
	state := newBatchState(1, input, output, &model.BatchPolicy{MaxSize: 1}, hookConfig{})

	state.tracker.timer = time.NewTimer(time.Hour)
	state.tracker.timerC = state.tracker.timer.C

	require.NoError(t, state.flush(context.Background()))
	assert.Nil(t, state.tracker.timerC)
}

func TestBatchStateHandleEntryFlushesOnSizeWithMetrics(t *testing.T) {
	t.Parallel()

	input := &Step[int]{Details: &StepInfo{Name: "input"}}
	output := &Step[[]int]{
		Details: &StepInfo{Name: "batch"},
		Output:  make(chan []int, 1),
	}
	metrics := &batchMetricsObserver{}

	cfg := hookConfig{
		metricsOpts:   []model.PipelineMetricsOption{metrics},
		outputMetrics: true,
	}

	state := newBatchState(1, input, output, &model.BatchPolicy{MaxSize: 1}, cfg)
	err := state.handleEntry(context.Background(), 10, 4*time.Millisecond)
	require.NoError(t, err)

	got := <-output.Output
	assert.Equal(t, []int{10}, got)
	assert.Equal(t, 1, metrics.calls)
}

func TestBatchStateHandleEntryReturnsErrorOnFlush(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	input := &Step[int]{Details: &StepInfo{Name: "input"}}
	output := &Step[[]int]{
		Details: &StepInfo{Name: "batch"},
		Output:  make(chan []int),
	}

	state := newBatchState(1, input, output, &model.BatchPolicy{MaxSize: 1, MaxWait: time.Millisecond}, hookConfig{})
	state.batch = []int{1}
	state.tracker.count = 1
	state.tracker.batchStart = time.Now().Add(-2 * time.Millisecond)

	err := state.handleEntry(ctx, 2, 0)
	require.ErrorIs(t, err, context.Canceled)
}

func TestBatchStateFlushDropsOnFull(t *testing.T) {
	t.Parallel()

	input := &Step[int]{Details: &StepInfo{Name: "input"}}
	output := &Step[[]int]{
		Details: &StepInfo{Name: "batch"},
		Output:  make(chan []int),
	}
	output.DropOnOutputFull = true

	observer := &dropObserver{}
	cfg := hookConfig{
		opts: []model.PipelineOption{observer},
		drop: true,
	}

	state := newBatchState(1, input, output, &model.BatchPolicy{MaxSize: 1}, cfg)
	state.batch = []int{1}
	state.tracker.count = 1

	require.NoError(t, state.flush(context.Background()))
	assert.Equal(t, 1, observer.calls)
}

func TestBatchStateFlushErrorsOnContextCancel(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	input := &Step[int]{Details: &StepInfo{Name: "input"}}
	output := &Step[[]int]{
		Details: &StepInfo{Name: "batch"},
		Output:  make(chan []int),
	}

	state := newBatchState(1, input, output, &model.BatchPolicy{MaxSize: 1}, hookConfig{})
	state.batch = []int{1}
	state.tracker.count = 1

	err := state.flush(ctx)
	require.Error(t, err)
	require.ErrorIs(t, err, context.Canceled)
}

func TestSequentialBatchFnInvalidPolicy(t *testing.T) {
	t.Parallel()

	input := &Step[int]{Output: make(chan int)}
	output := &Step[[]int]{Details: &StepInfo{Concurrent: 1}}

	err := sequentialBatchFn(context.Background(), 1, input, output, hookConfig{})
	require.ErrorIs(t, err, ErrBatchPolicyMustBeSet)
}

func TestSequentialBatchFnProcessesEntryWithMetrics(t *testing.T) {
	t.Parallel()

	input := &Step[int]{Output: make(chan int, 1)}
	input.Output <- 1

	close(input.Output)

	output := &Step[[]int]{
		Details:     &StepInfo{Concurrent: 1},
		Output:      make(chan []int, 1),
		BatchPolicy: &model.BatchPolicy{MaxSize: 1},
	}

	cfg := hookConfig{outputMetrics: true}
	require.NoError(t, sequentialBatchFn(context.Background(), 1, input, output, cfg))

	assert.Equal(t, []int{1}, <-output.Output)
}

func TestSequentialBatchFnTimerFlushError(t *testing.T) {
	t.Parallel()

	input := &Step[int]{Output: make(chan int, 1)}
	input.Output <- 1

	output := &Step[[]int]{
		Details:     &StepInfo{Concurrent: 1},
		Output:      make(chan []int, 1),
		BatchPolicy: &model.BatchPolicy{MaxSize: 10, MaxWait: 5 * time.Millisecond},
	}

	observer := &batchOutputObserver{err: errors.New("flush failed")}
	cfg := hookConfig{opts: []model.PipelineOption{observer}}

	err := sequentialBatchFn(context.Background(), 1, input, output, cfg)
	require.ErrorIs(t, err, observer.err)
}

func TestRunBatchConcurrent(t *testing.T) {
	t.Parallel()

	input := &Step[int]{Output: make(chan int)}
	close(input.Output)

	output := &Step[[]int]{
		Details:     &StepInfo{Concurrent: 2},
		Output:      make(chan []int, 1),
		BatchPolicy: &model.BatchPolicy{MaxSize: 1},
	}

	require.NoError(t, runBatch(context.Background(), input, output, hookConfig{}))
}

func TestRunBatchInvalidPolicy(t *testing.T) {
	t.Parallel()

	input := &Step[int]{Output: make(chan int)}
	output := &Step[[]int]{Details: &StepInfo{Concurrent: 1}}

	err := runBatch(context.Background(), input, output, hookConfig{})
	require.ErrorIs(t, err, ErrBatchPolicyMustBeSet)
}

func TestRunBatchDefaultsConcurrency(t *testing.T) {
	t.Parallel()

	input := &Step[int]{Output: make(chan int)}
	close(input.Output)

	output := &Step[[]int]{
		Details:     &StepInfo{Concurrent: 0},
		Output:      make(chan []int, 1),
		BatchPolicy: &model.BatchPolicy{MaxSize: 1},
	}

	require.NoError(t, runBatch(context.Background(), input, output, hookConfig{}))
	assert.Equal(t, 1, output.Details.Concurrent)
}

func TestRunBatchConcurrentError(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	input := &Step[int]{Output: make(chan int)}
	output := &Step[[]int]{
		Details:     &StepInfo{Concurrent: 2},
		Output:      make(chan []int, 1),
		BatchPolicy: &model.BatchPolicy{MaxSize: 1},
	}

	err := runBatch(ctx, input, output, hookConfig{})
	require.ErrorIs(t, err, context.Canceled)
}

func TestSequentialBatchFnContextDone(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	input := &Step[int]{Output: make(chan int)}
	output := &Step[[]int]{
		Details:     &StepInfo{Concurrent: 1},
		Output:      make(chan []int, 1),
		BatchPolicy: &model.BatchPolicy{MaxSize: 1},
	}

	err := sequentialBatchFn(ctx, 1, input, output, hookConfig{})
	require.ErrorIs(t, err, context.Canceled)
}

func TestBatchBuildErrors(t *testing.T) {
	t.Parallel()

	assert.Nil(t, Batch[int](nil, "batch", nil, BatchPolicy{MaxSize: 1}))

	pipe := &Pipeline{buildErr: errors.New("boom")}
	step := &Step[int]{Output: make(chan int)}
	assert.Nil(t, Batch(pipe, "batch", step, BatchPolicy{MaxSize: 1}))

	pipe = &Pipeline{}
	assert.Nil(t, Batch[int](pipe, "batch", nil, BatchPolicy{MaxSize: 1}))
	require.Error(t, pipe.Err())
}

func TestBatchNegativeMaxWaitClamps(t *testing.T) {
	t.Parallel()

	pipe, err := New(PipelineDefaults{})
	require.NoError(t, err)

	input := &Step[int]{Details: &StepInfo{Name: "input"}, Output: make(chan int)}
	batch := Batch(pipe, "batch", input, BatchPolicy{MaxSize: 1, MaxWait: -time.Second})
	require.NotNil(t, batch)
	require.NotNil(t, batch.BatchPolicy)
	assert.Equal(t, time.Duration(0), batch.BatchPolicy.MaxWait)
}

func TestBatchPrepareStepError(t *testing.T) {
	t.Parallel()

	observer := &prepareStepObserver{err: errors.New("prepare failed")}
	pipe := &Pipeline{
		opts: []model.PipelineOption{observer},
	}

	input := &Step[int]{Details: &StepInfo{Name: "input"}, Output: make(chan int)}
	batch := Batch(pipe, "batch", input, BatchPolicy{MaxSize: 1})
	assert.Nil(t, batch)
	require.Error(t, pipe.Err())
}

func TestBatchRunErrorClosesErrorOutput(t *testing.T) {
	t.Parallel()

	pipe, err := New(PipelineDefaults{})
	require.NoError(t, err)

	root := Root(pipe, "root", func(ctx context.Context, out chan<- int) error {
		return nil
	}, StepKeepOpen[int]())
	require.NotNil(t, root)

	batch := Batch(pipe, "batch", root, BatchPolicy{MaxSize: 1})
	require.NotNil(t, batch)

	batch.ErrorOutput = make(chan model.StepError)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	runErr := pipe.Run(ctx)
	require.Error(t, runErr)

	select {
	case _, ok := <-batch.ErrorOutput:
		if ok {
			t.Fatalf("expected batch error output to be closed")
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatalf("expected batch error output to close")
	}
}
