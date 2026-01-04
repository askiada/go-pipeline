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

func TestNewBatchChanStateSetsSendTimer(t *testing.T) {
	t.Parallel()

	input := &Step[int]{Details: &StepInfo{Name: "input"}}
	output := &Step[<-chan int]{Details: &StepInfo{Name: "batch"}}
	output.DropOnOutputTimeout = 5 * time.Millisecond

	state := newBatchChanState(1, input, output, &model.BatchPolicy{MaxSize: 1}, hookConfig{})
	require.NotNil(t, state.sendTimer)
	stopBatchTimer(state.sendTimer)
}

func TestBatchChanStateCloseBatchWithoutChannel(t *testing.T) {
	t.Parallel()

	input := &Step[int]{Details: &StepInfo{Name: "input"}}
	output := &Step[<-chan int]{Details: &StepInfo{Name: "batch"}}
	state := newBatchChanState(1, input, output, &model.BatchPolicy{MaxSize: 1}, hookConfig{})

	state.tracker.count = 2
	state.tracker.waitTotal = 5 * time.Millisecond

	require.NoError(t, state.closeBatch())
	assert.Equal(t, 0, state.tracker.count)
	assert.True(t, state.tracker.batchStart.IsZero())
}

func TestBatchChanStateStartBatchDropOnFull(t *testing.T) {
	t.Parallel()

	input := &Step[int]{Details: &StepInfo{Name: "input"}}
	output := &Step[<-chan int]{
		Details: &StepInfo{Name: "batch"},
		Output:  make(chan (<-chan int)),
	}
	output.DropOnOutputFull = true

	observer := &dropObserver{}
	cfg := hookConfig{
		opts: []model.PipelineOption{observer},
		drop: true,
	}

	state := newBatchChanState(1, input, output, &model.BatchPolicy{MaxSize: 1}, cfg)

	dropped, err := state.startBatch(context.Background())
	require.NoError(t, err)
	assert.True(t, dropped)
	assert.Nil(t, state.batchCh)
	assert.Equal(t, 1, observer.calls)
}

func TestBatchChanStateStartBatchErrorOnContextDone(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	input := &Step[int]{Details: &StepInfo{Name: "input"}}
	output := &Step[<-chan int]{
		Details: &StepInfo{Name: "batch"},
		Output:  make(chan (<-chan int)),
	}

	state := newBatchChanState(1, input, output, &model.BatchPolicy{MaxSize: 1}, hookConfig{})
	dropped, err := state.startBatch(ctx)
	require.Error(t, err)
	assert.False(t, dropped)
	assert.Nil(t, state.batchCh)
}

func TestBatchChanStateHandleEntryReturnsErrorOnStart(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	input := &Step[int]{Details: &StepInfo{Name: "input"}}
	output := &Step[<-chan int]{
		Details: &StepInfo{Name: "batch"},
		Output:  make(chan (<-chan int)),
	}

	state := newBatchChanState(1, input, output, &model.BatchPolicy{MaxSize: 1}, hookConfig{})
	err := state.handleEntry(ctx, 1, 0)
	require.ErrorIs(t, err, context.Canceled)
}

func TestBatchChanStateHandleEntryDropOnStart(t *testing.T) {
	t.Parallel()

	input := &Step[int]{Details: &StepInfo{Name: "input"}}
	output := &Step[<-chan int]{
		Details: &StepInfo{Name: "batch"},
		Output:  make(chan (<-chan int)),
	}
	output.DropOnOutputFull = true

	observer := &dropObserver{}
	cfg := hookConfig{
		opts: []model.PipelineOption{observer},
		drop: true,
	}

	state := newBatchChanState(1, input, output, &model.BatchPolicy{MaxSize: 1}, cfg)
	require.NoError(t, state.handleEntry(context.Background(), 1, 0))
	assert.Equal(t, 1, observer.calls)
}

func TestBatchChanStateHandleEntryContextDoneWhileSending(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	input := &Step[int]{Details: &StepInfo{Name: "input"}}
	output := &Step[<-chan int]{
		Details: &StepInfo{Name: "batch"},
		Output:  make(chan (<-chan int)),
	}

	state := newBatchChanState(1, input, output, &model.BatchPolicy{MaxSize: 1}, hookConfig{})
	state.batchCh = make(chan int)

	err := state.handleEntry(ctx, 1, 0)
	require.ErrorIs(t, err, context.Canceled)
}

func TestBatchChanStateHandleEntryFlushError(t *testing.T) {
	t.Parallel()

	input := &Step[int]{Details: &StepInfo{Name: "input"}}
	output := &Step[<-chan int]{Details: &StepInfo{Name: "batch"}}

	observer := &batchOutputObserver{err: errors.New("flush failed")}
	cfg := hookConfig{opts: []model.PipelineOption{observer}}

	state := newBatchChanState(1, input, output, &model.BatchPolicy{MaxSize: 1, MaxWait: time.Millisecond}, cfg)
	state.batchCh = make(chan int)
	state.tracker.batchStart = time.Now().Add(-2 * time.Millisecond)

	err := state.handleEntry(context.Background(), 1, 0)
	require.ErrorIs(t, err, observer.err)
}

func TestBatchChanStateHandleEntryFlushesOnSizeWithMetrics(t *testing.T) {
	t.Parallel()

	input := &Step[int]{Details: &StepInfo{Name: "input"}}
	output := &Step[<-chan int]{
		Details: &StepInfo{Name: "batch"},
		Output:  make(chan (<-chan int), 1),
	}
	metrics := &batchMetricsObserver{}

	cfg := hookConfig{
		metricsOpts:   []model.PipelineMetricsOption{metrics},
		outputMetrics: true,
	}

	state := newBatchChanState(1, input, output, &model.BatchPolicy{MaxSize: 1}, cfg)

	collected := make(chan []int, 1)

	go func() {
		batchCh := <-output.Output

		var got []int
		for item := range batchCh {
			got = append(got, item)
		}

		collected <- got
	}()

	err := state.handleEntry(context.Background(), 10, 4*time.Millisecond)
	require.NoError(t, err)

	assert.Equal(t, []int{10}, <-collected)
	assert.Equal(t, 1, metrics.calls)
}

func TestSequentialBatchChanFnInvalidPolicy(t *testing.T) {
	t.Parallel()

	input := &Step[int]{Output: make(chan int)}
	output := &Step[<-chan int]{Details: &StepInfo{Concurrent: 1}}

	err := sequentialBatchChanFn(context.Background(), 1, input, output, hookConfig{})
	require.ErrorIs(t, err, ErrBatchPolicyMustBeSet)
}

func TestSequentialBatchChanFnProcessesEntryWithMetrics(t *testing.T) {
	t.Parallel()

	input := &Step[int]{Output: make(chan int, 1)}
	input.Output <- 1

	close(input.Output)

	output := &Step[<-chan int]{
		Details:     &StepInfo{Concurrent: 1},
		Output:      make(chan (<-chan int), 1),
		BatchPolicy: &model.BatchPolicy{MaxSize: 1},
	}

	cfg := hookConfig{outputMetrics: true}
	collected := make(chan []int, 1)

	go func() {
		batchCh := <-output.Output

		var got []int
		for item := range batchCh {
			got = append(got, item)
		}

		collected <- got
	}()

	require.NoError(t, sequentialBatchChanFn(context.Background(), 1, input, output, cfg))
	assert.Equal(t, []int{1}, <-collected)
}

func TestSequentialBatchChanFnHandleEntryError(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()

	input := &Step[int]{Output: make(chan int, 1)}
	input.Output <- 1

	close(input.Output)

	output := &Step[<-chan int]{
		Details:     &StepInfo{Concurrent: 1},
		Output:      make(chan (<-chan int)),
		BatchPolicy: &model.BatchPolicy{MaxSize: 1},
	}

	err := sequentialBatchChanFn(ctx, 1, input, output, hookConfig{})
	require.Error(t, err)
	require.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestSequentialBatchChanFnContextDone(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	input := &Step[int]{Output: make(chan int)}
	output := &Step[<-chan int]{
		Details:     &StepInfo{Concurrent: 1},
		Output:      make(chan (<-chan int), 1),
		BatchPolicy: &model.BatchPolicy{MaxSize: 1},
	}

	err := sequentialBatchChanFn(ctx, 1, input, output, hookConfig{})
	require.ErrorIs(t, err, context.Canceled)
}

func TestRunBatchChanInvalidPolicy(t *testing.T) {
	t.Parallel()

	input := &Step[int]{Output: make(chan int)}
	output := &Step[<-chan int]{Details: &StepInfo{Concurrent: 1}}

	err := runBatchChan(context.Background(), input, output, hookConfig{})
	require.ErrorIs(t, err, ErrBatchPolicyMustBeSet)
}

func TestRunBatchChanDefaultsConcurrency(t *testing.T) {
	t.Parallel()

	input := &Step[int]{Output: make(chan int)}
	close(input.Output)

	output := &Step[<-chan int]{
		Details:     &StepInfo{Concurrent: 0},
		Output:      make(chan (<-chan int), 1),
		BatchPolicy: &model.BatchPolicy{MaxSize: 1},
	}

	require.NoError(t, runBatchChan(context.Background(), input, output, hookConfig{}))
	assert.Equal(t, 1, output.Details.Concurrent)
}

func TestRunBatchChanConcurrent(t *testing.T) {
	t.Parallel()

	input := &Step[int]{Output: make(chan int)}
	close(input.Output)

	output := &Step[<-chan int]{
		Details:     &StepInfo{Concurrent: 2},
		Output:      make(chan (<-chan int), 1),
		BatchPolicy: &model.BatchPolicy{MaxSize: 1},
	}

	require.NoError(t, runBatchChan(context.Background(), input, output, hookConfig{}))
}

func TestRunBatchChanConcurrentError(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	input := &Step[int]{Output: make(chan int)}
	output := &Step[<-chan int]{
		Details:     &StepInfo{Concurrent: 2},
		Output:      make(chan (<-chan int), 1),
		BatchPolicy: &model.BatchPolicy{MaxSize: 1},
	}

	err := runBatchChan(ctx, input, output, hookConfig{})
	require.ErrorIs(t, err, context.Canceled)
}

func TestBatchChanBuildErrors(t *testing.T) {
	t.Parallel()

	assert.Nil(t, BatchChan[int](nil, "batch", nil, BatchPolicy{MaxSize: 1}))

	pipe := &Pipeline{buildErr: errors.New("boom")}
	step := &Step[int]{Output: make(chan int)}
	assert.Nil(t, BatchChan(pipe, "batch", step, BatchPolicy{MaxSize: 1}))

	pipe = &Pipeline{}
	assert.Nil(t, BatchChan[int](pipe, "batch", nil, BatchPolicy{MaxSize: 1}))
	require.Error(t, pipe.Err())
}

func TestBatchChanNegativeMaxWaitClamps(t *testing.T) {
	t.Parallel()

	pipe, err := New(PipelineDefaults{})
	require.NoError(t, err)

	input := &Step[int]{Details: &StepInfo{Name: "input"}, Output: make(chan int)}
	batch := BatchChan(pipe, "batch", input, BatchPolicy{MaxSize: 1, MaxWait: -time.Second})
	require.NotNil(t, batch)
	require.NotNil(t, batch.BatchPolicy)
	assert.Equal(t, time.Duration(0), batch.BatchPolicy.MaxWait)
}

func TestBatchChanPrepareStepError(t *testing.T) {
	t.Parallel()

	observer := &prepareStepObserver{err: errors.New("prepare failed")}
	pipe := &Pipeline{
		opts: []model.PipelineOption{observer},
	}

	input := &Step[int]{Details: &StepInfo{Name: "input"}, Output: make(chan int)}
	batch := BatchChan(pipe, "batch", input, BatchPolicy{MaxSize: 1})
	assert.Nil(t, batch)
	require.Error(t, pipe.Err())
}

func TestBatchChanRunErrorClosesErrorOutput(t *testing.T) {
	t.Parallel()

	pipe, err := New(PipelineDefaults{})
	require.NoError(t, err)

	root := Root(pipe, "root", func(ctx context.Context, out chan<- int) error {
		return errors.New("root failed")
	})
	require.NotNil(t, root)

	batch := BatchChan(pipe, "batch", root, BatchPolicy{MaxSize: 1})
	require.NotNil(t, batch)

	batch.ErrorOutput = make(chan model.StepError)

	runErr := pipe.Run(context.Background())
	require.Error(t, runErr)

	select {
	case _, ok := <-batch.ErrorOutput:
		if ok {
			t.Fatalf("expected batch chan error output to be closed")
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatalf("expected batch chan error output to close")
	}
}
