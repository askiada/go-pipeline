package pipeline_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/askiada/go-pipeline/pkg/pipeline"
	"github.com/askiada/go-pipeline/pkg/pipeline/drawer"
	"github.com/askiada/go-pipeline/pkg/pipeline/measure"
	"github.com/askiada/go-pipeline/pkg/pipeline/model"
)

func TestOneToOneNilPipe(t *testing.T) {
	t.Parallel()

	outputChan := pipeline.OneToOne(nil, "root step", nil, func(ctx context.Context, input int) (int, error) {
		return input, nil
	})
	assert.Nil(t, outputChan)
}

func TestOneToOneNilInput(t *testing.T) {
	t.Parallel()

	pipe, err := pipeline.New(pipeline.PipelineDefaults{})
	require.NoError(t, err)

	outputChan := pipeline.OneToOne(pipe, "root step", nil, func(ctx context.Context, input int) (int, error) {
		return input, nil
	})
	assert.Nil(t, outputChan)
	require.ErrorIs(t, pipe.Err(), pipeline.ErrInputMustBeSet)
	require.ErrorIs(t, runPipeline(t, pipe), pipeline.ErrInputMustBeSet)
}

func TestRunNilContext(t *testing.T) {
	t.Parallel()

	pipe, err := pipeline.New(pipeline.PipelineDefaults{})
	require.NoError(t, err)
	//nolint:staticcheck // Intentionally validating nil context handling.
	require.ErrorIs(t, pipe.Run(nil), pipeline.ErrContextMustBeSet)
}

func TestRunTwiceReturnsError(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	pipe, err := pipeline.New(pipeline.PipelineDefaults{})
	require.NoError(t, err)

	root := pipeline.Root(pipe, "root", func(ctx context.Context, out chan<- int) error {
		return nil
	})
	require.NotNil(t, root)

	require.NoError(t, pipe.Run(ctx))
	require.ErrorIs(t, pipe.Run(ctx), pipeline.ErrPipelineAlreadyRan)
}

func TestOneToOne(t *testing.T) {
	t.Parallel()

	var got []int

	ctx := t.Context()
	pipe, err := pipeline.New(pipeline.PipelineDefaults{})
	require.NoError(t, err)
	step := pipeline.Step[int]{
		Output: createInputChan(t, 10),
	}
	outputChan := pipeline.OneToOne(pipe, "first step", &step, func(ctx context.Context, input int) (int, error) {
		return input, nil
	})
	require.NotNil(t, outputChan)

	done := make(chan struct{})

	go func() {
		got = processOutputChan(t, outputChan.Output)

		done <- struct{}{}
	}()

	err = runPipeline(t, pipe, ctx)
	require.NoError(t, err)
	<-done
	assert.ElementsMatch(t, []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, got)
}

func TestOneToOneError(t *testing.T) {
	t.Parallel()

	var got []int

	ctx := t.Context()
	pipe, err := pipeline.New(pipeline.PipelineDefaults{})
	require.NoError(t, err)
	step := pipeline.Step[int]{
		Output: createInputChan(t, 10),
	}
	outputChan := pipeline.OneToOne(pipe, "root step", &step, func(ctx context.Context, input int) (int, error) {
		if input == 5 {
			return 0, assert.AnError
		}

		return input, nil
	})

	require.NotNil(t, outputChan)

	done := make(chan struct{})

	go func() {
		got = processOutputChan(t, outputChan.Output)

		done <- struct{}{}
	}()

	err = runPipeline(t, pipe, ctx)
	assert.Error(t, err)
	<-done

	_ = got
}

func TestOneToOneCancel(t *testing.T) {
	t.Parallel()

	var got []int

	ctx, cancel := context.WithCancel(t.Context())
	pipe, err := pipeline.New(pipeline.PipelineDefaults{})
	require.NoError(t, err)
	step := pipeline.Step[int]{
		Output: createInputChanWithCancel(t, 10, 5, cancel),
	}
	outputChan := pipeline.OneToOne(pipe, "root step", &step, func(ctx context.Context, input int) (int, error) {
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		default:
			return input, nil
		}
	})
	require.NotNil(t, outputChan)

	done := make(chan struct{})

	go func() {
		got = processOutputChan(t, outputChan.Output)

		done <- struct{}{}
	}()

	err = runPipeline(t, pipe, ctx)
	assert.Error(t, err)
	<-done

	_ = got
}

func TestOneToOneConcurrency(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	pipe, err := pipeline.New(pipeline.PipelineDefaults{})
	require.NoError(t, err)

	input := &pipeline.Step[int]{
		Details: &model.StepInfo{
			Name:       "input",
			Concurrent: 1,
		},
		Output: make(chan int),
	}

	started := make(chan struct{}, 2)
	release := make(chan struct{})
	var releaseOnce sync.Once

	closeRelease := func() {
		releaseOnce.Do(func() {
			close(release)
		})
	}

	var mu sync.Mutex
	processed := make([]int, 0, 2)

	outputChan := pipeline.OneToOne(pipe, "step", input, func(ctx context.Context, input int) (int, error) {
		mu.Lock()

		processed = append(processed, input)

		mu.Unlock()

		started <- struct{}{}

		<-release

		return input, nil
	}, pipeline.StepConcurrency[int](2))
	require.NotNil(t, outputChan)

	got := make(chan []int, 1)

	go func() {
		got <- processOutputChan(t, outputChan.Output)
	}()

	runErr := make(chan error, 1)

	go func() {
		runErr <- pipe.Run(ctx)
	}()

	expected := []int{1, 2}
	input.Output <- expected[0]

	select {
	case <-started:
	case <-time.After(2 * time.Second):
		closeRelease()
		t.Fatal("expected first one-to-one worker to start")
	}

	input.Output <- expected[1]

	select {
	case <-started:
	case <-time.After(2 * time.Second):
		closeRelease()
		t.Fatal("expected second one-to-one worker to start")
	}

	close(input.Output)
	closeRelease()
	require.NoError(t, <-runErr)

	assert.ElementsMatch(t, expected, <-got)

	mu.Lock()

	gotProcessed := append([]int(nil), processed...)

	mu.Unlock()
	assert.ElementsMatch(t, expected, gotProcessed)
}

func TestOneToOneOrZeroNilPipe(t *testing.T) {
	t.Parallel()

	outputChan := pipeline.OneToOneOrZero(nil, "root step", nil, func(ctx context.Context, input int) (int, error) {
		return input, nil
	})
	assert.Nil(t, outputChan)
}

func TestOneToOneOrZeroNilInput(t *testing.T) {
	t.Parallel()

	pipe, err := pipeline.New(pipeline.PipelineDefaults{})
	require.NoError(t, err)

	outputChan := pipeline.OneToOneOrZero(pipe, "root step", nil, func(ctx context.Context, input int) (int, error) {
		return input, nil
	})
	assert.Nil(t, outputChan)
	require.ErrorIs(t, pipe.Err(), pipeline.ErrInputMustBeSet)
	require.ErrorIs(t, runPipeline(t, pipe), pipeline.ErrInputMustBeSet)
}

func TestOneToOneOrSZero(t *testing.T) {
	t.Parallel()

	var got []int

	ctx := t.Context()
	pipe, err := pipeline.New(pipeline.PipelineDefaults{})
	require.NoError(t, err)
	step := pipeline.Step[int]{
		Output: createInputChan(t, 10),
	}
	outputChan := pipeline.OneToOneOrZero(pipe, "first step", &step, func(ctx context.Context, input int) (int, error) {
		return input, nil
	})
	require.NotNil(t, outputChan)

	done := make(chan struct{})

	go func() {
		got = processOutputChan(t, outputChan.Output)

		done <- struct{}{}
	}()

	err = runPipeline(t, pipe, ctx)
	require.NoError(t, err)
	<-done
	assert.ElementsMatch(t, []int{1, 2, 3, 4, 5, 6, 7, 8, 9}, got)
}

func TestOneToOneOrZeroError(t *testing.T) {
	t.Parallel()

	var got []int

	ctx := t.Context()
	pipe, err := pipeline.New(pipeline.PipelineDefaults{})
	require.NoError(t, err)
	step := pipeline.Step[int]{
		Output: createInputChan(t, 10),
	}
	outputChan := pipeline.OneToOneOrZero(pipe, "root step", &step, func(ctx context.Context, input int) (int, error) {
		if input == 5 {
			return 0, assert.AnError
		}

		return input, nil
	})

	require.NotNil(t, outputChan)

	done := make(chan struct{})

	go func() {
		got = processOutputChan(t, outputChan.Output)

		done <- struct{}{}
	}()

	err = runPipeline(t, pipe, ctx)
	assert.Error(t, err)
	<-done

	_ = got
}

func TestOneToOneOrZeroCancel(t *testing.T) {
	t.Parallel()

	var got []int

	ctx, cancel := context.WithCancel(t.Context())
	pipe, err := pipeline.New(pipeline.PipelineDefaults{})
	require.NoError(t, err)
	step := pipeline.Step[int]{
		Output: createInputChanWithCancel(t, 10, 5, cancel),
	}
	outputChan := pipeline.OneToOneOrZero(pipe, "root step", &step, func(ctx context.Context, input int) (int, error) {
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		default:
			return input, nil
		}
	})
	require.NotNil(t, outputChan)

	done := make(chan struct{})

	go func() {
		got = processOutputChan(t, outputChan.Output)

		done <- struct{}{}
	}()

	err = runPipeline(t, pipe, ctx)
	assert.Error(t, err)
	<-done

	_ = got
}

func TestOneToManyNilPipe(t *testing.T) {
	t.Parallel()

	outputChan := pipeline.OneToMany(nil, "root step", nil, func(ctx context.Context, input int) ([]int, error) {
		return []int{input}, nil
	})
	assert.Nil(t, outputChan)
}

func TestOneToManyNilInput(t *testing.T) {
	t.Parallel()

	pipe, err := pipeline.New(pipeline.PipelineDefaults{})
	require.NoError(t, err)

	outputChan := pipeline.OneToMany(pipe, "root step", nil, func(ctx context.Context, input int) ([]int, error) {
		return []int{input}, nil
	})
	assert.Nil(t, outputChan)
	require.ErrorIs(t, pipe.Err(), pipeline.ErrInputMustBeSet)
	require.ErrorIs(t, runPipeline(t, pipe), pipeline.ErrInputMustBeSet)
}

func TestOneToMany(t *testing.T) {
	t.Parallel()

	var got []int

	ctx := t.Context()
	pipe, err := pipeline.New(pipeline.PipelineDefaults{})
	require.NoError(t, err)
	step := pipeline.Step[int]{
		Output: createInputChan(t, 10),
	}
	outputChan := pipeline.OneToMany(pipe, "first step", &step, func(ctx context.Context, input int) ([]int, error) {
		return []int{input}, nil
	})
	require.NotNil(t, outputChan)

	done := make(chan struct{})

	go func() {
		got = processOutputChan(t, outputChan.Output)

		done <- struct{}{}
	}()

	err = runPipeline(t, pipe, ctx)
	require.NoError(t, err)
	<-done
	assert.ElementsMatch(t, []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, got)
}

func TestOneToManyError(t *testing.T) {
	t.Parallel()

	var got []int

	ctx := t.Context()
	pipe, err := pipeline.New(pipeline.PipelineDefaults{})
	require.NoError(t, err)
	step := pipeline.Step[int]{
		Output: createInputChan(t, 10),
	}
	outputChan := pipeline.OneToMany(pipe, "root step", &step, func(ctx context.Context, input int) ([]int, error) {
		if input == 5 {
			return []int{0}, assert.AnError
		}

		return []int{input}, nil
	})

	require.NotNil(t, outputChan)

	done := make(chan struct{})

	go func() {
		got = processOutputChan(t, outputChan.Output)

		done <- struct{}{}
	}()

	err = runPipeline(t, pipe, ctx)
	require.Error(t, err)
	<-done

	_ = got
}

func TestOneToManyCancel(t *testing.T) {
	t.Parallel()

	var got []int

	ctx, cancel := context.WithCancel(t.Context())
	pipe, err := pipeline.New(pipeline.PipelineDefaults{})
	require.NoError(t, err)
	step := pipeline.Step[int]{
		Output: createInputChanWithCancel(t, 10, 5, cancel),
	}
	outputChan := pipeline.OneToMany(pipe, "root step", &step, func(ctx context.Context, input int) ([]int, error) {
		select {
		case <-ctx.Done():
			return []int{0}, ctx.Err()
		default:
			return []int{input}, nil
		}
	})
	require.NotNil(t, outputChan)

	done := make(chan struct{})

	go func() {
		got = processOutputChan(t, outputChan.Output)

		done <- struct{}{}
	}()

	err = runPipeline(t, pipe, ctx)
	assert.Error(t, err)
	<-done

	_ = got
}

func TestOneToManyConcurrency(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	pipe, err := pipeline.New(pipeline.PipelineDefaults{})
	require.NoError(t, err)

	input := &pipeline.Step[int]{
		Details: &model.StepInfo{
			Name:       "input",
			Concurrent: 1,
		},
		Output: make(chan int),
	}

	started := make(chan struct{}, 2)
	release := make(chan struct{})
	var releaseOnce sync.Once

	closeRelease := func() {
		releaseOnce.Do(func() {
			close(release)
		})
	}

	var mu sync.Mutex
	processed := make([]int, 0, 2)

	outputChan := pipeline.OneToMany(pipe, "step", input, func(ctx context.Context, input int) ([]int, error) {
		mu.Lock()

		processed = append(processed, input)

		mu.Unlock()

		started <- struct{}{}

		<-release

		return []int{input}, nil
	}, pipeline.StepConcurrency[int](2))
	require.NotNil(t, outputChan)

	got := make(chan []int, 1)

	go func() {
		got <- processOutputChan(t, outputChan.Output)
	}()

	runErr := make(chan error, 1)

	go func() {
		runErr <- pipe.Run(ctx)
	}()

	expected := []int{1, 2}
	input.Output <- expected[0]

	select {
	case <-started:
	case <-time.After(2 * time.Second):
		closeRelease()
		t.Fatal("expected first one-to-many worker to start")
	}

	input.Output <- expected[1]

	select {
	case <-started:
	case <-time.After(2 * time.Second):
		closeRelease()
		t.Fatal("expected second one-to-many worker to start")
	}

	close(input.Output)
	closeRelease()
	require.NoError(t, <-runErr)

	assert.ElementsMatch(t, expected, <-got)

	mu.Lock()

	gotProcessed := append([]int(nil), processed...)

	mu.Unlock()
	assert.ElementsMatch(t, expected, gotProcessed)
}

func TestFromChanConcurrency(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	pipe, err := pipeline.New(pipeline.PipelineDefaults{})
	require.NoError(t, err)

	input := &pipeline.Step[int]{
		Details: &model.StepInfo{
			Name:       "input",
			Concurrent: 1,
		},
		Output: make(chan int),
	}

	workerReady := make(chan struct{}, 2)
	startRead := make(chan struct{})

	outputChan := pipeline.FromChan(pipe, "step", input, func(ctx context.Context, input <-chan int, output chan int) error {
		workerReady <- struct{}{}

		<-startRead

		for entry := range input {
			output <- entry
		}

		return nil
	}, pipeline.StepConcurrency[int](2))
	require.NotNil(t, outputChan)

	got := make(chan []int, 1)

	go func() {
		got <- processOutputChan(t, outputChan.Output)
	}()

	runErr := make(chan error, 1)

	go func() {
		runErr <- pipe.Run(ctx)
	}()

	for range 2 {
		select {
		case <-workerReady:
		case <-time.After(2 * time.Second):
			t.Fatal("expected from-chan workers to start")
		}
	}

	close(startRead)

	expected := []int{1, 2, 3, 4}

	go func() {
		for _, item := range expected {
			input.Output <- item
		}

		close(input.Output)
	}()

	require.NoError(t, <-runErr)
	assert.ElementsMatch(t, expected, <-got)
}

func TestStepRetryOneToOne(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	msr := measure.NewDefaultMeasure()
	pipe, err := pipeline.New(measure.PipelineMeasure(msr))
	require.NoError(t, err)

	root := pipeline.Root(pipe, "root", func(ctx context.Context, out chan<- int) error {
		out <- 1

		return nil
	})
	require.NotNil(t, root)

	attempts := 0
	step := pipeline.OneToOne(pipe, "step", root, func(ctx context.Context, input int) (int, error) {
		attempts++
		if attempts < 3 {
			time.Sleep(2 * time.Millisecond)

			return 0, assert.AnError
		}

		time.Sleep(6 * time.Millisecond)

		return input + 1, nil
	}, pipeline.StepRetry[int](pipeline.RetryPolicy{MaxAttempts: 3}))
	require.NotNil(t, step)

	results := make(chan int, 1)
	sink := pipeline.Sink(pipe, "sink", step, func(ctx context.Context, input int) error {
		results <- input

		return nil
	})
	require.NotNil(t, sink)

	require.NoError(t, pipe.Run(ctx))
	require.Equal(t, 2, <-results)

	metric := msr.GetMetric(step.Details.Name)
	retryMetric, ok := metric.(measure.RetryMetric)
	require.True(t, ok)
	require.Equal(t, int64(2), retryMetric.RetryCount())
	require.GreaterOrEqual(t, retryMetric.AVGRetryDuration(), 2*time.Millisecond)
	require.GreaterOrEqual(t, metric.AVGDuration(), 6*time.Millisecond)
}

func TestSinkRetry(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	pipe, err := pipeline.New(pipeline.PipelineDefaults{})
	require.NoError(t, err)

	root := pipeline.Root(pipe, "root", func(ctx context.Context, out chan<- int) error {
		out <- 1

		return nil
	})
	require.NotNil(t, root)

	var mu sync.Mutex
	attempts := 0
	received := make([]int, 0, 3)

	sink := pipeline.Sink(pipe, "sink", root, func(ctx context.Context, input int) error {
		mu.Lock()

		attempts++
		attempt := attempts

		received = append(received, input)

		mu.Unlock()

		if attempt < 3 {
			return assert.AnError
		}

		return nil
	}, pipeline.StepRetry[int](pipeline.RetryPolicy{MaxAttempts: 3}))
	require.NotNil(t, sink)

	require.NoError(t, pipe.Run(ctx))

	mu.Lock()

	gotAttempts := attempts

	gotReceived := append([]int(nil), received...)

	mu.Unlock()

	require.Equal(t, 3, gotAttempts)
	assert.Equal(t, []int{1, 1, 1}, gotReceived)
}

func TestBatchStepFlushesOnSize(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	pipe, err := pipeline.New(pipeline.PipelineDefaults{})
	require.NoError(t, err)

	root := pipeline.Root(pipe, "root", func(ctx context.Context, out chan<- int) error {
		for i := range 5 {
			out <- i
		}

		return nil
	})
	require.NotNil(t, root)

	batch := pipeline.Batch(pipe, "batch", root, pipeline.BatchPolicy{MaxSize: 2})
	require.NotNil(t, batch)

	var mu sync.Mutex
	batches := make([][]int, 0, 3)

	sink := pipeline.Sink(pipe, "sink", batch, func(ctx context.Context, input []int) error {
		mu.Lock()

		batches = append(batches, input)

		mu.Unlock()

		return nil
	})
	require.NotNil(t, sink)

	require.NoError(t, pipe.Run(ctx))

	mu.Lock()

	got := append([][]int(nil), batches...)

	mu.Unlock()

	require.Len(t, got, 3)
	assert.Equal(t, []int{2, 2, 1}, []int{len(got[0]), len(got[1]), len(got[2])})

	var flat []int
	for _, batch := range got {
		flat = append(flat, batch...)
	}

	assert.ElementsMatch(t, []int{0, 1, 2, 3, 4}, flat)
}

func TestBatchStepFlushesOnWindow(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	pipe, err := pipeline.New(pipeline.PipelineDefaults{})
	require.NoError(t, err)

	root := pipeline.Root(pipe, "root", func(ctx context.Context, out chan<- int) error {
		out <- 1

		time.Sleep(30 * time.Millisecond)

		out <- 2

		return nil
	})
	require.NotNil(t, root)

	batch := pipeline.Batch(pipe, "batch", root, pipeline.BatchPolicy{
		MaxSize: 10,
		MaxWait: 10 * time.Millisecond,
	})
	require.NotNil(t, batch)

	var mu sync.Mutex
	batches := make([][]int, 0, 2)

	sink := pipeline.Sink(pipe, "sink", batch, func(ctx context.Context, input []int) error {
		mu.Lock()

		batches = append(batches, input)

		mu.Unlock()

		return nil
	})
	require.NotNil(t, sink)

	require.NoError(t, pipe.Run(ctx))

	mu.Lock()

	got := append([][]int(nil), batches...)

	mu.Unlock()

	require.Len(t, got, 2)
	assert.Len(t, got[0], 1)
	assert.Len(t, got[1], 1)

	var flat []int
	for _, batch := range got {
		flat = append(flat, batch...)
	}

	assert.ElementsMatch(t, []int{1, 2}, flat)
}

func TestBatchStepRequiresPolicy(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	pipe, err := pipeline.New(pipeline.PipelineDefaults{})
	require.NoError(t, err)

	root := pipeline.Root(pipe, "root", func(ctx context.Context, out chan<- int) error {
		out <- 1

		return nil
	})
	require.NotNil(t, root)

	batch := pipeline.Batch(pipe, "batch", root, pipeline.BatchPolicy{})
	require.Nil(t, batch)
	require.ErrorIs(t, pipe.Err(), pipeline.ErrBatchPolicyMustBeSet)
	require.ErrorIs(t, pipe.Run(ctx), pipeline.ErrBatchPolicyMustBeSet)
}

func TestBatchChanStepFlushesOnSize(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	pipe, err := pipeline.New(pipeline.PipelineDefaults{})
	require.NoError(t, err)

	root := pipeline.Root(pipe, "root", func(ctx context.Context, out chan<- int) error {
		for i := range 5 {
			out <- i
		}

		return nil
	})
	require.NotNil(t, root)

	batch := pipeline.BatchChan(pipe, "batch", root, pipeline.BatchPolicy{MaxSize: 2})
	require.NotNil(t, batch)

	var mu sync.Mutex
	batches := make([][]int, 0, 3)

	sink := pipeline.Sink(pipe, "sink", batch, func(ctx context.Context, input <-chan int) error {
		var items []int
		for item := range input {
			items = append(items, item)
		}

		mu.Lock()

		batches = append(batches, items)

		mu.Unlock()

		return nil
	})
	require.NotNil(t, sink)

	require.NoError(t, pipe.Run(ctx))

	mu.Lock()

	got := append([][]int(nil), batches...)

	mu.Unlock()

	require.Len(t, got, 3)
	assert.Equal(t, []int{2, 2, 1}, []int{len(got[0]), len(got[1]), len(got[2])})

	var flat []int
	for _, batch := range got {
		flat = append(flat, batch...)
	}

	assert.ElementsMatch(t, []int{0, 1, 2, 3, 4}, flat)
}

func TestBatchChanStepFlushesOnWindow(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	pipe, err := pipeline.New(pipeline.PipelineDefaults{})
	require.NoError(t, err)

	root := pipeline.Root(pipe, "root", func(ctx context.Context, out chan<- int) error {
		out <- 1

		time.Sleep(30 * time.Millisecond)

		out <- 2

		return nil
	})
	require.NotNil(t, root)

	batch := pipeline.BatchChan(pipe, "batch", root, pipeline.BatchPolicy{
		MaxSize: 10,
		MaxWait: 10 * time.Millisecond,
	})
	require.NotNil(t, batch)

	var mu sync.Mutex
	batches := make([][]int, 0, 2)

	sink := pipeline.Sink(pipe, "sink", batch, func(ctx context.Context, input <-chan int) error {
		var items []int
		for item := range input {
			items = append(items, item)
		}

		mu.Lock()

		batches = append(batches, items)

		mu.Unlock()

		return nil
	})
	require.NotNil(t, sink)

	require.NoError(t, pipe.Run(ctx))

	mu.Lock()

	got := append([][]int(nil), batches...)

	mu.Unlock()

	require.Len(t, got, 2)
	assert.Len(t, got[0], 1)
	assert.Len(t, got[1], 1)

	var flat []int
	for _, batch := range got {
		flat = append(flat, batch...)
	}

	assert.ElementsMatch(t, []int{1, 2}, flat)
}

func TestBatchChanStepRequiresPolicy(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	pipe, err := pipeline.New(pipeline.PipelineDefaults{})
	require.NoError(t, err)

	root := pipeline.Root(pipe, "root", func(ctx context.Context, out chan<- int) error {
		out <- 1

		return nil
	})
	require.NotNil(t, root)

	batch := pipeline.BatchChan(pipe, "batch", root, pipeline.BatchPolicy{})
	require.Nil(t, batch)
	require.ErrorIs(t, pipe.Err(), pipeline.ErrBatchPolicyMustBeSet)
	require.ErrorIs(t, pipe.Run(ctx), pipeline.ErrBatchPolicyMustBeSet)
}

func TestStepTimeoutOneToOne(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	pipe, err := pipeline.New(pipeline.PipelineDefaults{})
	require.NoError(t, err)

	root := pipeline.Root(pipe, "root", func(ctx context.Context, out chan<- int) error {
		out <- 1

		return nil
	})
	require.NotNil(t, root)

	step := pipeline.OneToOne(pipe, "timeout", root, func(ctx context.Context, input int) (int, error) {
		<-ctx.Done()

		return 0, ctx.Err()
	}, pipeline.StepTimeout[int](10*time.Millisecond))
	require.NotNil(t, step)

	sink := pipeline.Sink(pipe, "sink", step, func(ctx context.Context, input int) error {
		return nil
	})
	require.NotNil(t, sink)

	err = pipe.Run(ctx)
	require.Error(t, err)
	require.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestStepRateLimit(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	pipe, err := pipeline.New(pipeline.PipelineDefaults{})
	require.NoError(t, err)

	root := pipeline.Root(pipe, "root", func(ctx context.Context, out chan<- int) error {
		for i := range 3 {
			out <- i
		}

		return nil
	})
	require.NotNil(t, root)

	step := pipeline.OneToOne(pipe, "throttle", root, func(ctx context.Context, input int) (int, error) {
		return input, nil
	}, pipeline.StepRateLimit[int](pipeline.RateLimitPolicy{Every: 20 * time.Millisecond, Burst: 1}))
	require.NotNil(t, step)

	sink := pipeline.Sink(pipe, "sink", step, func(ctx context.Context, input int) error {
		return nil
	})
	require.NotNil(t, sink)

	start := time.Now()

	require.NoError(t, pipe.Run(ctx))
	require.GreaterOrEqual(t, time.Since(start), 35*time.Millisecond)
}

func TestStepMaxInFlightCapsConcurrency(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	pipe, err := pipeline.New(pipeline.PipelineDefaults{})
	require.NoError(t, err)

	root := pipeline.Root(pipe, "root", func(ctx context.Context, out chan<- int) error {
		out <- 1

		out <- 2

		return nil
	})
	require.NotNil(t, root)

	var active int32
	overlap := make(chan struct{}, 1)

	step := pipeline.OneToOne(pipe, "limited", root, func(ctx context.Context, input int) (int, error) {
		if atomic.AddInt32(&active, 1) > 1 {
			select {
			case overlap <- struct{}{}:
			default:
			}
		}

		time.Sleep(10 * time.Millisecond)

		atomic.AddInt32(&active, -1)

		return input, nil
	}, pipeline.StepConcurrency[int](2), pipeline.StepMaxInFlight[int](1))
	require.NotNil(t, step)

	var mu sync.Mutex
	var got []int

	sink := pipeline.Sink(pipe, "sink", step, func(ctx context.Context, input int) error {
		mu.Lock()

		got = append(got, input)

		mu.Unlock()

		return nil
	})
	require.NotNil(t, sink)

	require.NoError(t, pipe.Run(ctx))

	select {
	case <-overlap:
		t.Fatal("expected max in-flight to prevent concurrent processing")
	default:
	}

	mu.Lock()
	defer mu.Unlock()

	assert.ElementsMatch(t, []int{1, 2}, got)
}

func TestPipelineDefaultsApplyToSteps(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	defaults := pipeline.PipelineDefaults{
		StepConcurrency: 3,
		StepBufferSize:  4,
		StepKeepOpen:    true,
	}
	pipe, err := pipeline.New(defaults)
	require.NoError(t, err)

	root := pipeline.Root(pipe, "root", func(ctx context.Context, out chan<- int) error {
		close(out)

		return nil
	})
	require.NotNil(t, root)
	assert.Equal(t, 3, root.Details.Concurrent)
	assert.Equal(t, 4, root.Details.BufferSize)
	assert.True(t, root.KeepOpen)

	step := pipeline.OneToOne(pipe, "step", root, func(ctx context.Context, input int) (int, error) {
		return input, nil
	}, pipeline.StepConcurrency[int](1), pipeline.StepBufferSize[int](0))
	require.NotNil(t, step)
	assert.Equal(t, 1, step.Details.Concurrent)
	assert.Equal(t, 0, step.Details.BufferSize)
	assert.True(t, step.KeepOpen)

	require.NoError(t, runPipeline(t, pipe, ctx))
}

func TestPipelineDefaultsOverrideStepOptions(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	defaults := pipeline.PipelineDefaults{
		StepConcurrency: 2,
		StepBufferSize:  4,
		StepKeepOpen:    false,
	}
	pipe, err := pipeline.New(defaults)
	require.NoError(t, err)

	root := pipeline.Root(pipe, "root", func(ctx context.Context, out chan<- int) error {
		close(out)

		return nil
	}, pipeline.StepConcurrency[int](5), pipeline.StepBufferSize[int](1), pipeline.StepKeepOpen[int]())
	require.NotNil(t, root)
	assert.Equal(t, 5, root.Details.Concurrent)
	assert.Equal(t, 1, root.Details.BufferSize)
	assert.True(t, root.KeepOpen)

	require.NoError(t, runPipeline(t, pipe, ctx))
}

func TestSplitNilPipe(t *testing.T) {
	t.Parallel()

	splitter := pipeline.Split(nil, "root step", (*pipeline.Step[int])(nil), 5)
	require.Nil(t, splitter)
}

func TestSplitNilInput(t *testing.T) {
	t.Parallel()

	pipe, err := pipeline.New(pipeline.PipelineDefaults{})
	require.NoError(t, err)

	splitter := pipeline.Split(pipe, "root step", (*pipeline.Step[int])(nil), 5)
	require.Nil(t, splitter)
	require.ErrorIs(t, pipe.Err(), pipeline.ErrInputMustBeSet)
	require.ErrorIs(t, runPipeline(t, pipe), pipeline.ErrInputMustBeSet)
}

func TestSplitZero(t *testing.T) {
	t.Parallel()

	pipe, err := pipeline.New(pipeline.PipelineDefaults{})
	require.NoError(t, err)

	step := pipeline.Step[int]{
		Output: make(chan int),
	}
	splitter := pipeline.Split(pipe, "root step", &step, 0)
	assert.Nil(t, splitter)
	require.ErrorIs(t, pipe.Err(), pipeline.ErrSplitterTotal)
	require.ErrorIs(t, runPipeline(t, pipe), pipeline.ErrSplitterTotal)
}

func TestSplit(t *testing.T) {
	t.Parallel()

	tcs := map[string]struct {
		buffersize int
	}{
		"sequential":     {buffersize: 1},
		"sequential v2":  {buffersize: 0},
		"concurrent 2":   {buffersize: 2},
		"concurrent 100": {buffersize: 100},
	}

	for name, tc := range tcs {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			var got1, got2 []int

			ctx := t.Context()
			pipe, err := pipeline.New(pipeline.PipelineDefaults{})
			require.NoError(t, err)
			step := pipeline.Step[int]{
				Output: createInputChan(t, 10),
			}
			splitter := pipeline.Split(pipe, "root step", &step, 2, pipeline.SplitterBufferSize[int](tc.buffersize))
			require.NotNil(t, splitter)

			expected := []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
			wg := sync.WaitGroup{}

			if assert.Equal(t, 2, splitter.Total) {
				wg.Add(2)

				go func() {
					defer wg.Done()

					output, ok := splitter.Get()
					assert.True(t, ok)
					got1 = processOutputChan(t, output.Output)
				}()

				go func() {
					defer wg.Done()

					output, ok := splitter.Get()
					assert.True(t, ok)
					got2 = processOutputChan(t, output.Output)
				}()
			}

			err = runPipeline(t, pipe, ctx)
			require.NoError(t, err)
			wg.Wait()
			assert.ElementsMatch(t, expected, got1)
			assert.ElementsMatch(t, expected, got2)
		})
	}
}

func TestSplitCancel(t *testing.T) {
	t.Parallel()

	var got1, got2 []int

	ctx, cancel := context.WithCancel(t.Context())
	pipe, err := pipeline.New(pipeline.PipelineDefaults{})
	require.NoError(t, err)
	step := pipeline.Step[int]{
		Output: createInputChanWithCancel(t, 10, 5, cancel),
	}
	splitter := pipeline.Split(pipe, "root step", &step, 2)
	require.NotNil(t, splitter)

	wg := sync.WaitGroup{}

	if assert.Equal(t, 2, splitter.Total) {
		wg.Add(2)

		go func() {
			defer wg.Done()

			output, ok := splitter.Get()
			assert.True(t, ok)
			got1 = processOutputChan(t, output.Output)
		}()

		go func() {
			defer wg.Done()

			output, ok := splitter.Get()
			assert.True(t, ok)
			got2 = processOutputChan(t, output.Output)
		}()
	}

	err = runPipeline(t, pipe, ctx)
	require.Error(t, err)
	wg.Wait()
	// Otherwise the compiler ignores the output channel and checks the ctx.
	_ = got1
	_ = got2
}

func TestSinkNilPipe(t *testing.T) {
	t.Parallel()

	sinkStep := pipeline.Sink(nil, "root step", nil, func(ctx context.Context, input int) error {
		_ = input

		return nil
	})
	assert.Nil(t, sinkStep)
}

func TestSinkNilInput(t *testing.T) {
	t.Parallel()

	pipe, err := pipeline.New(pipeline.PipelineDefaults{})
	require.NoError(t, err)

	sinkStep := pipeline.Sink(pipe, "root step", nil, func(ctx context.Context, input int) error {
		_ = input

		return nil
	})
	require.Nil(t, sinkStep)
	require.ErrorIs(t, pipe.Err(), pipeline.ErrInputMustBeSet)
	require.ErrorIs(t, runPipeline(t, pipe), pipeline.ErrInputMustBeSet)
}

func TestSink(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	got := []int{}
	pipe, err := pipeline.New(pipeline.PipelineDefaults{})
	require.NoError(t, err)
	step := pipeline.Step[int]{
		Output: createInputChan(t, 10),
	}
	sinkStep := pipeline.Sink(pipe, "root step", &step, func(ctx context.Context, input int) error {
		got = append(got, input)

		return nil
	})
	require.NotNil(t, sinkStep)
	err = runPipeline(t, pipe, ctx)
	require.NoError(t, err)
	assert.ElementsMatch(t, []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, got)
}

func TestSinkError(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	got := []int{}
	pipe, err := pipeline.New(pipeline.PipelineDefaults{})
	require.NoError(t, err)
	step := pipeline.Step[int]{
		Output: createInputChan(t, 10),
	}
	sinkStep := pipeline.Sink(pipe, "root step", &step, func(ctx context.Context, input int) error {
		if input == 5 {
			return assert.AnError
		}

		got = append(got, input)

		return nil
	})
	require.NotNil(t, sinkStep)
	err = runPipeline(t, pipe, ctx)
	assert.Error(t, err)
}

func TestSinkConcurrency(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	pipe, err := pipeline.New(pipeline.PipelineDefaults{})
	require.NoError(t, err)

	input := &pipeline.Step[int]{
		Details: &model.StepInfo{
			Name:       "input",
			Concurrent: 1,
		},
		Output: make(chan int),
	}

	started := make(chan int, 2)
	release := make(chan struct{})
	var releaseOnce sync.Once

	closeRelease := func() {
		releaseOnce.Do(func() {
			close(release)
		})
	}

	var mu sync.Mutex
	processed := make([]int, 0, 2)

	sinkStep := pipeline.Sink(pipe, "sink", input, func(ctx context.Context, input int) error {
		mu.Lock()

		processed = append(processed, input)

		mu.Unlock()

		started <- input

		<-release

		return nil
	}, pipeline.StepConcurrency[int](2))
	require.NotNil(t, sinkStep)

	done := make(chan error, 1)

	go func() {
		done <- pipe.Run(ctx)
	}()

	expected := []int{1, 2}
	input.Output <- expected[0]

	select {
	case <-started:
	case <-time.After(2 * time.Second):
		closeRelease()
		t.Fatal("expected first sink worker to start")
	}

	input.Output <- expected[1]

	select {
	case <-started:
	case <-time.After(2 * time.Second):
		closeRelease()
		t.Fatal("expected second sink worker to start")
	}

	close(input.Output)
	closeRelease()
	require.NoError(t, <-done)

	mu.Lock()

	got := append([]int(nil), processed...)

	mu.Unlock()
	assert.ElementsMatch(t, expected, got)
}

func TestSinkFromChanConcurrency(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	pipe, err := pipeline.New(pipeline.PipelineDefaults{})
	require.NoError(t, err)

	input := &pipeline.Step[int]{
		Details: &model.StepInfo{
			Name:       "input",
			Concurrent: 1,
		},
		Output: make(chan int),
	}

	workerReady := make(chan int, 2)
	startRead := make(chan struct{})
	var mu sync.Mutex
	nextWorkerID := 0
	workerItems := map[int][]int{}

	sinkStep := pipeline.SinkFromChan(pipe, "sink", input, func(ctx context.Context, input <-chan int) error {
		mu.Lock()

		nextWorkerID++
		workerID := nextWorkerID

		mu.Unlock()

		workerReady <- workerID

		<-startRead

		for entry := range input {
			mu.Lock()

			workerItems[workerID] = append(workerItems[workerID], entry)

			mu.Unlock()
		}

		return nil
	}, pipeline.StepConcurrency[int](2))
	require.NotNil(t, sinkStep)

	done := make(chan error, 1)

	go func() {
		done <- pipe.Run(ctx)
	}()

	for range 2 {
		select {
		case <-workerReady:
		case <-time.After(2 * time.Second):
			t.Fatal("expected sink-from-chan workers to start")
		}
	}

	expected := []int{1, 2}
	for _, item := range expected {
		input.Output <- item
	}

	close(input.Output)
	close(startRead)
	require.NoError(t, <-done)

	mu.Lock()

	gotItems := make([]int, 0, len(expected))

	gotWorkers := len(workerItems)
	for _, items := range workerItems {
		gotItems = append(gotItems, items...)
	}

	mu.Unlock()

	assert.ElementsMatch(t, expected, gotItems)
	assert.GreaterOrEqual(t, gotWorkers, 2, "expected items from different workers")
}

func TestStepRetryUnsupportedForFromChan(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	pipe, err := pipeline.New(pipeline.PipelineDefaults{})
	require.NoError(t, err)

	root := pipeline.Root(pipe, "root", func(ctx context.Context, out chan<- int) error {
		out <- 1

		return nil
	})
	require.NotNil(t, root)

	step := pipeline.FromChan(pipe, "step", root, func(ctx context.Context, input <-chan int, output chan int) error {
		return nil
	}, pipeline.StepRetry[int](pipeline.RetryPolicy{MaxAttempts: 2}))
	require.Nil(t, step)
	require.ErrorIs(t, pipe.Err(), pipeline.ErrRetryUnsupported)
	require.ErrorIs(t, pipe.Run(ctx), pipeline.ErrRetryUnsupported)
}

func TestStepRetryUnsupportedForSinkFromChan(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	pipe, err := pipeline.New(pipeline.PipelineDefaults{})
	require.NoError(t, err)

	root := pipeline.Root(pipe, "root", func(ctx context.Context, out chan<- int) error {
		out <- 1

		return nil
	})
	require.NotNil(t, root)

	sink := pipeline.SinkFromChan(pipe, "sink", root, func(ctx context.Context, input <-chan int) error {
		return nil
	}, pipeline.StepRetry[int](pipeline.RetryPolicy{MaxAttempts: 2}))
	require.Nil(t, sink)
	require.ErrorIs(t, pipe.Err(), pipeline.ErrRetryUnsupported)
	require.ErrorIs(t, pipe.Run(ctx), pipeline.ErrRetryUnsupported)
}

func TestStepOptionsUnsupportedForFromChan(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		opt  pipeline.StepOption[int]
		err  error
	}{
		{
			name: "timeout",
			opt:  pipeline.StepTimeout[int](time.Millisecond),
			err:  pipeline.ErrTimeoutUnsupported,
		},
		{
			name: "rate limit",
			opt: pipeline.StepRateLimit[int](pipeline.RateLimitPolicy{
				Every: time.Millisecond,
				Burst: 1,
			}),
			err: pipeline.ErrRateLimitUnsupported,
		},
		{
			name: "max in-flight",
			opt:  pipeline.StepMaxInFlight[int](1),
			err:  pipeline.ErrMaxInFlightUnsupported,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			pipe, err := pipeline.New(pipeline.PipelineDefaults{})
			require.NoError(t, err)

			root := pipeline.Root(pipe, "root", func(ctx context.Context, out chan<- int) error {
				out <- 1

				return nil
			})
			require.NotNil(t, root)

			step := pipeline.FromChan(pipe, "step", root, func(ctx context.Context, input <-chan int, output chan int) error {
				return nil
			}, tc.opt)
			require.Nil(t, step)
			require.ErrorIs(t, pipe.Err(), tc.err)
			require.ErrorIs(t, pipe.Run(ctx), tc.err)
		})
	}
}

func TestStepOptionsUnsupportedForSinkFromChan(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		opt  pipeline.StepOption[int]
		err  error
	}{
		{
			name: "timeout",
			opt:  pipeline.StepTimeout[int](time.Millisecond),
			err:  pipeline.ErrTimeoutUnsupported,
		},
		{
			name: "rate limit",
			opt: pipeline.StepRateLimit[int](pipeline.RateLimitPolicy{
				Every: time.Millisecond,
				Burst: 1,
			}),
			err: pipeline.ErrRateLimitUnsupported,
		},
		{
			name: "max in-flight",
			opt:  pipeline.StepMaxInFlight[int](1),
			err:  pipeline.ErrMaxInFlightUnsupported,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			pipe, err := pipeline.New(pipeline.PipelineDefaults{})
			require.NoError(t, err)

			root := pipeline.Root(pipe, "root", func(ctx context.Context, out chan<- int) error {
				out <- 1

				return nil
			})
			require.NotNil(t, root)

			sink := pipeline.SinkFromChan(pipe, "sink", root, func(ctx context.Context, input <-chan int) error {
				return nil
			}, tc.opt)
			require.Nil(t, sink)
			require.ErrorIs(t, pipe.Err(), tc.err)
			require.ErrorIs(t, pipe.Run(ctx), tc.err)
		})
	}
}

func TestStepOptionsUnsupportedForRoot(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		opt  pipeline.StepOption[int]
		err  error
	}{
		{
			name: "timeout",
			opt:  pipeline.StepTimeout[int](time.Millisecond),
			err:  pipeline.ErrTimeoutUnsupported,
		},
		{
			name: "rate limit",
			opt: pipeline.StepRateLimit[int](pipeline.RateLimitPolicy{
				Every: time.Millisecond,
				Burst: 1,
			}),
			err: pipeline.ErrRateLimitUnsupported,
		},
		{
			name: "max in-flight",
			opt:  pipeline.StepMaxInFlight[int](1),
			err:  pipeline.ErrMaxInFlightUnsupported,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			pipe, err := pipeline.New(pipeline.PipelineDefaults{})
			require.NoError(t, err)

			root := pipeline.Root(pipe, "root", func(ctx context.Context, out chan<- int) error {
				return nil
			}, tc.opt)
			require.Nil(t, root)
			require.ErrorIs(t, pipe.Err(), tc.err)
			require.ErrorIs(t, pipe.Run(ctx), tc.err)
		})
	}
}

func TestStepOptionsUnsupportedForBatch(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		opt  pipeline.StepOption[[]int]
		err  error
	}{
		{
			name: "timeout",
			opt:  pipeline.StepTimeout[[]int](time.Millisecond),
			err:  pipeline.ErrTimeoutUnsupported,
		},
		{
			name: "rate limit",
			opt: pipeline.StepRateLimit[[]int](pipeline.RateLimitPolicy{
				Every: time.Millisecond,
				Burst: 1,
			}),
			err: pipeline.ErrRateLimitUnsupported,
		},
		{
			name: "max in-flight",
			opt:  pipeline.StepMaxInFlight[[]int](1),
			err:  pipeline.ErrMaxInFlightUnsupported,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			pipe, err := pipeline.New(pipeline.PipelineDefaults{})
			require.NoError(t, err)

			root := pipeline.Root(pipe, "root", func(ctx context.Context, out chan<- int) error {
				out <- 1

				return nil
			})
			require.NotNil(t, root)

			batch := pipeline.Batch(pipe, "batch", root, pipeline.BatchPolicy{MaxSize: 1}, tc.opt)
			require.Nil(t, batch)
			require.ErrorIs(t, pipe.Err(), tc.err)
			require.ErrorIs(t, pipe.Run(ctx), tc.err)
		})
	}
}

func TestStepOptionsUnsupportedForBatchChan(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		opt  pipeline.StepOption[<-chan int]
		err  error
	}{
		{
			name: "timeout",
			opt:  pipeline.StepTimeout[<-chan int](time.Millisecond),
			err:  pipeline.ErrTimeoutUnsupported,
		},
		{
			name: "rate limit",
			opt: pipeline.StepRateLimit[<-chan int](pipeline.RateLimitPolicy{
				Every: time.Millisecond,
				Burst: 1,
			}),
			err: pipeline.ErrRateLimitUnsupported,
		},
		{
			name: "max in-flight",
			opt:  pipeline.StepMaxInFlight[<-chan int](1),
			err:  pipeline.ErrMaxInFlightUnsupported,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			pipe, err := pipeline.New(pipeline.PipelineDefaults{})
			require.NoError(t, err)

			root := pipeline.Root(pipe, "root", func(ctx context.Context, out chan<- int) error {
				out <- 1

				return nil
			})
			require.NotNil(t, root)

			batch := pipeline.BatchChan(pipe, "batch", root, pipeline.BatchPolicy{MaxSize: 1}, tc.opt)
			require.Nil(t, batch)
			require.ErrorIs(t, pipe.Err(), tc.err)
			require.ErrorIs(t, pipe.Run(ctx), tc.err)
		})
	}
}

func TestMerge(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	got := []int{}
	pipe, err := pipeline.New(pipeline.PipelineDefaults{})
	require.NoError(t, err)
	step1 := pipeline.Step[int]{
		Details: &model.StepInfo{},
		Output:  createInputChan(t, 5),
	}

	step2 := pipeline.Step[int]{
		Details: &model.StepInfo{},
		Output:  createInputChan(t, 5),
	}

	outputChan := pipeline.Merge(pipe, "merge step", &step1, &step2)
	require.NotNil(t, outputChan)

	done := make(chan struct{})

	go func() {
		got = processOutputChan(t, outputChan.Output)

		done <- struct{}{}
	}()

	err = runPipeline(t, pipe, ctx)
	require.NoError(t, err)
	<-done
	assert.ElementsMatch(t, []int{0, 1, 2, 3, 4, 0, 1, 2, 3, 4}, got)
}

func buildPipeline(t *testing.T, pipe *pipeline.Pipeline, prefix string, conc int) {
	t.Helper()

	rootChan := pipeline.Root(pipe, prefix+" - root step", func(ctx context.Context, rootChan chan<- int) error {
		for i := range 5 {
			rootChan <- i
		}

		return nil
	})
	require.NotNil(t, rootChan)
	step1Chan := pipeline.OneToOne(pipe, prefix+" - step 1", rootChan, func(ctx context.Context, input int) (int, error) {
		// time.Sleep(100 * time.Millisecond)
		return input * 10, nil
	}, pipeline.StepConcurrency[int](conc))
	require.NotNil(t, step1Chan)

	splitter := pipeline.Split(pipe, prefix+" - split step 1", step1Chan, 2, pipeline.SplitterBufferSize[int](200))
	require.NotNil(t, splitter)

	split1Chan1, ok := splitter.Get()
	assert.True(t, ok)

	step21Chan := pipeline.OneToOne(pipe, prefix+" - step2 (1)", split1Chan1, func(ctx context.Context, input int) (int, error) {
		time.Sleep(20 * time.Millisecond)

		return input * 10, nil
	}, pipeline.StepConcurrency[int](conc))
	require.NotNil(t, step21Chan)

	split2Chan1, ok := splitter.Get()
	assert.True(t, ok)

	step22Chan := pipeline.OneToOne(pipe, prefix+" - step2 (2)", split2Chan1, func(ctx context.Context, input int) (int, error) {
		time.Sleep(100 * time.Millisecond)

		return input * 100, nil
	}, pipeline.StepConcurrency[int](conc))
	require.NotNil(t, step22Chan)

	outputChan := pipeline.Merge(pipe, prefix+" - merger", step21Chan, step22Chan)
	require.NotNil(t, outputChan)

	sinkStep := pipeline.Sink(pipe, prefix+" - sink", outputChan, func(ctx context.Context, input int) error {
		// time.Sleep(100 * time.Millisecond)
		_ = input

		return nil
	})
	require.NotNil(t, sinkStep)
}

func TestCompletePipeline(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	m := measure.NewDefaultMeasure()
	pipe, err := pipeline.New(
		pipeline.PipelineDefaults{},
		drawer.PipelineDrawer(drawer.NewSVGDrawer("./mygraph.dot"), m),
		measure.PipelineMeasure(m),
	)
	require.NoError(t, err)
	buildPipeline(t, pipe, "A", 10)
	buildPipeline(t, pipe, "B", 20)
	err = runPipeline(t, pipe, ctx)
	require.NoError(t, err)
}

func TestSimplePipeline(t *testing.T) {
	t.Parallel()

	// conc := 1
	ctx := t.Context()
	m := measure.NewDefaultMeasure()
	pipe, err := pipeline.New(
		pipeline.PipelineDefaults{},
		drawer.PipelineDrawer(drawer.NewSVGDrawer("./mygraph-simple.dot"), m),
		measure.PipelineMeasure(m),
	)
	require.NoError(t, err)

	rootChan := pipeline.Root(pipe, "root step", func(ctx context.Context, rootChan chan<- int) error {
		for i := range 10 {
			rootChan <- i
		}

		return nil
	})
	require.NotNil(t, rootChan)
	step1Chan := pipeline.OneToOne(pipe, "step 1", rootChan, func(ctx context.Context, input int) (int, error) {
		time.Sleep(100 * time.Millisecond)

		return input * 100, nil
	}, pipeline.StepConcurrency[int](1))
	require.NotNil(t, step1Chan)

	step2Chan := pipeline.OneToOne(pipe, "step 2", step1Chan, func(ctx context.Context, input int) (int, error) {
		time.Sleep(200 * time.Millisecond)

		return input * 200, nil
	}, pipeline.StepConcurrency[int](1))
	require.NotNil(t, step2Chan)
	sinkStep := pipeline.Sink(pipe, "sink", step2Chan, func(ctx context.Context, input int) error {
		_ = input

		return nil
	})
	require.NotNil(t, sinkStep)
	err = runPipeline(t, pipe, ctx)
	require.NoError(t, err)
}

func TestSimpleSplitterPipeline(t *testing.T) {
	t.Parallel()

	conc := 1
	ctx := t.Context()
	m := measure.NewDefaultMeasure()
	pipe, err := pipeline.New(
		pipeline.PipelineDefaults{},
		drawer.PipelineDrawer(drawer.NewSVGDrawer("./mygraph-simple-splitter.dot"), m),
		measure.PipelineMeasure(m),
	)
	require.NoError(t, err)

	rootChan := pipeline.Root(pipe, "root step", func(ctx context.Context, rootChan chan<- int) error {
		for i := range 10 {
			rootChan <- i
		}

		return nil
	})
	require.NotNil(t, rootChan)
	step1Chan := pipeline.OneToOne(pipe, "step 1", rootChan, func(ctx context.Context, input int) (int, error) {
		return input * 100, nil
	}, pipeline.StepConcurrency[int](conc))
	require.NotNil(t, step1Chan)

	splitterChans := pipeline.Split(pipe, "step 2", step1Chan, 2,
		pipeline.SplitterBufferSize[int](10),
	)
	require.NotNil(t, splitterChans)

	splitterChan1, _ := splitterChans.Get()
	splitterChan2, _ := splitterChans.Get()

	sinkStep := pipeline.Sink(pipe, "sink 1", splitterChan1, func(ctx context.Context, input int) error {
		time.Sleep(200 * time.Millisecond)

		_ = input

		return nil
	})
	require.NotNil(t, sinkStep)

	sinkStep = pipeline.Sink(pipe, "sink 2", splitterChan2, func(ctx context.Context, input int) error {
		time.Sleep(100 * time.Millisecond)

		_ = input

		return nil
	})
	require.NotNil(t, sinkStep)
	err = runPipeline(t, pipe, ctx)
	require.NoError(t, err)
}

func TestSimpleSplitterV2Pipeline(t *testing.T) {
	t.Parallel()

	conc := 1
	ctx := t.Context()
	m := measure.NewDefaultMeasure()
	pipe, err := pipeline.New(
		pipeline.PipelineDefaults{},
		drawer.PipelineDrawer(drawer.NewSVGDrawer("./mygraph-simple-splitter-v2.dot"), m),
		measure.PipelineMeasure(m),
	)
	require.NoError(t, err)

	rootChan := pipeline.Root(pipe, "root step", func(ctx context.Context, rootChan chan<- int) error {
		for i := range 10 {
			rootChan <- i
		}

		return nil
	})
	require.NotNil(t, rootChan)
	step1Chan := pipeline.OneToOne(pipe, "step 1", rootChan, func(ctx context.Context, input int) ([]int, error) {
		return []int{input * 100}, nil
	}, pipeline.StepConcurrency[[]int](conc))
	require.NotNil(t, step1Chan)

	step2Chan := pipeline.OneToOne(pipe, "step 2", step1Chan, func(ctx context.Context, input []int) (int, error) {
		return input[0] * 100, nil
	}, pipeline.StepConcurrency[int](conc))
	require.NotNil(t, step2Chan)

	splitterChans := pipeline.Split(pipe, "splitter", step2Chan, 2,
		pipeline.SplitterBufferSize[int](1),
	)
	require.NotNil(t, splitterChans)

	splitterChan1, _ := splitterChans.Get()
	splitterChan2, _ := splitterChans.Get()
	sinkStep := pipeline.Sink(pipe, "sink 1", splitterChan1, func(ctx context.Context, input int) error {
		time.Sleep(200 * time.Millisecond)

		_ = input

		return nil
	})
	require.NotNil(t, sinkStep)
	sinkStep = pipeline.Sink(pipe, "sink 2", splitterChan2, func(ctx context.Context, input int) error {
		time.Sleep(100 * time.Millisecond)

		_ = input

		return nil
	})
	require.NotNil(t, sinkStep)
	err = runPipeline(t, pipe, ctx)
	require.NoError(t, err)
}

func TestSimpleSplitterV3Pipeline(t *testing.T) {
	t.Parallel()

	conc := 1
	ctx := t.Context()
	m := measure.NewDefaultMeasure()
	pipe, err := pipeline.New(
		pipeline.PipelineDefaults{},
		drawer.PipelineDrawer(drawer.NewSVGDrawer("./mygraph-simple-splitter-v3.dot"), m),
		measure.PipelineMeasure(m),
	)
	require.NoError(t, err)

	rootChan := pipeline.Root(pipe, "root step", func(ctx context.Context, rootChan chan<- int) error {
		for i := range 10 {
			rootChan <- i
		}

		return nil
	})
	require.NotNil(t, rootChan)
	step1Chan := pipeline.OneToOne(pipe, "step 1", rootChan, func(ctx context.Context, input int) ([]int, error) {
		return []int{input * 100}, nil
	}, pipeline.StepConcurrency[[]int](conc))
	require.NotNil(t, step1Chan)

	step2Chan := pipeline.FromChan(pipe, "step 2", step1Chan,
		func(ctx context.Context, input <-chan []int, output chan int) error {
		outer:
			for {
				select {
				case <-ctx.Done():
					break outer
				case entry, ok := <-input:
					if !ok {
						break outer
					}

					select {
					case <-ctx.Done():
						break outer
					case output <- entry[0] * 100:
						time.Sleep(200 * time.Millisecond)
					}
				}
			}

			return nil
		}, pipeline.StepConcurrency[int](50))
	require.NotNil(t, step2Chan)

	splitterChans := pipeline.Split(pipe, "splitter", step2Chan, 2,
		pipeline.SplitterBufferSize[int](10),
	)
	require.NotNil(t, splitterChans)

	splitterChan1, _ := splitterChans.Get()
	splitterChan2, _ := splitterChans.Get()
	sinkStep := pipeline.Sink(pipe, "sink 1", splitterChan1, func(ctx context.Context, input int) error {
		time.Sleep(200 * time.Millisecond)

		_ = input

		return nil
	})
	require.NotNil(t, sinkStep)
	sinkStep = pipeline.Sink(pipe, "sink 2", splitterChan2, func(ctx context.Context, input int) error {
		time.Sleep(100 * time.Millisecond)

		_ = input

		return nil
	})
	require.NotNil(t, sinkStep)
	err = runPipeline(t, pipe, ctx)
	require.NoError(t, err)
}

func TestSimpleSplitterV4Pipeline(t *testing.T) {
	t.Parallel()

	conc := 2
	ctx := t.Context()
	m := measure.NewDefaultMeasure()
	pipe, err := pipeline.New(
		pipeline.PipelineDefaults{},
		drawer.PipelineDrawer(drawer.NewSVGDrawer("./mygraph-simple-splitter-v4.dot"), m),
		measure.PipelineMeasure(m),
	)
	require.NoError(t, err)

	rootChan := pipeline.Root(pipe, "root step", func(ctx context.Context, rootChan chan<- int) error {
		for i := range 10 {
			rootChan <- i
		}

		return nil
	})
	require.NotNil(t, rootChan)

	step0Chan := pipeline.OneToOne(pipe, "step 0", rootChan, func(ctx context.Context, input int) (int, error) {
		return input, nil
	}, pipeline.StepConcurrency[int](conc))
	require.NotNil(t, step0Chan)

	step1Chan := pipeline.OneToMany(pipe, "step 1", step0Chan, func(ctx context.Context, input int) ([]int, error) {
		return []int{input * 100}, nil
	}, pipeline.StepConcurrency[int](conc))
	require.NotNil(t, step1Chan)

	splitterChans := pipeline.Split(pipe, "splitter", step1Chan, 2,
		pipeline.SplitterBufferSize[int](1),
	)
	require.NotNil(t, splitterChans)

	splitterChan1, _ := splitterChans.Get()
	splitterChan2, _ := splitterChans.Get()

	splittedChan1 := pipeline.OneToOne(pipe, "splitted step 1", splitterChan1, func(ctx context.Context, input int) (int, error) {
		time.Sleep(200 * time.Millisecond)

		return input / 100, nil
	}, pipeline.StepConcurrency[int](conc))
	require.NotNil(t, splittedChan1)

	sinkStep := pipeline.SinkFromChan(pipe, "sink 1", splittedChan1, func(ctx context.Context, input <-chan int) error {
		for elem := range input {
			_ = elem
		}

		return nil
	})
	require.NotNil(t, sinkStep)

	splittedChan2 := pipeline.OneToMany(pipe, "splitted step 2", splitterChan2,
		func(ctx context.Context, input int) ([]int, error) {
			return []int{input / 100}, nil
		}, pipeline.StepConcurrency[int](conc))
	require.NotNil(t, splittedChan2)

	sinkStep = pipeline.Sink(pipe, "sink 2", splittedChan2, func(ctx context.Context, input int) error {
		_ = input

		return nil
	})
	require.NotNil(t, sinkStep)
	err = runPipeline(t, pipe, ctx)
	require.NoError(t, err)
}
