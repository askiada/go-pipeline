package pipeline

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestAddRootStepNilPipe(t *testing.T) {
	_, err := AddRootStep(nil, "root step", func(ctx context.Context, rootChan chan<- int) error {
		for i := 0; i < 10; i++ {
			rootChan <- i
		}
		return nil
	})
	assert.Error(t, err)
}

func TestAddRootStep(t *testing.T) {
	pipe, err := New(context.Background())
	assert.NoError(t, err)
	var got []int

	outputChan, err := AddRootStep(pipe, "root step", func(ctx context.Context, rootChan chan<- int) error {
		for i := 0; i < 10; i++ {
			rootChan <- i
		}
		return nil
	})
	assert.Nil(t, err)
	done := make(chan struct{})
	go func() {
		got = processOutputChan(t, outputChan.Output)
		done <- struct{}{}
	}()
	err = pipe.Run()
	assert.Nil(t, err)
	<-done
	assert.ElementsMatch(t, []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, got)
}

func TestAddRootStepError(t *testing.T) {
	pipe, err := New(context.Background())
	assert.NoError(t, err)
	var got []int

	outputChan, err := AddRootStep(pipe, "root step", func(ctx context.Context, rootChan chan<- int) error {
		for i := 0; i < 10; i++ {
			if i == 5 {
				return assert.AnError
			}
			rootChan <- i
		}
		return nil
	})
	assert.Nil(t, err)
	done := make(chan struct{})
	go func() {
		got = processOutputChan(t, outputChan.Output)
		done <- struct{}{}
	}()
	err = pipe.Run()
	assert.Error(t, err)
	<-done
	_ = got
}

func TestAddRootStepCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	pipe, err := New(ctx)
	assert.NoError(t, err)
	var got []int

	outputChan, err := AddRootStep(pipe, "root step", func(ctx context.Context, rootChan chan<- int) error {
		for i := 0; i < 10; i++ {
			if i == 5 {
				cancel()
				return assert.AnError
			}
			rootChan <- i
		}
		return nil
	})
	assert.Nil(t, err)
	done := make(chan struct{})
	go func() {
		got = processOutputChan(t, outputChan.Output)
		done <- struct{}{}
	}()
	err = pipe.Run()
	assert.Error(t, err)
	<-done
	_ = got
}

func TestAddStepNilPipe(t *testing.T) {
	_, err := AddStepOneToOne(nil, "root step", nil, func(ctx context.Context, input int) (int, error) {
		return input, nil
	})
	assert.Error(t, err)
}

func TestAddStepNilInput(t *testing.T) {
	pipe, err := New(context.Background())
	assert.NoError(t, err)
	_, err = AddStepOneToOne(pipe, "root step", nil, func(ctx context.Context, input int) (int, error) {
		return input, nil
	})
	assert.Error(t, err)
}

func TestAddStep(t *testing.T) {
	var got []int
	ctx := context.Background()
	pipe, err := New(ctx)
	assert.NoError(t, err)
	step := Step[int]{
		Output: createInputChan(t, ctx, 10),
	}
	outputChan, err := AddStepOneToOne(pipe, "first step", &step, func(ctx context.Context, input int) (int, error) {
		return input, nil
	})
	assert.Nil(t, err)
	done := make(chan struct{})
	go func() {
		got = processOutputChan(t, outputChan.Output)
		done <- struct{}{}
	}()
	err = pipe.Run()
	assert.Nil(t, err)
	<-done
	assert.ElementsMatch(t, []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, got)
}

func TestAddStepError(t *testing.T) {
	var got []int
	ctx := context.Background()
	pipe, err := New(ctx)
	assert.NoError(t, err)
	step := Step[int]{
		Output: createInputChan(t, ctx, 10),
	}
	outputChan, err := AddStepOneToOne(pipe, "root step", &step, func(ctx context.Context, input int) (int, error) {
		if input == 5 {
			return 0, assert.AnError
		}
		return input, nil
	})

	assert.Nil(t, err)
	done := make(chan struct{})
	go func() {
		got = processOutputChan(t, outputChan.Output)
		done <- struct{}{}
	}()
	err = pipe.Run()
	assert.Error(t, err)
	<-done
	_ = got
}

func TestAddStepCancel(t *testing.T) {
	var got []int
	ctx, cancel := context.WithCancel(context.Background())
	pipe, err := New(ctx)
	assert.NoError(t, err)
	step := Step[int]{
		Output: createInputChanWithCancel(t, ctx, 10, 5, cancel),
	}
	outputChan, err := AddStepOneToOne(pipe, "root step", &step, func(ctx context.Context, input int) (int, error) {
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		default:
			return input, nil
		}
	})
	assert.Nil(t, err)
	done := make(chan struct{})
	go func() {
		got = processOutputChan(t, outputChan.Output)
		done <- struct{}{}
	}()
	err = pipe.Run()
	assert.Error(t, err)
	<-done
	_ = got
}

func TestAddSplitterNilPipe(t *testing.T) {
	_, err := AddSplitter(nil, "root step", (*Step[int])(nil), 5)
	assert.Error(t, err)
	assert.Error(t, err)
}

func TestAddSplitterNilInput(t *testing.T) {
	pipe, err := New(context.Background())
	assert.NoError(t, err)
	_, err = AddSplitter(pipe, "root step", (*Step[int])(nil), 5)
	assert.Error(t, err)
}

func TestAddSplitterZero(t *testing.T) {
	pipe, err := New(context.Background())
	assert.NoError(t, err)
	step := Step[int]{
		Output: make(chan int),
	}
	_, err = AddSplitter(pipe, "root step", &step, 0)
	assert.Error(t, err)
}

func TestAddSplitter(t *testing.T) {
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
			var got1, got2 []int
			ctx := context.Background()
			pipe, err := New(ctx)
			assert.NoError(t, err)
			step := Step[int]{
				Output: createInputChan(t, ctx, 10),
			}
			splitter, err := AddSplitter(pipe, "root step", &step, 2, SplitterBufferSize[int](tc.buffersize))
			assert.Nil(t, err)
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
			err = pipe.Run()
			assert.Nil(t, err)
			wg.Wait()
			assert.ElementsMatch(t, expected, got1)
			assert.ElementsMatch(t, expected, got2)
		})
	}
}

func TestAddSplitterCancel(t *testing.T) {
	var got1, got2 []int
	ctx, cancel := context.WithCancel(context.Background())
	pipe, err := New(ctx)
	assert.NoError(t, err)
	step := Step[int]{
		Output: createInputChanWithCancel(t, ctx, 10, 5, cancel),
	}
	splitter, err := AddSplitter(pipe, "root step", &step, 2)
	assert.Nil(t, err)
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
	err = pipe.Run()
	assert.Error(t, err)
	wg.Wait()
	// Otherwise the compiler ignores the output channel and checks the ctx.
	_ = got1
	_ = got2
}

func TestAddSinkNilPipe(t *testing.T) {
	err := AddSink(nil, "root step", nil, func(ctx context.Context, input <-chan int) error {
		for i := range input {
			_ = i
		}
		return nil
	})
	assert.Error(t, err)
}

func TestAddSinkNilInput(t *testing.T) {
	pipe, err := New(context.Background())
	assert.NoError(t, err)
	err = AddSink(pipe, "root step", nil, func(ctx context.Context, input <-chan int) error {
		for i := range input {
			_ = i
		}
		return nil
	})
	assert.Error(t, err)
}

func TestAddSink(t *testing.T) {
	ctx := context.Background()
	got := []int{}
	pipe, err := New(ctx)
	assert.NoError(t, err)
	step := Step[int]{
		Output: createInputChan(t, ctx, 10),
	}
	err = AddSink(pipe, "root step", &step, func(ctx context.Context, input int) error {
		got = append(got, input)
		return nil
	})
	assert.Nil(t, err)
	err = pipe.Run()
	assert.Nil(t, err)
	assert.ElementsMatch(t, []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, got)
}

func TestAddSinkError(t *testing.T) {
	ctx := context.Background()
	got := []int{}
	pipe, err := New(ctx)
	assert.NoError(t, err)
	step := Step[int]{
		Output: createInputChan(t, ctx, 10),
	}
	err = AddSink(pipe, "root step", &step, func(ctx context.Context, input int) error {
		if input == 5 {
			return assert.AnError
		}
		got = append(got, input)
		return nil
	})
	assert.Nil(t, err)
	err = pipe.Run()
	assert.Error(t, err)
}

func TestAddMerger(t *testing.T) {
	ctx := context.Background()
	got := []int{}
	pipe, err := New(ctx)
	assert.NoError(t, err)
	step1 := Step[int]{
		Output: createInputChan(t, ctx, 5),
	}

	step2 := Step[int]{
		Output: createInputChan(t, ctx, 5),
	}

	outputChan, err := AddMerger(pipe, "merge step", &step1, &step2)
	assert.Nil(t, err)
	done := make(chan struct{})
	go func() {
		got = processOutputChan(t, outputChan.Output)
		done <- struct{}{}
	}()
	err = pipe.Run()
	assert.Nil(t, err)
	<-done
	assert.ElementsMatch(t, []int{0, 1, 2, 3, 4, 0, 1, 2, 3, 4}, got)
}

func buildPipeline(t *testing.T, pipe *Pipeline, prefix string, conc int) {
	rootChan, err := AddRootStep(pipe, prefix+" - root step", func(ctx context.Context, rootChan chan<- int) error {
		for i := 0; i < 5; i++ {
			rootChan <- i
		}
		return nil
	})
	assert.NoError(t, err)
	step1Chan, err := AddStepOneToOne(pipe, prefix+" - step 1", rootChan, func(ctx context.Context, input int) (int, error) {
		// time.Sleep(100 * time.Millisecond)
		return input * 10, nil
	}, StepConcurrency[int](conc))
	assert.NoError(t, err)

	splitter, err := AddSplitter(pipe, prefix+" - split step 1", step1Chan, 2, SplitterBufferSize[int](200))
	assert.NoError(t, err)
	split1Chan1, ok := splitter.Get()
	assert.True(t, ok)
	step21Chan, err := AddStepOneToOne(pipe, prefix+" - step2 (1)", split1Chan1, func(ctx context.Context, input int) (int, error) {
		time.Sleep(20 * time.Millisecond)
		return input * 10, nil
	}, StepConcurrency[int](conc))
	assert.NoError(t, err)
	split2Chan1, ok := splitter.Get()
	assert.True(t, ok)
	step22Chan, err := AddStepOneToOne(pipe, prefix+" - step2 (2)", split2Chan1, func(ctx context.Context, input int) (int, error) {
		time.Sleep(100 * time.Millisecond)
		return input * 100, nil
	}, StepConcurrency[int](conc))
	assert.NoError(t, err)

	outputChan, err := AddMerger(pipe, prefix+" - merger", step21Chan, step22Chan)
	assert.NoError(t, err)

	err = AddSink(pipe, prefix+" - sink", outputChan, func(ctx context.Context, input int) error {
		// time.Sleep(100 * time.Millisecond)
		_ = input
		return nil
	})
	assert.NoError(t, err)
}

func TestCompletePipeline(t *testing.T) {
	ctx := context.Background()
	pipe, err := New(ctx, PipelineDrawer("./mygraph.gv"), PipelineMeasure())
	assert.NoError(t, err)
	buildPipeline(t, pipe, "A", 10)
	buildPipeline(t, pipe, "B", 10)
	err = pipe.Run()
	assert.NoError(t, err)
}

func TestSimplePipeline(t *testing.T) {
	// conc := 1
	ctx := context.Background()
	pipe, err := New(ctx, PipelineDrawer("./mygraph-simple.gv"), PipelineMeasure())
	assert.NoError(t, err)
	rootChan, err := AddRootStep(pipe, "root step", func(ctx context.Context, rootChan chan<- int) error {
		for i := 0; i < 10; i++ {
			rootChan <- i
		}
		return nil
	})
	assert.NoError(t, err)
	step1Chan, err := AddStepOneToOne(pipe, "step 1", rootChan, func(ctx context.Context, input int) (int, error) {
		time.Sleep(100 * time.Millisecond)
		return input * 100, nil
	}, StepConcurrency[int](1))
	assert.NoError(t, err)

	step2Chan, err := AddStepOneToOne(pipe, "step 2", step1Chan, func(ctx context.Context, input int) (int, error) {
		time.Sleep(200 * time.Millisecond)
		return input * 200, nil
	}, StepConcurrency[int](1))
	assert.NoError(t, err)
	err = AddSink(pipe, "sink", step2Chan, func(ctx context.Context, input int) error {
		_ = input
		return nil
	})
	assert.NoError(t, err)
	err = pipe.Run()
	assert.NoError(t, err)
}

func TestSimpleSplitterPipeline(t *testing.T) {
	conc := 1
	ctx := context.Background()
	pipe, err := New(ctx, PipelineDrawer("./mygraph-simple-splitter.gv"), PipelineMeasure())
	assert.NoError(t, err)
	rootChan, err := AddRootStep(pipe, "root step", func(ctx context.Context, rootChan chan<- int) error {
		for i := 0; i < 10; i++ {
			rootChan <- i
		}
		return nil
	})
	assert.NoError(t, err)
	step1Chan, err := AddStepOneToOne(pipe, "step 1", rootChan, func(ctx context.Context, input int) (int, error) {
		return input * 100, nil
	}, StepConcurrency[int](conc))
	assert.NoError(t, err)

	splitterChans, err := AddSplitter(pipe, "step 2", step1Chan, 2,
		SplitterBufferSize[int](10),
	)
	assert.NoError(t, err)

	splitterChan1, _ := splitterChans.Get()
	splitterChan2, _ := splitterChans.Get()
	err = AddSink(pipe, "sink 1", splitterChan1, func(ctx context.Context, input int) error {
		time.Sleep(200 * time.Millisecond)
		_ = input
		return nil
	})
	err = AddSink(pipe, "sink 2", splitterChan2, func(ctx context.Context, input int) error {
		time.Sleep(100 * time.Millisecond)
		_ = input
		return nil
	})
	assert.NoError(t, err)
	err = pipe.Run()
	assert.Error(t, err)
}
