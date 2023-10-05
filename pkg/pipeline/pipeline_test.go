package pipeline

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestAddStepOneToOneNilPipe(t *testing.T) {
	_, err := AddStepOneToOne(nil, "root step", nil, func(ctx context.Context, input int) (int, error) {
		return input, nil
	})
	assert.Error(t, err)
}

func TestAddStepOneToOneNilInput(t *testing.T) {
	pipe, err := New(context.Background())
	assert.NoError(t, err)
	_, err = AddStepOneToOne(pipe, "root step", nil, func(ctx context.Context, input int) (int, error) {
		return input, nil
	})
	assert.Error(t, err)
}

func TestAddStepOneToOne(t *testing.T) {
	var got []int
	ctx := context.Background()
	pipe, err := New(ctx)
	assert.NoError(t, err)
	step := Step[int]{
		Output: createInputChan(t, 10),
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

func TestAddStepOneToOneError(t *testing.T) {
	var got []int
	ctx := context.Background()
	pipe, err := New(ctx)
	assert.NoError(t, err)
	step := Step[int]{
		Output: createInputChan(t, 10),
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

func TestAddStepOneToOneCancel(t *testing.T) {
	var got []int
	ctx, cancel := context.WithCancel(context.Background())
	pipe, err := New(ctx)
	assert.NoError(t, err)
	step := Step[int]{
		Output: createInputChanWithCancel(t, 10, 5, cancel),
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

func TestAddStepOneToOneOrZeroNilPipe(t *testing.T) {
	_, err := AddStepOneToOneOrZero(nil, "root step", nil, func(ctx context.Context, input int) (int, error) {
		return input, nil
	})
	assert.Error(t, err)
}

func TestAddStepOneToOneOrZeroNilInput(t *testing.T) {
	pipe, err := New(context.Background())
	assert.NoError(t, err)
	_, err = AddStepOneToOneOrZero(pipe, "root step", nil, func(ctx context.Context, input int) (int, error) {
		return input, nil
	})
	assert.Error(t, err)
}

func TestAddStepOneToOneOrSZero(t *testing.T) {
	var got []int
	ctx := context.Background()
	pipe, err := New(ctx)
	assert.NoError(t, err)
	step := Step[int]{
		Output: createInputChan(t, 10),
	}
	outputChan, err := AddStepOneToOneOrZero(pipe, "first step", &step, func(ctx context.Context, input int) (int, error) {
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
	assert.ElementsMatch(t, []int{1, 2, 3, 4, 5, 6, 7, 8, 9}, got)
}

func TestAddStepOneToOneOrZeroError(t *testing.T) {
	var got []int
	ctx := context.Background()
	pipe, err := New(ctx)
	assert.NoError(t, err)
	step := Step[int]{
		Output: createInputChan(t, 10),
	}
	outputChan, err := AddStepOneToOneOrZero(pipe, "root step", &step, func(ctx context.Context, input int) (int, error) {
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

func TestAddStepOneToOneOrZeroCancel(t *testing.T) {
	var got []int
	ctx, cancel := context.WithCancel(context.Background())
	pipe, err := New(ctx)
	assert.NoError(t, err)
	step := Step[int]{
		Output: createInputChanWithCancel(t, 10, 5, cancel),
	}
	outputChan, err := AddStepOneToOneOrZero(pipe, "root step", &step, func(ctx context.Context, input int) (int, error) {
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

func TestAddStepOneToManyNilPipe(t *testing.T) {
	_, err := AddStepOneToMany(nil, "root step", nil, func(ctx context.Context, input int) ([]int, error) {
		return []int{input}, nil
	})
	assert.Error(t, err)
}

func TestAddStepOneToManyNilInput(t *testing.T) {
	pipe, err := New(context.Background())
	assert.NoError(t, err)
	_, err = AddStepOneToMany(pipe, "root step", nil, func(ctx context.Context, input int) ([]int, error) {
		return []int{input}, nil
	})
	assert.Error(t, err)
}

func TestAddStepOneToMany(t *testing.T) {
	var got []int
	ctx := context.Background()
	pipe, err := New(ctx)
	assert.NoError(t, err)
	step := Step[int]{
		Output: createInputChan(t, 10),
	}
	outputChan, err := AddStepOneToMany(pipe, "first step", &step, func(ctx context.Context, input int) ([]int, error) {
		return []int{input}, nil
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

func TestAddStepOneToManyError(t *testing.T) {
	var got []int
	ctx := context.Background()
	pipe, err := New(ctx)
	assert.NoError(t, err)
	step := Step[int]{
		Output: createInputChan(t, 10),
	}
	outputChan, err := AddStepOneToMany(pipe, "root step", &step, func(ctx context.Context, input int) ([]int, error) {
		if input == 5 {
			return []int{0}, assert.AnError
		}
		return []int{input}, nil
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

func TestAddStepOneToManyCancel(t *testing.T) {
	var got []int
	ctx, cancel := context.WithCancel(context.Background())
	pipe, err := New(ctx)
	assert.NoError(t, err)
	step := Step[int]{
		Output: createInputChanWithCancel(t, 10, 5, cancel),
	}
	outputChan, err := AddStepOneToMany(pipe, "root step", &step, func(ctx context.Context, input int) ([]int, error) {
		select {
		case <-ctx.Done():
			return []int{0}, ctx.Err()
		default:
			return []int{input}, nil
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
				Output: createInputChan(t, 10),
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
		Output: createInputChanWithCancel(t, 10, 5, cancel),
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
		Output: createInputChan(t, 10),
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
		Output: createInputChan(t, 10),
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
		details: &StepInfo{},
		Output:  createInputChan(t, 5),
	}

	step2 := Step[int]{
		details: &StepInfo{},
		Output:  createInputChan(t, 5),
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

/*
func TestCompletePipeline(t *testing.T) {
	ctx := context.Background()
	m := measure.NewDefaultMeasure()
	pipe, err := New(ctx, PipelineDrawer(drawer.NewSVGDrawer("./mygraph.gv"), m), PipelineMeasure(m))
	assert.NoError(t, err)
	buildPipeline(t, pipe, "A", 10)
	buildPipeline(t, pipe, "B", 10)
	err = pipe.Run()
	assert.NoError(t, err)
}

func TestSimplePipeline(t *testing.T) {
	// conc := 1
	ctx := context.Background()
	m := measure.NewDefaultMeasure()
	pipe, err := New(ctx, PipelineDrawer(drawer.NewSVGDrawer("./mygraph-simple.gv"), m), PipelineMeasure(m))
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
	m := measure.NewDefaultMeasure()
	pipe, err := New(ctx, PipelineDrawer(drawer.NewSVGDrawer("./mygraph-simple-splitter.gv"), m), PipelineMeasure(m))
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
	assert.NoError(t, err)
	err = AddSink(pipe, "sink 2", splitterChan2, func(ctx context.Context, input int) error {
		time.Sleep(100 * time.Millisecond)
		_ = input
		return nil
	})
	assert.NoError(t, err)
	err = pipe.Run()
	assert.NoError(t, err)
}

func TestSimpleSplitterV2Pipeline(t *testing.T) {
	conc := 1
	ctx := context.Background()
	m := measure.NewDefaultMeasure()
	pipe, err := New(ctx, PipelineDrawer(drawer.NewSVGDrawer("./mygraph-simple-splitter-v2.gv"), m), PipelineMeasure(m))
	assert.NoError(t, err)
	rootChan, err := AddRootStep(pipe, "root step", func(ctx context.Context, rootChan chan<- int) error {
		for i := 0; i < 10; i++ {
			rootChan <- i
		}
		return nil
	})
	assert.NoError(t, err)
	step1Chan, err := AddStepOneToOne[int, []int](pipe, "step 1", rootChan, func(ctx context.Context, input int) ([]int, error) {
		return []int{input * 100}, nil
	}, StepConcurrency[[]int](conc))
	assert.NoError(t, err)

	step2Chan, err := AddStepOneToOne(pipe, "step 2", step1Chan, func(ctx context.Context, input []int) (int, error) {
		return input[0] * 100, nil
	}, StepConcurrency[int](conc))
	assert.NoError(t, err)

	splitterChans, err := AddSplitter(pipe, "splitter", step2Chan, 2,
		SplitterBufferSize[int](1),
	)
	assert.NoError(t, err)

	splitterChan1, _ := splitterChans.Get()
	splitterChan2, _ := splitterChans.Get()
	err = AddSink(pipe, "sink 1", splitterChan1, func(ctx context.Context, input int) error {
		time.Sleep(200 * time.Millisecond)
		_ = input
		return nil
	})
	assert.NoError(t, err)
	err = AddSink(pipe, "sink 2", splitterChan2, func(ctx context.Context, input int) error {
		time.Sleep(100 * time.Millisecond)
		_ = input
		return nil
	})
	assert.NoError(t, err)
	err = pipe.Run()
	assert.Error(t, err)
}

func TestSimpleSplitterV3Pipeline(t *testing.T) {
	conc := 1
	ctx := context.Background()
	m := measure.NewDefaultMeasure()
	pipe, err := New(ctx, PipelineDrawer(drawer.NewSVGDrawer("./mygraph-simple-splitter-v3.gv"), m), PipelineMeasure(m))
	assert.NoError(t, err)
	rootChan, err := AddRootStep(pipe, "root step", func(ctx context.Context, rootChan chan<- int) error {
		for i := 0; i < 10; i++ {
			rootChan <- i
		}
		return nil
	})
	assert.NoError(t, err)
	step1Chan, err := AddStepOneToOne[int, []int](pipe, "step 1", rootChan, func(ctx context.Context, input int) ([]int, error) {
		return []int{input * 100}, nil
	}, StepConcurrency[[]int](conc))
	assert.NoError(t, err)

	step2Chan, err := AddStepFromChan(pipe, "step 2", step1Chan, func(ctx context.Context, input <-chan []int, output chan int) error {
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
				}
			}
		}
		return nil
	}, StepConcurrency[int](conc))
	assert.NoError(t, err)

	splitterChans, err := AddSplitter(pipe, "splitter", step2Chan, 2,
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
	assert.NoError(t, err)
	err = AddSink(pipe, "sink 2", splitterChan2, func(ctx context.Context, input int) error {
		time.Sleep(100 * time.Millisecond)
		_ = input
		return nil
	})
	assert.NoError(t, err)
	err = pipe.Run()
	assert.Error(t, err)
}

func TestSimpleSplitterV4Pipeline(t *testing.T) {
	conc := 2
	ctx := context.Background()
	m := measure.NewDefaultMeasure()
	pipe, err := New(ctx, PipelineDrawer(drawer.NewSVGDrawer("./mygraph-simple-splitter-v4.gv"), m), PipelineMeasure(m))
	assert.NoError(t, err)
	rootChan, err := AddRootStep(pipe, "root step", func(ctx context.Context, rootChan chan<- int) error {
		for i := 0; i < 10; i++ {
			rootChan <- i
		}
		return nil
	})
	assert.NoError(t, err)

	step0Chan, err := AddStepOneToOne(pipe, "step 0", rootChan, func(ctx context.Context, input int) (int, error) {
		return input, nil
	}, StepConcurrency[int](conc))
	assert.NoError(t, err)

	step1Chan, err := AddStepOneToMany(pipe, "step 1", step0Chan, func(ctx context.Context, input int) ([]int, error) {
		return []int{input * 100}, nil
	}, StepConcurrency[int](conc))
	assert.NoError(t, err)

	splitterChans, err := AddSplitter(pipe, "splitter", step1Chan, 2,
		SplitterBufferSize[int](1),
	)
	assert.NoError(t, err)

	splitterChan1, _ := splitterChans.Get()
	splitterChan2, _ := splitterChans.Get()

	splittedChan1, err := AddStepOneToOne(pipe, "splitted step 1", splitterChan1, func(ctx context.Context, input int) (int, error) {
		time.Sleep(200 * time.Millisecond)
		return input / 100, nil
	}, StepConcurrency[int](conc))
	assert.NoError(t, err)

	err = AddSinkFromChan(pipe, "sink 1", splittedChan1, func(ctx context.Context, input <-chan int) error {
		for elem := range input {
			_ = elem
		}
		return nil
	})
	assert.NoError(t, err)

	splittedChan2, err := AddStepOneToMany(pipe, "splitted step 2", splitterChan2, func(ctx context.Context, input int) ([]int, error) {
		return []int{input / 100}, nil
	}, StepConcurrency[int](conc))
	assert.NoError(t, err)

	err = AddSink(pipe, "sink 2", splittedChan2, func(ctx context.Context, input int) error {
		_ = input
		return nil
	})
	assert.NoError(t, err)
	err = pipe.Run()
	assert.Error(t, err)
}
*/
