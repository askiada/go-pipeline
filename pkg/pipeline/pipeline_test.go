package pipeline_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/askiada/go-pipeline/pkg/pipeline"
	"github.com/askiada/go-pipeline/pkg/pipeline/drawer"
	"github.com/askiada/go-pipeline/pkg/pipeline/measure"
	"github.com/askiada/go-pipeline/pkg/pipeline/model"
)

func TestAddStepOneToOneNilPipe(t *testing.T) {
	t.Parallel()

	_, err := pipeline.AddStepOneToOne(nil, "root step", nil, func(ctx context.Context, input int) (int, error) {
		return input, nil
	})
	assert.Error(t, err)
}

func TestAddStepOneToOneNilInput(t *testing.T) {
	t.Parallel()

	pipe, err := pipeline.New(context.Background())
	require.NoError(t, err)
	_, err = pipeline.AddStepOneToOne(pipe, "root step", nil, func(ctx context.Context, input int) (int, error) {
		return input, nil
	})
	require.Error(t, err)
}

func TestAddStepOneToOne(t *testing.T) {
	t.Parallel()

	var got []int

	ctx := context.Background()
	pipe, err := pipeline.New(ctx)
	require.NoError(t, err)
	step := model.Step[int]{
		Output: createInputChan(t, 10),
	}
	outputChan, err := pipeline.AddStepOneToOne(pipe, "first step", &step, func(ctx context.Context, input int) (int, error) {
		return input, nil
	})
	require.NoError(t, err)

	done := make(chan struct{})

	go func() {
		got = processOutputChan(t, outputChan.Output)
		done <- struct{}{}
	}()

	err = pipe.Run()
	require.NoError(t, err)
	<-done
	assert.ElementsMatch(t, []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, got)
}

func TestAddStepOneToOneError(t *testing.T) {
	t.Parallel()

	var got []int

	ctx := context.Background()
	pipe, err := pipeline.New(ctx)
	require.NoError(t, err)
	step := model.Step[int]{
		Output: createInputChan(t, 10),
	}
	outputChan, err := pipeline.AddStepOneToOne(pipe, "root step", &step, func(ctx context.Context, input int) (int, error) {
		if input == 5 {
			return 0, assert.AnError
		}

		return input, nil
	})

	require.NoError(t, err)

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
	t.Parallel()

	var got []int

	ctx, cancel := context.WithCancel(context.Background())
	pipe, err := pipeline.New(ctx)
	require.NoError(t, err)
	step := model.Step[int]{
		Output: createInputChanWithCancel(t, 10, 5, cancel),
	}
	outputChan, err := pipeline.AddStepOneToOne(pipe, "root step", &step, func(ctx context.Context, input int) (int, error) {
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		default:
			return input, nil
		}
	})
	require.NoError(t, err)

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
	t.Parallel()

	_, err := pipeline.AddStepOneToOneOrZero(nil, "root step", nil, func(ctx context.Context, input int) (int, error) {
		return input, nil
	})
	assert.Error(t, err)
}

func TestAddStepOneToOneOrZeroNilInput(t *testing.T) {
	t.Parallel()

	pipe, err := pipeline.New(context.Background())
	require.NoError(t, err)
	_, err = pipeline.AddStepOneToOneOrZero(pipe, "root step", nil, func(ctx context.Context, input int) (int, error) {
		return input, nil
	})
	assert.Error(t, err)
}

func TestAddStepOneToOneOrSZero(t *testing.T) {
	t.Parallel()

	var got []int

	ctx := context.Background()
	pipe, err := pipeline.New(ctx)
	require.NoError(t, err)
	step := model.Step[int]{
		Output: createInputChan(t, 10),
	}
	outputChan, err := pipeline.AddStepOneToOneOrZero(pipe, "first step", &step, func(ctx context.Context, input int) (int, error) {
		return input, nil
	})
	require.NoError(t, err)

	done := make(chan struct{})

	go func() {
		got = processOutputChan(t, outputChan.Output)
		done <- struct{}{}
	}()

	err = pipe.Run()
	require.NoError(t, err)
	<-done
	assert.ElementsMatch(t, []int{1, 2, 3, 4, 5, 6, 7, 8, 9}, got)
}

func TestAddStepOneToOneOrZeroError(t *testing.T) {
	t.Parallel()

	var got []int

	ctx := context.Background()
	pipe, err := pipeline.New(ctx)
	require.NoError(t, err)
	step := model.Step[int]{
		Output: createInputChan(t, 10),
	}
	outputChan, err := pipeline.AddStepOneToOneOrZero(pipe, "root step", &step, func(ctx context.Context, input int) (int, error) {
		if input == 5 {
			return 0, assert.AnError
		}

		return input, nil
	})

	require.NoError(t, err)

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
	t.Parallel()

	var got []int

	ctx, cancel := context.WithCancel(context.Background())
	pipe, err := pipeline.New(ctx)
	require.NoError(t, err)
	step := model.Step[int]{
		Output: createInputChanWithCancel(t, 10, 5, cancel),
	}
	outputChan, err := pipeline.AddStepOneToOneOrZero(pipe, "root step", &step, func(ctx context.Context, input int) (int, error) {
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		default:
			return input, nil
		}
	})
	require.NoError(t, err)

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
	t.Parallel()

	_, err := pipeline.AddStepOneToMany(nil, "root step", nil, func(ctx context.Context, input int) ([]int, error) {
		return []int{input}, nil
	})
	assert.Error(t, err)
}

func TestAddStepOneToManyNilInput(t *testing.T) {
	t.Parallel()

	pipe, err := pipeline.New(context.Background())
	require.NoError(t, err)
	_, err = pipeline.AddStepOneToMany(pipe, "root step", nil, func(ctx context.Context, input int) ([]int, error) {
		return []int{input}, nil
	})
	require.Error(t, err)
}

func TestAddStepOneToMany(t *testing.T) {
	t.Parallel()

	var got []int

	ctx := context.Background()
	pipe, err := pipeline.New(ctx)
	require.NoError(t, err)
	step := model.Step[int]{
		Output: createInputChan(t, 10),
	}
	outputChan, err := pipeline.AddStepOneToMany(pipe, "first step", &step, func(ctx context.Context, input int) ([]int, error) {
		return []int{input}, nil
	})
	require.NoError(t, err)

	done := make(chan struct{})

	go func() {
		got = processOutputChan(t, outputChan.Output)
		done <- struct{}{}
	}()

	err = pipe.Run()
	require.NoError(t, err)
	<-done
	assert.ElementsMatch(t, []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, got)
}

func TestAddStepOneToManyError(t *testing.T) {
	t.Parallel()

	var got []int

	ctx := context.Background()
	pipe, err := pipeline.New(ctx)
	require.NoError(t, err)
	step := model.Step[int]{
		Output: createInputChan(t, 10),
	}
	outputChan, err := pipeline.AddStepOneToMany(pipe, "root step", &step, func(ctx context.Context, input int) ([]int, error) {
		if input == 5 {
			return []int{0}, assert.AnError
		}

		return []int{input}, nil
	})

	require.NoError(t, err)

	done := make(chan struct{})

	go func() {
		got = processOutputChan(t, outputChan.Output)
		done <- struct{}{}
	}()

	err = pipe.Run()
	require.Error(t, err)
	<-done

	_ = got
}

func TestAddStepOneToManyCancel(t *testing.T) {
	t.Parallel()

	var got []int

	ctx, cancel := context.WithCancel(context.Background())
	pipe, err := pipeline.New(ctx)
	require.NoError(t, err)
	step := model.Step[int]{
		Output: createInputChanWithCancel(t, 10, 5, cancel),
	}
	outputChan, err := pipeline.AddStepOneToMany(pipe, "root step", &step, func(ctx context.Context, input int) ([]int, error) {
		select {
		case <-ctx.Done():
			return []int{0}, ctx.Err()
		default:
			return []int{input}, nil
		}
	})
	require.NoError(t, err)

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
	t.Parallel()

	_, err := pipeline.AddSplitter(nil, "root step", (*model.Step[int])(nil), 5)
	require.Error(t, err)
	require.Error(t, err)
}

func TestAddSplitterNilInput(t *testing.T) {
	t.Parallel()

	pipe, err := pipeline.New(context.Background())
	require.NoError(t, err)
	_, err = pipeline.AddSplitter(pipe, "root step", (*model.Step[int])(nil), 5)
	require.Error(t, err)
}

func TestAddSplitterZero(t *testing.T) {
	t.Parallel()

	pipe, err := pipeline.New(context.Background())
	require.NoError(t, err)

	step := model.Step[int]{
		Output: make(chan int),
	}
	_, err = pipeline.AddSplitter(pipe, "root step", &step, 0)
	assert.Error(t, err)
}

func TestAddSplitter(t *testing.T) {
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

			ctx := context.Background()
			pipe, err := pipeline.New(ctx)
			require.NoError(t, err)
			step := model.Step[int]{
				Output: createInputChan(t, 10),
			}
			splitter, err := pipeline.AddSplitter(pipe, "root step", &step, 2, pipeline.SplitterBufferSize[int](tc.buffersize))
			require.NoError(t, err)

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
			require.NoError(t, err)
			wg.Wait()
			assert.ElementsMatch(t, expected, got1)
			assert.ElementsMatch(t, expected, got2)
		})
	}
}

func TestAddSplitterCancel(t *testing.T) {
	t.Parallel()

	var got1, got2 []int

	ctx, cancel := context.WithCancel(context.Background())
	pipe, err := pipeline.New(ctx)
	require.NoError(t, err)
	step := model.Step[int]{
		Output: createInputChanWithCancel(t, 10, 5, cancel),
	}
	splitter, err := pipeline.AddSplitter(pipe, "root step", &step, 2)
	require.NoError(t, err)

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
	require.Error(t, err)
	wg.Wait()
	// Otherwise the compiler ignores the output channel and checks the ctx.
	_ = got1
	_ = got2
}

func TestAddSinkNilPipe(t *testing.T) {
	t.Parallel()

	err := pipeline.AddSink(nil, "root step", nil, func(ctx context.Context, input <-chan int) error {
		for i := range input {
			_ = i
		}

		return nil
	})
	assert.Error(t, err)
}

func TestAddSinkNilInput(t *testing.T) {
	t.Parallel()

	pipe, err := pipeline.New(context.Background())
	require.NoError(t, err)
	err = pipeline.AddSink(pipe, "root step", nil, func(ctx context.Context, input <-chan int) error {
		for i := range input {
			_ = i
		}

		return nil
	})
	require.Error(t, err)
}

func TestAddSink(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	got := []int{}
	pipe, err := pipeline.New(ctx)
	require.NoError(t, err)
	step := model.Step[int]{
		Output: createInputChan(t, 10),
	}
	err = pipeline.AddSink(pipe, "root step", &step, func(ctx context.Context, input int) error {
		got = append(got, input)

		return nil
	})
	require.NoError(t, err)
	err = pipe.Run()
	require.NoError(t, err)
	assert.ElementsMatch(t, []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, got)
}

func TestAddSinkError(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	got := []int{}
	pipe, err := pipeline.New(ctx)
	require.NoError(t, err)
	step := model.Step[int]{
		Output: createInputChan(t, 10),
	}
	err = pipeline.AddSink(pipe, "root step", &step, func(ctx context.Context, input int) error {
		if input == 5 {
			return assert.AnError
		}

		got = append(got, input)

		return nil
	})
	require.NoError(t, err)
	err = pipe.Run()
	assert.Error(t, err)
}

func TestAddMerger(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	got := []int{}
	pipe, err := pipeline.New(ctx)
	require.NoError(t, err)
	step1 := model.Step[int]{
		Details: &model.StepInfo{},
		Output:  createInputChan(t, 5),
	}

	step2 := model.Step[int]{
		Details: &model.StepInfo{},
		Output:  createInputChan(t, 5),
	}

	outputChan, err := pipeline.AddMerger(pipe, "merge step", &step1, &step2)
	require.NoError(t, err)

	done := make(chan struct{})

	go func() {
		got = processOutputChan(t, outputChan.Output)
		done <- struct{}{}
	}()

	err = pipe.Run()
	require.NoError(t, err)
	<-done
	assert.ElementsMatch(t, []int{0, 1, 2, 3, 4, 0, 1, 2, 3, 4}, got)
}

func buildPipeline(t *testing.T, pipe *pipeline.Pipeline, prefix string, conc int) {
	t.Helper()

	rootChan, err := pipeline.AddRootStep(pipe, prefix+" - root step", func(ctx context.Context, rootChan chan<- int) error {
		for i := range 5 {
			rootChan <- i
		}

		return nil
	})
	require.NoError(t, err)
	step1Chan, err := pipeline.AddStepOneToOne(pipe, prefix+" - step 1", rootChan, func(ctx context.Context, input int) (int, error) {
		// time.Sleep(100 * time.Millisecond)
		return input * 10, nil
	}, pipeline.StepConcurrency[int](conc))
	require.NoError(t, err)

	splitter, err := pipeline.AddSplitter(pipe, prefix+" - split step 1", step1Chan, 2, pipeline.SplitterBufferSize[int](200))
	require.NoError(t, err)

	split1Chan1, ok := splitter.Get()
	assert.True(t, ok)

	step21Chan, err := pipeline.AddStepOneToOne(pipe, prefix+" - step2 (1)", split1Chan1, func(ctx context.Context, input int) (int, error) {
		time.Sleep(20 * time.Millisecond)

		return input * 10, nil
	}, pipeline.StepConcurrency[int](conc))
	require.NoError(t, err)

	split2Chan1, ok := splitter.Get()
	assert.True(t, ok)

	step22Chan, err := pipeline.AddStepOneToOne(pipe, prefix+" - step2 (2)", split2Chan1, func(ctx context.Context, input int) (int, error) {
		time.Sleep(100 * time.Millisecond)

		return input * 100, nil
	}, pipeline.StepConcurrency[int](conc))
	require.NoError(t, err)

	outputChan, err := pipeline.AddMerger(pipe, prefix+" - merger", step21Chan, step22Chan)
	require.NoError(t, err)

	err = pipeline.AddSink(pipe, prefix+" - sink", outputChan, func(ctx context.Context, input int) error {
		// time.Sleep(100 * time.Millisecond)
		_ = input

		return nil
	})
	require.NoError(t, err)
}

func TestCompletePipeline(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	m := measure.NewDefaultMeasure()
	pipe, err := pipeline.New(ctx, drawer.PipelineDrawer(drawer.NewSVGDrawer("./mygraph.gv"), m), measure.PipelineMeasure(m))
	require.NoError(t, err)
	buildPipeline(t, pipe, "A", 10)
	buildPipeline(t, pipe, "B", 20)
	err = pipe.Run()
	require.NoError(t, err)
}

func TestSimplePipeline(t *testing.T) {
	t.Parallel()

	// conc := 1
	ctx := context.Background()
	m := measure.NewDefaultMeasure()
	pipe, err := pipeline.New(ctx, drawer.PipelineDrawer(drawer.NewSVGDrawer("./mygraph-simple.gv"), m), measure.PipelineMeasure(m))
	require.NoError(t, err)
	rootChan, err := pipeline.AddRootStep(pipe, "root step", func(ctx context.Context, rootChan chan<- int) error {
		for i := range 10 {
			rootChan <- i
		}

		return nil
	})
	require.NoError(t, err)
	step1Chan, err := pipeline.AddStepOneToOne(pipe, "step 1", rootChan, func(ctx context.Context, input int) (int, error) {
		time.Sleep(100 * time.Millisecond)

		return input * 100, nil
	}, pipeline.StepConcurrency[int](1))
	require.NoError(t, err)

	step2Chan, err := pipeline.AddStepOneToOne(pipe, "step 2", step1Chan, func(ctx context.Context, input int) (int, error) {
		time.Sleep(200 * time.Millisecond)

		return input * 200, nil
	}, pipeline.StepConcurrency[int](1))
	require.NoError(t, err)
	err = pipeline.AddSink(pipe, "sink", step2Chan, func(ctx context.Context, input int) error {
		_ = input

		return nil
	})
	require.NoError(t, err)
	err = pipe.Run()
	require.NoError(t, err)
}

func TestSimpleSplitterPipeline(t *testing.T) {
	t.Parallel()

	conc := 1
	ctx := context.Background()
	m := measure.NewDefaultMeasure()
	pipe, err := pipeline.New(ctx, drawer.PipelineDrawer(drawer.NewSVGDrawer("./mygraph-simple-splitter.gv"), m), measure.PipelineMeasure(m))
	require.NoError(t, err)
	rootChan, err := pipeline.AddRootStep(pipe, "root step", func(ctx context.Context, rootChan chan<- int) error {
		for i := range 10 {
			rootChan <- i
		}

		return nil
	})
	require.NoError(t, err)
	step1Chan, err := pipeline.AddStepOneToOne(pipe, "step 1", rootChan, func(ctx context.Context, input int) (int, error) {
		return input * 100, nil
	}, pipeline.StepConcurrency[int](conc))
	require.NoError(t, err)

	splitterChans, err := pipeline.AddSplitter(pipe, "step 2", step1Chan, 2,
		pipeline.SplitterBufferSize[int](10),
	)
	require.NoError(t, err)

	splitterChan1, _ := splitterChans.Get()
	splitterChan2, _ := splitterChans.Get()

	err = pipeline.AddSink(pipe, "sink 1", splitterChan1, func(ctx context.Context, input int) error {
		time.Sleep(200 * time.Millisecond)

		_ = input

		return nil
	})
	require.NoError(t, err)

	err = pipeline.AddSink(pipe, "sink 2", splitterChan2, func(ctx context.Context, input int) error {
		time.Sleep(100 * time.Millisecond)

		_ = input

		return nil
	})
	require.NoError(t, err)
	err = pipe.Run()
	require.NoError(t, err)
}

func TestSimpleSplitterV2Pipeline(t *testing.T) {
	t.Parallel()

	conc := 1
	ctx := context.Background()
	m := measure.NewDefaultMeasure()
	pipe, err := pipeline.New(
		ctx,
		drawer.PipelineDrawer(drawer.NewSVGDrawer("./mygraph-simple-splitter-v2.gv"), m),
		measure.PipelineMeasure(m),
	)
	require.NoError(t, err)
	rootChan, err := pipeline.AddRootStep(pipe, "root step", func(ctx context.Context, rootChan chan<- int) error {
		for i := range 10 {
			rootChan <- i
		}

		return nil
	})
	require.NoError(t, err)
	step1Chan, err := pipeline.AddStepOneToOne[int, []int](pipe, "step 1", rootChan, func(ctx context.Context, input int) ([]int, error) {
		return []int{input * 100}, nil
	}, pipeline.StepConcurrency[[]int](conc))
	require.NoError(t, err)

	step2Chan, err := pipeline.AddStepOneToOne(pipe, "step 2", step1Chan, func(ctx context.Context, input []int) (int, error) {
		return input[0] * 100, nil
	}, pipeline.StepConcurrency[int](conc))
	require.NoError(t, err)

	splitterChans, err := pipeline.AddSplitter(pipe, "splitter", step2Chan, 2,
		pipeline.SplitterBufferSize[int](1),
	)
	require.NoError(t, err)

	splitterChan1, _ := splitterChans.Get()
	splitterChan2, _ := splitterChans.Get()
	err = pipeline.AddSink(pipe, "sink 1", splitterChan1, func(ctx context.Context, input int) error {
		time.Sleep(200 * time.Millisecond)

		_ = input

		return nil
	})
	require.NoError(t, err)
	err = pipeline.AddSink(pipe, "sink 2", splitterChan2, func(ctx context.Context, input int) error {
		time.Sleep(100 * time.Millisecond)

		_ = input

		return nil
	})
	require.NoError(t, err)
	err = pipe.Run()
	require.NoError(t, err)
}

func TestSimpleSplitterV3Pipeline(t *testing.T) {
	t.Parallel()

	conc := 1
	ctx := context.Background()
	m := measure.NewDefaultMeasure()
	pipe, err := pipeline.New(
		ctx,
		drawer.PipelineDrawer(drawer.NewSVGDrawer("./mygraph-simple-splitter-v3.gv"), m),
		measure.PipelineMeasure(m),
	)
	require.NoError(t, err)
	rootChan, err := pipeline.AddRootStep(pipe, "root step", func(ctx context.Context, rootChan chan<- int) error {
		for i := range 10 {
			rootChan <- i
		}

		return nil
	})
	require.NoError(t, err)
	step1Chan, err := pipeline.AddStepOneToOne[int, []int](pipe, "step 1", rootChan, func(ctx context.Context, input int) ([]int, error) {
		return []int{input * 100}, nil
	}, pipeline.StepConcurrency[[]int](conc))
	require.NoError(t, err)

	step2Chan, err := pipeline.AddStepFromChan(pipe, "step 2", step1Chan,
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
	require.NoError(t, err)

	splitterChans, err := pipeline.AddSplitter(pipe, "splitter", step2Chan, 2,
		pipeline.SplitterBufferSize[int](10),
	)
	require.NoError(t, err)

	splitterChan1, _ := splitterChans.Get()
	splitterChan2, _ := splitterChans.Get()
	err = pipeline.AddSink(pipe, "sink 1", splitterChan1, func(ctx context.Context, input int) error {
		time.Sleep(200 * time.Millisecond)

		_ = input

		return nil
	})
	require.NoError(t, err)
	err = pipeline.AddSink(pipe, "sink 2", splitterChan2, func(ctx context.Context, input int) error {
		time.Sleep(100 * time.Millisecond)

		_ = input

		return nil
	})
	require.NoError(t, err)
	err = pipe.Run()
	require.NoError(t, err)
}

func TestSimpleSplitterV4Pipeline(t *testing.T) {
	t.Parallel()

	conc := 2
	ctx := context.Background()
	m := measure.NewDefaultMeasure()
	pipe, err := pipeline.New(
		ctx,
		drawer.PipelineDrawer(drawer.NewSVGDrawer("./mygraph-simple-splitter-v4.gv"), m),
		measure.PipelineMeasure(m),
	)
	require.NoError(t, err)
	rootChan, err := pipeline.AddRootStep(pipe, "root step", func(ctx context.Context, rootChan chan<- int) error {
		for i := range 10 {
			rootChan <- i
		}

		return nil
	})
	require.NoError(t, err)

	step0Chan, err := pipeline.AddStepOneToOne(pipe, "step 0", rootChan, func(ctx context.Context, input int) (int, error) {
		return input, nil
	}, pipeline.StepConcurrency[int](conc))
	require.NoError(t, err)

	step1Chan, err := pipeline.AddStepOneToMany(pipe, "step 1", step0Chan, func(ctx context.Context, input int) ([]int, error) {
		return []int{input * 100}, nil
	}, pipeline.StepConcurrency[int](conc))
	require.NoError(t, err)

	splitterChans, err := pipeline.AddSplitter(pipe, "splitter", step1Chan, 2,
		pipeline.SplitterBufferSize[int](1),
	)
	require.NoError(t, err)

	splitterChan1, _ := splitterChans.Get()
	splitterChan2, _ := splitterChans.Get()

	splittedChan1, err := pipeline.AddStepOneToOne(pipe, "splitted step 1", splitterChan1, func(ctx context.Context, input int) (int, error) {
		time.Sleep(200 * time.Millisecond)

		return input / 100, nil
	}, pipeline.StepConcurrency[int](conc))
	require.NoError(t, err)

	err = pipeline.AddSinkFromChan(pipe, "sink 1", splittedChan1, func(ctx context.Context, input <-chan int) error {
		for elem := range input {
			_ = elem
		}

		return nil
	})
	require.NoError(t, err)

	splittedChan2, err := pipeline.AddStepOneToMany(pipe, "splitted step 2", splitterChan2,
		func(ctx context.Context, input int) ([]int, error) {
			return []int{input / 100}, nil
		}, pipeline.StepConcurrency[int](conc))
	require.NoError(t, err)

	err = pipeline.AddSink(pipe, "sink 2", splittedChan2, func(ctx context.Context, input int) error {
		_ = input

		return nil
	})
	require.NoError(t, err)
	err = pipe.Run()
	require.NoError(t, err)
}
