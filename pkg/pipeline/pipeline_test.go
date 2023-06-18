package pipeline_test

import (
	"context"
	"sync"
	"testing"

	"github.com/askiada/go-pipeline/pkg/pipeline"
	"github.com/stretchr/testify/assert"
)

func TestAddRootStepNilPipe(t *testing.T) {
	_, err := pipeline.AddRootStep(nil, "root step", func(ctx context.Context, rootChan chan<- int) error {
		for i := 0; i < 10; i++ {
			rootChan <- i
		}
		return nil
	})
	assert.Error(t, err)
}

func TestAddRootStep(t *testing.T) {
	pipe := pipeline.New(context.Background())
	var got []int

	outputChan, err := pipeline.AddRootStep(pipe, "root step", func(ctx context.Context, rootChan chan<- int) error {
		for i := 0; i < 10; i++ {
			rootChan <- i
		}
		return nil
	})
	assert.Nil(t, err)
	done := make(chan struct{})
	go func() {
		got = processOutputChan(t, outputChan)
		done <- struct{}{}
	}()
	err = pipe.Run()
	assert.Nil(t, err)
	<-done
	assert.ElementsMatch(t, []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, got)
}

func TestAddRootStepError(t *testing.T) {
	pipe := pipeline.New(context.Background())
	var got []int

	outputChan, err := pipeline.AddRootStep(pipe, "root step", func(ctx context.Context, rootChan chan<- int) error {
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
		got = processOutputChan(t, outputChan)
		done <- struct{}{}
	}()
	err = pipe.Run()
	assert.Error(t, err)
	<-done
	_ = got
}

func TestAddRootStepCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	pipe := pipeline.New(ctx)
	var got []int

	outputChan, err := pipeline.AddRootStep(pipe, "root step", func(ctx context.Context, rootChan chan<- int) error {
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
		got = processOutputChan(t, outputChan)
		done <- struct{}{}
	}()
	err = pipe.Run()
	assert.Error(t, err)
	<-done
	_ = got
}

func TestAddStepNilPipe(t *testing.T) {
	_, err := pipeline.AddStep(nil, "root step", nil, func(ctx context.Context, input <-chan int, output chan<- int) error {
		for i := range input {
			output <- i
		}
		return nil
	})
	assert.Error(t, err)
}

func TestAddStepNilInput(t *testing.T) {
	pipe := pipeline.New(context.Background())
	_, err := pipeline.AddStep(pipe, "root step", nil, func(ctx context.Context, input <-chan int, output chan<- int) error {
		for i := range input {
			output <- i
		}
		return nil
	})
	assert.Error(t, err)
}

func TestAddStep(t *testing.T) {
	var got []int
	ctx := context.Background()
	pipe := pipeline.New(ctx)
	input := createInputChan(t, ctx, 10)
	outputChan, err := pipeline.AddStep(pipe, "root step", input, func(ctx context.Context, input <-chan int, output chan<- int) error {
		for i := range input {
			output <- i
		}
		return nil
	})
	assert.Nil(t, err)
	done := make(chan struct{})
	go func() {
		got = processOutputChan(t, outputChan)
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
	pipe := pipeline.New(ctx)
	input := createInputChan(t, ctx, 10)
	outputChan, err := pipeline.AddStep(pipe, "root step", input, func(ctx context.Context, input <-chan int, output chan<- int) error {
		for i := range input {
			if i == 5 {
				return assert.AnError
			}
			output <- i
		}
		return nil
	})
	assert.Nil(t, err)
	done := make(chan struct{})
	go func() {
		got = processOutputChan(t, outputChan)
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
	pipe := pipeline.New(ctx)
	input := createInputChanWithCancel(t, ctx, 10, 5, cancel)
	outputChan, err := pipeline.AddStep(pipe, "root step", input, func(ctx context.Context, input <-chan int, output chan<- int) error {
		for i := range input {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case output <- i:
			}
		}
		return nil
	})
	assert.Nil(t, err)
	done := make(chan struct{})
	go func() {
		got = processOutputChan(t, outputChan)
		done <- struct{}{}
	}()
	err = pipe.Run()
	assert.Error(t, err)
	<-done
	_ = got
}

func TestAddSplitterNilPipe(t *testing.T) {
	_, err := pipeline.AddSplitter(nil, "root step", (chan int)(nil), 5)
	assert.Error(t, err)
	assert.Error(t, err)
}

func TestAddSplitterNilInput(t *testing.T) {
	pipe := pipeline.New(context.Background())
	_, err := pipeline.AddSplitter(pipe, "root step", (chan int)(nil), 5)
	assert.Error(t, err)
}

func TestAddSplitterZero(t *testing.T) {
	pipe := pipeline.New(context.Background())
	_, err := pipeline.AddSplitter(pipe, "root step", make(chan int), 0)
	assert.Error(t, err)
}

func TestAddSplitter(t *testing.T) {
	var got1, got2 []int
	ctx := context.Background()
	pipe := pipeline.New(ctx)
	intpuChan := createInputChan(t, ctx, 10)
	outputChans, err := pipeline.AddSplitter(pipe, "root step", intpuChan, 2)
	assert.Nil(t, err)
	expected := []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
	wg := sync.WaitGroup{}
	if assert.Equal(t, 2, len(outputChans)) {
		wg.Add(2)
		go func() {
			defer wg.Done()
			got1 = processOutputChan(t, outputChans[0])
		}()
		go func() {
			defer wg.Done()
			got2 = processOutputChan(t, outputChans[1])
		}()
	}
	err = pipe.Run()
	assert.Nil(t, err)
	wg.Wait()
	assert.ElementsMatch(t, expected, got1)
	assert.ElementsMatch(t, expected, got2)
}

func TestAddSplitterCancel(t *testing.T) {
	var got1, got2 []int
	ctx, cancel := context.WithCancel(context.Background())
	pipe := pipeline.New(ctx)
	intpuChan := createInputChanWithCancel(t, ctx, 10, 5, cancel)
	outputChans, err := pipeline.AddSplitter(pipe, "root step", intpuChan, 2)
	assert.Nil(t, err)
	wg := sync.WaitGroup{}
	if assert.Equal(t, 2, len(outputChans)) {
		wg.Add(2)
		go func() {
			defer wg.Done()
			got1 = processOutputChan(t, outputChans[0])
		}()
		go func() {
			defer wg.Done()
			got2 = processOutputChan(t, outputChans[1])
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
	err := pipeline.AddSink(nil, "root step", nil, func(ctx context.Context, input <-chan int) error {
		for i := range input {
			_ = i
		}
		return nil
	})
	assert.Error(t, err)
}

func TestAddSinkNilInput(t *testing.T) {
	pipe := pipeline.New(context.Background())
	err := pipeline.AddSink(pipe, "root step", nil, func(ctx context.Context, input <-chan int) error {
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
	pipe := pipeline.New(ctx)
	inputChan := createInputChan(t, ctx, 10)
	err := pipeline.AddSink(pipe, "root step", inputChan, func(ctx context.Context, input <-chan int) error {
		got = processOutputChan(t, input)
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
	pipe := pipeline.New(ctx)
	inputChan := createInputChan(t, ctx, 10)
	err := pipeline.AddSink(pipe, "root step", inputChan, func(ctx context.Context, input <-chan int) error {
		for out := range inputChan {
			if out == 5 {
				return assert.AnError
			}
			got = append(got, out)
		}
		return nil
	})
	assert.Nil(t, err)
	err = pipe.Run()
	assert.Error(t, err)
}
