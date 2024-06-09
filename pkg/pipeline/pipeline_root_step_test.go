package pipeline_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/askiada/go-pipeline/pkg/pipeline"
)

func TestAddRootStepNilPipe(t *testing.T) {
	t.Parallel()

	_, err := pipeline.AddRootStep(nil, "root step", func(ctx context.Context, rootChan chan<- int) error {
		for i := range 10 {
			rootChan <- i
		}

		return nil
	})
	assert.Error(t, err)
}

func TestAddRootStep(t *testing.T) {
	t.Parallel()

	pipe, err := pipeline.New(context.Background())
	require.NoError(t, err)

	var got []int

	outputChan, err := pipeline.AddRootStep(pipe, "root step", func(ctx context.Context, rootChan chan<- int) error {
		for i := range 10 {
			rootChan <- i
		}

		return nil
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

func TestAddRootStepError(t *testing.T) {
	t.Parallel()

	pipe, err := pipeline.New(context.Background())
	require.NoError(t, err)

	var got []int

	outputChan, err := pipeline.AddRootStep(pipe, "root step", func(ctx context.Context, rootChan chan<- int) error {
		for i := range 10 {
			if i == 5 {
				return assert.AnError
			}
			rootChan <- i
		}

		return nil
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

func TestAddRootStepCancel(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	pipe, err := pipeline.New(ctx)
	require.NoError(t, err)

	var got []int

	outputChan, err := pipeline.AddRootStep(pipe, "root step", func(ctx context.Context, rootChan chan<- int) error {
		for i := range 10 {
			if i == 5 {
				cancel()

				return assert.AnError
			}
			rootChan <- i
		}

		return nil
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

func TestAddRootStepNoCloseNilPipe(t *testing.T) {
	t.Parallel()

	_, err := pipeline.AddRootStep(nil, "root step", func(ctx context.Context, rootChan chan<- int) error {
		defer close(rootChan)

		for i := range 10 {
			rootChan <- i
		}

		return nil
	}, pipeline.StepKeepOpen[int]())
	assert.Error(t, err)
}

func TestAddRootStepNoClose(t *testing.T) {
	t.Parallel()

	pipe, err := pipeline.New(context.Background())
	require.NoError(t, err)

	var got []int

	outputChan, err := pipeline.AddRootStep(pipe, "root step", func(ctx context.Context, rootChan chan<- int) error {
		defer close(rootChan)

		for i := range 10 {
			rootChan <- i
		}

		return nil
	}, pipeline.StepKeepOpen[int]())
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

func TestAddRootStepNoCloseError(t *testing.T) {
	t.Parallel()

	pipe, err := pipeline.New(context.Background())
	require.NoError(t, err)

	var got []int

	outputChan, err := pipeline.AddRootStep(pipe, "root step", func(ctx context.Context, rootChan chan<- int) error {
		defer close(rootChan)

		for i := range 10 {
			if i == 5 {
				return assert.AnError
			}
			rootChan <- i
		}

		return nil
	}, pipeline.StepKeepOpen[int]())
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

func TestAddRootStepNoCloseCancel(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	pipe, err := pipeline.New(ctx)
	require.NoError(t, err)

	var got []int

	outputChan, err := pipeline.AddRootStep(pipe, "root step", func(ctx context.Context, rootChan chan<- int) error {
		defer close(rootChan)

		for i := range 10 {
			if i == 5 {
				cancel()

				return assert.AnError
			}
			rootChan <- i
		}

		return nil
	}, pipeline.StepKeepOpen[int]())
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
