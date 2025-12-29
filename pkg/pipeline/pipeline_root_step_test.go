package pipeline_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/askiada/go-pipeline/pkg/pipeline"
)

func TestRootNilPipe(t *testing.T) {
	t.Parallel()

	outputChan := pipeline.Root(nil, "root step", func(ctx context.Context, rootChan chan<- int) error {
		for i := range 10 {
			rootChan <- i
		}

		return nil
	})
	assert.Nil(t, outputChan)
}

func TestRoot(t *testing.T) {
	t.Parallel()

	pipe, err := pipeline.New(t.Context(), pipeline.PipelineDefaults{})
	require.NoError(t, err)

	var got []int

	outputChan := pipeline.Root(pipe, "root step", func(ctx context.Context, rootChan chan<- int) error {
		for i := range 10 {
			rootChan <- i
		}

		return nil
	})
	require.NotNil(t, outputChan)

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

func TestRootError(t *testing.T) {
	t.Parallel()

	pipe, err := pipeline.New(t.Context(), pipeline.PipelineDefaults{})
	require.NoError(t, err)

	var got []int

	outputChan := pipeline.Root(pipe, "root step", func(ctx context.Context, rootChan chan<- int) error {
		for i := range 10 {
			if i == 5 {
				return assert.AnError
			}

			rootChan <- i
		}

		return nil
	})
	require.NotNil(t, outputChan)

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

func TestRootCancel(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(t.Context())
	pipe, err := pipeline.New(ctx, pipeline.PipelineDefaults{})
	require.NoError(t, err)

	var got []int

	outputChan := pipeline.Root(pipe, "root step", func(ctx context.Context, rootChan chan<- int) error {
		for i := range 10 {
			if i == 5 {
				cancel()

				return assert.AnError
			}

			rootChan <- i
		}

		return nil
	})

	require.NotNil(t, outputChan)

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

func TestRootNoCloseNilPipe(t *testing.T) {
	t.Parallel()

	outputChan := pipeline.Root(nil, "root step", func(ctx context.Context, rootChan chan<- int) error {
		defer close(rootChan)

		for i := range 10 {
			rootChan <- i
		}

		return nil
	}, pipeline.StepKeepOpen[int]())
	assert.Nil(t, outputChan)
}

func TestRootNoClose(t *testing.T) {
	t.Parallel()

	pipe, err := pipeline.New(t.Context(), pipeline.PipelineDefaults{})
	require.NoError(t, err)

	var got []int

	outputChan := pipeline.Root(pipe, "root step", func(ctx context.Context, rootChan chan<- int) error {
		defer close(rootChan)

		for i := range 10 {
			rootChan <- i
		}

		return nil
	}, pipeline.StepKeepOpen[int]())
	require.NotNil(t, outputChan)

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

func TestRootNoCloseError(t *testing.T) {
	t.Parallel()

	pipe, err := pipeline.New(t.Context(), pipeline.PipelineDefaults{})
	require.NoError(t, err)

	var got []int

	outputChan := pipeline.Root(pipe, "root step", func(ctx context.Context, rootChan chan<- int) error {
		defer close(rootChan)

		for i := range 10 {
			if i == 5 {
				return assert.AnError
			}

			rootChan <- i
		}

		return nil
	}, pipeline.StepKeepOpen[int]())
	require.NotNil(t, outputChan)

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

func TestRootNoCloseCancel(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(t.Context())
	pipe, err := pipeline.New(ctx, pipeline.PipelineDefaults{})
	require.NoError(t, err)

	var got []int

	outputChan := pipeline.Root(pipe, "root step", func(ctx context.Context, rootChan chan<- int) error {
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
	require.NotNil(t, outputChan)

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
