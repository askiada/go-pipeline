package pipeline_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/askiada/go-pipeline/pkg/pipeline"
	"github.com/askiada/go-pipeline/pkg/pipeline/model"
)

func TestSplitByRoutesItems(t *testing.T) {
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

	splitter := pipeline.SplitBy(pipe, "split", input, []pipeline.SplitFn[int]{
		func(_ context.Context, value int) (bool, error) {
			return value%2 == 0, nil
		},
		func(_ context.Context, value int) (bool, error) {
			return value%2 == 1, nil
		},
	})
	require.NotNil(t, splitter)

	left, ok := splitter.Get()
	require.True(t, ok)
	right, ok := splitter.Get()
	require.True(t, ok)

	leftDone := make(chan []int, 1)
	rightDone := make(chan []int, 1)

	go func() {
		leftDone <- processOutputChan(t, left.Output)
	}()

	go func() {
		rightDone <- processOutputChan(t, right.Output)
	}()

	expected := []int{1, 2, 3, 4}

	go func() {
		for _, item := range expected {
			input.Output <- item
		}

		close(input.Output)
	}()

	require.NoError(t, runPipeline(t, pipe, ctx))

	assert.ElementsMatch(t, []int{2, 4}, <-leftDone)
	assert.ElementsMatch(t, []int{1, 3}, <-rightDone)
}

func TestSplitByReturnsError(t *testing.T) {
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

	expectedErr := errors.New("split failure")
	splitter := pipeline.SplitBy(pipe, "split", input, []pipeline.SplitFn[int]{
		func(_ context.Context, _ int) (bool, error) {
			return true, nil
		},
		func(_ context.Context, value int) (bool, error) {
			if value == 2 {
				return false, expectedErr
			}

			return true, nil
		},
	})
	require.NotNil(t, splitter)

	left, ok := splitter.Get()
	require.True(t, ok)
	right, ok := splitter.Get()
	require.True(t, ok)

	go func() {
		_ = processOutputChan(t, left.Output)
	}()

	go func() {
		_ = processOutputChan(t, right.Output)
	}()

	go func() {
		input.Output <- 1

		input.Output <- 2

		close(input.Output)
	}()

	err = runPipeline(t, pipe, ctx)
	require.Error(t, err)
	assert.ErrorContains(t, err, expectedErr.Error())
}
