package pipeline

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/askiada/go-pipeline/pkg/pipeline/model"
)

func TestPipelineDefaultsApplyToSplitter(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	defaults := PipelineDefaults{
		SplitterBufferSize: 7,
	}
	pipe, err := New(ctx, defaults)
	require.NoError(t, err)

	input := &model.Step[int]{
		Details: &model.StepInfo{
			Name:       "input",
			Concurrent: 1,
		},
		Output: make(chan int),
	}
	close(input.Output)

	splitter := Split(pipe, "split", input, 2)
	require.NotNil(t, splitter)
	require.Equal(t, 7, splitter.bufferSize)
}
