package pipeline

import (
	"bytes"
	"log"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/askiada/go-pipeline/v2/pkg/pipeline/model"
)

func TestPipelineDefaultsApplyToSplitter(t *testing.T) {
	t.Parallel()

	defaults := PipelineDefaults{
		SplitterBufferSize: 7,
	}
	pipe, err := New(defaults)
	require.NoError(t, err)

	input := &Step[int]{
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

func TestPipelineDefaultsOverrideSplitterBufferSize(t *testing.T) {
	t.Parallel()

	defaults := PipelineDefaults{
		SplitterBufferSize: 7,
	}
	pipe, err := New(defaults)
	require.NoError(t, err)

	input := &Step[int]{
		Details: &model.StepInfo{
			Name:       "input",
			Concurrent: 1,
		},
		Output: make(chan int),
	}
	close(input.Output)

	splitter := Split(pipe, "split", input, 2, SplitterBufferSize[int](2))
	require.NotNil(t, splitter)
	require.Equal(t, 2, splitter.bufferSize)
}

func TestWarnSplitterBufferSmallLogs(t *testing.T) {
	var buf bytes.Buffer
	origOutput := log.Writer()
	origFlags := log.Flags()

	log.SetOutput(&buf)
	log.SetFlags(0)

	defer func() {
		log.SetOutput(origOutput)
		log.SetFlags(origFlags)
	}()

	warnSplitterBuffer("split", 1, 4)

	require.Contains(t, buf.String(), "smaller than input concurrency")
}

func TestWarnSplitterBufferLargeLogs(t *testing.T) {
	var buf bytes.Buffer
	origOutput := log.Writer()
	origFlags := log.Flags()

	log.SetOutput(&buf)
	log.SetFlags(0)

	defer func() {
		log.SetOutput(origOutput)
		log.SetFlags(origFlags)
	}()

	warnSplitterBuffer("split", 32, 2)

	require.Contains(t, buf.String(), "much larger than input concurrency")
}

func TestSplitterAllowsNilInputDetails(t *testing.T) {
	t.Parallel()

	pipe, err := New(PipelineDefaults{})
	require.NoError(t, err)

	input := &Step[int]{
		Output: make(chan int),
	}
	close(input.Output)

	splitter := Split(pipe, "split", input, 1)
	require.NotNil(t, splitter)
}
