package pipeline_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/askiada/go-pipeline/pkg/pipeline"
	"github.com/askiada/go-pipeline/pkg/pipeline/model"
)

func TestPipelineDefaultsHooksAreNoop(t *testing.T) {
	t.Parallel()

	defaults := pipeline.PipelineDefaults{}
	step := &model.StepInfo{Name: "step", Concurrent: 1}
	parent := &model.StepInfo{Name: "parent", Concurrent: 1}

	require.NoError(t, defaults.New())
	require.NoError(t, defaults.Finish())
	require.NoError(t, defaults.PrepareStep(parent, step))
	require.NoError(t, defaults.OnStepOutput(parent, step, time.Millisecond, time.Millisecond))
	require.NoError(t, defaults.PrepareSplitter(parent, step))
	require.NoError(t, defaults.OnSplitterOutput(parent, step, time.Millisecond, time.Millisecond))
	require.NoError(t, defaults.PrepareMerger([]*model.StepInfo{parent}, step))
	require.NoError(t, defaults.OnMergerOutput(parent, step, time.Millisecond))
	require.NoError(t, defaults.PrepareSink(parent, step))
	require.NoError(t, defaults.OnSinkOutput(parent, step, time.Millisecond, time.Millisecond))
	require.NoError(t, defaults.AfterSink(step, time.Millisecond))
}
