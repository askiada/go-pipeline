package measure_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/askiada/go-pipeline/pkg/pipeline/measure"
	"github.com/askiada/go-pipeline/pkg/pipeline/model"
)

func TestPipelineMeasureHooks(t *testing.T) {
	t.Parallel()

	msr := measure.NewDefaultMeasure()
	opt := measure.PipelineMeasure(msr)

	parent := &model.StepInfo{Name: "parent", Concurrent: 1}
	step := &model.StepInfo{Name: "step", Concurrent: 1}
	splitter := &model.StepInfo{Name: "splitter", Concurrent: 1}
	merger := &model.StepInfo{Name: "merger", Concurrent: 1}
	sink := &model.StepInfo{Name: "sink", Concurrent: 1}

	require.NoError(t, opt.New())
	require.NoError(t, opt.PrepareStep(parent, step))
	require.NoError(t, opt.PrepareSplitter(parent, splitter))
	require.NoError(t, opt.PrepareMerger([]*model.StepInfo{parent}, merger))
	require.NoError(t, opt.PrepareSink(parent, sink))

	iteration := 5 * time.Millisecond
	computation := 2 * time.Millisecond
	total := 12 * time.Millisecond

	require.NoError(t, opt.OnStepOutput(parent, step, iteration, computation))
	require.NoError(t, opt.OnSplitterOutput(parent, splitter, iteration, computation))
	require.NoError(t, opt.OnMergerOutput(parent, merger, iteration))
	require.NoError(t, opt.OnSinkOutput(parent, sink, iteration, computation))
	require.NoError(t, opt.AfterSink(sink, total))

	stepMetric := msr.GetMetric(step.Name)
	require.NotNil(t, stepMetric)
	require.Equal(t, computation, stepMetric.AVGDuration())
	require.Equal(t, iteration, stepMetric.AVGTransportDuration()[parent.Name].Elapsed)

	sinkMetric := msr.GetMetric(sink.Name)
	require.NotNil(t, sinkMetric)
	require.Equal(t, total, sinkMetric.GetTotalDuration())
}
