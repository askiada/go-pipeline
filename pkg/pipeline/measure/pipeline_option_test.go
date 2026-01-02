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

	metricsOpt, ok := opt.(model.PipelineMetricsOption)
	require.True(t, ok)
	require.NoError(t, metricsOpt.OnStepOutputMetrics(parent, step, iteration, computation))
	require.NoError(t, metricsOpt.OnSplitterOutputMetrics(parent, splitter, iteration, computation))
	require.NoError(t, metricsOpt.OnMergerOutputMetrics(parent, merger, iteration))
	require.NoError(t, metricsOpt.OnSinkOutputMetrics(parent, sink, iteration, computation))
	require.NoError(t, metricsOpt.AfterSinkMetrics(sink, total))

	stepMetric := msr.GetMetric(step.Name)
	require.NotNil(t, stepMetric)
	require.Equal(t, computation, stepMetric.AVGDuration())
	require.Equal(t, iteration, stepMetric.AVGTransportDuration()[parent.Name].Elapsed)

	sinkMetric := msr.GetMetric(sink.Name)
	require.NotNil(t, sinkMetric)
	require.Equal(t, total, sinkMetric.GetTotalDuration())
}

func TestPipelineMeasureDropHooks(t *testing.T) {
	t.Parallel()

	msr := measure.NewDefaultMeasure()
	opt := measure.PipelineMeasure(msr)

	parent := &model.StepInfo{Name: "parent", Concurrent: 1}
	step := &model.StepInfo{Name: "step", Concurrent: 1}

	require.NoError(t, opt.New())
	require.NoError(t, opt.PrepareStep(parent, step))

	dropObserver, ok := opt.(model.StepDropObserver)
	require.True(t, ok)
	require.NoError(t, dropObserver.OnStepDrop(step, model.StepDropBufferFull))
	require.NoError(t, dropObserver.OnStepDrop(step, model.StepDropError))

	routeObserver, ok := opt.(model.StepErrorRouteObserver)
	require.True(t, ok)
	require.NoError(t, routeObserver.OnStepErrorRoute(step))

	metric := msr.GetMetric(step.Name)
	dropMetric, ok := metric.(measure.DropMetric)
	require.True(t, ok)
	require.Equal(t, int64(1), dropMetric.DropCount(model.StepDropBufferFull))
	require.Equal(t, int64(1), dropMetric.DropCount(model.StepDropError))
	require.Equal(t, int64(2), dropMetric.TotalDropCount())
	require.Equal(t, int64(1), dropMetric.RoutedErrorCount())
}
