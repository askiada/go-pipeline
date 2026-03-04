package measure_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/askiada/go-pipeline/v2/pkg/pipeline"
	"github.com/askiada/go-pipeline/v2/pkg/pipeline/measure"
	"github.com/askiada/go-pipeline/v2/pkg/pipeline/model"
)

func TestPipelineMeasureNonMetricHooksNoop(t *testing.T) {
	t.Parallel()

	msr := measure.NewDefaultMeasure()
	opt := measure.PipelineMeasure(msr)

	parent := &model.StepInfo{Name: "parent", Concurrent: 1}
	step := &model.StepInfo{Name: "step", Concurrent: 1}

	require.NoError(t, opt.New())
	require.NoError(t, opt.OnStepOutput(parent, step))
	require.NoError(t, opt.OnSplitterOutput(parent, step))
	require.NoError(t, opt.OnMergerOutput(parent, step))
	require.NoError(t, opt.OnSinkOutput(parent, step))
	require.NoError(t, opt.AfterSink(step))
	require.NoError(t, opt.Finish())
}

func TestPipelineMeasureRetryHookUpdatesMetric(t *testing.T) {
	t.Parallel()

	msr := measure.NewDefaultMeasure()
	opt := measure.PipelineMeasure(msr)

	parent := &pipeline.StepInfo{Name: "parent", Concurrent: 1}
	step := &pipeline.StepInfo{Name: "step", Concurrent: 1}

	require.NoError(t, opt.New())
	require.NoError(t, opt.PrepareStep(parent, step))

	retryObserver, ok := any(opt).(interface {
		OnStepRetry(parentStep, step *pipeline.StepInfo, attempt int, computationDuration time.Duration) error
	})
	require.True(t, ok)

	require.NoError(t, retryObserver.OnStepRetry(parent, step, 1, 10*time.Millisecond))

	metric := msr.GetMetric(step.Name)
	require.NotNil(t, metric)

	retryMetric, ok := metric.(measure.RetryMetric)
	require.True(t, ok)
	require.Equal(t, int64(1), retryMetric.RetryCount())
	require.Equal(t, 10*time.Millisecond, retryMetric.AVGRetryDuration())
}
