package measure_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/askiada/go-pipeline/v2/pkg/pipeline/measure"
	"github.com/askiada/go-pipeline/v2/pkg/pipeline/model"
)

func TestDefaultMetricZeroDurationsAreIgnored(t *testing.T) {
	t.Parallel()

	msr := measure.NewDefaultMeasure()
	metric := msr.AddMetric("step", 1)

	metric.AddDuration(0)

	retryMetric, ok := metric.(measure.RetryMetric)
	require.True(t, ok)
	retryMetric.AddRetryDuration(0)

	metric.AddTransportDuration("input", 0)

	require.Equal(t, time.Duration(0), metric.AVGDuration())
	require.Equal(t, time.Duration(0), retryMetric.AVGRetryDuration())
	require.Equal(t, int64(0), retryMetric.RetryCount())
	require.Empty(t, metric.AVGTransportDuration())
}

func TestDefaultMetricConcurrencyClamp(t *testing.T) {
	t.Parallel()

	msr := measure.NewDefaultMeasure()
	metric := msr.AddMetric("step", 0)
	metric.AddTransportDuration("input", 10*time.Millisecond)

	avg := metric.AVGTransportDuration()
	require.Equal(t, 10*time.Millisecond, avg["input"].Elapsed)
}

func TestDefaultMetricDropCountUnknownKind(t *testing.T) {
	t.Parallel()

	msr := measure.NewDefaultMeasure()
	metric := msr.AddMetric("step", 1)

	dropMetric, ok := metric.(measure.DropMetric)
	require.True(t, ok)
	require.Equal(t, int64(0), dropMetric.DropCount(model.StepDropKind("unknown")))
}

func TestDefaultMetricAVGDurationRounds(t *testing.T) {
	t.Parallel()

	msr := measure.NewDefaultMeasure()
	metric := msr.AddMetric("step", 1)

	metric.AddDuration(1500 * time.Millisecond)
	require.Equal(t, 2*time.Second, metric.AVGDuration())

	metric = msr.AddMetric("step-ms", 1)
	metric.AddDuration(1500 * time.Microsecond)
	require.Equal(t, 2*time.Millisecond, metric.AVGDuration())

	metric = msr.AddMetric("step-us", 1)
	metric.AddDuration(1500 * time.Nanosecond)
	require.Equal(t, 2*time.Microsecond, metric.AVGDuration())
}
