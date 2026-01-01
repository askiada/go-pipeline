package drawer_test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/askiada/go-pipeline/pkg/pipeline/drawer"
	"github.com/askiada/go-pipeline/pkg/pipeline/measure"
	"github.com/askiada/go-pipeline/pkg/pipeline/model"
)

func TestSVGDrawerDrawWritesDotFile(t *testing.T) {
	t.Parallel()

	outPath := filepath.Join(t.TempDir(), "graph.dot")
	drw := drawer.NewSVGDrawer(outPath)

	require.NoError(t, drw.AddStep("input"))
	require.NoError(t, drw.AddStep("step"))
	require.NoError(t, drw.AddLink("input", "step"))

	msr := measure.NewDefaultMeasure()
	metric := msr.AddMetric("step", 2)
	metric.AddTransportDuration("input", 8*time.Millisecond)
	metric.AddTransportDuration("input", 8*time.Millisecond)

	require.NoError(t, drw.AddMeasure(msr))
	require.NoError(t, drw.Draw())

	data, err := os.ReadFile(outPath)
	require.NoError(t, err)

	content := string(data)
	require.Contains(t, content, "\"input\" -> \"step\"")
	require.Contains(t, content, "label=\"4ms\"")
}

func TestSVGDrawerDrawCreateError(t *testing.T) {
	t.Parallel()

	outPath := filepath.Join(t.TempDir(), "missing", "graph.dot")
	drw := drawer.NewSVGDrawer(outPath)

	err := drw.Draw()
	require.Error(t, err)
}

func TestSVGDrawerSetTotalTimeWritesLabel(t *testing.T) {
	t.Parallel()

	outPath := filepath.Join(t.TempDir(), "graph.dot")
	drw := drawer.NewSVGDrawer(outPath)

	require.NoError(t, drw.AddStep("step"))

	start := time.Now().Add(-time.Hour)
	require.NoError(t, drw.SetTotalTime("step", start))
	require.NoError(t, drw.Draw())

	data, err := os.ReadFile(outPath)
	require.NoError(t, err)

	content := string(data)
	require.Contains(t, content, "\"step\"")
	require.Contains(t, content, "1h")
}

func TestSVGDrawerIncludesRetryMetrics(t *testing.T) {
	t.Parallel()

	outPath := filepath.Join(t.TempDir(), "graph.dot")
	drw := drawer.NewSVGDrawer(outPath)

	require.NoError(t, drw.AddStep("step"))

	msr := measure.NewDefaultMeasure()
	metric := msr.AddMetric("step", 1)
	metric.AddDuration(2 * time.Millisecond)

	retryMetric, ok := metric.(measure.RetryMetric)
	require.True(t, ok)
	retryMetric.AddRetryDuration(3 * time.Millisecond)
	retryMetric.AddRetryDuration(3 * time.Millisecond)

	require.NoError(t, drw.AddMeasure(msr))
	require.NoError(t, drw.Draw())

	data, err := os.ReadFile(outPath)
	require.NoError(t, err)

	content := string(data)
	require.Contains(t, content, "retry avg: 3ms")
	require.Contains(t, content, "retries: 2")
}

func TestSVGDrawerIncludesDropMetrics(t *testing.T) {
	t.Parallel()

	outPath := filepath.Join(t.TempDir(), "graph.dot")
	drw := drawer.NewSVGDrawer(outPath)

	require.NoError(t, drw.AddStep("step"))

	msr := measure.NewDefaultMeasure()
	metric := msr.AddMetric("step", 1)

	dropMetric, ok := metric.(measure.DropMetric)
	require.True(t, ok)
	dropMetric.AddDrop(model.StepDropBufferFull)
	dropMetric.AddDrop(model.StepDropSendTimeout)
	dropMetric.AddDrop(model.StepDropError)
	dropMetric.AddRoutedError()
	dropMetric.AddRoutedError()

	require.NoError(t, drw.AddMeasure(msr))
	require.NoError(t, drw.Draw())

	data, err := os.ReadFile(outPath)
	require.NoError(t, err)

	content := string(data)
	require.Contains(t, content, "drops: 3 (full:1, timeout:1, error:1)")
	require.Contains(t, content, "error routed: 2")
}
