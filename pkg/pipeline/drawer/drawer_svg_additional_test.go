package drawer_test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/askiada/go-pipeline/v2/pkg/pipeline/drawer"
	"github.com/askiada/go-pipeline/v2/pkg/pipeline/measure"
)

func TestSVGDrawerAddStepDuplicateReturnsError(t *testing.T) {
	t.Parallel()

	outPath := filepath.Join(t.TempDir(), "graph.dot")
	drw := drawer.NewSVGDrawer(outPath)

	require.NoError(t, drw.AddStep("step"))
	require.Error(t, drw.AddStep("step"))
}

func TestSVGDrawerSetTotalTimeMissingStep(t *testing.T) {
	t.Parallel()

	outPath := filepath.Join(t.TempDir(), "graph.dot")
	drw := drawer.NewSVGDrawer(outPath)

	require.Error(t, drw.SetTotalTime("missing", time.Now()))
}

func TestSVGDrawerAddMeasureWithoutTransports(t *testing.T) {
	t.Parallel()

	outPath := filepath.Join(t.TempDir(), "graph.dot")
	drw := drawer.NewSVGDrawer(outPath)

	require.NoError(t, drw.AddStep("step"))

	msr := measure.NewDefaultMeasure()
	metric := msr.AddMetric("step", 1)
	metric.AddDuration(2 * time.Millisecond)

	require.NoError(t, drw.AddMeasure(msr))
}

func TestSVGDrawerAddMeasureMissingVertex(t *testing.T) {
	t.Parallel()

	outPath := filepath.Join(t.TempDir(), "graph.dot")
	drw := drawer.NewSVGDrawer(outPath)

	msr := measure.NewDefaultMeasure()
	msr.AddMetric("missing", 1)

	require.Error(t, drw.AddMeasure(msr))
}

func TestSVGDrawerAddLinkMissingVertex(t *testing.T) {
	t.Parallel()

	outPath := filepath.Join(t.TempDir(), "graph.dot")
	drw := drawer.NewSVGDrawer(outPath)

	require.NoError(t, drw.AddStep("parent"))
	require.Error(t, drw.AddLink("parent", "child"))
}

func TestSVGDrawerIncludesEndDuration(t *testing.T) {
	t.Parallel()

	outPath := filepath.Join(t.TempDir(), "graph.dot")
	drw := drawer.NewSVGDrawer(outPath)

	require.NoError(t, drw.AddStep("step"))

	msr := measure.NewDefaultMeasure()
	metric := msr.AddMetric("step", 1)
	metric.SetTotalDuration(2 * time.Second)

	require.NoError(t, drw.AddMeasure(msr))
	require.NoError(t, drw.Draw())

	data, err := os.ReadFile(outPath)
	require.NoError(t, err)
	require.Contains(t, string(data), "end: 2s")
}

func TestSVGDrawerAddMeasureMultipleTransportDurations(t *testing.T) {
	t.Parallel()

	outPath := filepath.Join(t.TempDir(), "graph.dot")
	drw := drawer.NewSVGDrawer(outPath)

	require.NoError(t, drw.AddStep("input-a"))
	require.NoError(t, drw.AddStep("input-b"))
	require.NoError(t, drw.AddStep("step"))
	require.NoError(t, drw.AddLink("input-a", "step"))
	require.NoError(t, drw.AddLink("input-b", "step"))

	msr := measure.NewDefaultMeasure()
	metric := msr.AddMetric("step", 1)
	metric.AddTransportDuration("input-a", 4*time.Millisecond)
	metric.AddTransportDuration("input-b", 8*time.Millisecond)

	require.NoError(t, drw.AddMeasure(msr))
}

func TestSVGDrawerAddMeasureDeduplicatesDurations(t *testing.T) {
	t.Parallel()

	outPath := filepath.Join(t.TempDir(), "graph.dot")
	drw := drawer.NewSVGDrawer(outPath)

	require.NoError(t, drw.AddStep("input-a"))
	require.NoError(t, drw.AddStep("input-b"))
	require.NoError(t, drw.AddStep("step"))
	require.NoError(t, drw.AddLink("input-a", "step"))
	require.NoError(t, drw.AddLink("input-b", "step"))

	msr := measure.NewDefaultMeasure()
	metric := msr.AddMetric("step", 1)
	metric.AddTransportDuration("input-a", 4*time.Millisecond)
	metric.AddTransportDuration("input-b", 4*time.Millisecond)

	require.NoError(t, drw.AddMeasure(msr))
}
