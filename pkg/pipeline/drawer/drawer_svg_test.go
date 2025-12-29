package drawer_test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/askiada/go-pipeline/pkg/pipeline/drawer"
	"github.com/askiada/go-pipeline/pkg/pipeline/measure"
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
