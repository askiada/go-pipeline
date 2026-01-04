package drawer

import (
	"errors"
	"io"
	"path/filepath"
	"testing"

	"github.com/dominikbraun/graph"
	"github.com/stretchr/testify/require"
)

type adjacencyErrorGraph struct {
	graph.Graph[string, string]

	err error
}

func (g adjacencyErrorGraph) AdjacencyMap() (map[string]map[string]graph.Edge[string], error) {
	return nil, g.err
}

type vertexErrorGraph struct {
	graph.Graph[string, string]

	err error
}

func (g vertexErrorGraph) VertexWithProperties(_ string) (string, graph.VertexProperties, error) {
	return "", graph.VertexProperties{}, g.err
}

type errWriter struct{}

func (errWriter) Write([]byte) (int, error) {
	return 0, errors.New("write failed")
}

func TestDotReturnsAdjacencyError(t *testing.T) {
	t.Parallel()

	base := graph.New(graph.StringHash, graph.Directed())
	expectedErr := errors.New("adjacency failed")

	err := dot(adjacencyErrorGraph{Graph: base, err: expectedErr}, io.Discard)
	require.ErrorIs(t, err, expectedErr)
}

func TestGenerateDOTVertexError(t *testing.T) {
	t.Parallel()

	base := graph.New(graph.StringHash, graph.Directed())
	require.NoError(t, base.AddVertex("a"))

	expectedErr := errors.New("vertex failed")
	_, err := generateDOT(vertexErrorGraph{Graph: base, err: expectedErr})
	require.ErrorIs(t, err, expectedErr)
}

func TestRenderDOTWriterError(t *testing.T) {
	t.Parallel()

	desc := description{
		GraphType:    "graph",
		Attributes:   map[string]string{},
		EdgeOperator: "--",
	}

	err := renderDOT(errWriter{}, desc)
	require.ErrorContains(t, err, "unable to execute template")
}

func TestSVGDrawerDrawReturnsDotError(t *testing.T) {
	t.Parallel()

	outPath := filepath.Join(t.TempDir(), "graph.dot")
	drw := NewSVGDrawer(outPath)
	expectedErr := errors.New("dot failed")

	drw.graph = adjacencyErrorGraph{Graph: drw.graph, err: expectedErr}

	err := drw.Draw()
	require.ErrorIs(t, err, expectedErr)
}
