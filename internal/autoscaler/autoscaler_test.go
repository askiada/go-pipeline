package autoscaler

import (
	"testing"

	"github.com/dominikbraun/graph"
	"github.com/stretchr/testify/assert"
)

func TestXxx(t *testing.T) {
	g := graph.New(graph.IntHash, graph.Directed())

	_ = g.AddVertex(1)
	_ = g.AddVertex(2)
	_ = g.AddVertex(3)
	_ = g.AddVertex(4)
	_ = g.AddVertex(5)

	_ = g.AddEdge(1, 2)
	_ = g.AddEdge(2, 3)
	_ = g.AddEdge(2, 4)
	_ = g.AddEdge(3, 5)
	_ = g.AddEdge(4, 5)

	_, _ = graph.ShortestPath(g, 1, 2)
	assert.False(t, true)
}
