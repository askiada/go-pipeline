package autoscaler

import (
	"github.com/dominikbraun/graph"
)

type AutoScaler[K comparable, T any] struct {
	graph graph.Graph[K, T]
}
