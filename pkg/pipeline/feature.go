package pipeline

import (
	"time"

	"github.com/askiada/go-pipeline/internal/store"
	"github.com/dominikbraun/graph"
)

type feature[K comparable, T any] struct {
	drawer  *drawer
	measure *measure
	store   store.CustomStore[K, T]
	graph   graph.Graph[K, T]

	maxAvgStep, maxAvgEdge time.Duration
}

func (f *feature[K, T]) addStep(name T) error {
	if f.graph == nil {
		return nil
	}
	err := f.graph.AddVertex(name)
	if err != nil {
		return err
	}

	return nil
}

func (f *feature[K, T]) addLink(parentName, childrenName K) error {
	if f.graph == nil {
		return nil
	}
	err := f.graph.AddEdge(parentName, childrenName)
	if err != nil {
		return err
	}

	return nil
}
