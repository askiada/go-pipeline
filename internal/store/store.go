package store

import (
	"fmt"
	"sync"

	"github.com/dominikbraun/graph"
)

type CustomStore[K comparable, T any] interface {
	graph.Store[K, T]
	UpdateVertex(k K, options ...func(*graph.VertexProperties))
}

type MemoryStore[K comparable, T any] struct {
	lock             sync.RWMutex
	vertices         map[K]T
	vertexProperties map[K]*graph.VertexProperties

	// outEdges and inEdges store all outgoing and ingoing edges for all vertices. For O(1) access,
	// these edges themselves are stored in maps whose keys are the hashes of the target vertices.
	outEdges map[K]map[K]graph.Edge[K] // source -> target
	inEdges  map[K]map[K]graph.Edge[K] // target -> source
}

func NewMemoryStore[K comparable, T any]() CustomStore[K, T] {
	return &MemoryStore[K, T]{
		vertices:         make(map[K]T),
		vertexProperties: make(map[K]*graph.VertexProperties),
		outEdges:         make(map[K]map[K]graph.Edge[K]),
		inEdges:          make(map[K]map[K]graph.Edge[K]),
	}
}

func (s *MemoryStore[K, T]) AddVertex(k K, t T, p graph.VertexProperties) error {
	s.lock.Lock()
	defer s.lock.Unlock()

	if _, ok := s.vertices[k]; ok {
		return graph.ErrVertexAlreadyExists
	}

	s.vertices[k] = t
	s.vertexProperties[k] = &p

	return nil
}

func (s *MemoryStore[K, T]) ListVertices() ([]K, error) {
	s.lock.RLock()
	defer s.lock.RUnlock()

	hashes := make([]K, 0, len(s.vertices))
	for k := range s.vertices {
		hashes = append(hashes, k)
	}

	return hashes, nil
}

func (s *MemoryStore[K, T]) VertexCount() (int, error) {
	s.lock.RLock()
	defer s.lock.RUnlock()

	return len(s.vertices), nil
}

func (s *MemoryStore[K, T]) Vertex(k K) (T, graph.VertexProperties, error) {
	s.lock.RLock()
	defer s.lock.RUnlock()

	v, ok := s.vertices[k]
	if !ok {
		return v, graph.VertexProperties{}, graph.ErrVertexNotFound
	}

	p := s.vertexProperties[k]

	return v, *p, nil
}

func (s *MemoryStore[K, T]) RemoveVertex(k K) error {
	s.lock.RLock()
	defer s.lock.RUnlock()

	if _, ok := s.vertices[k]; !ok {
		return graph.ErrVertexNotFound
	}

	if edges, ok := s.inEdges[k]; ok {
		if len(edges) > 0 {
			return graph.ErrVertexHasEdges
		}
		delete(s.inEdges, k)
	}

	if edges, ok := s.outEdges[k]; ok {
		if len(edges) > 0 {
			return graph.ErrVertexHasEdges
		}
		delete(s.outEdges, k)
	}

	delete(s.vertices, k)
	delete(s.vertexProperties, k)

	return nil
}

func (s *MemoryStore[K, T]) AddEdge(sourceHash, targetHash K, edge graph.Edge[K]) error {
	s.lock.Lock()
	defer s.lock.Unlock()

	if _, ok := s.outEdges[sourceHash]; !ok {
		s.outEdges[sourceHash] = make(map[K]graph.Edge[K])
	}

	s.outEdges[sourceHash][targetHash] = edge

	if _, ok := s.inEdges[targetHash]; !ok {
		s.inEdges[targetHash] = make(map[K]graph.Edge[K])
	}

	s.inEdges[targetHash][sourceHash] = edge

	return nil
}

func (s *MemoryStore[K, T]) UpdateVertex(k K, options ...func(*graph.VertexProperties)) {
	s.lock.RLock()
	defer s.lock.RUnlock()

	for _, opt := range options {
		opt(s.vertexProperties[k])
	}
}

func (s *MemoryStore[K, T]) UpdateEdge(sourceHash, targetHash K, edge graph.Edge[K]) error {
	if _, err := s.Edge(sourceHash, targetHash); err != nil {
		return err
	}

	s.lock.Lock()
	defer s.lock.Unlock()

	s.outEdges[sourceHash][targetHash] = edge
	s.inEdges[targetHash][sourceHash] = edge

	return nil
}

func (s *MemoryStore[K, T]) RemoveEdge(sourceHash, targetHash K) error {
	s.lock.Lock()
	defer s.lock.Unlock()

	delete(s.inEdges[targetHash], sourceHash)
	delete(s.outEdges[sourceHash], targetHash)
	return nil
}

func (s *MemoryStore[K, T]) Edge(sourceHash, targetHash K) (graph.Edge[K], error) {
	s.lock.RLock()
	defer s.lock.RUnlock()

	sourceEdges, ok := s.outEdges[sourceHash]
	if !ok {
		return graph.Edge[K]{}, graph.ErrEdgeNotFound
	}

	edge, ok := sourceEdges[targetHash]
	if !ok {
		return graph.Edge[K]{}, graph.ErrEdgeNotFound
	}

	return edge, nil
}

func (s *MemoryStore[K, T]) ListEdges() ([]graph.Edge[K], error) {
	s.lock.RLock()
	defer s.lock.RUnlock()

	res := make([]graph.Edge[K], 0)
	for _, edges := range s.outEdges {
		for _, edge := range edges {
			res = append(res, edge)
		}
	}
	return res, nil
}

// CreatesCycle is a fastpath version of [CreatesCycle] that avoids calling
// [PredecessorMap], which generates large amounts of garbage to collect.
//
// Because CreatesCycle doesn't need to modify the PredecessorMap, we can use
// inEdges instead to compute the same thing without creating any copies.
func (s *MemoryStore[K, T]) CreatesCycle(source, target K) (bool, error) {
	if _, _, err := s.Vertex(source); err != nil {
		return false, fmt.Errorf("could not get vertex with hash %v: %w", source, err)
	}

	if _, _, err := s.Vertex(target); err != nil {
		return false, fmt.Errorf("could not get vertex with hash %v: %w", target, err)
	}

	if source == target {
		return true, nil
	}

	stack := make([]K, 0)
	visited := make(map[K]struct{})

	stack = append(stack, source)
	for len(stack) > 0 {
		currentHash := stack[len(stack)-1]
		stack = stack[:len(stack)-1]

		if _, ok := visited[currentHash]; !ok {
			// If the adjacent vertex also is the target vertex, the target is a
			// parent of the source vertex. An edge would introduce a cycle.
			if currentHash == target {
				return true, nil
			}

			visited[currentHash] = struct{}{}

			for adjacency := range s.inEdges[currentHash] {
				stack = append(stack, adjacency)
			}
		}
	}

	return false, nil
}
