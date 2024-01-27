package pipeline

import (
	"math"
	"sort"

	"github.com/dominikbraun/graph"
)

type flow struct {
	stepName     string
	inEdgeWeight int64
	capacity     int64
}

func (p *Pipeline) MaximumStepTime() ([]flow, error) {
	slowestPath, err := graph.ShortestPath(p.feature.graph, "start", "end")
	if err != nil {
		return nil, err
	}

	flows := make([]flow, len(slowestPath))
	for i, stepName := range slowestPath {
		_, properties, err := p.feature.graph.VertexWithProperties(stepName)
		if err != nil {
			return nil, err
		}
		f := flow{
			stepName: stepName,
		}
		if properties.Weight > 0 {
			f.capacity = int64(p.feature.maxAvgStep) - properties.Weight
		}
		if i > 0 {
			e, err := p.feature.graph.Edge(slowestPath[i-1], stepName)
			if err != nil {
				return nil, err
			}
			if e.Properties.Weight > 0 {
				f.inEdgeWeight = int64(p.feature.maxAvgEdge) - e.Properties.Weight
			}
		}
		flows[i] = f
	}

	sort.Slice(flows, func(i, j int) bool {
		return math.Abs(float64(flows[i].capacity-flows[i].inEdgeWeight)) < math.Abs(float64(flows[j].capacity-flows[j].inEdgeWeight))
	})

	return flows, nil
}
