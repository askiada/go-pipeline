package measure

import (
	"sync"
)

type DefaultMeasure struct {
	Steps map[string]Metric
}

func NewDefaultMeasure() *DefaultMeasure {
	return &DefaultMeasure{
		Steps: make(map[string]Metric),
	}
}

func (m *DefaultMeasure) AddMetric(name string, concurrent int) Metric {
	mt := &DefaultMetric{
		mu:            &sync.Mutex{},
		allTransports: make(map[string]*TransportInfo),
		concurrent:    concurrent,
	}
	m.Steps[name] = mt

	return mt
}

func (m *DefaultMeasure) GetMetric(name string) Metric {
	return m.Steps[name]
}

func (m *DefaultMeasure) AllMetrics() map[string]Metric {
	return m.Steps
}

var _ Measure = (*DefaultMeasure)(nil)
