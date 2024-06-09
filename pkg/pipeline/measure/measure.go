package measure

import (
	"sync"
)

// DefaultMeasure is a default implementation of the Measure interface.
type DefaultMeasure struct {
	mu    sync.Mutex
	Steps map[string]Metric
}

// NewDefaultMeasure creates a new default measure.
func NewDefaultMeasure() *DefaultMeasure {
	return &DefaultMeasure{
		Steps: make(map[string]Metric),
	}
}

// AddMetric adds a metric.
func (m *DefaultMeasure) AddMetric(name string, concurrent int) Metric { //nolint:ireturn // it must implement the interface
	mt := &DefaultMetric{
		mu:            &sync.Mutex{},
		allTransports: make(map[string]*TransportInfo),
		concurrent:    concurrent,
	}

	m.mu.Lock()

	m.Steps[name] = mt
	m.mu.Unlock()

	return mt
}

// GetMetric returns the metric.
func (m *DefaultMeasure) GetMetric(name string) Metric { //nolint:ireturn // it must implement the interface
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.Steps[name]
}

// AllMetrics returns all metrics.
func (m *DefaultMeasure) AllMetrics() map[string]Metric {
	return m.Steps
}

var _ Measure = (*DefaultMeasure)(nil)
