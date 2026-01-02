package measure

import (
	"maps"
	"sync"
)

// DefaultMeasure is an in-memory Measure implementation.
type DefaultMeasure struct {
	mu    sync.Mutex
	Steps map[string]Metric
}

// NewDefaultMeasure creates a new DefaultMeasure.
func NewDefaultMeasure() *DefaultMeasure {
	return &DefaultMeasure{
		Steps: make(map[string]Metric),
	}
}

// AddMetric adds or replaces a metric entry for a step.
func (m *DefaultMeasure) AddMetric(name string, concurrent int) Metric { //nolint:ireturn // it must implement the interface
	if concurrent < 1 {
		concurrent = 1
	}

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

// GetMetric returns the metric for a step name.
func (m *DefaultMeasure) GetMetric(name string) Metric { //nolint:ireturn // it must implement the interface
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.Steps[name]
}

// AllMetrics returns a snapshot of all metrics.
func (m *DefaultMeasure) AllMetrics() map[string]Metric {
	m.mu.Lock()
	defer m.mu.Unlock()

	metrics := make(map[string]Metric, len(m.Steps))
	maps.Copy(metrics, m.Steps)

	return metrics
}

var _ Measure = (*DefaultMeasure)(nil)
