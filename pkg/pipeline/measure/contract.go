package measure

import "time"

// Measure is an interface that defines the methods for measuring the performance of a pipeline.
type Measure interface {
	GetMetric(name string) Metric
	AddMetric(name string, concurrent int) Metric
	AllMetrics() map[string]Metric
}

// Metric is an interface that defines the methods for measuring the performance of a step.
type Metric interface {
	// AddDuration adds the duration.
	AddDuration(elapsed time.Duration)
	// AddTransportDuration adds the transport duration.
	AddTransportDuration(inputStepName string, elapsed time.Duration)
	// AVGDuration returns the average duration.
	AVGDuration() time.Duration
	// AVGTransportDuration returns the average transport duration.
	AVGTransportDuration() map[string]*TransportInfo
	// SetTotalDuration sets the total duration.
	SetTotalDuration(endDuration time.Duration)
	// GetTotalDuration returns the total duration.
	GetTotalDuration() time.Duration
	// AllTransports returns all transports.
	AllTransports() map[string]*TransportInfo
}
