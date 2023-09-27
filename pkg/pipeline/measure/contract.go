package measure

import "time"

type Measure interface {
	AddMetric(name string, concurrent int) Metric
	AllMetrics() map[string]Metric
}

type Metric interface {
	AddDuration(elapsed time.Duration)
	AddTransportDuration(inputStepName string, elapsed time.Duration)
	AVGDuration() time.Duration
	AVGTransportDuration() map[string]*TransportInfo
	SetTotalDuration(endDuration time.Duration)
	GetTotalDuration() time.Duration
	AllTransports() map[string]*TransportInfo
}
