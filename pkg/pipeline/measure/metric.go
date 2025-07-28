package measure

import (
	"sync"
	"time"
)

// TransportInfo is a struct that contains the transport info.
type TransportInfo struct {
	Elapsed time.Duration
	total   int64
}

// DefaultMetric is a default implementation of the Metric interface.
type DefaultMetric struct {
	allTransports map[string]*TransportInfo
	mu            *sync.Mutex
	EndDuration   time.Duration
	stepElapsed   time.Duration
	total         int64
	concurrent    int
}

// AddDuration adds the duration.
func (mt *DefaultMetric) AddDuration(elapsed time.Duration) {
	if elapsed == 0 {
		return
	}

	mt.mu.Lock()
	defer mt.mu.Unlock()

	mt.total++
	mt.stepElapsed += elapsed
}

// SetTotalDuration sets the total duration.
func (mt *DefaultMetric) SetTotalDuration(endDuration time.Duration) {
	mt.mu.Lock()
	defer mt.mu.Unlock()

	mt.EndDuration = endDuration
}

// GetTotalDuration returns the total duration.
func (mt *DefaultMetric) GetTotalDuration() time.Duration {
	mt.mu.Lock()
	defer mt.mu.Unlock()

	return mt.EndDuration
}

// AddTransportDuration adds the transport duration.
func (mt *DefaultMetric) AddTransportDuration(inputStepName string, elapsed time.Duration) {
	if elapsed == 0 {
		return
	}

	mt.mu.Lock()
	defer mt.mu.Unlock()

	if mt.allTransports[inputStepName] == nil {
		mt.allTransports[inputStepName] = &TransportInfo{}
	}

	ch := mt.allTransports[inputStepName]
	ch.Elapsed += elapsed
	ch.total++
}

// AVGDuration returns the average duration.
func (mt *DefaultMetric) AVGDuration() time.Duration {
	mt.mu.Lock()
	defer mt.mu.Unlock()

	if mt.total == 0 {
		return time.Duration(0)
	}

	return round(time.Duration(float64(mt.stepElapsed) / float64(mt.total)))
}

// AVGTransportDuration returns the average transport duration.
func (mt *DefaultMetric) AVGTransportDuration() map[string]*TransportInfo {
	mt.mu.Lock()
	defer mt.mu.Unlock()

	for name, ch := range mt.allTransports {
		if ch.Elapsed == 0 {
			continue
		}

		mt.allTransports[name].Elapsed = round(time.Duration((float64(ch.Elapsed) / (float64(ch.total))) / float64(mt.concurrent)))
	}

	return mt.allTransports
}

// AllTransports returns all transport info.
func (mt *DefaultMetric) AllTransports() map[string]*TransportInfo {
	mt.mu.Lock()
	defer mt.mu.Unlock()

	return mt.allTransports
}

func round(dur time.Duration) time.Duration {
	switch {
	case dur > time.Second:
		dur = dur.Round(time.Second)
	case dur > time.Millisecond:
		dur = dur.Round(time.Millisecond)
	case dur > time.Microsecond:
		dur = dur.Round(time.Microsecond)
	case dur > time.Minute:
		dur = dur.Round(time.Minute)
	case dur > time.Hour:
		dur = dur.Round(time.Hour)
	}

	return dur
}
