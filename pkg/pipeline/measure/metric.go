package measure

import (
	"sync"
	"time"
)

type TransportInfo struct {
	Elapsed time.Duration
	total   int64
}

type DefaultMetric struct {
	allTransports map[string]*TransportInfo
	mu            *sync.Mutex
	EndDuration   time.Duration
	stepElapsed   time.Duration
	total         int64
	concurrent    int
}

func (mt *DefaultMetric) AddDuration(elapsed time.Duration) {
	mt.mu.Lock()
	defer mt.mu.Unlock()
	mt.total++
	mt.stepElapsed += elapsed
}

func (mt *DefaultMetric) SetTotalDuration(endDuration time.Duration) {
	mt.mu.Lock()
	defer mt.mu.Unlock()
	mt.EndDuration = endDuration
}

func (mt *DefaultMetric) GetTotalDuration() time.Duration {
	mt.mu.Lock()
	defer mt.mu.Unlock()

	return mt.EndDuration
}

func (mt *DefaultMetric) AddTransportDuration(inputStepName string, elapsed time.Duration) {
	mt.mu.Lock()
	defer mt.mu.Unlock()
	if mt.allTransports[inputStepName] == nil {
		mt.allTransports[inputStepName] = &TransportInfo{}
	}
	ch := mt.allTransports[inputStepName]
	ch.Elapsed += elapsed
	ch.total++
}

func (mt *DefaultMetric) AVGDuration() time.Duration {
	mt.mu.Lock()
	defer mt.mu.Unlock()
	if mt.total == 0 {
		return time.Duration(0)
	}

	return round(time.Duration(float64(mt.stepElapsed) / float64(mt.total)))
}

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

func (mt *DefaultMetric) AllTransports() map[string]*TransportInfo {
	mt.mu.Lock()
	defer mt.mu.Unlock()

	return mt.allTransports
}

func round(d time.Duration) time.Duration {
	switch {
	case d > time.Second:
		d = d.Round(time.Second)
	case d > time.Millisecond:
		d = d.Round(time.Millisecond)
	case d > time.Microsecond:
		d = d.Round(time.Microsecond)
	case d > time.Minute:
		d = d.Round(time.Minute)
	case d > time.Hour:
		d = d.Round(time.Hour)
	}

	return d
}
