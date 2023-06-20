package pipeline

import (
	"sync"
	"time"
)

type chanInfo struct {
	elapsed time.Duration
	total   int64
}

type metric struct {
	concurrent     int
	mu             *sync.Mutex
	stepElapsed    time.Duration
	channelElapsed map[string]*chanInfo
	total          int64
	endDuration    time.Duration
}

func (mt *metric) add(elapsed time.Duration) {
	mt.mu.Lock()
	defer mt.mu.Unlock()
	mt.total++
	mt.stepElapsed += elapsed
}

func (mt *metric) end(endDuration time.Duration) {
	mt.mu.Lock()
	defer mt.mu.Unlock()
	mt.endDuration = endDuration
}

func (mt *metric) addChannel(inputStepName string, elapsed time.Duration) {
	mt.mu.Lock()
	defer mt.mu.Unlock()
	if mt.channelElapsed[inputStepName] == nil {
		mt.channelElapsed[inputStepName] = &chanInfo{}
	}
	ch := mt.channelElapsed[inputStepName]
	ch.elapsed += elapsed
	ch.total++
}

func (mt *metric) avgStep() time.Duration {
	mt.mu.Lock()
	defer mt.mu.Unlock()
	if mt.total == 0 {
		return time.Duration(0)
	}
	return round(time.Duration(float64(mt.stepElapsed) / float64(mt.total)))
}

func (mt *metric) avgChannel() map[string]*chanInfo {
	mt.mu.Lock()
	defer mt.mu.Unlock()

	for name, ch := range mt.channelElapsed {
		if ch.elapsed == 0 {
			continue
		}
		mt.channelElapsed[name].elapsed = round(time.Duration((float64(ch.elapsed) / (float64(ch.total))) / float64(mt.concurrent)))
	}

	return mt.channelElapsed
}

type measure struct {
	start, end *metric
	steps      map[string]*metric
}

func newMeasure() *measure {
	return &measure{
		steps: make(map[string]*metric),
	}
}

func (m *measure) addStep(name string, concurrent int) *metric {
	mt := &metric{
		mu:             &sync.Mutex{},
		channelElapsed: make(map[string]*chanInfo),
		concurrent:     concurrent,
	}
	m.steps[name] = mt
	return mt
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
