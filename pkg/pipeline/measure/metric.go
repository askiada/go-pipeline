package measure

import (
	"sync"
	"time"

	"github.com/askiada/go-pipeline/v2/pkg/pipeline/model"
)

// TransportInfo stores transport timing data.
type TransportInfo struct {
	Elapsed time.Duration
	total   int64
}

// DefaultMetric is an in-memory Metric implementation.
type DefaultMetric struct {
	allTransports map[string]*TransportInfo
	mu            *sync.Mutex
	EndDuration   time.Duration
	stepElapsed   time.Duration
	retryElapsed  time.Duration
	total         int64
	retryTotal    int64
	dropBuffer    int64
	dropTimeout   int64
	dropError     int64
	routedErrors  int64
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

// AddRetryDuration adds the duration for a retry attempt.
func (mt *DefaultMetric) AddRetryDuration(elapsed time.Duration) {
	if elapsed == 0 {
		return
	}

	mt.mu.Lock()
	defer mt.mu.Unlock()

	mt.retryTotal++
	mt.retryElapsed += elapsed
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

// AVGRetryDuration returns the average retry duration.
func (mt *DefaultMetric) AVGRetryDuration() time.Duration {
	mt.mu.Lock()
	defer mt.mu.Unlock()

	if mt.retryTotal == 0 {
		return time.Duration(0)
	}

	return round(time.Duration(float64(mt.retryElapsed) / float64(mt.retryTotal)))
}

// RetryCount returns the number of retry attempts recorded.
func (mt *DefaultMetric) RetryCount() int64 {
	mt.mu.Lock()
	defer mt.mu.Unlock()

	return mt.retryTotal
}

// AddDrop increments the drop counter for the given kind.
func (mt *DefaultMetric) AddDrop(kind model.StepDropKind) {
	mt.mu.Lock()
	defer mt.mu.Unlock()

	switch kind {
	case model.StepDropBufferFull:
		mt.dropBuffer++
	case model.StepDropSendTimeout:
		mt.dropTimeout++
	case model.StepDropError:
		mt.dropError++
	}
}

// DropCount returns the number of drops for the given kind.
func (mt *DefaultMetric) DropCount(kind model.StepDropKind) int64 {
	mt.mu.Lock()
	defer mt.mu.Unlock()

	switch kind {
	case model.StepDropBufferFull:
		return mt.dropBuffer
	case model.StepDropSendTimeout:
		return mt.dropTimeout
	case model.StepDropError:
		return mt.dropError
	default:
		return 0
	}
}

// TotalDropCount returns the total number of drops.
func (mt *DefaultMetric) TotalDropCount() int64 {
	mt.mu.Lock()
	defer mt.mu.Unlock()

	return mt.dropBuffer + mt.dropTimeout + mt.dropError
}

// AddRoutedError increments the routed error count.
func (mt *DefaultMetric) AddRoutedError() {
	mt.mu.Lock()
	defer mt.mu.Unlock()

	mt.routedErrors++
}

// RoutedErrorCount returns the number of routed errors.
func (mt *DefaultMetric) RoutedErrorCount() int64 {
	mt.mu.Lock()
	defer mt.mu.Unlock()

	return mt.routedErrors
}

// AVGTransportDuration returns the average transport duration.
func (mt *DefaultMetric) AVGTransportDuration() map[string]*TransportInfo {
	mt.mu.Lock()
	defer mt.mu.Unlock()

	transport := make(map[string]*TransportInfo, len(mt.allTransports))
	concurrent := mt.concurrent

	concurrent = max(concurrent, 1)

	for name, ch := range mt.allTransports {
		if ch == nil {
			continue
		}

		avg := time.Duration(0)
		if ch.total > 0 && ch.Elapsed > 0 {
			avg = round(time.Duration((float64(ch.Elapsed) / float64(ch.total)) / float64(concurrent)))
		}

		transport[name] = &TransportInfo{
			Elapsed: avg,
			total:   ch.total,
		}
	}

	return transport
}

// AllTransports returns all transport info.
func (mt *DefaultMetric) AllTransports() map[string]*TransportInfo {
	mt.mu.Lock()
	defer mt.mu.Unlock()

	transports := make(map[string]*TransportInfo, len(mt.allTransports))
	for name, info := range mt.allTransports {
		if info == nil {
			continue
		}

		transports[name] = &TransportInfo{
			Elapsed: info.Elapsed,
			total:   info.total,
		}
	}

	return transports
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
