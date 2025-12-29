package measure_test

import (
	"sync"
	"testing"
	"time"

	"github.com/askiada/go-pipeline/pkg/pipeline/measure"
)

func TestDefaultMeasureAllMetricsReturnsCopy(t *testing.T) {
	m := measure.NewDefaultMeasure()
	m.AddMetric("step", 1)

	metrics := m.AllMetrics()
	if len(metrics) != 1 {
		t.Fatalf("expected 1 metric, got %d", len(metrics))
	}

	delete(metrics, "step")

	if m.GetMetric("step") == nil {
		t.Fatal("expected AllMetrics to return a copy of the internal map")
	}
}

func TestDefaultMetricAllTransportsReturnsCopy(t *testing.T) {
	m := measure.NewDefaultMeasure()
	mt := m.AddMetric("step", 1)
	mt.AddTransportDuration("input", 10*time.Millisecond)

	transports := mt.AllTransports()
	if transports["input"] == nil {
		t.Fatal("expected transport info to be present")
	}

	transports["input"].Elapsed = 0
	delete(transports, "input")

	internal := mt.AllTransports()
	if internal["input"] == nil || internal["input"].Elapsed != 10*time.Millisecond {
		t.Fatalf("expected internal transport elapsed to remain %s, got %v", 10*time.Millisecond, internal["input"])
	}
}

func TestDefaultMetricAVGTransportDurationDoesNotMutate(t *testing.T) {
	m := measure.NewDefaultMeasure()
	mt := m.AddMetric("step", 2)
	mt.AddTransportDuration("input", 8*time.Millisecond)
	mt.AddTransportDuration("input", 8*time.Millisecond)

	avg := mt.AVGTransportDuration()
	if avg["input"] == nil {
		t.Fatal("expected averaged transport info to be present")
	}

	if avg["input"].Elapsed != 4*time.Millisecond {
		t.Fatalf("expected average to be %s, got %s", 4*time.Millisecond, avg["input"].Elapsed)
	}

	internal := mt.AllTransports()

	if internal["input"] == nil || internal["input"].Elapsed != 16*time.Millisecond {
		t.Fatalf("expected internal transport elapsed to remain %s, got %v", 16*time.Millisecond, internal["input"])
	}
}

func TestMetricsConcurrentAccess(t *testing.T) {
	m := measure.NewDefaultMeasure()
	metric := m.AddMetric("step", 4)

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()

		for range 200 {
			metric.AddDuration(1 * time.Millisecond)
			metric.AddTransportDuration("input", 1*time.Millisecond)
		}
	}()

	go func() {
		defer wg.Done()

		for range 200 {
			for _, mt := range m.AllMetrics() {
				_ = mt.AVGDuration()
				_ = mt.AVGTransportDuration()
				_ = mt.AllTransports()
			}
		}
	}()

	wg.Wait()
}
