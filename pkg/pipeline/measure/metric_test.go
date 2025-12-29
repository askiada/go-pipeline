package measure

import (
	"testing"
	"time"
)

func TestDefaultMeasureAllMetricsReturnsCopy(t *testing.T) {
	m := NewDefaultMeasure()
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
	m := NewDefaultMeasure()
	mt, ok := m.AddMetric("step", 1).(*DefaultMetric)
	if !ok {
		t.Fatal("expected default metric type")
	}
	mt.AddTransportDuration("input", 10*time.Millisecond)

	transports := mt.AllTransports()
	if transports["input"] == nil {
		t.Fatal("expected transport info to be present")
	}

	transports["input"].Elapsed = 0
	delete(transports, "input")

	mt.mu.Lock()
	internal := mt.allTransports["input"].Elapsed
	mt.mu.Unlock()

	if internal != 10*time.Millisecond {
		t.Fatalf("expected internal transport elapsed to remain %s, got %s", 10*time.Millisecond, internal)
	}
}

func TestDefaultMetricAVGTransportDurationDoesNotMutate(t *testing.T) {
	m := NewDefaultMeasure()
	mt, ok := m.AddMetric("step", 2).(*DefaultMetric)
	if !ok {
		t.Fatal("expected default metric type")
	}
	mt.AddTransportDuration("input", 8*time.Millisecond)
	mt.AddTransportDuration("input", 8*time.Millisecond)

	avg := mt.AVGTransportDuration()
	if avg["input"] == nil {
		t.Fatal("expected averaged transport info to be present")
	}

	if avg["input"].Elapsed != 4*time.Millisecond {
		t.Fatalf("expected average to be %s, got %s", 4*time.Millisecond, avg["input"].Elapsed)
	}

	mt.mu.Lock()
	internal := mt.allTransports["input"].Elapsed
	mt.mu.Unlock()

	if internal != 16*time.Millisecond {
		t.Fatalf("expected internal transport elapsed to remain %s, got %s", 16*time.Millisecond, internal)
	}
}
