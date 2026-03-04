package monitor

import (
	"maps"
	"net/http"
	"sync"
	"time"
)

type monitorEvent struct {
	Measurement string            `json:"measurement"`
	Tags        map[string]string `json:"tags,omitempty"`
	Fields      map[string]any    `json:"fields,omitempty"`
	Timestamp   int64             `json:"ts"`
}

type uiState struct {
	mu         sync.Mutex
	once       sync.Once
	hub        *uiHub
	server     *http.Server
	addr       string
	err        error
	meta       []monitorEvent
	seq        int64
	totals     uiTotals
	runStarted time.Time
}

func (s *uiState) markRunStarted(now time.Time) {
	if s == nil {
		return
	}

	s.mu.Lock()

	if s.runStarted.IsZero() {
		s.runStarted = now
	}

	s.mu.Unlock()
}

func (s *uiState) recordEvent(
	measurement string,
	tags map[string]string,
	fields map[string]any,
	ts time.Time,
) (monitorEvent, *uiHub, bool) {
	if s == nil {
		return monitorEvent{}, nil, false
	}

	eventFields := copyFields(fields)
	if eventFields == nil {
		eventFields = make(map[string]any, 1)
	}

	s.mu.Lock()

	s.seq++
	eventFields["seq"] = s.seq
	s.totals.apply(measurement, tags, fields)
	hub := s.hub

	storeMeta := hub == nil && isMetaMeasurement(measurement)
	if storeMeta {
		s.meta = append(s.meta, monitorEvent{
			Measurement: measurement,
			Tags:        tags,
			Fields:      eventFields,
			Timestamp:   ts.UnixNano(),
		})
	}

	s.mu.Unlock()

	if storeMeta || hub == nil {
		return monitorEvent{}, nil, false
	}

	return monitorEvent{
		Measurement: measurement,
		Tags:        tags,
		Fields:      eventFields,
		Timestamp:   ts.UnixNano(),
	}, hub, true
}

func (s *uiState) startOnce(fn func()) {
	if s == nil {
		return
	}

	s.once.Do(fn)
}

func (s *uiState) setServer(hub *uiHub, server *http.Server, addr string) {
	if s == nil {
		return
	}

	s.mu.Lock()
	s.hub = hub
	s.server = server
	s.addr = addr
	s.mu.Unlock()
}

func (s *uiState) clearServer() (*http.Server, *uiHub) {
	if s == nil {
		return nil, nil
	}

	s.mu.Lock()
	server := s.server
	hub := s.hub
	s.server = nil
	s.hub = nil
	s.mu.Unlock()

	return server, hub
}

func (s *uiState) hubValue() *uiHub {
	if s == nil {
		return nil
	}

	s.mu.Lock()
	hub := s.hub
	s.mu.Unlock()

	return hub
}

func (s *uiState) addrValue() string {
	if s == nil {
		return ""
	}

	s.mu.Lock()
	addr := s.addr
	s.mu.Unlock()

	return addr
}

func (s *uiState) metaSnapshot() []monitorEvent {
	if s == nil {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.meta) == 0 {
		return nil
	}

	meta := make([]monitorEvent, len(s.meta))
	copy(meta, s.meta)

	return meta
}

func (s *uiState) snapshot() (uiTotalsSnapshot, int64, time.Time) {
	if s == nil {
		return uiTotalsSnapshot{}, 0, time.Time{}
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	return s.totals.snapshot(), s.seq, s.runStarted
}

func (s *uiState) setErr(err error) {
	if s == nil || err == nil {
		return
	}

	s.mu.Lock()

	if s.err == nil {
		s.err = err
	}

	s.mu.Unlock()
}

func (s *uiState) errValue() error {
	if s == nil {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	return s.err
}

func copyFields(fields map[string]any) map[string]any {
	if len(fields) == 0 {
		return nil
	}

	out := make(map[string]any, len(fields))
	maps.Copy(out, fields)

	return out
}

func isMetaMeasurement(measurement string) bool {
	switch measurement {
	case "pipeline_step", "pipeline_link":
		return true
	default:
		return false
	}
}
