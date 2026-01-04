package monitor

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net"
	"net/http"
	"sync"
	"time"
)

const (
	uiShutdownTimeout   = 5 * time.Second
	uiReadHeaderTimeout = 5 * time.Second
)

type monitorEvent struct {
	Measurement string            `json:"measurement"`
	Tags        map[string]string `json:"tags,omitempty"`
	Fields      map[string]any    `json:"fields,omitempty"`
	Timestamp   int64             `json:"ts"`
}

type uiHub struct {
	mu      sync.RWMutex
	buffer  int
	clients map[chan monitorEvent]struct{}
	closed  bool
}

type uiOutputTotals struct {
	Count       int64 `json:"count"`
	DurationMs  int64 `json:"duration_ms"`
	TransportMs int64 `json:"transport_ms"`
}

type uiTotals struct {
	outputs      map[string]uiOutputTotals
	drops        map[string]int64
	retries      map[string]int64
	errorRoutes  map[string]int64
	runTotalMs   int64
	runTotalSeen bool
}

type uiTotalsSnapshot struct {
	outputs      map[string]uiOutputTotals
	drops        map[string]int64
	retries      map[string]int64
	errorRoutes  map[string]int64
	runTotalMs   int64
	runTotalSeen bool
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

func newUIHub(buffer int) *uiHub {
	if buffer < 1 {
		buffer = defaultBufferSize
	}

	return &uiHub{
		buffer:  buffer,
		clients: make(map[chan monitorEvent]struct{}),
	}
}

func (h *uiHub) subscribe() chan monitorEvent {
	ch := make(chan monitorEvent, h.buffer)

	h.mu.Lock()
	defer h.mu.Unlock()

	if h.closed {
		close(ch)

		return ch
	}

	h.clients[ch] = struct{}{}

	return ch
}

func (h *uiHub) unsubscribe(ch chan monitorEvent) {
	if ch == nil {
		return
	}

	h.mu.Lock()
	defer h.mu.Unlock()

	if _, ok := h.clients[ch]; !ok {
		return
	}

	delete(h.clients, ch)
	close(ch)
}

func (h *uiHub) publish(event monitorEvent) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	if h.closed {
		return
	}

	for ch := range h.clients {
		select {
		case ch <- event:
		default:
		}
	}
}

func (h *uiHub) publishPriority(event monitorEvent) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	if h.closed {
		return
	}

	for ch := range h.clients {
		if h.trySend(ch, event) {
			continue
		}

		// Drop one queued event to make room for the snapshot.
		select {
		case <-ch:
		default:
		}

		h.trySend(ch, event)
	}
}

func (h *uiHub) trySend(ch chan monitorEvent, event monitorEvent) bool {
	select {
	case ch <- event:
		return true
	default:
		return false
	}
}

func (h *uiHub) close() {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.closed {
		return
	}

	for ch := range h.clients {
		close(ch)
	}

	h.clients = nil
	h.closed = true
}

func (totals *uiTotals) apply(measurement string, tags map[string]string, fields map[string]any) {
	switch measurement {
	case "step_output", "splitter_output", "merger_output", "sink_output":
		totals.applyOutput(tags, fields)
	case "step_drop":
		totals.drops = totals.applyCount(tags, fields, totals.drops)
	case "step_retry":
		totals.retries = totals.applyCount(tags, fields, totals.retries)
	case "step_error_route":
		totals.errorRoutes = totals.applyCount(tags, fields, totals.errorRoutes)
	case "run_total":
		totals.applyRunTotal(fields)
	default:
	}
}

func (totals *uiTotals) applyOutput(tags map[string]string, fields map[string]any) {
	stepName := tagStepName(tags)
	if stepName == "" {
		return
	}

	if totals.outputs == nil {
		totals.outputs = make(map[string]uiOutputTotals)
	}

	current := totals.outputs[stepName]
	current.Count += fieldInt64(fields, "count")
	current.DurationMs += fieldInt64(fields, "duration_ms")
	current.TransportMs += fieldInt64(fields, "transport_ms")
	totals.outputs[stepName] = current
}

func (totals *uiTotals) applyCount(tags map[string]string, fields map[string]any, target map[string]int64) map[string]int64 {
	stepName := tagStepName(tags)
	if stepName == "" {
		return target
	}

	if target == nil {
		target = make(map[string]int64)
	}

	target[stepName] += fieldInt64(fields, "count")

	return target
}

func (totals *uiTotals) applyRunTotal(fields map[string]any) {
	totals.runTotalMs = fieldInt64(fields, "duration_ms")
	totals.runTotalSeen = true
}

func tagStepName(tags map[string]string) string {
	if tags == nil {
		return ""
	}

	return tags["step_name"]
}

func (totals *uiTotals) snapshot() uiTotalsSnapshot {
	if totals == nil {
		return uiTotalsSnapshot{}
	}

	snapshot := uiTotalsSnapshot{
		runTotalMs:   totals.runTotalMs,
		runTotalSeen: totals.runTotalSeen,
	}

	if len(totals.outputs) > 0 {
		snapshot.outputs = make(map[string]uiOutputTotals, len(totals.outputs))
		maps.Copy(snapshot.outputs, totals.outputs)
	}

	if len(totals.drops) > 0 {
		snapshot.drops = make(map[string]int64, len(totals.drops))
		maps.Copy(snapshot.drops, totals.drops)
	}

	if len(totals.retries) > 0 {
		snapshot.retries = make(map[string]int64, len(totals.retries))
		maps.Copy(snapshot.retries, totals.retries)
	}

	if len(totals.errorRoutes) > 0 {
		snapshot.errorRoutes = make(map[string]int64, len(totals.errorRoutes))
		maps.Copy(snapshot.errorRoutes, totals.errorRoutes)
	}

	return snapshot
}

func (pm *pipelineMonitor) emitUI(
	measurement string,
	tags map[string]string,
	fields map[string]any,
	ts time.Time,
) {
	if pm == nil || !pm.cfg.EnableUI {
		return
	}

	event, hub, publish := pm.ui.recordEvent(measurement, pm.mergeTags(tags), fields, ts)
	if !publish {
		return
	}

	hub.publish(event)
}

func fieldInt64(fields map[string]any, key string) int64 {
	if len(fields) == 0 {
		return 0
	}

	value, ok := fields[key]
	if !ok || value == nil {
		return 0
	}

	switch typed := value.(type) {
	case int64:
		return typed
	case int:
		return int64(typed)
	case float32:
		return int64(typed)
	case float64:
		return int64(typed)
	case json.Number:
		num, err := typed.Int64()
		if err != nil {
			return 0
		}

		return num
	default:
		return 0
	}
}

func (pm *pipelineMonitor) mergeTags(tags map[string]string) map[string]string {
	merged := make(map[string]string, len(pm.baseTagMap)+len(tags))

	for key, value := range pm.baseTagMap {
		if value == "" {
			continue
		}

		merged[key] = value
	}

	for key, value := range tags {
		if value == "" {
			continue
		}

		merged[key] = value
	}

	return merged
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

func (pm *pipelineMonitor) startUI() {
	pm.ui.startOnce(func() {
		if pm == nil || !pm.cfg.EnableUI {
			return
		}

		listener, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", pm.cfg.BindAddr)
		if err != nil {
			pm.setUIErr(fmt.Errorf("listen monitoring ui: %w", err))

			return
		}

		hub := newUIHub(pm.cfg.BufferSize)
		mux := http.NewServeMux()
		mux.HandleFunc("/", pm.serveIndex)
		mux.HandleFunc("/events", pm.serveEvents)
		server := &http.Server{
			Handler:           mux,
			ReadHeaderTimeout: uiReadHeaderTimeout,
		}

		pm.ui.setServer(hub, server, listener.Addr().String())

		go func() {
			err := server.Serve(listener)
			if err != nil && !errors.Is(err, http.ErrServerClosed) {
				pm.setUIErr(fmt.Errorf("serve monitoring ui: %w", err))
			}
		}()
	})
}

func (pm *pipelineMonitor) stopUI() error {
	server, hub := pm.ui.clearServer()

	if hub != nil {
		snapshot := pm.uiSnapshotEvent()
		if snapshot != nil {
			hub.publishPriority(*snapshot)
		}

		hub.close()
	}

	if server == nil {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), uiShutdownTimeout)
	defer cancel()

	err := server.Shutdown(ctx)
	if err != nil {
		return fmt.Errorf("shutdown monitoring ui: %w", err)
	}

	return nil
}

func (pm *pipelineMonitor) serveIndex(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write([]byte(uiIndexHTML))
}

func (pm *pipelineMonitor) serveEvents(writer http.ResponseWriter, request *http.Request) {
	if pm == nil {
		http.NotFound(writer, request)

		return
	}

	hub := pm.ui.hubValue()

	if hub == nil {
		http.NotFound(writer, request)

		return
	}

	flusher, ok := writer.(http.Flusher)
	if !ok {
		http.Error(writer, "streaming unsupported", http.StatusInternalServerError)

		return
	}

	writer.Header().Set("Content-Type", "text/event-stream")
	writer.Header().Set("Cache-Control", "no-cache")
	writer.Header().Set("Connection", "keep-alive")

	stream := hub.subscribe()
	defer hub.unsubscribe(stream)

	pm.writeEvent(writer, pm.runInfoEvent())
	pm.writeMetaEvents(writer)
	pm.writeSnapshotEvent(writer)
	flusher.Flush()

	for {
		select {
		case <-request.Context().Done():
			pm.drainEvents(writer, stream, flusher)

			return
		case event, ok := <-stream:
			if !ok {
				return
			}

			pm.writeEvent(writer, event)
			flusher.Flush()
		}
	}
}

func (pm *pipelineMonitor) writeMetaEvents(writer http.ResponseWriter) {
	meta := pm.metaSnapshot()
	for _, event := range meta {
		pm.writeEvent(writer, event)
	}
}

func (pm *pipelineMonitor) writeSnapshotEvent(writer http.ResponseWriter) {
	snapshot := pm.uiSnapshotEvent()
	if snapshot == nil {
		return
	}

	pm.writeEvent(writer, *snapshot)
}

func (pm *pipelineMonitor) drainEvents(
	writer http.ResponseWriter,
	stream <-chan monitorEvent,
	flusher http.Flusher,
) {
	for {
		select {
		case event, ok := <-stream:
			if !ok {
				return
			}

			pm.writeEvent(writer, event)
			flusher.Flush()
		default:
			return
		}
	}
}

func (pm *pipelineMonitor) metaSnapshot() []monitorEvent {
	if pm == nil {
		return nil
	}

	return pm.ui.metaSnapshot()
}

func (pm *pipelineMonitor) uiSnapshotEvent() *monitorEvent {
	if pm == nil || !pm.cfg.EnableUI {
		return nil
	}

	snapshot, snapshotSeq, runStarted := pm.ui.snapshot()

	fields := make(map[string]any)
	if len(snapshot.outputs) > 0 {
		fields["outputs"] = snapshot.outputs
	}

	if len(snapshot.drops) > 0 {
		fields["drops"] = snapshot.drops
	}

	if len(snapshot.retries) > 0 {
		fields["retries"] = snapshot.retries
	}

	if len(snapshot.errorRoutes) > 0 {
		fields["error_routes"] = snapshot.errorRoutes
	}

	if snapshot.runTotalSeen {
		fields["run_total_ms"] = snapshot.runTotalMs
	}

	if !runStarted.IsZero() {
		fields["run_started_at_ms"] = runStarted.UnixMilli()
	}

	fields["snapshot_seq"] = snapshotSeq

	return &monitorEvent{
		Measurement: "monitor_snapshot",
		Tags:        pm.mergeTags(nil),
		Fields:      fields,
		Timestamp:   time.Now().UnixNano(),
	}
}

func (pm *pipelineMonitor) runInfoEvent() monitorEvent {
	return monitorEvent{
		Measurement: "monitor_status",
		Tags:        pm.mergeTags(nil),
		Fields: map[string]any{
			"status": "connected",
		},
		Timestamp: time.Now().UnixNano(),
	}
}

func (pm *pipelineMonitor) writeEvent(writer http.ResponseWriter, event monitorEvent) {
	payload, err := json.Marshal(event)
	if err != nil {
		return
	}

	_, _ = writer.Write([]byte("data: "))
	_, _ = writer.Write(payload)
	_, _ = writer.Write([]byte("\n\n"))
}

func (pm *pipelineMonitor) UIAddr() string {
	if pm == nil {
		return ""
	}

	if pm.cfg.EnableUI && pm.runOptsSet && !pm.runOpts.DryRun {
		pm.startUI()
	}

	return pm.ui.addrValue()
}

func (pm *pipelineMonitor) setUIErr(err error) {
	if pm == nil {
		return
	}

	pm.ui.setErr(err)
}

func (pm *pipelineMonitor) uiError() error {
	if pm == nil {
		return nil
	}

	return pm.ui.errValue()
}

//go:embed ui_index.html
var uiIndexHTML string
