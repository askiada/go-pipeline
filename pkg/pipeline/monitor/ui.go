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

func (pm *pipelineMonitor) emitUI(
	measurement string,
	tags map[string]string,
	fields map[string]any,
	ts time.Time,
) {
	if pm == nil || !pm.cfg.EnableUI {
		return
	}

	event := monitorEvent{
		Measurement: measurement,
		Tags:        pm.mergeTags(tags),
		Fields:      copyFields(fields),
		Timestamp:   ts.UnixNano(),
	}

	pm.uiMu.Lock()
	hub := pm.uiHub

	if hub == nil && isMetaMeasurement(measurement) {
		pm.uiMeta = append(pm.uiMeta, event)
		pm.uiMu.Unlock()

		return
	}

	pm.uiMu.Unlock()

	if hub == nil {
		return
	}

	hub.publish(event)
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
	pm.uiOnce.Do(func() {
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

		pm.uiMu.Lock()
		pm.uiHub = hub
		pm.uiServer = server
		pm.uiAddr = listener.Addr().String()
		pm.uiMu.Unlock()

		go func() {
			err := server.Serve(listener)
			if err != nil && !errors.Is(err, http.ErrServerClosed) {
				pm.setUIErr(fmt.Errorf("serve monitoring ui: %w", err))
			}
		}()
	})
}

func (pm *pipelineMonitor) stopUI() error {
	pm.uiMu.Lock()
	server := pm.uiServer
	hub := pm.uiHub
	pm.uiServer = nil
	pm.uiHub = nil
	pm.uiMu.Unlock()

	if hub != nil {
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

	pm.uiMu.Lock()
	hub := pm.uiHub
	pm.uiMu.Unlock()

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

	pm.writeEvent(writer, pm.runInfoEvent())
	pm.writeMetaEvents(writer)
	flusher.Flush()

	stream := hub.subscribe()
	defer hub.unsubscribe(stream)

	for {
		select {
		case <-request.Context().Done():
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

func (pm *pipelineMonitor) metaSnapshot() []monitorEvent {
	pm.uiMu.Lock()
	defer pm.uiMu.Unlock()

	if len(pm.uiMeta) == 0 {
		return nil
	}

	meta := make([]monitorEvent, len(pm.uiMeta))
	copy(meta, pm.uiMeta)

	return meta
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

	pm.uiMu.Lock()
	defer pm.uiMu.Unlock()

	return pm.uiAddr
}

func (pm *pipelineMonitor) setUIErr(err error) {
	if err == nil {
		return
	}

	pm.uiMu.Lock()
	defer pm.uiMu.Unlock()

	if pm.uiErr == nil {
		pm.uiErr = err
	}
}

func (pm *pipelineMonitor) uiError() error {
	pm.uiMu.Lock()
	defer pm.uiMu.Unlock()

	return pm.uiErr
}

//go:embed ui_index.html
var uiIndexHTML string
