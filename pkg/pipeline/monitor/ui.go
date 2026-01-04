package monitor

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"time"
)

const (
	uiShutdownTimeout   = 5 * time.Second
	uiReadHeaderTimeout = 5 * time.Second
)

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
