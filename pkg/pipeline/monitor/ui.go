package monitor

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"sync"
	"time"

	pkgerrors "github.com/pkg/errors"
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
	for key, value := range fields {
		out[key] = value
	}

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
			pm.setUIErr(pkgerrors.Wrap(err, "listen monitoring ui"))

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
				pm.setUIErr(pkgerrors.Wrap(err, "serve monitoring ui"))
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
		return pkgerrors.Wrap(err, "shutdown monitoring ui")
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

//nolint:misspell // CSS uses American spelling.
const uiIndexHTML = `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>go-pipeline live monitoring</title>
  <style>
    :root {
      color-scheme: light;
      --bg: #f6f4ef;
      --panel: #ffffff;
      --ink: #1d1c1a;
      --muted: #6a655d;
      --accent: #4b6fff;
      --border: #e0dad2;
      font-family: "Space Grotesk", "Avenir Next", "Segoe UI", sans-serif;
    }
    body {
      margin: 0;
      background: radial-gradient(circle at top, #fff4d6, #f6f4ef 60%);
      color: var(--ink);
    }
    header {
      padding: 32px 40px 24px;
      display: flex;
      justify-content: space-between;
      align-items: center;
      gap: 16px;
      flex-wrap: wrap;
    }
    .title {
      font-size: 26px;
      font-weight: 700;
      letter-spacing: -0.02em;
    }
    .status {
      font-size: 14px;
      color: var(--muted);
    }
    main {
      padding: 0 40px 40px;
      display: grid;
      grid-template-columns: minmax(0, 2fr) minmax(260px, 1fr);
      gap: 24px;
    }
    section {
      background: var(--panel);
      border: 1px solid var(--border);
      border-radius: 20px;
      padding: 20px 24px;
      box-shadow: 0 12px 30px rgba(0, 0, 0, 0.05);
    }
    h2 {
      font-size: 16px;
      margin: 0 0 12px;
      color: var(--muted);
      text-transform: uppercase;
      letter-spacing: 0.08em;
    }
    table {
      width: 100%;
      border-collapse: collapse;
      font-size: 14px;
    }
    th, td {
      text-align: left;
      padding: 8px 6px;
      border-bottom: 1px solid var(--border);
    }
    th {
      color: var(--muted);
      font-weight: 600;
    }
    .pill {
      display: inline-block;
      padding: 2px 8px;
      border-radius: 999px;
      background: rgba(75, 111, 255, 0.12);
      color: var(--accent);
      font-size: 12px;
    }
    .muted {
      color: var(--muted);
    }
    .topology {
      display: flex;
      flex-direction: column;
      gap: 6px;
      font-size: 14px;
    }
    .grid {
      display: grid;
      gap: 12px;
    }
    .graph {
      min-height: 240px;
      border: 1px dashed var(--border);
      border-radius: 12px;
      padding: 12px;
      background: #f8f6f2;
      overflow: auto;
    }
    .graph svg {
      width: 100%;
      height: auto;
    }
    @media (max-width: 980px) {
      main {
        grid-template-columns: 1fr;
      }
    }
  </style>
</head>
<body>
  <header>
    <div>
      <div class="title">Live pipeline monitoring</div>
      <div class="status" id="connectionStatus">Connecting...</div>
    </div>
    <div class="status" id="runMeta"></div>
  </header>
  <main>
    <section>
      <h2>Graph</h2>
      <div class="graph" id="graph">Waiting for topology...</div>
    </section>
    <section>
      <h2>Topology</h2>
      <div class="topology" id="topology"></div>
    </section>
    <section>
      <h2>Run status</h2>
      <div class="grid">
        <div><span class="pill" id="runState">idle</span></div>
        <div class="muted" id="runTiming"></div>
        <div class="muted" id="runTotals"></div>
      </div>
    </section>
    <section>
      <h2>Step throughput</h2>
      <table>
        <thead>
          <tr>
            <th>Step</th>
            <th>Type</th>
            <th>Count</th>
            <th>Avg duration</th>
            <th>Avg transport</th>
          </tr>
        </thead>
        <tbody id="stepTable"></tbody>
      </table>
    </section>
    <section>
      <h2>Quality signals</h2>
      <table>
        <thead>
          <tr>
            <th>Step</th>
            <th>Drops</th>
            <th>Retries</th>
            <th>Error routes</th>
          </tr>
        </thead>
        <tbody id="qualityTable"></tbody>
      </table>
    </section>
  </main>
  <script type="module">
    const state = {
      run: {},
      steps: {},
      stepOrder: [],
      links: [],
      outputs: {},
      drops: {},
      retries: {},
      errorRoutes: {},
      runTotal: null,
    };
    const graphState = {
      dot: "",
      rendering: false,
    };

    let hpccWasm = null;
    let graphviz = null;
    let graphvizLoading = null;
    let renderPending = false;

    function scheduleRender() {
      if (renderPending) return;
      renderPending = true;
      window.requestAnimationFrame(() => {
        renderPending = false;
        render();
      });
    }

    import("https://cdn.jsdelivr.net/npm/@hpcc-js/wasm@2.13.0/dist/index.min.js")
      .then((mod) => {
        hpccWasm = mod;
        window.hpccWasm = hpccWasm;
        ensureGraphviz();
      })
      .catch((err) => {
        console.warn("Graphviz WASM failed to load", err);
      });

    function ensureGraphviz() {
      if (graphviz || graphvizLoading || !hpccWasm) {
        return;
      }

      if (hpccWasm.graphviz) {
        graphviz = hpccWasm.graphviz;
        scheduleRender();
        return;
      }

      if (hpccWasm.Graphviz && typeof hpccWasm.Graphviz.load === "function") {
        graphvizLoading = hpccWasm.Graphviz.load()
          .then((instance) => {
            graphviz = instance;
            graphvizLoading = null;
            scheduleRender();
          })
          .catch((err) => {
            graphvizLoading = null;
            console.warn("Graphviz WASM failed to initialize", err);
          });
        return;
      }

      console.warn("Graphviz module loaded without graphviz export", hpccWasm);
    }

    function handleEvent(evt) {
      if (!evt || !evt.measurement) return;
      const tags = evt.tags || {};
      const fields = evt.fields || {};

      if (!state.run.run_id && tags.run_id) {
        state.run = {
          run_id: tags.run_id,
          run_name: tags.run_name || "",
          origin: tags.origin || "",
          pipeline_name: tags.pipeline_name || "",
          started_at: Date.now(),
        };
      }

      switch (evt.measurement) {
        case "pipeline_step":
          if (tags.step_name) {
            if (!state.steps[tags.step_name]) {
              state.stepOrder.push(tags.step_name);
            }

            state.steps[tags.step_name] = {
              name: tags.step_name,
              type: tags.step_type || "",
              concurrent: fields.concurrent || 0,
              buffer: fields.buffer_size || 0,
            };
          }
          break;
        case "pipeline_link":
          if (tags.from_step && tags.to_step) {
            const key = tags.from_step + "->" + tags.to_step;
            if (!state.links.find((link) => link.key === key)) {
              state.links.push({ key, from: tags.from_step, to: tags.to_step });
            }
          }
          break;
        case "step_output":
        case "splitter_output":
        case "merger_output":
        case "sink_output": {
          const stepName = tags.step_name || "unknown";
          const bucket = state.outputs[stepName] || { count: 0, duration: 0, transport: 0 };
          bucket.count += Number(fields.count || 0);
          bucket.duration += Number(fields.duration_ms || 0);
          bucket.transport += Number(fields.transport_ms || 0);
          state.outputs[stepName] = bucket;
          break;
        }
        case "step_drop": {
          const stepName = tags.step_name || "unknown";
          state.drops[stepName] = (state.drops[stepName] || 0) + Number(fields.count || 0);
          break;
        }
        case "step_retry": {
          const stepName = tags.step_name || "unknown";
          state.retries[stepName] = (state.retries[stepName] || 0) + Number(fields.count || 0);
          break;
        }
        case "step_error_route": {
          const stepName = tags.step_name || "unknown";
          state.errorRoutes[stepName] = (state.errorRoutes[stepName] || 0) + Number(fields.count || 0);
          break;
        }
        case "run_total":
          state.runTotal = fields.duration_ms || null;
          break;
        default:
          break;
      }

      scheduleRender();
    }

    function render() {
      renderGraph();
      renderRunMeta();
      renderTopology();
      renderStepTable();
      renderQualityTable();
    }

    function renderRunMeta() {
      const meta = document.getElementById("runMeta");
      const stateBadge = document.getElementById("runState");
      const timing = document.getElementById("runTiming");
      const totals = document.getElementById("runTotals");
      if (!state.run.run_id) {
        meta.textContent = "Waiting for run metadata...";
        return;
      }
      meta.textContent = (state.run.run_name || state.run.run_id) + " - " + state.run.origin;
      stateBadge.textContent = "running";
      const elapsed = state.run.started_at ? ((Date.now() - state.run.started_at) / 1000).toFixed(1) : "--";
      timing.textContent = "Elapsed: " + elapsed + "s";
      if (state.runTotal) {
        totals.textContent = "Run total: " + Number(state.runTotal).toFixed(1) + "ms";
      }
    }

    function renderTopology() {
      const container = document.getElementById("topology");
      if (state.links.length === 0) {
        container.textContent = "No topology events yet.";
        return;
      }
      container.innerHTML = "";
      state.links.forEach((link) => {
        const div = document.createElement("div");
        div.textContent = link.from + " -> " + link.to;
        container.appendChild(div);
      });
    }

    function renderGraph() {
      const container = document.getElementById("graph");
      if (!container) return;

      const dot = buildDot();
      if (!dot) {
        container.textContent = "Waiting for topology...";
        return;
      }

      if (graphState.rendering || graphState.dot === dot) {
        return;
      }

      ensureGraphviz();
      if (graphvizLoading) {
        container.textContent = "Graphviz WASM loading...";
        return;
      }
      if (!graphviz) {
        container.textContent = "Graphviz WASM not loaded. Check network access.";
        return;
      }

      graphState.rendering = true;
      try {
        const result = graphviz.layout(dot, "svg", "dot");
        if (result && typeof result.then === "function") {
          result
            .then((svg) => {
              graphState.dot = dot;
              container.innerHTML = svg;
            })
            .catch((err) => {
              container.textContent = "Graph render failed: " + err;
            })
            .finally(() => {
              graphState.rendering = false;
            });
        } else {
          graphState.dot = dot;
          container.innerHTML = result;
          graphState.rendering = false;
        }
      } catch (err) {
        graphState.rendering = false;
        container.textContent = "Graph render failed: " + err;
      }
    }

    function buildDot() {
      const names = Object.keys(state.steps);
      if (names.length === 0) {
        return "";
      }

      const lines = [];
      lines.push("digraph pipeline {");
      lines.push("  rankdir=LR;");
      lines.push("  node [shape=box, style=rounded, fontsize=11];");

      names.sort().forEach((name) => {
        const step = state.steps[name] || {};
        let label = name;
        if (step.type) {
          label = label + "\\n(" + step.type + ")";
        }
        lines.push("  " + quoteId(name) + " [label=\"" + escapeLabel(label) + "\"];");
      });

      state.links.forEach((link) => {
        lines.push("  " + quoteId(link.from) + " -> " + quoteId(link.to) + ";");
      });

      lines.push("}");

      return lines.join("\n");
    }

    function quoteId(value) {
      const raw = String(value);
      if (/^[A-Za-z_][A-Za-z0-9_]*$/.test(raw)) {
        return raw;
      }

      return "\"" + escapeLabel(raw) + "\"";
    }

    function escapeLabel(value) {
      return String(value)
        .replace(/\\/g, "\\\\")
        .replace(/"/g, "\\\"");
    }

    function renderStepTable() {
      const body = document.getElementById("stepTable");
      const names = state.stepOrder.length
        ? state.stepOrder.filter((name) => state.outputs[name])
        : Object.keys(state.outputs);
      if (names.length === 0) {
        body.innerHTML = "<tr><td colspan=\"5\" class=\"muted\">No output yet.</td></tr>";
        return;
      }
      body.innerHTML = "";
      names.forEach((name) => {
        const step = state.steps[name] || { type: "" };
        const stats = state.outputs[name];
        const avgDuration = stats.count ? (stats.duration / stats.count).toFixed(2) : "0";
        const avgTransport = stats.count ? (stats.transport / stats.count).toFixed(2) : "0";
        body.innerHTML += "<tr>" +
          "<td>" + name + "</td>" +
          "<td class=\"muted\">" + step.type + "</td>" +
          "<td>" + stats.count + "</td>" +
          "<td>" + avgDuration + "ms</td>" +
          "<td>" + avgTransport + "ms</td>" +
          "</tr>";
      });
    }

    function renderQualityTable() {
      const body = document.getElementById("qualityTable");
      const names = new Set([
        ...Object.keys(state.drops),
        ...Object.keys(state.retries),
        ...Object.keys(state.errorRoutes),
      ]);
      if (names.size === 0) {
        body.innerHTML = "<tr><td colspan=\"4\" class=\"muted\">No drops/retries/errors yet.</td></tr>";
        return;
      }
      body.innerHTML = "";
      const ordered = state.stepOrder.length
        ? state.stepOrder.filter((name) => names.has(name))
        : Array.from(names);
      ordered.forEach((name) => {
        body.innerHTML += "<tr>" +
          "<td>" + name + "</td>" +
          "<td>" + (state.drops[name] || 0) + "</td>" +
          "<td>" + (state.retries[name] || 0) + "</td>" +
          "<td>" + (state.errorRoutes[name] || 0) + "</td>" +
          "</tr>";
      });
    }

    const statusEl = document.getElementById("connectionStatus");
    const eventSource = new EventSource("/events");
    eventSource.onopen = () => {
      statusEl.textContent = "Connected";
    };
    eventSource.onerror = () => {
      statusEl.textContent = "Disconnected - retrying...";
    };
    eventSource.onmessage = (e) => {
      try {
        handleEvent(JSON.parse(e.data));
      } catch (err) {
        console.error(err);
      }
    };
  </script>
</body>
</html>
`
