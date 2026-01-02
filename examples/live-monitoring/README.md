# Live monitoring

Streams pipeline metrics to Telegraf (Influx line protocol), serves a local
monitoring UI, and optionally writes a drawer `.dot` file. The pipeline includes
split/merge, batching, retries, and drop-on-full/blocked/error scenarios so you
can see counters update live. The graph panel renders DOT using `@hpcc-js/wasm`
from a CDN.

Prerequisites:
- Telegraf running with a socket listener for line protocol (default `udp://127.0.0.1:8094`).
- InfluxDB running for storage/query (optional for this example, required for the UI).

Run (UI enabled by default on `http://127.0.0.1:8096`):
```
go run ./examples/live-monitoring
```

Run without the UI:
```
go run ./examples/live-monitoring -ui=false
```

Run with drawer output:
```
go run ./examples/live-monitoring -drawer
```

Use a custom UI bind address:
```
go run ./examples/live-monitoring -ui-addr 127.0.0.1:8097
```

Render the PNG:
```
dot -Tpng examples/live-monitoring/pipeline.dot -o examples/live-monitoring/pipeline.png
```

Local dev stack (Telegraf + InfluxDB):
```
docker compose -f docs/live-monitoring/docker-compose.yml up
```

Troubleshooting:
- If the graph panel says "Graphviz WASM not loaded", confirm `window.hpccWasm` is defined and the browser fetches `graphviz.wasm` from the CDN.
- If the module loads but the graph is still blank, check the console for "Graphviz module loaded without graphviz export" or "Graphviz WASM failed to initialize".
