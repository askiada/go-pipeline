# Examples

This document provides a high-level index of examples. Each example directory
contains a `README.md` with its purpose and expected output. See
`../examples/README.md` for detailed run instructions and helper `make` targets.

## Running examples
Each example directory includes a README with expected output. Use
`make examples_<name>` to run with drawer output and generate a PNG,
`make examples_run EXAMPLE=<name>` to run by folder name, or `make examples_all`
to run everything.

## Core examples
- Quick start: `go run ./examples/quick-start`
- One-to-many: `go run ./examples/one-to-many`
- FromChan: `go run ./examples/from-chan`
- Splitter + merger: `go run ./examples/splitter-merger`
- Sink: `go run ./examples/sink`
- SinkFromChan: `go run ./examples/sink-from-chan`
- Retry (sink): `go run ./examples/retry`
- Timeout + retry: `go run ./examples/timeout-retry`
- Batching/windowing: `go run ./examples/batching`
- Batching/windowing (chan): `go run ./examples/batching-chan`
- Pipeline defaults: `go run ./examples/pipeline-defaults`
- Dry-run: `go run ./examples/dry-run`
- Step options: `go run ./examples/step-options`
- Step limits: `go run ./examples/step-limits`
- Rate limit: `go run ./examples/rate-limit`
- Max in-flight: `go run ./examples/max-inflight`
- Drop on full: `go run ./examples/drop-on-full`
- Drop on blocked: `go run ./examples/drop-on-blocked`
- Drop on error: `go run ./examples/drop-on-error`
- Drop overload: `go run ./examples/drop-overload`
- Metrics + drawer: `go run ./examples/metrics-drawer`
- Live monitoring (Telegraf): `go run ./examples/live-monitoring`

## Advanced examples
- Split + merge + metrics: `go run ./examples/split-merge-metrics`
- Concurrency + aggregation: `go run ./examples/concurrency-aggregate`
- Backpressure + buffering: `go run ./examples/backpressure-buffering`
- SplitBy routing: `go run ./examples/splitby-routing`

## Timing diagrams
See `../examples/timing-diagrams.md` for timing/behavior diagrams covering every
example.

![Pipeline diagram](../examples/metrics-drawer/pipeline.png)
