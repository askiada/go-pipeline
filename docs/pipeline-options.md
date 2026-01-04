# Pipeline options and run options

This document covers pipeline-level options such as metrics, drawer output, and
monitoring, plus run-time options like dry-run validation.

## Metrics + drawer
You can attach pipeline options to collect metrics and emit Graphviz-ready
output:

```go
package main

import (
	"github.com/askiada/go-pipeline/v2/pkg/pipeline"
	"github.com/askiada/go-pipeline/v2/pkg/pipeline/drawer"
	"github.com/askiada/go-pipeline/v2/pkg/pipeline/measure"
)

func buildPipeline() (*pipeline.Pipeline, error) {
	msr := measure.NewDefaultMeasure()
	drw := drawer.NewSVGDrawer("pipeline.dot")

	return pipeline.New(
		measure.PipelineMeasure(msr),
		drawer.PipelineDrawer(drw, msr),
	)
}
```
Run `dot -Tpng pipeline.dot -O` if you want to render the output file as an
image.

Timing semantics:
- `duration_ms` / AVGDuration: time spent inside the step's processing function per output event. For one-to-one/one-to-many/sink steps, this is the user function runtime; splitters/batchers report internal processing time; mergers emit no duration.
- `transport_ms` / AVGTransportDuration: time spent waiting to receive input from the parent step (channel receive) per output event.
- Channel-based steps (`FromChan`, `SinkFromChan`, `Batch`, `BatchChan`) report per-item averages across their loop; these include time spent waiting inside the user function when applicable.

## Live monitoring (Telegraf)
For live monitoring with Telegraf (Influx line protocol), use the monitoring
option:

```go
import "github.com/askiada/go-pipeline/v2/pkg/pipeline/monitor"

func buildPipeline() (*pipeline.Pipeline, error) {
	cfg := monitor.Config{
		RunName:      "example",
		Origin:       "local",
		PipelineName: "demo",
		TelegrafAddr: "127.0.0.1:8094",
		TelegrafNet:  "udp",
		EnableUI:     true,
		BindAddr:     "127.0.0.1:8096",
	}

	return pipeline.New(monitor.PipelineMonitor(&cfg))
}
```

When `EnableUI` is true, the local dashboard is served at the `BindAddr` while
the pipeline is running. Use `PipelineMonitor.UIAddr()` after the run starts to
discover the actual host:port when binding to `:0`.

## Timing hooks
Pipeline options use timing-free hooks (`OnStepOutput`, `OnSplitterOutput`,
etc.). If you need durations, implement `model.PipelineMetricsOption` and the
corresponding `On*Metrics` methods so timing is only collected when needed.

## Dry-run validation
Use `RunDry()` to validate wiring and emit drawer output without executing any
runners. Dry-run does not mark the pipeline as “ran”, so you can run it
afterward. Drawer output in dry-run omits metrics.

```go
if err := pipe.Run(ctx, pipeline.RunDry()); err != nil {
	log.Fatal(err)
}
```
