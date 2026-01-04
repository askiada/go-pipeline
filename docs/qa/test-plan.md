# QA Test Plan

Date: 2026-01-04

## Scope
- Core pipeline runtime (`pkg/pipeline`)
- Step execution and concurrency helpers, including one-to-one/one-to-many/from-chan/sink concurrency
- Drop policies and per-step error routing (`StepDropOnFull`, `StepDropOnBlocked`, `StepDropOnError`, `StepErrorOutput`)
- Error-channel utilities
- Deferred construction errors, run-time context handling, and pipeline defaults
- Monitoring option (Telegraf line protocol emission) (`pkg/pipeline/monitor`)
- Benchmark suite for baseline vs pipeline overhead (`pkg/pipeline/benchmarks_test.go`)
- Pipeline option hook split (base hooks without timings + timing-aware metrics hooks)

## Test execution
- Local: `make unit_test` (runs `go test -race -timeout 30s ./...` and Graphviz rendering).
- CI: `.github/workflows/go.yml` runs `golangci-lint` and `go test -v ./...` on Go 1.25.

## Data requirements
- None. Tests create in-memory channels and use synthetic inputs, including UI snapshot counters.

## Implemented tests
- `pkg/pipeline/pipeline_test.go` (Implemented): pipeline composition (root, one-to-one/one-to-many, split/merge, from-chan, sinks), one-to-one/one-to-many/from-chan/sink concurrency, error/cancellation paths, run-time context requirements, deferred construction errors, pipeline defaults, and runtime behavior.
- `pkg/pipeline/pipeline_root_step_test.go` (Implemented): root step creation, error handling, and cancellation behavior.
- `pkg/pipeline/step_internal_test.go` (Implemented): step execution helpers with sequential and concurrent settings, including cancellation behavior.
- `pkg/pipeline/errors_internal_test.go` (Implemented): error channel aggregation and merge behavior.
- `pkg/pipeline/helpers_internal_test.go` (Implemented): internal test helpers for input/output channel creation.
- `pkg/pipeline/helpers_test.go` (Implemented): external test helpers for input/output channel creation.
- `pkg/pipeline/context_helpers_test.go` (Implemented): `SendWithContext` and `DrainWithContext` helpers.
- `pkg/pipeline/measure/metric_test.go` (Implemented): metric snapshots return copies, averaged transport metrics do not mutate internal state, and concurrent access remains safe under race testing.
- `pkg/pipeline/drawer/drawer_svg_test.go` (Implemented): DOT output includes edge labels based on averaged transport metrics, includes retry metrics in node labels, and draw errors are surfaced on create failure.
- `pkg/pipeline/splitter_internal_test.go` (Implemented): splitter buffer warnings are emitted for small/large buffers and nil input details do not crash preparation.
- `pkg/pipeline/pipeline_test.go` (Implemented): pipeline reuse is rejected on the second `Run` call.
- `pkg/pipeline/splitter_test.go` (Implemented): `SplitBy` routing behavior and error propagation.
- `pkg/pipeline/drawer/pipeline_option_test.go` (Implemented): PipelineDrawer lifecycle hooks, output hook no-ops, and graph attribute/label updates.
- `pkg/pipeline/drawer/pipeline_option_test.go` (Implemented): dry-run omits metrics in drawer output.
- `pkg/pipeline/measure/pipeline_option_test.go` (Implemented): pipeline measure hooks populate metrics and record transport/total durations.
- `pkg/pipeline/defaults_test.go` (Implemented): PipelineDefaults option hooks return nil without side effects.
- `pkg/pipeline/pipeline_test.go` (Implemented): retry option success paths for one-to-one and sink steps, retry metrics separation, and unsupported retry use on channel-based steps.
- `pkg/pipeline/measure/metric_test.go` (Implemented): retry metrics average and count tracking.
- `pkg/pipeline/pipeline_test.go` (Implemented): drain-on-return for `FromChan`/`SinkFromChan` and early cancellation on first error.
- `pkg/pipeline/pipeline_test.go` (Implemented): batch (slice + channel) flushes on size and window timeout, and rejects missing batch policy.
- `pkg/pipeline/pipeline_test.go` (Implemented): step timeout, rate limit, and max in-flight behavior, plus unsupported option errors for root, from-chan/sink-from-chan, and batch steps.
- `pkg/pipeline/pipeline_test.go` (Implemented): dry-run skips runner execution and still allows a later real run.
- `pkg/pipeline/pipeline_test.go` (Implemented): drop-on-full, drop-on-blocked, drop-on-error with error routing, and unsupported drop options for root/from-chan/sink-from-chan/batch steps.
- `pkg/pipeline/measure/metric_test.go` (Implemented): drop counters and routed error counts.
- `pkg/pipeline/measure/pipeline_option_test.go` (Implemented): drop and error-route hooks populate metrics.
- `pkg/pipeline/drawer/drawer_svg_test.go` (Implemented): drawer labels include drop counts and routed error counts.
- `pkg/pipeline/monitor/monitor_test.go` (Implemented): monitoring line protocol includes run identity tags, skips emission during dry-run, and UI starts only on real runs.
- `pkg/pipeline/monitor/monitor_test.go` (Implemented): UI snapshot event reports absolute output/drop/retry/error totals plus run total and sequence marker.
- `pkg/pipeline/monitor/monitor_test.go` (Implemented): UI snapshot events are prioritized when the UI stream buffer is full.
- `pkg/pipeline/pipeline_test.go` (Implemented): base output hooks run without metrics options; timing hooks are exercised via metrics options.
- `pkg/pipeline/step_internal_test.go` (Implemented): transport timing captures input receive wait time for per-step metrics.

## Planned tests
- None defined yet.

## Benchmarks
- `pkg/pipeline/benchmarks_test.go` (Implemented): baseline loop vs ad-hoc channel vs pipeline comparisons for single-stage, two-stage, and split/merge flows, plus pipeline-only coverage for OneToMany, FromChan, SinkFromChan, Batch, and BatchChan.
