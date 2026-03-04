# Step Options Guide

This guide explains the step options available in go-pipeline, how they interact, and when to use them.

## Pipeline defaults
Pass `PipelineDefaults` as a pipeline option to set step concurrency, buffers,
and splitter buffering:

```go
defaults := pipeline.PipelineDefaults{
	StepConcurrency:    4,
	StepBufferSize:     16,
	StepKeepOpen:       false,
	SplitterBufferSize: 4,
}

pipe, err := pipeline.New(defaults)
if err != nil {
	log.Fatal(err)
}
```

## Supported step types
Applies to:
- `OneToOne`, `OneToMany`, `Sink`: all step options listed below.
- `FromChan`, `SinkFromChan`: `StepConcurrency`, `StepBufferSize`, `StepKeepOpen`.
- `Batch`, `BatchChan`: `BatchPolicy` (required), `StepConcurrency`, `StepBufferSize`, `StepKeepOpen`, `StepDropOnFull`, `StepDropOnBlocked`.
- `Root`: `StepBufferSize`, `StepKeepOpen`.

Not supported:
- `StepRetry`, `StepTimeout`, `StepRateLimit`, `StepMaxInFlight` do not apply to `Root`, `FromChan`, `SinkFromChan`, `Batch`, or `BatchChan`.
- `StepDropOnFull` and `StepDropOnBlocked` do not apply to `Root`, `FromChan`, `SinkFromChan`, or `Sink`.
- `StepDropOnError` and `StepErrorOutput` do not apply to `Root`, `FromChan`, `SinkFromChan`, `Batch`, or `BatchChan`.

## Option reference

| Option | Purpose | Typical use |
| --- | --- | --- |
| `StepConcurrency` | Worker count for a step. | Increase throughput on CPU/I/O bound work. |
| `StepBufferSize` | Output channel buffer size. | Reduce backpressure or smooth bursts. |
| `StepKeepOpen` | Keep output channel open after a step completes. | Manual ownership of output lifecycle. |
| `StepRetry` | Retry failed items (per item). | Transient failures (network, external APIs). |
| `StepTimeout` | Per-item deadline for a step invocation. | Fail fast on slow or stuck work. |
| `StepRateLimit` | Throttle per-item execution (shared across workers). | Respect QPS limits or smooth traffic. |
| `StepMaxInFlight` | Limit active work per step. | Cap memory and CPU pressure. |
| `StepDropOnFull` | Drop items if output buffer is full. | Best-effort workloads under overload. |
| `StepDropOnBlocked` | Drop items if output send blocks longer than a timeout. | Allow brief backpressure, drop on sustained stalls. |
| `StepDropOnError` | Drop errored items after retries are exhausted. | Keep pipelines moving when errors are tolerable. |
| `StepErrorOutput` | Create an error step + option to route errored items. | Inspect failed items without halting the pipeline. |

## Interaction rules
- `StepConcurrency` defines how many workers run in parallel.
- `StepRateLimit` is shared across workers, so overall throughput is capped even with high concurrency.
- `StepMaxInFlight` limits only the step function execution. Output sends happen after the slot is released.
- `StepBufferSize` controls output buffering; it does not increase the in-flight limit.
- `StepTimeout` starts after the rate-limit wait and applies across all retries for an item.
- `StepRetry` runs under the same per-item context, so timeouts are not reset between retries.
- `StepRateLimit` is applied once per item, not per retry attempt.
- `BatchPolicy` changes the unit of work: buffer size applies to batches, not individual items.
- `StepDropOnFull` is non-blocking and takes precedence over `StepDropOnBlocked` if both are set.
- `StepDropOnBlocked` uses a per-item timer; each blocked send is dropped after the configured duration.
- Dropped outputs do not record `OnStepOutput` metrics; drop counts are tracked separately.
- `StepDropOnError` fires after retries are exhausted; errors are still routed to `StepErrorOutput`.
- `StepErrorOutput` sends block and respects the run context; drain the error step to avoid backpressure.

### Timeout + retry details
- `StepTimeout` applies to the entire item lifecycle for that step, including all retries.
- The timeout does not reset between attempts; once the deadline is reached, retries stop.
- Rate limiting happens before the timeout window starts.

### Drop policies and error routing
- `StepDropOnFull` drops immediately when output buffers are full or receivers are not ready.
- `StepDropOnBlocked` drops after waiting `timeout` to send an item downstream.
- `StepDropOnError` drops items after retries fail; the pipeline continues.
- `StepErrorOutput` exposes a per-step error channel (pipeline-owned; sends block and respects context).
The error channel is pipeline-owned and closed when the step finishes; `StepErrorOutput` returns the error step and buffer size controls its channel.
Ensure the error step is drained so error routing does not block the main pipeline.
The error step is named after the source step with an " error" suffix.

```go
errStep, errOpt := pipeline.StepErrorOutput[int](16)
work := pipeline.OneToOne(pipe, "work", root, workFn,
    pipeline.StepDropOnBlocked[int](20*time.Millisecond),
    pipeline.StepDropOnError[int](),
    errOpt,
)

pipeline.Sink(pipe, "work errors", errStep, func(ctx context.Context, errItem model.StepError) error {
    log.Printf("failed item: %v (err=%v)", errItem.Item, errItem.Err)
    return nil
})
```

### Why set `StepConcurrency` higher than `StepMaxInFlight`?
Most of the time you can set them equal. Use a higher concurrency when output handoff can block and you still want to keep the compute portion saturated:
- Workers release the in-flight slot after the step function returns, before sending outputs downstream.
- If downstream is slow (small buffers or slow sinks), workers can block on the send.
- Extra workers let the step keep using all in-flight slots even while some goroutines are stuck on output handoff.

Rule of thumb: start with `StepConcurrency == StepMaxInFlight`. Increase concurrency only if you see the compute stage underutilized because workers are waiting to send outputs.

## Use cases and examples
Each use case has a dedicated example in `../examples/`.

### Rate limiting external systems
Use `StepRateLimit` when a downstream service enforces QPS limits or when you want to smooth bursty inputs. This keeps your pipeline honest even if you set higher concurrency.

Example: `../examples/rate-limit`

### Capping memory-heavy work
Use `StepMaxInFlight` when per-item processing allocates large buffers, performs heavy transforms, or holds external resources. This prevents spikes even when upstream is fast.

Example: `../examples/max-inflight`

### Timeout + retry for flaky dependencies
Use `StepTimeout` with `StepRetry` to fail fast while still tolerating transient errors. The timeout applies across all attempts for a single item.

Example: `../examples/timeout-retry`

### Concurrency and buffering control
Use `StepConcurrency`, `StepBufferSize`, and `StepKeepOpen` to tune throughput and backpressure without changing pipeline structure.

Example: `../examples/step-options`

### Batching for I/O efficiency
Use `Batch` and `BatchChan` when downstream writes are more efficient in larger payloads. `BatchChan` is the low-memory option for large batches.

Examples: `../examples/batching`, `../examples/batching-chan`

### Multi-constraint pipeline limits
Combine rate limiting, max in-flight, and timeouts for complex workloads that need both throughput control and strict resource caps.

Example: `../examples/step-limits`

### Overload shedding for slow downstreams
Use `StepDropOnFull` or `StepDropOnBlocked` when you prefer dropping items instead of stalling upstream. This is common for best-effort analytics or telemetry pipelines.

Examples: `../examples/drop-on-full`, `../examples/drop-on-blocked`

### Error-tolerant flows
Use `StepDropOnError` with `StepErrorOutput` when you want to keep processing while routing failed items elsewhere.

Example: `../examples/drop-on-error`

### Combined overload scenarios
Combine drop-on-blocked and drop-on-error when both slow downstreams and occasional failures are expected.

Example: `../examples/drop-overload`
