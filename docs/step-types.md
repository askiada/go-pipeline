# Step types

This document describes the step types and when to use them.

## Splitter and merger example
```go
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/askiada/go-pipeline/v2/pkg/pipeline"
)

func main() {
	ctx := context.Background()
	pipe, err := pipeline.New(pipeline.PipelineDefaults{})
	if err != nil {
		log.Fatal(err)
	}

	root := pipeline.Root(pipe, "root", func(ctx context.Context, out chan<- int) error {
		for _, v := range []int{1, 2, 3, 4, 5} {
			out <- v
		}
		return nil
	})

	split := pipeline.Split(pipe, "split", root, 2)

	double := pipeline.OneToOne(pipe, "double", split[0], func(ctx context.Context, in int) (int, error) {
		return in * 2, nil
	})

	hundred := pipeline.OneToOne(pipe, "hundred", split[1], func(ctx context.Context, in int) (int, error) {
		return in * 100, nil
	})

	merged := pipeline.Merge(pipe, "merge", double, hundred)

	pipeline.Sink(pipe, "print", merged, func(ctx context.Context, in int) error {
		fmt.Println(in)
		return nil
	})

	if err := pipe.Run(ctx); err != nil {
		log.Fatal(err)
	}
}
```

## Step types and when to use them
Use `OneToOne` when each input item maps to a single output item. Use `OneToMany`
when each input item should expand into multiple output items (fan-out).

### OneToOne vs OneToMany
- `OneToOne`: transform one input into one output (map/transform).
- `OneToMany`: expand one input into many outputs (fan-out or split).

### Per-step concurrency
Use `pipeline.StepConcurrency[...]` on `OneToOne`, `OneToMany`, `FromChan`, and
sink steps to control worker concurrency.

### Per-step retry
Use `pipeline.StepRetry[...]` on `OneToOne`, `OneToMany`, and `Sink` to retry
failed items without blocking other workers. Retries do not apply to
`FromChan`/`SinkFromChan`. Retry metrics are tracked separately (average retry
duration and count) so step averages remain focused on successful attempts.
Drawer output includes retry averages and counts in step labels.
```go
step := pipeline.OneToOne(pipe, "enrich", root, enrichFn,
	pipeline.StepRetry[int](pipeline.RetryPolicy{
		MaxAttempts: 3,
		Backoff:     50 * time.Millisecond,
	}),
)
```

### Per-step timeout, rate limit, and max in-flight
Use `pipeline.StepTimeout[...]`, `pipeline.StepRateLimit[...]`, and
`pipeline.StepMaxInFlight[...]` on `OneToOne`, `OneToMany`, and `Sink` steps to
bound per-item execution. These options do not apply to `Root`, `FromChan`,
`SinkFromChan`, `Batch`, or `BatchChan`.

- `StepTimeout`: per-item deadline for the step function; applies across retries.
  Rate limiting happens before the timeout window starts.
- `StepRateLimit`: shared rate limit across all workers; each item waits for a
  token before executing the step function.
- `StepMaxInFlight`: caps the number of items actively processed by the step
  function, even if `StepConcurrency` is higher; output handoff can still block
  independently. When unset, no in-flight semaphore is acquired.

`StepBufferSize` still controls output channel buffering; rate limiting and max
in-flight apply before items are enqueued downstream. For deeper interaction
notes (including when to set `StepConcurrency` above `StepMaxInFlight`), see
`step-options.md`.

```go
step := pipeline.OneToOne(pipe, "limit", root, workFn,
	pipeline.StepTimeout[int](150*time.Millisecond),
	pipeline.StepRateLimit[int](pipeline.RateLimitPolicy{Every: 20 * time.Millisecond, Burst: 1}),
	pipeline.StepMaxInFlight[int](2),
)
```

Real-world use cases for rate limiting and max in-flight:
- Protecting external APIs: use `StepRateLimit` to stay under vendor QPS limits,
  and `StepTimeout` to avoid hanging calls when the service is unhealthy.
- Smoothing bursty inputs: use `StepRateLimit` to spread traffic from an
  upstream queue or file ingest; pair it with `StepBufferSize` if you need to
  absorb short spikes.
- Bounding memory-heavy work: use `StepMaxInFlight` to cap concurrent processing
  when each item allocates large buffers or holds large payloads in memory.
- Avoiding per-item fanout overload: use `StepMaxInFlight` on a `OneToMany` step
  to avoid spawning too many concurrent expansions at once.
- Limiting contention on shared resources: use `StepMaxInFlight` to keep the
  number of active DB or cache operations low, even if `StepConcurrency` is
  higher for scheduling flexibility.
- Cooperative fairness across pipelines: use `StepRateLimit` to reserve a
  steady slice of throughput for each pipeline rather than letting the fastest
  one monopolize a downstream service.

Interaction notes:
- `StepRateLimit` is shared across workers, so the overall step throughput is
  bounded even with high `StepConcurrency`.
- `StepMaxInFlight` limits only the step function execution; output sends happen
  after the slot is released. If downstream backpressure matters, tune
  `StepBufferSize` and/or reduce `StepConcurrency`.
- `StepTimeout` starts after the rate-limit wait and applies across retries; the
  total wall-clock time per item is rate-limit wait + timeout + any retry
  backoff.

### Drop policies and error routing
Drop policies are opt-in. By default, output sends block and errors stop the
pipeline. Use these options to keep pipelines moving under overload:
- `StepDropOnFull`: non-blocking output sends; drops immediately when the output
  is full.
- `StepDropOnBlocked`: drops after waiting `timeout` to send downstream.
- `StepDropOnError`: drops items after retries are exhausted instead of
  propagating the error.
- `StepErrorOutput`: returns an error step + option to route failed items
  (pipeline-owned; sends block and respects context).

Dropped items are omitted from step averages; drop counters and routed error
counts show up in metrics/drawer output. Error routing does not change default
error propagation unless `StepDropOnError` is set. For one-to-many steps, output
observers still run if at least one output is delivered, even when some outputs
drop. Error channels are pipeline-owned and closed when the step finishes;
`StepErrorOutput` sets the buffer size on the error step. The error step is
named after the source step with an " error" suffix.

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

### Batching/windowing
Use `pipeline.Batch` to group items into slices before processing, or
`pipeline.BatchChan` to stream each batch over a channel for lower memory usage.
`MaxSize` is required; `MaxWait` flushes partial batches on a timer.
`StepBufferSize` applies to the number of batches buffered, not individual
items.
```go
batch := pipeline.Batch(pipe, "batch", root, pipeline.BatchPolicy{
	MaxSize: 10,
	MaxWait: 50 * time.Millisecond,
})

pipeline.Sink(pipe, "sink", batch, func(ctx context.Context, input []int) error {
	fmt.Println(input)
	return nil
})
```

```go
batch := pipeline.BatchChan(pipe, "batch", root, pipeline.BatchPolicy{
	MaxSize: 10,
	MaxWait: 50 * time.Millisecond,
})

pipeline.Sink(pipe, "sink", batch, func(ctx context.Context, input <-chan int) error {
	for item := range input {
		fmt.Println(item)
	}
	return nil
})
```

### Channel closing behavior
By default, the library closes step output channels when a step finishes,
including when using `FromChan`. To keep a channel open, pass the keep-open
option (for example, `pipeline.StepKeepOpen[...]()`).
