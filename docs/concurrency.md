# Concurrency and backpressure

This document explains concurrency, buffering, and backpressure behavior. See
`step-options.md` for option details.

## Splitter buffer sizing and backpressure
`SplitterBufferSize` controls the per-branch buffer used by splitters. Each
input item is copied into every branch buffer, so memory use scales with
`buffer size × branches`. Smaller buffers apply backpressure to the upstream
step; larger buffers allow more in-flight items but can amplify memory use when
downstream steps are slow. As a starting point, set the buffer size close to the
upstream step concurrency and tune from there. The splitter logs a warning if
the buffer size is much smaller or much larger than the input concurrency.

## Thread safety and run lifecycle
Build the pipeline before calling `Run(ctx)`; avoid mutating steps while a run
is in progress. Pipelines are single-run; create a new pipeline instance for
each execution.
