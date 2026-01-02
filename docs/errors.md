# Errors and recovery

By default, the pipeline stops on the first error and cancels the run context.
Error handling options let you keep processing when failures are expected.

## Default behavior
- Step errors are returned from `Run` and halt the pipeline.
- Construction errors are deferred until `Run(ctx)`, unless you call `Err()` to
  preflight.

## Retry and drop policies
- `StepRetry` retries per-item failures without blocking other workers.
- `StepDropOnError` drops items after retries are exhausted instead of
  propagating the error.
- `StepDropOnFull` and `StepDropOnBlocked` shed load when downstream is slow.

## Error routing
Use `StepErrorOutput` to route failed items into a dedicated error step while
the main pipeline continues. The error channel is pipeline-owned and sends
block; if the run context is canceled, errors may not be routed. Ensure the
error step is drained if you want to avoid backpressure.

See `step-options.md` for full option details and `../examples/drop-on-error`
for a runnable example.
