# FAQ

## Do I need to call `pipe.Err()` before `Run`?
No. Construction errors are deferred until `Run(ctx)` so you can wire the
pipeline without per-step error checks. `pipe.Err()` is available if you want a
preflight check.

## Do steps close their output channels?
Yes. The library closes step output channels when a step finishes, including
when using `FromChan`. Use `pipeline.StepKeepOpen[...]()` to keep a channel open.

## Are pipelines reusable?
Pipelines are single-run. Build a pipeline before calling `Run(ctx)` and create
a new pipeline instance for each execution.

## When should I use `Batch` vs `BatchChan`?
Use `Batch` when downstream prefers slices for bulk processing. Use `BatchChan`
when you want to stream items in a batch without allocating a large slice.
