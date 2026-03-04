# Concepts

This document explains the core mental model of go-pipeline. For step-by-step
API usage, see `step-types.md` and `step-options.md`.

## Pipeline
A pipeline is an ordered graph of steps that process data concurrently using
typed channels. The pipeline owns channel wiring and lifecycle so the steps can
focus on processing.

## Step
A step consumes input values, performs work, and emits outputs to the next step.
There are multiple step types (one-to-one, one-to-many, batching, etc.) to cover
common patterns without forcing custom channel plumbing.

## Execution model
Each step runs concurrently. Backpressure is enforced by channel operations and
buffers; tuning concurrency and buffering is how you balance throughput vs
memory. See `concurrency.md` for guidance.

## Errors and cancellation
By default, the pipeline stops on the first error and cancels the run context.
Retry, drop policies, and error routing let you keep processing when errors are
expected. See `errors.md` and `step-options.md` for details.

## Single-run semantics
Pipelines are single-run. Build a pipeline before calling `Run(ctx)`, and create
a new pipeline instance for each execution.
