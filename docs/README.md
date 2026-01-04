# Docs

This folder contains documentation for go-pipeline.

## Public docs
- docs/concepts.md: core mental model and execution semantics.
- docs/step-types.md: step types, usage guidance, and examples.
- docs/step-options.md: step options, interactions, and defaults.
- docs/pipeline-options.md: metrics, drawer, monitoring, and run options.
- docs/concurrency.md: backpressure and concurrency guidance.
- docs/errors.md: error propagation, retries, and drop policies.
- docs/faq.md: common questions and usage reminders.
- docs/performance.md: performance guidance with links to benchmarks.
- docs/benchmarks.md: benchmark scenarios, overhead guidance, and results tracking.
- docs/examples.md: example index and usage map.

## Internal docs (not published)
- docs/diagnoses.md: running repository diagnosis log.
- docs/qa/test-plan.md: QA data requirements and scenarios.
- docs/live-monitoring/: local Telegraf/InfluxDB docker stack for monitoring.
- docs/plans/: accepted, draft, completed, and archived plans (internal only).
- docs/plans/order.md: plan execution order and dependency tracking.

## Status (internal)
Latest diagnosis update: 2026-01-04 (repo diagnosis logged).
Latest diagnosis refactor cleanup plan: 2026-01-04 (completed).
Latest QA test plan update: 2026-01-04 (tests implemented).
Latest lint cleanup plan: 2025-12-29 (accepted).
Latest examples plan: 2025-12-29 (accepted).
Latest step options plan: 2025-12-29 (completed).
Latest benchmarks plan: 2025-12-30 (completed).
Latest benchmarks update: 2026-01-04 (count=5 run posted after removing splitter worker goroutines).
Latest splitter update: 2026-01-04 (no per-branch worker goroutines when concurrency is 1).
Latest dry-run plan: 2025-12-30 (completed).
Latest drop policies plan: 2025-12-30 (completed).
Latest live monitoring plan: 2026-01-01 (accepted).
Latest docs restructure plan: 2026-01-02 (completed).
