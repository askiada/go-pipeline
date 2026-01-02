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
- docs/qa/test-plan.md: QA data requirements and scenarios.
- docs/live-monitoring/: local Telegraf/InfluxDB docker stack for monitoring.
- docs/plans/: accepted, draft, completed, and archived plans (internal only).
- docs/plans/order.md: plan execution order and dependency tracking.

## Status (internal)
Latest QA test plan update: 2026-01-02 (tests implemented).
Latest lint cleanup plan: 2025-12-29 (accepted).
Latest examples plan: 2025-12-29 (accepted).
Latest step options plan: 2025-12-29 (completed).
Latest benchmarks plan: 2025-12-30 (completed).
Latest benchmarks update: 2026-01-02 (run-only timing; overhead model refreshed with medians from 5 runs; composite stages capped at 12; channels baselines respect benchmark context cancellation).
Latest dry-run plan: 2025-12-30 (completed).
Latest drop policies plan: 2025-12-30 (completed).
Latest live monitoring plan: 2026-01-01 (accepted).
Latest docs restructure plan: 2026-01-02 (completed).
