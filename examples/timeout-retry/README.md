# Timeout + retry

Demonstrates `StepTimeout` with `StepRetry`. The timeout applies across all attempts for a single item.

Run:
```
go run ./examples/timeout-retry
```

Run with drawer output:
```
go run ./examples/timeout-retry -drawer
```

Render the PNG:
```
dot -Tpng examples/timeout-retry/pipeline.dot -o examples/timeout-retry/pipeline.png
```

Expected output:
```
value: 100
value: 200
```

Tip: Reduce the timeout to see retries fail once the per-item deadline is exceeded.
