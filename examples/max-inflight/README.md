# Max in-flight

Demonstrates `StepMaxInFlight` to cap active work while keeping worker concurrency higher. The example uses `StepConcurrency(4)` with `StepMaxInFlight(2)` so two workers can block on output handoff while two continue processing.

Run:
```
go run ./examples/max-inflight
```

Run with drawer output:
```
go run ./examples/max-inflight -drawer
```

Render the PNG:
```
dot -Tpng examples/max-inflight/pipeline.dot -o examples/max-inflight/pipeline.png
```

Expected output (order may vary due to concurrency):
```
value: 0
value: 10
value: 20
value: 30
```
