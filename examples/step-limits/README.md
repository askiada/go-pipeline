# Step limits

Demonstrates `StepTimeout`, `StepRateLimit`, and `StepMaxInFlight` together, with a small batch of integers.

Why these options are useful:
- `StepRateLimit` keeps a steady request rate when calling rate-limited APIs or shared infrastructure.
- `StepMaxInFlight` caps how many items are actively being processed to control memory and CPU pressure.
- `StepTimeout` fails fast on slow or stuck work and is especially helpful when paired with retries.

This example uses `StepConcurrency(2)` with `StepMaxInFlight(1)` so one worker can block on output while the other keeps compute moving.

Run:
```
go run ./examples/step-limits
```

Run with drawer output:
```
go run ./examples/step-limits -drawer
```

Render the PNG:
```
dot -Tpng examples/step-limits/pipeline.dot -o examples/step-limits/pipeline.png
```

Expected output:
```
value: 0
value: 2
value: 4
value: 6
value: 8
```
