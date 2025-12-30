# Rate limit

Demonstrates `StepRateLimit` to respect downstream QPS limits while keeping worker concurrency higher.

Run:
```
go run ./examples/rate-limit
```

Run with drawer output:
```
go run ./examples/rate-limit -drawer
```

Render the PNG:
```
dot -Tpng examples/rate-limit/pipeline.dot -o examples/rate-limit/pipeline.png
```

Expected output (order may vary due to concurrency):
```
value: 0
value: 1
value: 2
value: 3
value: 4
```
