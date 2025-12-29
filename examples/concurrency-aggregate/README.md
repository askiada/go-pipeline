# Concurrency + aggregation

Runs concurrent workers and aggregates deterministic output with `SinkFromChan`.

Run:
```
go run ./examples/concurrency-aggregate
```

Run with drawer output:
```
go run ./examples/concurrency-aggregate -drawer
```

Render the PNG:
```
dot -Tpng examples/concurrency-aggregate/pipeline.dot -o examples/concurrency-aggregate/pipeline.png
```

Expected output:
```
job-1 -> 20
job-2 -> 40
job-3 -> 60
job-4 -> 80
job-5 -> 100
job-6 -> 120
```
