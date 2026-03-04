# SinkFromChan

Aggregates all outputs with `SinkFromChan` and prints a summary.

Run:
```
go run ./examples/sink-from-chan
```

Run with drawer output:
```
go run ./examples/sink-from-chan -drawer
```

Render the PNG:
```
dot -Tpng examples/sink-from-chan/pipeline.dot -o examples/sink-from-chan/pipeline.png
```

Expected output:
```
values: [0 1 2 3 4 5]
```
