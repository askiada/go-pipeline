# Sink

Consumes a stream with a simple `Sink` function.

Run:
```
go run ./examples/sink
```

Run with drawer output:
```
go run ./examples/sink -drawer
```

Render the PNG:
```
dot -Tpng examples/sink/pipeline.dot -o examples/sink/pipeline.png
```

Expected output:
```
saved: alpha
saved: beta
saved: gamma
```
