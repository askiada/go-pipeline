# Backpressure + buffering

Uses a small `SplitterBufferSize` with uneven branch latencies to demonstrate backpressure.

Run:
```
go run ./examples/backpressure-buffering
```

Run with drawer output:
```
go run ./examples/backpressure-buffering -drawer
```

Render the PNG:
```
dot -Tpng examples/backpressure-buffering/pipeline.dot -o examples/backpressure-buffering/pipeline.png
```

Expected output (order may vary):
```
left-0
left-1
left-2
right-0
right-1
right-2
```
