# Drop on full

Demonstrates `StepDropOnFull`, which drops items when the output channel is full or the receiver is not ready.

Run:
```
go run ./examples/drop-on-full
```

Run with drawer output:
```
go run ./examples/drop-on-full -drawer
```

Render the PNG:
```
dot -Tpng examples/drop-on-full/pipeline.dot -o examples/drop-on-full/pipeline.png
```

Expected output (order and counts can vary with scheduling):
```
processed: 0
dropped (full): 5
```
