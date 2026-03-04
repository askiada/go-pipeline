# Drop on blocked

Demonstrates `StepDropOnBlocked`, which drops items when output sends block longer than the configured timeout.

Run:
```
go run ./examples/drop-on-blocked
```

Run with drawer output:
```
go run ./examples/drop-on-blocked -drawer
```

Render the PNG:
```
dot -Tpng examples/drop-on-blocked/pipeline.dot -o examples/drop-on-blocked/pipeline.png
```

Expected output (order and counts can vary with scheduling):
```
processed: 0
processed: 3
dropped (timeout): 4
```
