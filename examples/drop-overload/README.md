# Drop overload

Demonstrates a combined overload setup: drop on blocked output sends, drop on error, and error routing.

Run:
```
go run ./examples/drop-overload
```

Run with drawer output:
```
go run ./examples/drop-overload -drawer
```

Render the PNG:
```
dot -Tpng examples/drop-overload/pipeline.dot -o examples/drop-overload/pipeline.png
```

Expected output (order and counts can vary with scheduling):
```
error: 0 (bad item 0)
processed: 10
error: 5 (bad item 5)
processed: 40
error: 10 (bad item 10)
processed: 80
dropped (timeout): 6
dropped (error): 3
errors routed: 3
```
