# Drop on error

Demonstrates `StepDropOnError` with `StepErrorOutput` for error routing.

Run:
```
go run ./examples/drop-on-error
```

Run with drawer output:
```
go run ./examples/drop-on-error -drawer
```

Render the PNG:
```
dot -Tpng examples/drop-on-error/pipeline.dot -o examples/drop-on-error/pipeline.png
```

Expected output (order and counts can vary with scheduling):
```
error: 0 (boom on 0)
processed: 2
processed: 4
error: 3 (boom on 3)
processed: 8
processed: 10
dropped (error): 2
errors routed: 2
```
