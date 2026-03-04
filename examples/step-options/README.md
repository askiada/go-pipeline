# Step options

Demonstrates `StepConcurrency`, `StepKeepOpen`, and `StepBufferSize`.

Run:
```
go run ./examples/step-options
```

Run with drawer output:
```
go run ./examples/step-options -drawer
```

Render the PNG:
```
dot -Tpng examples/step-options/pipeline.dot -o examples/step-options/pipeline.png
```

Expected output (order may vary due to concurrency):
```
task-0
task-1
task-2
task-3
task-4
```
