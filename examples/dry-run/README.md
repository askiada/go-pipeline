# Dry run

Builds a pipeline, validates wiring, and emits a drawer diagram without executing any runners.

Run:
```
go run ./examples/dry-run
```

Run with drawer output:
```
go run ./examples/dry-run -drawer
```

Render the PNG:
```
dot -Tpng examples/dry-run/pipeline.dot -o examples/dry-run/pipeline.png
```

Expected output:
```
(no stdout; dry-run does not execute step functions)
```
