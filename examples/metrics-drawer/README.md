# Metrics + drawer

Attaches pipeline metrics and emits a Graphviz-ready `.dot` file.

Run:
```
go run ./examples/metrics-drawer
```

Run with drawer output:
```
go run ./examples/metrics-drawer -drawer
```

Render the PNG:
```
dot -Tpng examples/metrics-drawer/pipeline.dot -o examples/metrics-drawer/pipeline.png
```

Expected output:
```
(no console output)
```
