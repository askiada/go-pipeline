# Pipeline defaults

Sets `PipelineDefaults` once and relies on defaults for step concurrency and buffers.

Run:
```
go run ./examples/pipeline-defaults
```

Run with drawer output:
```
go run ./examples/pipeline-defaults -drawer
```

Render the PNG:
```
dot -Tpng examples/pipeline-defaults/pipeline.dot -o examples/pipeline-defaults/pipeline.png
```

Expected output (order may vary):
```
1
3
5
7
10
12
14
16
```
