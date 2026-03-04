# Split + merge + metrics

Combines fan-out/fan-in with metrics and drawer output.

Run:
```
go run ./examples/split-merge-metrics
```

Run with drawer output:
```
go run ./examples/split-merge-metrics -drawer
```

Render the PNG:
```
dot -Tpng examples/split-merge-metrics/pipeline.dot -o examples/split-merge-metrics/pipeline.png
```

Expected output (order may vary):
```
11
12
13
14
21
22
23
24
31
32
33
34
```
