# Splitter + merger

Splits a stream into two branches and merges them back together.

Run:
```
go run ./examples/splitter-merger
```

Run with drawer output:
```
go run ./examples/splitter-merger -drawer
```

Render the PNG:
```
dot -Tpng examples/splitter-merger/pipeline.dot -o examples/splitter-merger/pipeline.png
```

Expected output (order may vary):
```
10
20
30
40
50
100
200
300
400
500
```
