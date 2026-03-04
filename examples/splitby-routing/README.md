# SplitBy routing

Routes items to multiple branches using predicates with `SplitBy`.

Run:
```
go run ./examples/splitby-routing
```

Run with drawer output:
```
go run ./examples/splitby-routing -drawer
```

Render the PNG:
```
dot -Tpng examples/splitby-routing/pipeline.dot -o examples/splitby-routing/pipeline.png
```

Expected output (order may vary; some items appear multiple times):
```
odd:1
even:2
odd:3
mul3:3
even:4
odd:5
even:6
mul3:6
```
