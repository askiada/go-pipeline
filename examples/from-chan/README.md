# FromChan

Uses `FromChan` to transform an input channel into a different output type.

Run:
```
go run ./examples/from-chan
```

Run with drawer output:
```
go run ./examples/from-chan -drawer
```

Render the PNG:
```
dot -Tpng examples/from-chan/pipeline.dot -o examples/from-chan/pipeline.png
```

Expected output:
```
0:id-0
1:id-1
2:id-2
3:id-3
4:id-4
```
