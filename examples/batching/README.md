# Batching and windowing

Groups items into batches using `MaxSize` and flushes partial batches when `MaxWait` expires.

Run:
```
go run ./examples/batching
```

Run with drawer output:
```
go run ./examples/batching -drawer
```

Render the PNG:
```
dot -Tpng examples/batching/pipeline.dot -o examples/batching/pipeline.png
```

Expected output:
```
batch: [1 2]
batch: [3 4]
```
