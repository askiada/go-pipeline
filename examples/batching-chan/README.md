# Batching with channel output

Streams each batch over a channel to avoid materializing a full slice in memory.

Run:
```
go run ./examples/batching-chan
```

Run with drawer output:
```
go run ./examples/batching-chan -drawer
```

Render the PNG:
```
dot -Tpng examples/batching-chan/pipeline.dot -o examples/batching-chan/pipeline.png
```

Expected output:
```
batch: [1 2]
batch: [3 4]
```
