# Retry

A minimal Root -> Sink pipeline that retries sink failures per item.

Run:
```
go run ./examples/retry
```

Run with drawer output:
```
go run ./examples/retry -drawer
```

Render the PNG:
```
dot -Tpng examples/retry/pipeline.dot -o examples/retry/pipeline.png
```

Expected output:
```
retry 1
done 1
retry 2
done 2
```
