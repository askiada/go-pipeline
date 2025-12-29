# Quick start

A minimal Root -> OneToOne -> Sink pipeline that also demonstrates a type change (int -> string).

Run:
```
go run ./examples/quick-start
```

Run with drawer output:
```
go run ./examples/quick-start -drawer
```

Render the PNG:
```
dot -Tpng examples/quick-start/pipeline.dot -o examples/quick-start/pipeline.png
```

Expected output:
```
item-0
item-1
item-2
```
