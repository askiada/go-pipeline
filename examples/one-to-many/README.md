# One-to-many

Expands each input into multiple outputs using `OneToMany`.

Run:
```
go run ./examples/one-to-many
```

Run with drawer output:
```
go run ./examples/one-to-many -drawer
```

Render the PNG:
```
dot -Tpng examples/one-to-many/pipeline.dot -o examples/one-to-many/pipeline.png
```

Expected output:
```
job-1-a
job-1-b
job-2-a
job-2-b
job-3-a
job-3-b
```
