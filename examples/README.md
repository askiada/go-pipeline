# Examples

This folder contains runnable example programs. Each example directory has a `README.md` with its purpose and expected output.
See `docs/examples.md` for the high-level example index and `examples/timing-diagrams.md` for timing/behavior diagrams covering every example.

## Run one example with drawer output
From repo root:
```
make examples_<name>
make examples_run EXAMPLE=<name>
```

Or from this folder:
```
make <name>
make run EXAMPLE=<name>
```

This runs `go run ./examples/<name> -drawer` and generates `pipeline.dot` and `pipeline.png` in the example folder.

## Run all examples
```
make examples_all
```

Or from this folder:
```
make all
```

## List available examples
```
make examples_list
```

Or from this folder:
```
make list
```
