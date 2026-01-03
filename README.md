# go-pipeline

[![Go Reference](https://pkg.go.dev/badge/github.com/askiada/go-pipeline/v2.svg)](https://pkg.go.dev/github.com/askiada/go-pipeline/v2)
[![Go Report Card](https://goreportcard.com/badge/github.com/askiada/go-pipeline)](https://goreportcard.com/report/github.com/askiada/go-pipeline)
[![CI](https://github.com/askiada/go-pipeline/actions/workflows/go.yml/badge.svg)](https://github.com/askiada/go-pipeline/actions/workflows/go.yml)

go-pipeline is a Go library for building data-processing pipelines with composable steps, splitters, mergers, and sinks.

## Table of contents
- Features
- Requirements
- Installation
- Quick start
- Concepts
- Advanced usage
- Examples
- Documentation
- Testing
- Linting
- Contributing
- Support
- Release & versioning
- Changelog
- License

## Features
- Generic steps: one-to-one, one-to-many, and channel-based step functions.
- Fan-out and fan-in via splitters and mergers.
- Concurrency, buffering, and backpressure controls.
- Retry, timeout, and drop policies for overload handling.
- Optional metrics collection, Graphviz drawer output, and live monitoring.

## Requirements
- Go 1.25 (per `go.mod`).
- Graphviz `dot` (local-only) to render `.dot` files and to run `make unit_test`; CI uses `go test -v ./...` without Graphviz.
- No external runtime dependencies; error wrapping uses the standard library.

## Installation
```bash
go get github.com/askiada/go-pipeline/v2
```

## Quick start
```go
package main

import (
	"context"
	"fmt"

	"github.com/askiada/go-pipeline/v2/pkg/pipeline"
)

func main() {
	pipe, _ := pipeline.New()

	root := pipeline.Root(pipe, "root", func(ctx context.Context, out chan<- int) error {
		for i := range 3 {
			out <- i
		}

		return nil
	})

	doubled := pipeline.OneToOne(pipe, "double", root, func(ctx context.Context, v int) (int, error) {
		return v * 2, nil
	})

	pipeline.Sink(pipe, "print", doubled, func(ctx context.Context, v int) error {
		fmt.Println(v)

		return nil
	})

	_ = pipe.Run(context.Background())
}
```
Construction errors are deferred until `pipe.Run(ctx)` so you can wire the
pipeline without per-step error checks. `pipe.Err()` is available if you want a
preflight check before running.

## Concepts
- Pipelines are ordered graphs of concurrent steps connected by channels.
- Steps transform or route data; splitters and mergers fan out and fan in.
- Backpressure comes from channel operations and configured buffers.

See `docs/concepts.md` and `docs/step-types.md` for the full mental model.

## Advanced usage
- Step defaults and option interactions: `docs/step-options.md`
- Pipeline options (metrics, drawer, monitoring, dry-run): `docs/pipeline-options.md`
- Error handling and recovery: `docs/errors.md`
- Concurrency and backpressure guidance: `docs/concurrency.md`
- Performance guidance and benchmarks: `docs/performance.md`, `docs/benchmarks.md`

## Performance snapshot
- Pipeline overhead is typically within ~0.8-1.8x of a direct channels baseline in the current benchmarks; loop-only baselines are much faster because they avoid channels entirely.
- Measured overhead per item is about 1-3 μs plus roughly 1.3 μs per step at conc=1 in the published run.
- As work per item increases, the overhead ratio shrinks; see `docs/benchmarks.md` for the work/step sweeps and sizing guidance.

## Examples
See `docs/examples.md` for a categorized index and `examples/README.md` for run
commands. Timing diagrams live in `examples/timing-diagrams.md`.

## Documentation
- Go doc (primary API reference): https://pkg.go.dev/github.com/askiada/go-pipeline/v2
- `docs/README.md` is the docs index.
- `docs/concepts.md` explains the pipeline mental model.
- `docs/step-types.md` covers step types and usage guidance.
- `docs/step-options.md` details step options and defaults.
- `docs/pipeline-options.md` covers metrics, drawer, monitoring, and dry-run.
- `docs/concurrency.md` explains backpressure and buffering.
- `docs/errors.md` documents error propagation and recovery.
- `docs/faq.md` answers common usage questions.
- `docs/performance.md` and `docs/benchmarks.md` cover performance guidance.
- `docs/examples.md` indexes runnable examples.

## Testing
```bash
make unit_test
```
`make unit_test` runs `go test -race -timeout 30s ./...` and invokes Graphviz to render example diagrams. CI runs `go test -v ./...` without Graphviz.

## Linting
```bash
make lint
```

## Contributing
See `CONTRIBUTING.md` and `CODE_OF_CONDUCT.md`.

## Support
Please use GitHub Issues for all questions and communication.

## Release & versioning
This project uses SemVer tags and GitHub Releases.

## Changelog
See `CHANGELOG.md`.

## License
MIT. See `LICENSE`.
