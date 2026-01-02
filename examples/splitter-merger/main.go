package main

import (
	"context"
	"flag"
	"fmt"
	"log"

	"github.com/askiada/go-pipeline/v2/pkg/pipeline"
	"github.com/askiada/go-pipeline/v2/pkg/pipeline/drawer"
	"github.com/askiada/go-pipeline/v2/pkg/pipeline/measure"
)

func newPipeline(withDrawer bool) (*pipeline.Pipeline, error) {
	if !withDrawer {
		return pipeline.New()
	}

	msr := measure.NewDefaultMeasure()
	drw := drawer.NewSVGDrawer("examples/splitter-merger/pipeline.dot")

	return pipeline.New(
		measure.PipelineMeasure(msr),
		drawer.PipelineDrawer(drw, msr),
	)
}

func main() {
	drawerEnabled := flag.Bool("drawer", false, "write examples/splitter-merger/pipeline.dot with metrics")
	flag.Parse()

	pipe, err := newPipeline(*drawerEnabled)
	if err != nil {
		log.Fatal(err)
	}

	root := pipeline.Root(pipe, "source", func(ctx context.Context, out chan<- int) error {
		for i := range 5 {
			out <- i
		}
		return nil
	})

	step1 := pipeline.OneToOne(pipe, "step-1", root, func(ctx context.Context, in int) (int, error) {
		return in + 1, nil
	})

	splitter := pipeline.Split(pipe, "split", step1, 2)

	left, _ := splitter.Get()
	right, _ := splitter.Get()

	leftOut := pipeline.OneToOne(pipe, "left", left, func(ctx context.Context, in int) (int, error) {
		return in * 10, nil
	})

	rightOut := pipeline.OneToOne(pipe, "right", right, func(ctx context.Context, in int) (int, error) {
		return in * 100, nil
	})

	merged := pipeline.Merge(pipe, "merge", leftOut, rightOut)

	pipeline.Sink(pipe, "print", merged, func(ctx context.Context, in int) error {
		fmt.Println(in)
		return nil
	})

	if err := pipe.Run(context.Background()); err != nil {
		log.Fatal(err)
	}
}
