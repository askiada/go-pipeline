package main

import (
	"context"
	"flag"
	"fmt"
	"log"

	"github.com/askiada/go-pipeline/pkg/pipeline"
	"github.com/askiada/go-pipeline/pkg/pipeline/drawer"
	"github.com/askiada/go-pipeline/pkg/pipeline/measure"
)

func newPipeline(defaults pipeline.PipelineDefaults, withDrawer bool) (*pipeline.Pipeline, error) {
	if !withDrawer {
		return pipeline.New(defaults)
	}

	msr := measure.NewDefaultMeasure()
	drw := drawer.NewSVGDrawer("examples/pipeline-defaults/pipeline.dot")

	return pipeline.New(
		defaults,
		measure.PipelineMeasure(msr),
		drawer.PipelineDrawer(drw, msr),
	)
}

func main() {
	drawerEnabled := flag.Bool("drawer", false, "write examples/pipeline-defaults/pipeline.dot with metrics")
	flag.Parse()

	defaults := pipeline.PipelineDefaults{
		StepConcurrency:    2,
		StepBufferSize:     2,
		SplitterBufferSize: 2,
	}

	pipe, err := newPipeline(defaults, *drawerEnabled)
	if err != nil {
		log.Fatal(err)
	}

	root := pipeline.Root(pipe, "source", func(ctx context.Context, out chan<- int) error {
		for i := range 4 {
			out <- i
		}
		return nil
	})

	step := pipeline.OneToOne(pipe, "double", root, func(ctx context.Context, in int) (int, error) {
		return in * 2, nil
	})

	splitter := pipeline.Split(pipe, "split", step, 2)

	left, _ := splitter.Get()
	right, _ := splitter.Get()

	leftOut := pipeline.OneToOne(pipe, "left", left, func(ctx context.Context, in int) (int, error) {
		return in + 1, nil
	})

	rightOut := pipeline.OneToOne(pipe, "right", right, func(ctx context.Context, in int) (int, error) {
		return in + 10, nil
	}, pipeline.StepConcurrency[int](1))

	merged := pipeline.Merge(pipe, "merge", leftOut, rightOut)

	pipeline.Sink(pipe, "print", merged, func(ctx context.Context, in int) error {
		fmt.Println(in)
		return nil
	})

	if err := pipe.Run(context.Background()); err != nil {
		log.Fatal(err)
	}
}
