package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/askiada/go-pipeline/v2/pkg/pipeline"
	"github.com/askiada/go-pipeline/v2/pkg/pipeline/drawer"
	"github.com/askiada/go-pipeline/v2/pkg/pipeline/measure"
)

func newPipeline(withDrawer bool) (*pipeline.Pipeline, error) {
	if !withDrawer {
		return pipeline.New()
	}

	msr := measure.NewDefaultMeasure()
	drw := drawer.NewSVGDrawer("examples/split-merge-metrics/pipeline.dot")

	return pipeline.New(
		measure.PipelineMeasure(msr),
		drawer.PipelineDrawer(drw, msr),
	)
}

func main() {
	drawerEnabled := flag.Bool("drawer", false, "write examples/split-merge-metrics/pipeline.dot with metrics")
	flag.Parse()

	pipe, err := newPipeline(*drawerEnabled)
	if err != nil {
		log.Fatal(err)
	}

	root := pipeline.Root(pipe, "source", func(ctx context.Context, out chan<- int) error {
		for i := range 4 {
			out <- i + 1
		}
		return nil
	})

	prep := pipeline.OneToOne(pipe, "prep", root, func(ctx context.Context, in int) (int, error) {
		time.Sleep(5 * time.Millisecond)
		return in, nil
	})

	splitter := pipeline.Split(pipe, "fan-out", prep, 3)

	left, _ := splitter.Get()
	middle, _ := splitter.Get()
	right, _ := splitter.Get()

	leftOut := pipeline.OneToOne(pipe, "left", left, func(ctx context.Context, in int) (int, error) {
		time.Sleep(8 * time.Millisecond)
		return in + 10, nil
	})

	middleOut := pipeline.OneToOne(pipe, "middle", middle, func(ctx context.Context, in int) (int, error) {
		time.Sleep(4 * time.Millisecond)
		return in + 20, nil
	})

	rightOut := pipeline.OneToOne(pipe, "right", right, func(ctx context.Context, in int) (int, error) {
		time.Sleep(2 * time.Millisecond)
		return in + 30, nil
	})

	merged := pipeline.Merge(pipe, "merge", leftOut, middleOut, rightOut)

	pipeline.Sink(pipe, "print", merged, func(ctx context.Context, in int) error {
		fmt.Println(in)
		return nil
	})

	if err := pipe.Run(context.Background()); err != nil {
		log.Fatal(err)
	}
}
