package main

import (
	"context"
	"log"
	"time"

	"github.com/askiada/go-pipeline/pkg/pipeline"
	"github.com/askiada/go-pipeline/pkg/pipeline/drawer"
	"github.com/askiada/go-pipeline/pkg/pipeline/measure"
)

func main() {
	ctx := context.Background()
	msr := measure.NewDefaultMeasure()
	drw := drawer.NewSVGDrawer("examples/metrics-drawer/pipeline.dot")

	pipe, err := pipeline.New(pipeline.PipelineDefaults{}, measure.PipelineMeasure(msr), drawer.PipelineDrawer(drw, msr))
	if err != nil {
		log.Fatal(err)
	}

	root := pipeline.Root(pipe, "root", func(ctx context.Context, out chan<- int) error {
		for i := range 5 {
			out <- i
		}
		return nil
	})

	step1 := pipeline.OneToOne(pipe, "step-1", root, func(ctx context.Context, in int) (int, error) {
		time.Sleep(15 * time.Millisecond)
		return in * 2, nil
	})

	step2 := pipeline.OneToOne(pipe, "step-2", step1, func(ctx context.Context, in int) (int, error) {
		time.Sleep(5 * time.Millisecond)
		return in + 1, nil
	})

	pipeline.Sink(pipe, "sink", step2, func(ctx context.Context, in int) error {
		time.Sleep(10 * time.Millisecond)
		return nil
	})

	if err := pipe.Run(ctx); err != nil {
		log.Fatal(err)
	}
}
