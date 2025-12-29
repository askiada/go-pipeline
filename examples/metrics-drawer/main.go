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

	pipe, err := pipeline.New(ctx, measure.PipelineMeasure(msr), drawer.PipelineDrawer(drw, msr))
	if err != nil {
		log.Fatal(err)
	}

	root, err := pipeline.AddRootStep(pipe, "root", func(ctx context.Context, out chan<- int) error {
		for i := range 5 {
			out <- i
		}
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}

	step1, err := pipeline.AddStepOneToOne(pipe, "step-1", root, func(ctx context.Context, in int) (int, error) {
		time.Sleep(15 * time.Millisecond)
		return in * 2, nil
	})
	if err != nil {
		log.Fatal(err)
	}

	step2, err := pipeline.AddStepOneToOne(pipe, "step-2", step1, func(ctx context.Context, in int) (int, error) {
		time.Sleep(5 * time.Millisecond)
		return in + 1, nil
	})
	if err != nil {
		log.Fatal(err)
	}

	if err := pipeline.AddSink(pipe, "sink", step2, func(ctx context.Context, in int) error {
		time.Sleep(10 * time.Millisecond)
		return nil
	}); err != nil {
		log.Fatal(err)
	}

	if err := pipe.Run(); err != nil {
		log.Fatal(err)
	}
}
