package main

import (
	"context"
	"flag"
	"log"
	"time"

	"github.com/askiada/go-pipeline/pkg/pipeline"
	"github.com/askiada/go-pipeline/pkg/pipeline/drawer"
	"github.com/askiada/go-pipeline/pkg/pipeline/measure"
)

func newPipeline(withDrawer bool) (*pipeline.Pipeline, error) {
	if !withDrawer {
		return pipeline.New()
	}

	msr := measure.NewDefaultMeasure()
	drw := drawer.NewSVGDrawer("examples/metrics-drawer/pipeline.dot")

	return pipeline.New(
		measure.PipelineMeasure(msr),
		drawer.PipelineDrawer(drw, msr),
	)
}

func main() {
	drawerEnabled := flag.Bool("drawer", false, "write examples/metrics-drawer/pipeline.dot with metrics")
	flag.Parse()

	pipe, err := newPipeline(*drawerEnabled)
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

	if err := pipe.Run(context.Background()); err != nil {
		log.Fatal(err)
	}
}
