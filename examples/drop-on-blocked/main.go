package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/askiada/go-pipeline/pkg/pipeline"
	"github.com/askiada/go-pipeline/pkg/pipeline/drawer"
	"github.com/askiada/go-pipeline/pkg/pipeline/measure"
	"github.com/askiada/go-pipeline/pkg/pipeline/model"
)

func newPipeline(withDrawer bool) (*pipeline.Pipeline, *measure.DefaultMeasure, error) {
	msr := measure.NewDefaultMeasure()
	if !withDrawer {
		pipe, err := pipeline.New(measure.PipelineMeasure(msr))
		return pipe, msr, err
	}

	drw := drawer.NewSVGDrawer("examples/drop-on-blocked/pipeline.dot")
	pipe, err := pipeline.New(
		measure.PipelineMeasure(msr),
		drawer.PipelineDrawer(drw, msr),
	)

	return pipe, msr, err
}

func main() {
	drawerEnabled := flag.Bool("drawer", false, "write examples/drop-on-blocked/pipeline.dot with metrics")
	flag.Parse()

	pipe, msr, err := newPipeline(*drawerEnabled)
	if err != nil {
		log.Fatal(err)
	}

	root := pipeline.Root(pipe, "source", func(ctx context.Context, out chan<- int) error {
		for i := range 6 {
			out <- i
		}

		return nil
	})

	work := pipeline.OneToOne(pipe, "work", root, func(ctx context.Context, input int) (int, error) {
		return input, nil
	}, pipeline.StepDropOnBlocked[int](10*time.Millisecond))

	pipeline.Sink(pipe, "sink", work, func(ctx context.Context, input int) error {
		time.Sleep(25 * time.Millisecond)
		fmt.Printf("processed: %d\n", input)
		return nil
	})

	if err := pipe.Run(context.Background()); err != nil {
		log.Fatal(err)
	}

	metric := msr.GetMetric("work")
	dropMetric, ok := metric.(measure.DropMetric)
	if !ok {
		log.Fatal("drop metrics not available")
	}

	fmt.Printf("dropped (timeout): %d\n", dropMetric.DropCount(model.StepDropSendTimeout))
}
