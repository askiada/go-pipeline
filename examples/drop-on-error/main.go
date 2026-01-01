package main

import (
	"context"
	"flag"
	"fmt"
	"log"

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

	drw := drawer.NewSVGDrawer("examples/drop-on-error/pipeline.dot")
	pipe, err := pipeline.New(
		measure.PipelineMeasure(msr),
		drawer.PipelineDrawer(drw, msr),
	)

	return pipe, msr, err
}

func main() {
	drawerEnabled := flag.Bool("drawer", false, "write examples/drop-on-error/pipeline.dot with metrics")
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

	errorStep, errorOpt := pipeline.StepErrorOutput[int](8)
	work := pipeline.OneToOne(pipe, "work", root, func(ctx context.Context, input int) (int, error) {
		if input%3 == 0 {
			return 0, fmt.Errorf("boom on %d", input)
		}

		return input * 2, nil
	}, pipeline.StepDropOnError[int](), errorOpt)

	pipeline.Sink(pipe, "sink", work, func(ctx context.Context, input int) error {
		fmt.Printf("processed: %d\n", input)
		return nil
	})

	pipeline.Sink(pipe, "errors", errorStep, func(ctx context.Context, input model.StepError) error {
		fmt.Printf("error: %v (%v)\n", input.Item, input.Err)
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

	fmt.Printf("dropped (error): %d\n", dropMetric.DropCount(model.StepDropError))
	fmt.Printf("errors routed: %d\n", dropMetric.RoutedErrorCount())
}
