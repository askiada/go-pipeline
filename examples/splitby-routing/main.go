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

func newPipeline(withDrawer bool) (*pipeline.Pipeline, error) {
	if !withDrawer {
		return pipeline.New()
	}

	msr := measure.NewDefaultMeasure()
	drw := drawer.NewSVGDrawer("examples/splitby-routing/pipeline.dot")

	return pipeline.New(
		measure.PipelineMeasure(msr),
		drawer.PipelineDrawer(drw, msr),
	)
}

func main() {
	drawerEnabled := flag.Bool("drawer", false, "write examples/splitby-routing/pipeline.dot with metrics")
	flag.Parse()

	pipe, err := newPipeline(*drawerEnabled)
	if err != nil {
		log.Fatal(err)
	}

	root := pipeline.Root(pipe, "source", func(ctx context.Context, out chan<- int) error {
		for i := range 6 {
			out <- i + 1
		}
		return nil
	})

	splitter := pipeline.SplitBy(pipe, "route", root, []pipeline.SplitFn[int]{
		func(ctx context.Context, input int) (bool, error) {
			return input%2 == 0, nil
		},
		func(ctx context.Context, input int) (bool, error) {
			return input%2 != 0, nil
		},
		func(ctx context.Context, input int) (bool, error) {
			return input%3 == 0, nil
		},
	})

	even, _ := splitter.Get()
	odd, _ := splitter.Get()
	multThree, _ := splitter.Get()

	evenOut := pipeline.OneToOne(pipe, "even", even, func(ctx context.Context, in int) (string, error) {
		return fmt.Sprintf("even:%d", in), nil
	})

	oddOut := pipeline.OneToOne(pipe, "odd", odd, func(ctx context.Context, in int) (string, error) {
		return fmt.Sprintf("odd:%d", in), nil
	})

	multiOut := pipeline.OneToOne(pipe, "multiple-of-three", multThree, func(ctx context.Context, in int) (string, error) {
		return fmt.Sprintf("mul3:%d", in), nil
	})

	merged := pipeline.Merge(pipe, "merge", evenOut, oddOut, multiOut)

	pipeline.Sink(pipe, "print", merged, func(ctx context.Context, in string) error {
		fmt.Println(in)
		return nil
	})

	if err := pipe.Run(context.Background()); err != nil {
		log.Fatal(err)
	}
}
