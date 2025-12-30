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
	drw := drawer.NewSVGDrawer("examples/quick-start/pipeline.dot")

	return pipeline.New(
		measure.PipelineMeasure(msr),
		drawer.PipelineDrawer(drw, msr),
	)
}

func sourceFn(_ context.Context, out chan<- int) error {
	for i := range 3 {
		out <- i
	}
	return nil
}

func formatFn(_ context.Context, in int) (string, error) {
	return fmt.Sprintf("item-%d", in), nil
}

func printFn(_ context.Context, in string) error {
	fmt.Println(in)
	return nil
}

func main() {
	drawerEnabled := flag.Bool("drawer", false, "write examples/quick-start/pipeline.dot with metrics")
	flag.Parse()

	pipe, err := newPipeline(*drawerEnabled)
	if err != nil {
		log.Fatal(err)
	}

	root := pipeline.Root(pipe, "source", sourceFn)
	formatted := pipeline.OneToOne(pipe, "format", root, formatFn)
	pipeline.Sink(pipe, "print", formatted, printFn)

	if err := pipe.Run(context.Background()); err != nil {
		log.Fatal(err)
	}
}
