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

type record struct {
	ID    int
	Label string
}

func newPipeline(withDrawer bool) (*pipeline.Pipeline, error) {
	if !withDrawer {
		return pipeline.New()
	}

	msr := measure.NewDefaultMeasure()
	drw := drawer.NewSVGDrawer("examples/from-chan/pipeline.dot")

	return pipeline.New(
		measure.PipelineMeasure(msr),
		drawer.PipelineDrawer(drw, msr),
	)
}

func main() {
	drawerEnabled := flag.Bool("drawer", false, "write examples/from-chan/pipeline.dot with metrics")
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

	formatted := pipeline.FromChan(pipe, "format", root, func(ctx context.Context, input <-chan int, output chan record) error {
		for in := range input {
			output <- record{ID: in, Label: fmt.Sprintf("id-%d", in)}
		}
		return nil
	})

	pipeline.Sink(pipe, "print", formatted, func(ctx context.Context, in record) error {
		fmt.Printf("%d:%s\n", in.ID, in.Label)
		return nil
	})

	if err := pipe.Run(context.Background()); err != nil {
		log.Fatal(err)
	}
}
