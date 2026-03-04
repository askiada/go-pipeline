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
	drw := drawer.NewSVGDrawer("examples/backpressure-buffering/pipeline.dot")

	return pipeline.New(
		measure.PipelineMeasure(msr),
		drawer.PipelineDrawer(drw, msr),
	)
}

func main() {
	drawerEnabled := flag.Bool("drawer", false, "write examples/backpressure-buffering/pipeline.dot with metrics")
	flag.Parse()

	pipe, err := newPipeline(*drawerEnabled)
	if err != nil {
		log.Fatal(err)
	}

	root := pipeline.Root(pipe, "source", func(ctx context.Context, out chan<- int) error {
		for i := range 3 {
			out <- i
		}
		return nil
	})

	splitter := pipeline.Split(pipe, "split", root, 2, pipeline.SplitterBufferSize[int](1))

	left, _ := splitter.Get()
	right, _ := splitter.Get()

	leftOut := pipeline.OneToOne(pipe, "left", left, func(ctx context.Context, in int) (string, error) {
		time.Sleep(40 * time.Millisecond)
		return fmt.Sprintf("left-%d", in), nil
	})

	rightOut := pipeline.OneToOne(pipe, "right", right, func(ctx context.Context, in int) (string, error) {
		time.Sleep(5 * time.Millisecond)
		return fmt.Sprintf("right-%d", in), nil
	})

	merged := pipeline.Merge(pipe, "merge", leftOut, rightOut)

	pipeline.Sink(pipe, "print", merged, func(ctx context.Context, in string) error {
		fmt.Println(in)
		return nil
	})

	if err := pipe.Run(context.Background()); err != nil {
		log.Fatal(err)
	}
}
