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
	drw := drawer.NewSVGDrawer("examples/batching-chan/pipeline.dot")

	return pipeline.New(
		measure.PipelineMeasure(msr),
		drawer.PipelineDrawer(drw, msr),
	)
}

func main() {
	drawerEnabled := flag.Bool("drawer", false, "write examples/batching-chan/pipeline.dot with metrics")
	flag.Parse()

	pipe, err := newPipeline(*drawerEnabled)
	if err != nil {
		log.Fatal(err)
	}

	root := pipeline.Root(pipe, "source", func(ctx context.Context, out chan<- int) error {
		out <- 1
		out <- 2
		time.Sleep(60 * time.Millisecond)
		out <- 3
		out <- 4

		return nil
	})

	batch := pipeline.BatchChan(pipe, "batch", root, pipeline.BatchPolicy{
		MaxSize: 10,
		MaxWait: 25 * time.Millisecond,
	})
	if batch == nil {
		log.Fatal("batch not created")
	}

	sink := pipeline.Sink(pipe, "print", batch, func(ctx context.Context, input <-chan int) error {
		var items []int
		for item := range input {
			items = append(items, item)
		}
		fmt.Printf("batch: %v\n", items)
		return nil
	})
	if sink == nil {
		log.Fatal("sink not created")
	}

	if err := pipe.Run(context.Background()); err != nil {
		log.Fatal(err)
	}
}
