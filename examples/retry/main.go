package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"sync"

	"github.com/askiada/go-pipeline/v2/pkg/pipeline"
	"github.com/askiada/go-pipeline/v2/pkg/pipeline/drawer"
	"github.com/askiada/go-pipeline/v2/pkg/pipeline/measure"
)

func newPipeline(withDrawer bool) (*pipeline.Pipeline, error) {
	if !withDrawer {
		return pipeline.New()
	}

	msr := measure.NewDefaultMeasure()
	drw := drawer.NewSVGDrawer("examples/retry/pipeline.dot")

	return pipeline.New(
		measure.PipelineMeasure(msr),
		drawer.PipelineDrawer(drw, msr),
	)
}

func main() {
	drawerEnabled := flag.Bool("drawer", false, "write examples/retry/pipeline.dot with metrics")
	flag.Parse()

	pipe, err := newPipeline(*drawerEnabled)
	if err != nil {
		log.Fatal(err)
	}

	root := pipeline.Root(pipe, "source", func(ctx context.Context, out chan<- int) error {
		out <- 1
		out <- 2
		return nil
	})

	var mu sync.Mutex
	attempts := map[int]int{}

	sink := pipeline.Sink(pipe, "sink", root, func(ctx context.Context, input int) error {
		mu.Lock()
		attempts[input]++
		attempt := attempts[input]
		mu.Unlock()

		if attempt < 2 {
			fmt.Printf("retry %d\n", input)
			return fmt.Errorf("transient error on %d", input)
		}

		fmt.Printf("done %d\n", input)
		return nil
	}, pipeline.StepRetry[int](pipeline.RetryPolicy{MaxAttempts: 2}))
	if sink == nil {
		log.Fatal("sink not created")
	}

	if err := pipe.Run(context.Background()); err != nil {
		log.Fatal(err)
	}
}
