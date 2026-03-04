package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"sync"
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
	drw := drawer.NewSVGDrawer("examples/timeout-retry/pipeline.dot")

	return pipeline.New(
		measure.PipelineMeasure(msr),
		drawer.PipelineDrawer(drw, msr),
	)
}

func main() {
	drawerEnabled := flag.Bool("drawer", false, "write examples/timeout-retry/pipeline.dot with metrics")
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
	if root == nil {
		log.Fatal("root not created")
	}

	attempts := make(map[int]int)
	var mu sync.Mutex

	step := pipeline.OneToOne(pipe, "retry", root, func(ctx context.Context, input int) (int, error) {
		mu.Lock()
		attempts[input]++
		attempt := attempts[input]
		mu.Unlock()

		if attempt < 2 {
			time.Sleep(40 * time.Millisecond)
			return 0, errors.New("transient")
		}

		time.Sleep(20 * time.Millisecond)

		return input * 100, nil
	},
		pipeline.StepRetry[int](pipeline.RetryPolicy{MaxAttempts: 3, Backoff: 10 * time.Millisecond}),
		pipeline.StepTimeout[int](200*time.Millisecond),
	)
	if step == nil {
		log.Fatal("retry step not created")
	}

	sink := pipeline.Sink(pipe, "sink", step, func(ctx context.Context, input int) error {
		fmt.Printf("value: %d\n", input)
		return nil
	})
	if sink == nil {
		log.Fatal("sink not created")
	}

	if err := pipe.Run(context.Background()); err != nil {
		log.Fatal(err)
	}
}
