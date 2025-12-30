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
)

func newPipeline(withDrawer bool) (*pipeline.Pipeline, error) {
	if !withDrawer {
		return pipeline.New()
	}

	msr := measure.NewDefaultMeasure()
	drw := drawer.NewSVGDrawer("examples/step-limits/pipeline.dot")

	return pipeline.New(
		measure.PipelineMeasure(msr),
		drawer.PipelineDrawer(drw, msr),
	)
}

func main() {
	drawerEnabled := flag.Bool("drawer", false, "write examples/step-limits/pipeline.dot with metrics")
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
	if root == nil {
		log.Fatal("root not created")
	}

	limited := pipeline.OneToOne(pipe, "rate-limit", root, func(ctx context.Context, input int) (int, error) {
		return input, nil
	}, pipeline.StepRateLimit[int](pipeline.RateLimitPolicy{Every: 20 * time.Millisecond, Burst: 1}))
	if limited == nil {
		log.Fatal("rate-limit step not created")
	}

	processed := pipeline.OneToOne(pipe, "in-flight", limited, func(ctx context.Context, input int) (int, error) {
		select {
		case <-time.After(15 * time.Millisecond):
			return input * 2, nil
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	},
		pipeline.StepConcurrency[int](2),
		pipeline.StepMaxInFlight[int](1),
		pipeline.StepTimeout[int](100*time.Millisecond),
	)
	if processed == nil {
		log.Fatal("in-flight step not created")
	}

	sink := pipeline.Sink(pipe, "sink", processed, func(ctx context.Context, input int) error {
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
