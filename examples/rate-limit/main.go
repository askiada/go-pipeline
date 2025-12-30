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
	drw := drawer.NewSVGDrawer("examples/rate-limit/pipeline.dot")

	return pipeline.New(
		measure.PipelineMeasure(msr),
		drawer.PipelineDrawer(drw, msr),
	)
}

func main() {
	drawerEnabled := flag.Bool("drawer", false, "write examples/rate-limit/pipeline.dot with metrics")
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
	},
		pipeline.StepConcurrency[int](3),
		pipeline.StepRateLimit[int](pipeline.RateLimitPolicy{Every: 30 * time.Millisecond, Burst: 1}),
	)
	if limited == nil {
		log.Fatal("rate-limit step not created")
	}

	sink := pipeline.Sink(pipe, "sink", limited, func(ctx context.Context, input int) error {
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
