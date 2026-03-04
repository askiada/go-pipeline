package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"sort"
	"time"

	"github.com/askiada/go-pipeline/v2/pkg/pipeline"
	"github.com/askiada/go-pipeline/v2/pkg/pipeline/drawer"
	"github.com/askiada/go-pipeline/v2/pkg/pipeline/measure"
)

type job struct {
	ID    int
	Value int
}

type result struct {
	ID     int
	Output int
}

func newPipeline(withDrawer bool) (*pipeline.Pipeline, error) {
	if !withDrawer {
		return pipeline.New()
	}

	msr := measure.NewDefaultMeasure()
	drw := drawer.NewSVGDrawer("examples/concurrency-aggregate/pipeline.dot")

	return pipeline.New(
		measure.PipelineMeasure(msr),
		drawer.PipelineDrawer(drw, msr),
	)
}

func main() {
	drawerEnabled := flag.Bool("drawer", false, "write examples/concurrency-aggregate/pipeline.dot with metrics")
	flag.Parse()

	pipe, err := newPipeline(*drawerEnabled)
	if err != nil {
		log.Fatal(err)
	}

	root := pipeline.Root(pipe, "source", func(ctx context.Context, out chan<- job) error {
		for i := range 6 {
			out <- job{ID: i + 1, Value: (i + 1) * 2}
		}
		return nil
	})

	processed := pipeline.OneToOne(pipe, "process", root, func(ctx context.Context, in job) (result, error) {
		time.Sleep(10 * time.Millisecond)
		return result{ID: in.ID, Output: in.Value * 10}, nil
	}, pipeline.StepConcurrency[result](3))

	pipeline.SinkFromChan(pipe, "collect", processed, func(ctx context.Context, input <-chan result) error {
		results := make([]result, 0, 6)
		for res := range input {
			results = append(results, res)
		}

		sort.Slice(results, func(i, j int) bool {
			return results[i].ID < results[j].ID
		})

		for _, res := range results {
			fmt.Printf("job-%d -> %d\n", res.ID, res.Output)
		}

		return nil
	})

	if err := pipe.Run(context.Background()); err != nil {
		log.Fatal(err)
	}
}
