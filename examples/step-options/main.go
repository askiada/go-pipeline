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
	drw := drawer.NewSVGDrawer("examples/step-options/pipeline.dot")

	return pipeline.New(
		measure.PipelineMeasure(msr),
		drawer.PipelineDrawer(drw, msr),
	)
}

func main() {
	drawerEnabled := flag.Bool("drawer", false, "write examples/step-options/pipeline.dot with metrics")
	flag.Parse()

	pipe, err := newPipeline(*drawerEnabled)
	if err != nil {
		log.Fatal(err)
	}

	root := pipeline.Root(pipe, "source", func(ctx context.Context, out chan<- int) error {
		go func() {
			defer close(out)
			for i := range 5 {
				out <- i
			}
		}()

		return nil
	}, pipeline.StepKeepOpen[int](), pipeline.StepBufferSize[int](2))

	work := pipeline.OneToOne(pipe, "work", root, func(ctx context.Context, in int) (string, error) {
		time.Sleep(10 * time.Millisecond)
		return fmt.Sprintf("task-%d", in), nil
	}, pipeline.StepConcurrency[string](2), pipeline.StepBufferSize[string](2))

	pipeline.Sink(pipe, "print", work, func(ctx context.Context, in string) error {
		fmt.Println(in)
		return nil
	})

	if err := pipe.Run(context.Background()); err != nil {
		log.Fatal(err)
	}
}
