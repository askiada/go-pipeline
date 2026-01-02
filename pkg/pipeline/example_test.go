package pipeline_test

import (
	"context"
	"fmt"

	"github.com/askiada/go-pipeline/v2/pkg/pipeline"
)

func ExamplePipeline() {
	pipe, _ := pipeline.New()

	root := pipeline.Root(pipe, "root", func(ctx context.Context, out chan<- int) error {
		for i := range 3 {
			out <- i
		}

		return nil
	})

	doubled := pipeline.OneToOne(pipe, "double", root, func(ctx context.Context, v int) (int, error) {
		return v * 2, nil
	})

	pipeline.Sink(pipe, "print", doubled, func(ctx context.Context, v int) error {
		fmt.Println(v)

		return nil
	})

	_ = pipe.Run(context.Background())

	// Output:
	// 0
	// 2
	// 4
}
