package main

import (
	"context"
	"fmt"
	"log"

	"github.com/askiada/go-pipeline/pkg/pipeline"
)

func main() {
	ctx := context.Background()
	pipe, err := pipeline.New(ctx, pipeline.PipelineDefaults{})
	if err != nil {
		log.Fatal(err)
	}

	root := pipeline.Root(pipe, "root", func(ctx context.Context, out chan<- int) error {
		for i := range 5 {
			out <- i
		}
		return nil
	})

	step1 := pipeline.OneToOne(pipe, "step-1", root, func(ctx context.Context, in int) (int, error) {
		return in + 1, nil
	})

	splitter := pipeline.Split(pipe, "split", step1, 2)

	left, _ := splitter.Get()
	right, _ := splitter.Get()

	leftOut := pipeline.OneToOne(pipe, "left", left, func(ctx context.Context, in int) (int, error) {
		return in * 10, nil
	})

	rightOut := pipeline.OneToOne(pipe, "right", right, func(ctx context.Context, in int) (int, error) {
		return in * 100, nil
	})

	merged := pipeline.Merge(pipe, "merge", leftOut, rightOut)

	pipeline.Sink(pipe, "print", merged, func(ctx context.Context, in int) error {
		fmt.Println(in)
		return nil
	})

	if err := pipe.Run(); err != nil {
		log.Fatal(err)
	}
}
