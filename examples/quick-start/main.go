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

	double := pipeline.OneToOne(pipe, "double", root, func(ctx context.Context, in int) (int, error) {
		return in * 2, nil
	})

	pipeline.Sink(pipe, "print", double, func(ctx context.Context, in int) error {
		fmt.Println(in)
		return nil
	})

	if err := pipe.Run(); err != nil {
		log.Fatal(err)
	}
}
