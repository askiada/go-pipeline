package main

import (
	"context"
	"fmt"
	"log"

	"github.com/askiada/go-pipeline/pkg/pipeline"
)

func main() {
	ctx := context.Background()
	pipe, err := pipeline.New(ctx)
	if err != nil {
		log.Fatal(err)
	}

	root, err := pipeline.AddRootStep(pipe, "root", func(ctx context.Context, out chan<- int) error {
		for i := range 5 {
			out <- i
		}
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}

	double, err := pipeline.AddStepOneToOne(pipe, "double", root, func(ctx context.Context, in int) (int, error) {
		return in * 2, nil
	})
	if err != nil {
		log.Fatal(err)
	}

	if err := pipeline.AddSink(pipe, "print", double, func(ctx context.Context, in int) error {
		fmt.Println(in)
		return nil
	}); err != nil {
		log.Fatal(err)
	}

	if err := pipe.Run(); err != nil {
		log.Fatal(err)
	}
}
