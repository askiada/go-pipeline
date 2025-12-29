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

	step1, err := pipeline.AddStepOneToOne(pipe, "step-1", root, func(ctx context.Context, in int) (int, error) {
		return in + 1, nil
	})
	if err != nil {
		log.Fatal(err)
	}

	splitter, err := pipeline.AddSplitter(pipe, "split", step1, 2)
	if err != nil {
		log.Fatal(err)
	}

	left, _ := splitter.Get()
	right, _ := splitter.Get()

	leftOut, err := pipeline.AddStepOneToOne(pipe, "left", left, func(ctx context.Context, in int) (int, error) {
		return in * 10, nil
	})
	if err != nil {
		log.Fatal(err)
	}

	rightOut, err := pipeline.AddStepOneToOne(pipe, "right", right, func(ctx context.Context, in int) (int, error) {
		return in * 100, nil
	})
	if err != nil {
		log.Fatal(err)
	}

	merged, err := pipeline.AddMerger(pipe, "merge", leftOut, rightOut)
	if err != nil {
		log.Fatal(err)
	}

	if err := pipeline.AddSink(pipe, "print", merged, func(ctx context.Context, in int) error {
		fmt.Println(in)
		return nil
	}); err != nil {
		log.Fatal(err)
	}

	if err := pipe.Run(); err != nil {
		log.Fatal(err)
	}
}
