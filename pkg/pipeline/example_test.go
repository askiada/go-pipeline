package pipeline_test

import (
	"context"
	"fmt"
	"sort"

	"github.com/askiada/go-pipeline/v2/pkg/pipeline"
	"github.com/askiada/go-pipeline/v2/pkg/pipeline/model"
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

func ExampleSplit() {
	pipe, _ := pipeline.New()

	root := pipeline.Root(pipe, "root", func(ctx context.Context, out chan<- int) error {
		for i := range 3 {
			out <- i
		}

		return nil
	})

	split := pipeline.Split(pipe, "split", root, 2)
	left, _ := split.Get()
	right, _ := split.Get()
	merged := pipeline.Merge(pipe, "merge", left, right)

	out := make([]int, 0, 6)

	pipeline.Sink(pipe, "collect", merged, func(ctx context.Context, v int) error {
		out = append(out, v)

		return nil
	})

	_ = pipe.Run(context.Background())

	sort.Ints(out)

	for _, v := range out {
		fmt.Println(v)
	}

	// Output:
	// 0
	// 0
	// 1
	// 1
	// 2
	// 2
}

func ExampleStepErrorOutput() {
	pipe, _ := pipeline.New()

	root := pipeline.Root(pipe, "root", func(ctx context.Context, out chan<- int) error {
		out <- 1

		out <- 3

		return nil
	})

	errStep, errOpt := pipeline.StepErrorOutput[int](1)

	pipeline.OneToOne(pipe, "parse", root, func(ctx context.Context, v int) (int, error) {
		return 0, fmt.Errorf("bad item %d", v)
	}, pipeline.StepDropOnError[int](), errOpt)

	var errs []string

	pipeline.Sink(pipe, "errors", errStep, func(ctx context.Context, err model.StepError) error {
		errs = append(errs, fmt.Sprintf("item=%v err=%v", err.Item, err.Err))

		return nil
	})

	_ = pipe.Run(context.Background())

	sort.Strings(errs)

	for _, line := range errs {
		fmt.Println(line)
	}

	// Output:
	// item=1 err=bad item 1
	// item=3 err=bad item 3
}
