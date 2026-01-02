package pipeline_test

import (
	"context"
	"testing"

	"github.com/askiada/go-pipeline/v2/pkg/pipeline"
)

func createInputChan(t *testing.T, total int) chan int {
	t.Helper()

	inputChan := make(chan int)

	go func() {
		defer close(inputChan)

		for i := range total {
			inputChan <- i
		}
	}()

	return inputChan
}

func createInputChanWithCancel(t *testing.T, total int, offset int, cancel context.CancelFunc) chan int {
	t.Helper()

	inputChan := make(chan int)

	go func() {
		defer close(inputChan)

		for i := range total {
			if i == offset {
				cancel()
			}

			inputChan <- i
		}
	}()

	return inputChan
}

func processOutputChan(t *testing.T, output <-chan int) []int {
	t.Helper()

	res := []int{}

	for out := range output {
		res = append(res, out)
	}

	return res
}

func runPipeline(t *testing.T, pipe *pipeline.Pipeline, ctxs ...context.Context) error {
	t.Helper()

	ctx := t.Context()
	if len(ctxs) > 0 {
		ctx = ctxs[0]
	}

	return pipe.Run(ctx)
}
