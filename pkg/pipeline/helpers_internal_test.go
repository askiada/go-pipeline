package pipeline

import (
	"context"
	"testing"
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
