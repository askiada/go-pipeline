package pipeline

import (
	"context"
	"strconv"
	"testing"
)

func createInputChan(t *testing.T, ctx context.Context, total int) chan int {
	t.Helper()
	inputChan := make(chan int)
	go func() {
		defer close(inputChan)
		for i := 0; i < total; i++ {
			inputChan <- i
		}
	}()
	return inputChan
}

func createInputStringChan(t *testing.T, ctx context.Context, total int) chan string {
	t.Helper()
	inputChan := make(chan string)
	go func() {
		defer close(inputChan)
		for i := 0; i < total; i++ {
			inputChan <- strconv.Itoa(i)
		}
	}()
	return inputChan
}

func createInputChanWithCancel(t *testing.T, ctx context.Context, total int, offset int, cancel context.CancelFunc) chan int {
	t.Helper()
	inputChan := make(chan int)
	go func() {
		defer close(inputChan)
		for i := 0; i < total; i++ {
			if i == offset {
				cancel()
			}
			inputChan <- i
		}
	}()
	return inputChan
}

func processOutputChan(t *testing.T, output <-chan int) (res []int) {
	t.Helper()
	for out := range output {
		res = append(res, out)
	}

	return res
}
