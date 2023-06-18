package pipeline_test

import (
	"context"
	"testing"

	"github.com/askiada/go-pipeline/pkg/pipeline"
	"github.com/stretchr/testify/assert"
)

func createInputChan(t *testing.T, ctx context.Context, total int) <-chan int {
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

func createInputChanWithCancel(t *testing.T, ctx context.Context, total int, offset int, cancel context.CancelFunc) <-chan int {
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
	for out := range output {
		res = append(res, out)
	}

	return res
}

func TestOneToOne(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	input := createInputChan(t, ctx, 10)
	got := make(chan []int, 1)
	output := make(chan int)
	go func() {
		got <- processOutputChan(t, output)
	}()
	go func() {
		defer close(output)
		err := pipeline.OneToOne(ctx, input, output, func(ctx context.Context, i int) (o int, err error) {
			return i, nil
		})
		assert.Nil(t, err)
	}()
	assert.ElementsMatch(t, []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, <-got)
}

func TestOneToOneCancelInput(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	input := createInputChanWithCancel(t, ctx, 10, 5, cancel)
	got := make(chan []int, 1)
	output := make(chan int)
	go func() {
		got <- processOutputChan(t, output)
	}()
	go func() {
		defer close(output)
		err := pipeline.OneToOne(ctx, input, output, func(ctx context.Context, i int) (o int, err error) {
			return i, nil
		})
		assert.Nil(t, err)
	}()
}

func TestOneToOneCancelOutput(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	input := createInputChan(t, ctx, 10)
	got := make(chan []int, 1)
	output := make(chan int)
	go func() {
		got <- processOutputChan(t, output)
	}()
	go func() {
		defer close(output)
		err := pipeline.OneToOne(ctx, input, output, func(ctx context.Context, i int) (o int, err error) {
			if i == 5 {
				cancel()
				return 0, assert.AnError
			}
			return i, nil
		})
		assert.Error(t, err)
	}()
	assert.ElementsMatch(t, []int{0, 1, 2, 3, 4}, <-got)
}

func TestOneToOneError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	input := createInputChan(t, ctx, 10)
	got := make(chan []int, 1)
	output := make(chan int)
	go func() {
		got <- processOutputChan(t, output)
	}()
	go func() {
		defer close(output)
		err := pipeline.OneToOne(ctx, input, output, func(ctx context.Context, i int) (o int, err error) {
			if i == 5 {
				return 0, assert.AnError
			}
			return i, nil
		})
		assert.Error(t, err)
	}()
	assert.ElementsMatch(t, []int{0, 1, 2, 3, 4}, <-got)
}

func TestOneToMany(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	input := createInputChan(t, ctx, 10)
	got := make(chan []int, 1)
	output := make(chan int)
	go func() {
		got <- processOutputChan(t, output)
	}()
	go func() {
		defer close(output)
		err := pipeline.OneToMany(ctx, input, output, func(ctx context.Context, i int) (o []int, err error) {
			return []int{i, i * 10}, nil
		})
		assert.Nil(t, err)
	}()
	assert.ElementsMatch(t, []int{0, 0, 1, 10, 2, 20, 3, 30, 4, 40, 5, 50, 6, 60, 7, 70, 8, 80, 9, 90}, <-got)
}

func TestOneToManyCancelInput(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	input := createInputChanWithCancel(t, ctx, 10, 5, cancel)
	got := make(chan []int, 1)
	output := make(chan int)
	go func() {
		got <- processOutputChan(t, output)
	}()
	go func() {
		defer close(output)
		err := pipeline.OneToMany(ctx, input, output, func(ctx context.Context, i int) (o []int, err error) {
			return []int{i, i * 10}, nil
		})
		assert.Nil(t, err)
	}()
}

func TestOneToManyCancelOutput(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	input := createInputChan(t, ctx, 10)
	got := make(chan []int, 1)
	output := make(chan int)
	go func() {
		got <- processOutputChan(t, output)
	}()
	go func() {
		defer close(output)
		err := pipeline.OneToMany(ctx, input, output, func(ctx context.Context, i int) (o []int, err error) {
			if i == 5 {
				cancel()
				return nil, assert.AnError
			}
			return []int{i, i * 10}, nil
		})
		assert.Error(t, err)
	}()
	assert.ElementsMatch(t, []int{0, 0, 1, 10, 2, 20, 3, 30, 4, 40}, <-got)
}

func TestOneToManyError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	input := createInputChan(t, ctx, 10)
	got := make(chan []int, 1)
	output := make(chan int)
	go func() {
		got <- processOutputChan(t, output)
	}()
	go func() {
		defer close(output)
		err := pipeline.OneToMany(ctx, input, output, func(ctx context.Context, i int) (o []int, err error) {
			if i == 5 {
				return nil, assert.AnError
			}
			return []int{i, i * 10}, nil
		})
		assert.Error(t, err)
	}()
	assert.ElementsMatch(t, []int{0, 0, 1, 10, 2, 20, 3, 30, 4, 40}, <-got)
}
