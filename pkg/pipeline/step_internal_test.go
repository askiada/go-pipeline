package pipeline

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/askiada/go-pipeline/pkg/pipeline/model"
)

func TestOneToOne(t *testing.T) {
	t.Parallel()

	tcs := map[string]struct {
		concurrent int
	}{
		"sequential":     {concurrent: 1},
		"sequential v2":  {concurrent: 0},
		"concurrent 2":   {concurrent: 2},
		"concurrent 100": {concurrent: 100},
	}

	for name, tc := range tcs {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()

			input := &model.Step[int]{Output: createInputChan(t, 10)}
			got := make(chan []int, 1)
			output := &model.Step[int]{Output: make(chan int), Details: &model.StepInfo{Concurrent: tc.concurrent}}

			go func() {
				got <- processOutputChan(t, output.Output)
			}()

			go func() {
				defer close(output.Output)
				err := runOneToOne(ctx, input, output, func(ctx context.Context, i int) (int, error) {
					return i, nil
				}, false)
				assert.NoError(t, err)
			}()

			assert.ElementsMatch(t, []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, <-got)
		})
	}
}

func TestOneToOneCancelInput(t *testing.T) {
	t.Parallel()

	tcs := map[string]struct {
		concurrent int
	}{
		"sequential":     {concurrent: 1},
		"sequential v2":  {concurrent: 0},
		"concurrent 2":   {concurrent: 2},
		"concurrent 100": {concurrent: 100},
	}

	for name, tc := range tcs {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()

			input := &model.Step[int]{Output: createInputChanWithCancel(t, 10, 5, cancel)}
			got := make(chan []int, 1)
			output := &model.Step[int]{Output: make(chan int), Details: &model.StepInfo{Concurrent: tc.concurrent}}

			go func() {
				got <- processOutputChan(t, output.Output)
			}()

			go func() {
				defer close(output.Output)
				err := runOneToOne(ctx, input, output, func(ctx context.Context, i int) (int, error) {
					return i, nil
				}, false)
				assert.Error(t, err)
			}()

			assert.NotZero(t, <-got)
		})
	}
}

func TestOneToOneCancelOutput(t *testing.T) {
	t.Parallel()

	tcs := map[string]struct {
		concurrent int
	}{
		"sequential":     {concurrent: 1},
		"sequential v2":  {concurrent: 0},
		"concurrent 2":   {concurrent: 2},
		"concurrent 100": {concurrent: 100},
	}

	for name, tc := range tcs {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()

			input := &model.Step[int]{Output: createInputChan(t, 10)}
			got := make(chan []int, 1)
			output := &model.Step[int]{Output: make(chan int), Details: &model.StepInfo{Concurrent: tc.concurrent}}

			go func() {
				got <- processOutputChan(t, output.Output)
			}()

			go func() {
				defer close(output.Output)
				err := runOneToOne(ctx, input, output, func(ctx context.Context, i int) (int, error) {
					if i == 5 {
						cancel()

						return 0, assert.AnError
					}

					return i, nil
				}, false)
				assert.Error(t, err)
			}()

			assert.NotZero(t, <-got)
		})
	}
}

func TestOneToOneError(t *testing.T) {
	t.Parallel()

	tcs := map[string]struct {
		concurrent int
	}{
		"sequential":     {concurrent: 1},
		"sequential v2":  {concurrent: 0},
		"concurrent 2":   {concurrent: 2},
		"concurrent 100": {concurrent: 100},
	}

	for name, tc := range tcs {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()

			input := &model.Step[int]{Output: createInputChan(t, 10)}
			got := make(chan []int, 1)
			output := &model.Step[int]{Output: make(chan int), Details: &model.StepInfo{Concurrent: tc.concurrent}}

			go func() {
				got <- processOutputChan(t, output.Output)
			}()

			go func() {
				defer close(output.Output)
				err := runOneToOne(ctx, input, output, func(ctx context.Context, i int) (int, error) {
					if i == 5 {
						return 0, assert.AnError
					}

					return i, nil
				}, false)
				assert.Error(t, err)
			}()

			assert.NotZero(t, <-got)
		})
	}
}

func TestOneToOneOrZero(t *testing.T) {
	t.Parallel()

	tcs := map[string]struct {
		concurrent int
	}{
		"sequential":     {concurrent: 1},
		"sequential v2":  {concurrent: 0},
		"concurrent 2":   {concurrent: 2},
		"concurrent 100": {concurrent: 100},
	}

	for name, tc := range tcs {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()

			input := &model.Step[int]{Output: createInputChan(t, 10)}
			got := make(chan []int, 1)
			output := &model.Step[int]{Output: make(chan int), Details: &model.StepInfo{Concurrent: tc.concurrent}}

			go func() {
				got <- processOutputChan(t, output.Output)
			}()

			go func() {
				defer close(output.Output)
				err := runOneToOne(ctx, input, output, func(ctx context.Context, i int) (int, error) {
					return i, nil
				}, true)
				assert.NoError(t, err)
			}()

			assert.ElementsMatch(t, []int{1, 2, 3, 4, 5, 6, 7, 8, 9}, <-got)
		})
	}
}

func TestOneToOneOrZeroCancelInput(t *testing.T) {
	t.Parallel()

	tcs := map[string]struct {
		concurrent int
	}{
		"sequential":     {concurrent: 1},
		"sequential v2":  {concurrent: 0},
		"concurrent 2":   {concurrent: 2},
		"concurrent 100": {concurrent: 100},
	}

	for name, tc := range tcs {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()

			input := &model.Step[int]{Output: createInputChanWithCancel(t, 10, 5, cancel)}
			got := make(chan []int, 1)
			output := &model.Step[int]{Output: make(chan int), Details: &model.StepInfo{Concurrent: tc.concurrent}}

			go func() {
				got <- processOutputChan(t, output.Output)
			}()

			go func() {
				defer close(output.Output)
				err := runOneToOne(ctx, input, output, func(ctx context.Context, i int) (int, error) {
					return i, nil
				}, false)
				assert.Error(t, err)
			}()

			assert.NotZero(t, <-got)
		})
	}
}

func TestOneToOneOrZeroCancelOutput(t *testing.T) {
	t.Parallel()

	tcs := map[string]struct {
		concurrent int
	}{
		"sequential":     {concurrent: 1},
		"sequential v2":  {concurrent: 0},
		"concurrent 2":   {concurrent: 2},
		"concurrent 100": {concurrent: 100},
	}

	for name, tc := range tcs {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()

			input := &model.Step[int]{Output: createInputChan(t, 10)}
			got := make(chan []int, 1)
			output := &model.Step[int]{Output: make(chan int), Details: &model.StepInfo{Concurrent: tc.concurrent}}

			go func() {
				got <- processOutputChan(t, output.Output)
			}()

			go func() {
				defer close(output.Output)
				err := runOneToOne(ctx, input, output, func(ctx context.Context, i int) (int, error) {
					if i == 5 {
						cancel()

						return 0, assert.AnError
					}

					return i, nil
				}, false)
				assert.Error(t, err)
			}()

			assert.NotZero(t, <-got)
		})
	}
}

func TestOneToOneOrZeroError(t *testing.T) {
	t.Parallel()

	tcs := map[string]struct {
		concurrent int
	}{
		"sequential":     {concurrent: 1},
		"sequential v2":  {concurrent: 0},
		"concurrent 2":   {concurrent: 2},
		"concurrent 100": {concurrent: 100},
	}

	for name, tc := range tcs {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()

			input := &model.Step[int]{Output: createInputChan(t, 10)}
			got := make(chan []int, 1)
			output := &model.Step[int]{Output: make(chan int), Details: &model.StepInfo{Concurrent: tc.concurrent}}

			go func() {
				got <- processOutputChan(t, output.Output)
			}()

			go func() {
				defer close(output.Output)
				err := runOneToOne(ctx, input, output, func(ctx context.Context, i int) (int, error) {
					if i == 5 {
						return 0, assert.AnError
					}

					return i, nil
				}, true)
				assert.Error(t, err)
			}()

			assert.NotZero(t, <-got)
		})
	}
}

func TestOneToMany(t *testing.T) {
	t.Parallel()

	tcs := map[string]struct {
		concurrent int
	}{
		"sequential":     {concurrent: 1},
		"sequential v2":  {concurrent: 0},
		"concurrent 2":   {concurrent: 2},
		"concurrent 100": {concurrent: 100},
	}
	for name, tc := range tcs {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()

			input := &model.Step[int]{Output: createInputChan(t, 10)}
			got := make(chan []int, 1)
			output := &model.Step[int]{Output: make(chan int), Details: &model.StepInfo{Concurrent: tc.concurrent}}

			go func() {
				got <- processOutputChan(t, output.Output)
			}()

			go func() {
				defer close(output.Output)
				err := runOneToMany(ctx, input, output, func(ctx context.Context, i int) ([]int, error) {
					return []int{i, i * 10}, nil
				})
				assert.NoError(t, err)
			}()

			assert.ElementsMatch(t, []int{0, 0, 1, 10, 2, 20, 3, 30, 4, 40, 5, 50, 6, 60, 7, 70, 8, 80, 9, 90}, <-got)
		})
	}
}

func TestOneToManyCancelInput(t *testing.T) {
	t.Parallel()

	tcs := map[string]struct {
		concurrent int
	}{
		"sequential":     {concurrent: 1},
		"sequential v2":  {concurrent: 0},
		"concurrent 2":   {concurrent: 2},
		"concurrent 100": {concurrent: 100},
	}
	for name, tc := range tcs {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()

			input := &model.Step[int]{Output: createInputChanWithCancel(t, 10, 5, cancel)}
			got := make(chan []int, 1)
			output := &model.Step[int]{Output: make(chan int), Details: &model.StepInfo{Concurrent: tc.concurrent}}

			go func() {
				got <- processOutputChan(t, output.Output)
			}()

			go func() {
				defer close(output.Output)
				err := runOneToMany(ctx, input, output, func(ctx context.Context, i int) ([]int, error) {
					return []int{i, i * 10}, nil
				})
				assert.Error(t, err)
			}()

			assert.NotZero(t, <-got)
		})
	}
}

func TestOneToManyCancelOutput(t *testing.T) {
	t.Parallel()

	tcs := map[string]struct {
		concurrent int
	}{
		"sequential":     {concurrent: 1},
		"sequential v2":  {concurrent: 0},
		"concurrent 2":   {concurrent: 2},
		"concurrent 100": {concurrent: 100},
	}
	for name, tc := range tcs {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()

			input := &model.Step[int]{Output: createInputChan(t, 10)}
			got := make(chan []int, 1)
			output := &model.Step[int]{Output: make(chan int), Details: &model.StepInfo{Concurrent: tc.concurrent}}

			go func() {
				got <- processOutputChan(t, output.Output)
			}()

			go func() {
				defer close(output.Output)
				err := runOneToMany(ctx, input, output, func(ctx context.Context, i int) ([]int, error) {
					if i == 5 {
						cancel()

						return nil, assert.AnError
					}

					return []int{i, i * 10}, nil
				})
				assert.Error(t, err)
			}()

			assert.NotZero(t, <-got)
		})
	}
}

func TestOneToManyError(t *testing.T) {
	t.Parallel()

	tcs := map[string]struct {
		concurrent int
	}{
		"sequential":     {concurrent: 1},
		"sequential v2":  {concurrent: 0},
		"concurrent 2":   {concurrent: 2},
		"concurrent 100": {concurrent: 100},
	}
	for name, tc := range tcs {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()

			input := &model.Step[int]{Output: createInputChan(t, 10)}
			got := make(chan []int, 1)
			output := &model.Step[int]{Output: make(chan int), Details: &model.StepInfo{Concurrent: tc.concurrent}}

			go func() {
				got <- processOutputChan(t, output.Output)
			}()

			go func() {
				defer close(output.Output)

				err := runOneToMany(ctx, input, output, func(ctx context.Context, i int) ([]int, error) {
					if i == 5 {
						// As we can have up to 100 go routines
						// It is possible we reach this error before the first 4 elements made it to the output
						time.Sleep(1 * time.Millisecond)

						return nil, assert.AnError
					}

					return []int{i, i * 10}, nil
				})
				assert.Error(t, err)
			}()

			res := <-got
			assert.NotZero(t, res)
		})
	}
}
