package pipeline

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestOneToOne(t *testing.T) {
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
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			input := &Step[int]{Output: createInputChan(t, ctx, 10), concurrent: tc.concurrent}
			got := make(chan []int, 1)
			output := &Step[int]{Output: make(chan int)}
			go func() {
				got <- processOutputChan(t, output.Output)
			}()
			go func() {
				defer close(output.Output)
				err := oneToOne(ctx, input, output, func(ctx context.Context, i int) (o int, err error) {
					return i, nil
				})
				assert.Nil(t, err)
			}()
			assert.ElementsMatch(t, []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, <-got)
		})
	}
}

func TestOneToOneCancelInput(t *testing.T) {
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
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			input := &Step[int]{Output: createInputChanWithCancel(t, ctx, 10, 5, cancel), concurrent: tc.concurrent}
			got := make(chan []int, 1)
			output := &Step[int]{Output: make(chan int)}
			go func() {
				got <- processOutputChan(t, output.Output)
			}()
			go func() {
				defer close(output.Output)
				err := oneToOne(ctx, input, output, func(ctx context.Context, i int) (o int, err error) {
					return i, nil
				})
				assert.Error(t, err)
			}()
			assert.NotZero(t, <-got)
		})
	}
}

func TestOneToOneCancelOutput(t *testing.T) {
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
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			input := &Step[int]{Output: createInputChan(t, ctx, 10), concurrent: tc.concurrent}
			got := make(chan []int, 1)
			output := &Step[int]{Output: make(chan int)}
			go func() {
				got <- processOutputChan(t, output.Output)
			}()
			go func() {
				defer close(output.Output)
				err := oneToOne(ctx, input, output, func(ctx context.Context, i int) (o int, err error) {
					if i == 5 {
						cancel()
						return 0, assert.AnError
					}
					return i, nil
				})
				assert.Error(t, err)
			}()
			assert.NotZero(t, <-got)
		})
	}
}

func TestOneToOneError(t *testing.T) {
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
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			input := &Step[int]{Output: createInputChan(t, ctx, 10), concurrent: tc.concurrent}
			got := make(chan []int, 1)
			output := &Step[int]{Output: make(chan int)}
			go func() {
				got <- processOutputChan(t, output.Output)
			}()
			go func() {
				defer close(output.Output)
				err := oneToOne(ctx, input, output, func(ctx context.Context, i int) (o int, err error) {
					if i == 5 {
						return 0, assert.AnError
					}
					return i, nil
				})
				assert.Error(t, err)
			}()
			assert.NotZero(t, <-got)
		})
	}
}

func TestOneToMany(t *testing.T) {
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
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			input := &Step[int]{Output: createInputChan(t, ctx, 10), concurrent: tc.concurrent}
			got := make(chan []int, 1)
			output := &Step[int]{Output: make(chan int)}
			go func() {
				got <- processOutputChan(t, output.Output)
			}()
			go func() {
				defer close(output.Output)
				err := oneToMany(ctx, input, output, func(ctx context.Context, i int) (o []int, err error) {
					return []int{i, i * 10}, nil
				})
				assert.Nil(t, err)
			}()
			assert.ElementsMatch(t, []int{0, 0, 1, 10, 2, 20, 3, 30, 4, 40, 5, 50, 6, 60, 7, 70, 8, 80, 9, 90}, <-got)
		})
	}
}

func TestOneToManyCancelInput(t *testing.T) {
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
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			input := &Step[int]{Output: createInputChanWithCancel(t, ctx, 10, 5, cancel), concurrent: tc.concurrent}
			got := make(chan []int, 1)
			output := &Step[int]{Output: make(chan int)}
			go func() {
				got <- processOutputChan(t, output.Output)
			}()
			go func() {
				defer close(output.Output)
				err := oneToMany(ctx, input, output, func(ctx context.Context, i int) (o []int, err error) {
					return []int{i, i * 10}, nil
				})
				assert.Error(t, err)
			}()
			assert.NotZero(t, <-got)
		})
	}
}

func TestOneToManyCancelOutput(t *testing.T) {
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
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			input := &Step[int]{Output: createInputChan(t, ctx, 10), concurrent: tc.concurrent}
			got := make(chan []int, 1)
			output := &Step[int]{Output: make(chan int)}
			go func() {
				got <- processOutputChan(t, output.Output)
			}()
			go func() {
				defer close(output.Output)
				err := oneToMany(ctx, input, output, func(ctx context.Context, i int) (o []int, err error) {
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
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			input := &Step[int]{Output: createInputChan(t, ctx, 10), concurrent: tc.concurrent}
			got := make(chan []int, 1)
			output := &Step[int]{Output: make(chan int)}
			go func() {
				got <- processOutputChan(t, output.Output)
			}()
			go func() {
				defer close(output.Output)
				err := oneToMany(ctx, input, output, func(ctx context.Context, i int) (o []int, err error) {
					if i == 5 {
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
