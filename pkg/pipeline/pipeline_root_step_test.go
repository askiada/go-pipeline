package pipeline

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAddRootStepNilPipe(t *testing.T) {
	_, err := AddRootStep(nil, "root step", func(ctx context.Context, rootChan chan<- int) error {
		for i := 0; i < 10; i++ {
			rootChan <- i
		}
		return nil
	})
	assert.Error(t, err)
}

func TestAddRootStep(t *testing.T) {
	pipe, err := New(context.Background())
	assert.NoError(t, err)
	var got []int

	outputChan, err := AddRootStep(pipe, "root step", func(ctx context.Context, rootChan chan<- int) error {
		for i := 0; i < 10; i++ {
			rootChan <- i
		}
		return nil
	})
	assert.Nil(t, err)
	done := make(chan struct{})
	go func() {
		got = processOutputChan(t, outputChan.Output)
		done <- struct{}{}
	}()
	err = pipe.Run()
	assert.Nil(t, err)
	<-done
	assert.ElementsMatch(t, []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, got)
}

func TestAddRootStepError(t *testing.T) {
	pipe, err := New(context.Background())
	assert.NoError(t, err)
	var got []int

	outputChan, err := AddRootStep(pipe, "root step", func(ctx context.Context, rootChan chan<- int) error {
		for i := 0; i < 10; i++ {
			if i == 5 {
				return assert.AnError
			}
			rootChan <- i
		}
		return nil
	})
	assert.Nil(t, err)
	done := make(chan struct{})
	go func() {
		got = processOutputChan(t, outputChan.Output)
		done <- struct{}{}
	}()
	err = pipe.Run()
	assert.Error(t, err)
	<-done
	_ = got
}

func TestAddRootStepCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	pipe, err := New(ctx)
	assert.NoError(t, err)
	var got []int

	outputChan, err := AddRootStep(pipe, "root step", func(ctx context.Context, rootChan chan<- int) error {
		for i := 0; i < 10; i++ {
			if i == 5 {
				cancel()
				return assert.AnError
			}
			rootChan <- i
		}
		return nil
	})
	assert.Nil(t, err)
	done := make(chan struct{})
	go func() {
		got = processOutputChan(t, outputChan.Output)
		done <- struct{}{}
	}()
	err = pipe.Run()
	assert.Error(t, err)
	<-done
	_ = got
}

func TestAddRootStepNoCloseNilPipe(t *testing.T) {
	_, err := AddRootStep(nil, "root step", func(ctx context.Context, rootChan chan<- int) error {
		defer close(rootChan)
		for i := 0; i < 10; i++ {
			rootChan <- i
		}
		return nil
	}, StepKeepOpen[int]())
	assert.Error(t, err)
}

func TestAddRootStepNoClose(t *testing.T) {
	pipe, err := New(context.Background())
	assert.NoError(t, err)
	var got []int

	outputChan, err := AddRootStep(pipe, "root step", func(ctx context.Context, rootChan chan<- int) error {
		defer close(rootChan)
		for i := 0; i < 10; i++ {
			rootChan <- i
		}
		return nil
	}, StepKeepOpen[int]())
	assert.Nil(t, err)
	done := make(chan struct{})
	go func() {
		got = processOutputChan(t, outputChan.Output)
		done <- struct{}{}
	}()
	err = pipe.Run()
	assert.Nil(t, err)
	<-done
	assert.ElementsMatch(t, []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, got)
}

func TestAddRootStepNoCloseError(t *testing.T) {
	pipe, err := New(context.Background())
	assert.NoError(t, err)
	var got []int

	outputChan, err := AddRootStep(pipe, "root step", func(ctx context.Context, rootChan chan<- int) error {
		defer close(rootChan)
		for i := 0; i < 10; i++ {
			if i == 5 {
				return assert.AnError
			}
			rootChan <- i
		}
		return nil
	}, StepKeepOpen[int]())
	assert.Nil(t, err)
	done := make(chan struct{})
	go func() {
		got = processOutputChan(t, outputChan.Output)
		done <- struct{}{}
	}()
	err = pipe.Run()
	assert.Error(t, err)
	<-done
	_ = got
}

func TestAddRootStepNoCloseCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	pipe, err := New(ctx)
	assert.NoError(t, err)
	var got []int

	outputChan, err := AddRootStep(pipe, "root step", func(ctx context.Context, rootChan chan<- int) error {
		defer close(rootChan)
		for i := 0; i < 10; i++ {
			if i == 5 {
				cancel()
				return assert.AnError
			}
			rootChan <- i
		}
		return nil
	}, StepKeepOpen[int]())
	assert.Nil(t, err)
	done := make(chan struct{})
	go func() {
		got = processOutputChan(t, outputChan.Output)
		done <- struct{}{}
	}()
	err = pipe.Run()
	assert.Error(t, err)
	<-done
	_ = got
}
