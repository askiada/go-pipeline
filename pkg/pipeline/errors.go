package pipeline

import (
	"sync"

	"github.com/pkg/errors"
)

var (
	// ErrPipelineMustBeSet is returned when the pipeline is not set.
	ErrPipelineMustBeSet = errors.New("pipe must be set")
	// ErrPipelineAlreadyRan is returned when a pipeline Run is invoked more than once.
	ErrPipelineAlreadyRan = errors.New("pipeline already ran")
	// ErrContextMustBeSet is returned when the context is not set.
	ErrContextMustBeSet = errors.New("context must be set")
	// ErrInputMustBeSet is returned when the input is not set.
	ErrInputMustBeSet = errors.New("input must be set")
	// ErrSplitterTotal is returned when the total is not set.
	ErrSplitterTotal = errors.New("total must be greater than 0")
	// ErrRetryUnsupported is returned when retry is configured on an unsupported step.
	ErrRetryUnsupported = errors.New("retry is not supported for this step type")
	// ErrBatchPolicyMustBeSet is returned when a batch step does not define a batch policy.
	ErrBatchPolicyMustBeSet = errors.New("batch policy must be set")
	// ErrTimeoutUnsupported is returned when timeout is configured on an unsupported step.
	ErrTimeoutUnsupported = errors.New("timeout is not supported for this step type")
	// ErrRateLimitUnsupported is returned when rate limiting is configured on an unsupported step.
	ErrRateLimitUnsupported = errors.New("rate limit is not supported for this step type")
	// ErrMaxInFlightUnsupported is returned when max in-flight is configured on an unsupported step.
	ErrMaxInFlightUnsupported = errors.New("max in-flight is not supported for this step type")
	// ErrDropOutputUnsupported is returned when output drop policies are configured on an unsupported step.
	ErrDropOutputUnsupported = errors.New("output drop policy is not supported for this step type")
	// ErrDropOnErrorUnsupported is returned when drop-on-error is configured on an unsupported step.
	ErrDropOnErrorUnsupported = errors.New("drop on error is not supported for this step type")
	// ErrErrorRouteUnsupported is returned when error routing is configured on an unsupported step.
	ErrErrorRouteUnsupported = errors.New("error routing is not supported for this step type")
)

type errorChans struct {
	list []*errorChan
	mu   sync.Mutex
}

func (ec *errorChans) add(errChan *errorChan) {
	ec.mu.Lock()
	defer ec.mu.Unlock()

	ec.list = append(ec.list, errChan)
}

type errorChan struct {
	c    <-chan error
	name string
}

func newErrorChan(name string, c <-chan error) *errorChan {
	return &errorChan{
		c:    c,
		name: name,
	}
}

// mergeErrors merges multiple channels of errors.
// Based on https://blog.golang.org/pipelines.
func mergeErrors(errChs ...*errorChan) <-chan error {
	var wgrp sync.WaitGroup
	// We must ensure that the output channel has the capacity to hold as many errors
	// as there are error channels. This will ensure that it never blocks, even
	// if WaitForPipeline returns early.
	out := make(chan error, len(errChs))

	// Start an output goroutine for each input channel in cs.  output
	// copies values from c to out until c is closed, then calls wg.Done.
	output := func(errC *errorChan) {
		defer wgrp.Done()

		if errC.c == nil {
			return
		}

		for n := range errC.c {
			out <- errors.Wrap(n, errC.name)
		}
	}

	wgrp.Add(len(errChs))

	for _, c := range errChs {
		go output(c)
	}

	// Start a goroutine to close out once all the output goroutines are
	// done.  This must start after the wg.Add call.
	go func() {
		wgrp.Wait()
		close(out)
	}()

	return out
}
