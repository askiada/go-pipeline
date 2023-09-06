package pipeline

import (
	"sync"

	"github.com/pkg/errors"
)

var (
	ErrPipelineMustBeSet = errors.New("p must be set")
	ErrInputMustBeSet    = errors.New("input must be set")
	ErrSplitterTotal     = errors.New("total must be greater than 0")
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
