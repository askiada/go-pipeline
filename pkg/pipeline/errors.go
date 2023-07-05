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
	mu   sync.Mutex
	list []*errorChan
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
func mergeErrors(cs ...*errorChan) <-chan error {
	var wg sync.WaitGroup
	// We must ensure that the output channel has the capacity to hold as many errors
	// as there are error channels. This will ensure that it never blocks, even
	// if WaitForPipeline returns early.
	out := make(chan error, len(cs))

	// Start an output goroutine for each input channel in cs.  output
	// copies values from c to out until c is closed, then calls wg.Done.
	output := func(c *errorChan) {
		defer wg.Done()
		if c.c == nil {
			return
		}
		for n := range c.c {
			out <- errors.Wrap(n, c.name)
		}
	}
	wg.Add(len(cs))
	for _, c := range cs {
		go output(c)
	}

	// Start a goroutine to close out once all the output goroutines are
	// done.  This must start after the wg.Add call.
	go func() {
		wg.Wait()
		close(out)
	}()

	return out
}
