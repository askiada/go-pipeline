package pipeline

import (
	"context"
)

type Pipeline struct {
	ctx      context.Context
	errcList *errorChans
	cancel   context.CancelFunc
}

func New(ctx context.Context) *Pipeline {
	dCtx, cancel := context.WithCancel(ctx)

	return &Pipeline{
		ctx:      dCtx,
		errcList: &errorChans{},
		cancel:   cancel,
	}
}

func AddRootStep[O any](p *Pipeline, name string, stepFn func(ctx context.Context, rootChan chan<- O) error) (<-chan O, error) {
	if p == nil {
		return nil, ErrPipelineMustBeSet
	}
	errC := make(chan error, 1)
	decoratedError := newErrorChan(name, errC)
	output := make(chan O)
	go func() {
		defer func() {
			close(output)
			close(errC)
		}()
		err := stepFn(p.ctx, output)
		if err != nil {
			errC <- err
		}
	}()
	p.errcList.add(decoratedError)

	return output, nil
}

func AddStep[I any, O any](p *Pipeline, name string, input <-chan I, stepFn func(ctx context.Context, input <-chan I, output chan<- O) error) (<-chan O, error) {
	if p == nil {
		return nil, ErrPipelineMustBeSet
	}
	if input == nil {
		return nil, ErrInputMustBeSet
	}
	errC := make(chan error, 1)
	decoratedError := newErrorChan(name, errC)
	output := make(chan O)
	go func() {
		defer func() {
			close(errC)
			close(output)
		}()
		err := stepFn(p.ctx, input, output)
		if err != nil {
			errC <- err
		}
	}()
	p.errcList.add(decoratedError)

	return output, nil
}

func AddSplitter[I any](p *Pipeline, name string, input <-chan I, total int) ([]chan I, error) {
	if p == nil {
		return nil, ErrPipelineMustBeSet
	}
	if input == nil {
		return nil, ErrInputMustBeSet
	}
	if total == 0 {
		return nil, ErrSplitterTotal
	}
	errC := make(chan error, 1)
	decoratedError := newErrorChan(name, errC)

	outSlice := make([]chan I, total)
	for i := 0; i < total; i++ {
		outSlice[i] = make(chan I)
	}

	go func() {
		defer func() {
			for _, out := range outSlice {
				close(out)
			}
			close(errC)
		}()
		for entry := range input {
			// Send the data to the output channel 1 but return early
			// if the context has been cancelled.
			for _, out := range outSlice {
				select {
				case out <- entry:
				case <-p.ctx.Done():
					errC <- p.ctx.Err()

					return
				}
			}
		}
	}()
	p.errcList.add(decoratedError)

	return outSlice, nil
}

func AddSink[I any](p *Pipeline, name string, input <-chan I, stepFn func(ctx context.Context, input <-chan I) error) error {
	if p == nil {
		return ErrPipelineMustBeSet
	}
	if input == nil {
		return ErrInputMustBeSet
	}
	errC := make(chan error, 1)
	decoratedError := newErrorChan(name, errC)

	go func() {
		defer func() {
			close(errC)
		}()
		err := stepFn(p.ctx, input)
		if err != nil {
			errC <- err
		}
	}()
	p.errcList.add(decoratedError)

	return nil
}

// waitForPipeline waits for results from all error channels.
// It returns early on the first error.
func waitForPipeline(errs ...*errorChan) error {
	errc := mergeErrors(errs...)
	for err := range errc {
		if err != nil {
			return err
		}
	}

	return nil
}

func (p *Pipeline) Run() error {
	defer p.cancel()

	return waitForPipeline(p.errcList.list...)
}

/*
// TODO !!!!! Errors are not correctly captured.
func AddMerger[I1, I2, O any](p *Pipeline, name string,
	input1 <-chan I1,
	input2 <-chan I2,
	transformFn1 func(input1 I1) (output O, err error),
	transformFn2 func(input2 I2) (output O, err error),
) chan O {
	errC := make(chan error, 2)
	decoratedError := newErrorChan(name, errC)
	output := make(chan O)
	go func() {
		defer func() {
			close(errC)
			close(output)
		}()
		wg := sync.WaitGroup{}
		wg.Add(2)
		go func() {
			defer wg.Done()
		outer:
			for {
				select {
				case <-p.ctx.Done():
					return
				case entry, ok := <-input1:
					if !ok {
						break outer
					}
					o, err := transformFn1(entry)
					if err != nil {
						errC <- err
						return
					}
					select {
					case <-p.ctx.Done():
						return
					case output <- o:
					}
					output <- o
				}
			}
		}()
		go func() {
			defer wg.Done()
		outer:
			for {
				select {
				case <-p.ctx.Done():
					return
				case entry, ok := <-input2:
					if !ok {
						break outer
					}
					o, err := transformFn2(entry)
					if err != nil {
						errC <- err

						return
					}
					select {
					case <-p.ctx.Done():
						return
					case output <- o:
					}
					output <- o
				}
			}
		}()
		wg.Wait()
	}()
	p.errcList.add(decoratedError)
	return output
}
*/
