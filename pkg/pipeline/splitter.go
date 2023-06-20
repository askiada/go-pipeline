package pipeline

import (
	"time"

	"golang.org/x/sync/errgroup"
)

type Splitter[I any] struct {
	currIdx       int
	mainStep      *Step[I]
	splittedSteps []*Step[I]
	concurrent    int
	bufferSize    int
	Total         int
}

func (s *Splitter[I]) Get() (*Step[I], bool) {
	defer func() {
		s.currIdx++
	}()
	if s.currIdx >= len(s.splittedSteps) {
		return nil, false
	}
	return s.splittedSteps[s.currIdx], true
}

func prepareSplitter[I any](p *Pipeline, name string, input *Step[I], splitter *Splitter[I]) error {
	if p.drawer != nil {
		err := p.drawer.addStep(splitter.mainStep.Name)
		if err != nil {
			return err
		}
		err = p.drawer.addLink(input.Name, splitter.mainStep.Name)
		if err != nil {
			return err
		}
	}
	if p.measure != nil {
		mt := p.measure.addStep(splitter.mainStep.Name)
		splitter.mainStep.metric = mt
	}
	return nil
}

func AddSplitter[I any](p *Pipeline, name string, input *Step[I], total int, opts ...SplitterOption[I]) (*Splitter[I], error) {
	if p == nil {
		return nil, ErrPipelineMustBeSet
	}
	if input == nil {
		return nil, ErrInputMustBeSet
	}
	if total == 0 {
		return nil, ErrSplitterTotal
	}
	splitter := &Splitter[I]{
		Total: total,
		mainStep: &Step[I]{
			Type: splitterStepType,
			Name: name,
		},
	}
	for _, opt := range opts {
		opt(splitter)
	}
	if splitter.concurrent == 0 {
		splitter.concurrent = total
	}
	errC := make(chan error, 1)
	decoratedError := newErrorChan(name, errC)
	splitter.splittedSteps = make([]*Step[I], total)

	for i := 0; i < total; i++ {
		step := Step[I]{
			Type:   splitterStepType,
			Name:   name,
			Output: make(chan I),
		}
		splitter.splittedSteps[i] = &step
	}

	err := prepareSplitter(p, name, input, splitter)
	if err != nil {
		return nil, err
	}

	go func() {
		errG, dCtx := errgroup.WithContext(p.ctx)
		errG.SetLimit(splitter.concurrent)
		defer func() {
			for _, out := range splitter.splittedSteps {
				close(out.Output)
			}
			close(errC)
		}()

	outer:
		for {
			startInputCHan := time.Now()
			select {
			case <-p.ctx.Done():
				errC <- p.ctx.Err()

				break outer
			case entry, ok := <-input.Output:
				if !ok {
					break outer
				}
				endInputChan := time.Since(startInputCHan)

				for _, out := range splitter.splittedSteps {
					localEntry := entry
					localOut := out
					errG.Go(func() error {
						start := time.Now()
						select {
						case localOut.Output <- localEntry:
							if splitter.mainStep.metric != nil {
								splitter.mainStep.metric.addChannel(input.Name, time.Since(start)+endInputChan)
							}
						case <-dCtx.Done():
							return dCtx.Err()
						}
						return nil
					})
				}
			}
		}

		if err := errG.Wait(); err != nil {
			errC <- err
		}
	}()
	p.errcList.add(decoratedError)

	return splitter, nil
}
