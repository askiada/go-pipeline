package pipeline

import (
	"sync"
	"time"

	"github.com/pkg/errors"
)

type Splitter[I any] struct {
	currIdx       int
	mainStep      *Step[I]
	splittedSteps []*Step[I]
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

func prepareSplitter[I any](pipe *Pipeline, input *Step[I], splitter *Splitter[I]) error {
	for _, opt := range pipe.opts {
		err := opt.BeforeSplitter(input.details, splitter.mainStep.details)
		if err != nil {
			return errors.Wrap(err, "unable to run before step function")
		}
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
			details: &StepInfo{
				Type:       splitterStepType,
				Name:       name,
				Concurrent: 1,
			},
		},
	}
	for _, opt := range opts {
		opt(splitter)
	}
	errC := make(chan error, 1)
	decoratedError := newErrorChan(name, errC)
	splitter.splittedSteps = make([]*Step[I], total)
	if splitter.bufferSize == 0 {
		splitter.bufferSize = 1
	}
	splitterBuffer := make([]chan I, total)

	for i := range splitterBuffer {
		splitterBuffer[i] = make(chan I, splitter.bufferSize)
	}

	for i := 0; i < total; i++ {
		step := Step[I]{
			details: &StepInfo{
				Type: splitterStepType,
				Name: name,
			},
			Output: make(chan I),
		}
		splitter.splittedSteps[i] = &step
	}

	err := prepareSplitter(p, input, splitter)
	if err != nil {
		return nil, err
	}
	wgrp := &sync.WaitGroup{}
	wgrp.Add(len(splitterBuffer))
	for i, buf := range splitterBuffer {
		localBuf := buf
		localI := i
		go func() {
			defer wgrp.Done()
		outer:
			for {
				select {
				case elem, ok := <-localBuf:
					if !ok {
						break outer
					}
					splitter.splittedSteps[localI].Output <- elem
				case <-p.ctx.Done():
					errC <- p.ctx.Err()

					break outer
				}
			}
			close(splitter.splittedSteps[localI].Output)
		}()
	}

	go func() {
		defer func() {
			for _, buf := range splitterBuffer {
				close(buf)
			}
			wgrp.Wait()
			close(errC)
		}()

	outer:
		for {
			start := time.Now()
			select {
			case <-p.ctx.Done():
				errC <- p.ctx.Err()

				break outer
			case entry, ok := <-input.Output:
				if !ok {
					break outer
				}
				startFn := time.Now()
				for _, buf := range splitterBuffer {
					localEntry := entry
					localBuf := buf

					select {
					case localBuf <- localEntry:

					case <-p.ctx.Done():
						errC <- p.ctx.Err()

						break outer
					}
				}
				endFn := time.Since(startFn)
				if splitter.mainStep.details.Metric != nil {
					splitter.mainStep.details.Metric.AddDuration(endFn)
					splitter.mainStep.details.Metric.AddTransportDuration(input.details.Name, time.Since(start)-endFn)
				}
			}
		}
	}()
	p.errcList.add(decoratedError)

	return splitter, nil
}
