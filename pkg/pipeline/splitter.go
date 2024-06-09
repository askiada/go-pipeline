package pipeline

import (
	"sync"
	"time"

	"github.com/askiada/go-pipeline/pkg/pipeline/model"
	"github.com/pkg/errors"
)

type Splitter[I any] struct {
	mu            sync.Mutex
	currIdx       int
	mainStep      *model.Step[I]
	splittedSteps []*model.Step[I]
	bufferSize    int
	Total         int
}

func (s *Splitter[I]) Get() (*model.Step[I], bool) {
	s.mu.Lock()
	defer func() {
		s.currIdx++
		s.mu.Unlock()
	}()
	if s.currIdx >= len(s.splittedSteps) {
		return nil, false
	}
	return s.splittedSteps[s.currIdx], true
}

func prepareSplitter[I any](pipe *Pipeline, input *model.Step[I], splitter *Splitter[I]) error {
	for _, opt := range pipe.opts {
		err := opt.PrepareSplitter(input.Details, splitter.mainStep.Details)
		if err != nil {
			return errors.Wrap(err, "unable to run before step function")
		}
	}
	return nil
}

func AddSplitter[I any](p *Pipeline, name string, input *model.Step[I], total int, opts ...SplitterOption[I]) (*Splitter[I], error) {
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
		mainStep: &model.Step[I]{
			Details: &model.StepInfo{
				Type:       model.SplitterStepType,
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
	splitter.splittedSteps = make([]*model.Step[I], total)
	if splitter.bufferSize == 0 {
		splitter.bufferSize = 1
	}
	splitterBuffer := make([]chan I, total)

	for i := range splitterBuffer {
		splitterBuffer[i] = make(chan I, splitter.bufferSize)
	}

	for i := 0; i < total; i++ {
		step := model.Step[I]{
			Details: &model.StepInfo{
				Type: model.SplitterStepType,
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
			startIter := time.Now()
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
					case <-p.ctx.Done():
						errC <- p.ctx.Err()

						break outer
					case localBuf <- localEntry:
					}
				}

				endFn := time.Since(startFn)
				endIter := time.Since(startIter) - endFn

				for _, opt := range p.opts {
					err := opt.OnSplitterOutput(input.Details, splitter.mainStep.Details, endIter, endFn)
					if err != nil {
						errC <- errors.Wrap(err, "unable to run before merger function")
					}
				}
			}
		}
	}()
	p.errcList.add(decoratedError)

	return splitter, nil
}

type SplitterFn[I any] func(input I) (bool, error)

func AddSplitterFn[I any](p *Pipeline, name string, input *model.Step[I], fns []SplitterFn[I], opts ...SplitterOption[I]) (*Splitter[I], error) {
	if p == nil {
		return nil, ErrPipelineMustBeSet
	}
	if input == nil {
		return nil, ErrInputMustBeSet
	}
	total := len(fns)
	if total == 0 {
		return nil, ErrSplitterTotal
	}
	splitter := &Splitter[I]{
		Total: total,
		mainStep: &model.Step[I]{
			Details: &model.StepInfo{
				Type:       model.SplitterStepType,
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
	splitter.splittedSteps = make([]*model.Step[I], total)
	if splitter.bufferSize == 0 {
		splitter.bufferSize = 1
	}
	splitterBuffer := make([]chan I, total)

	for i := range splitterBuffer {
		splitterBuffer[i] = make(chan I, splitter.bufferSize)
	}

	for i := 0; i < total; i++ {
		step := model.Step[I]{
			Details: &model.StepInfo{
				Type: model.SplitterStepType,
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
					ok, err := fns[localI](elem)
					if err != nil {
						errC <- errors.Wrap(err, "unable to run splitter function")
					}
					if !ok {
						continue
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
			startIter := time.Now()
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
					case <-p.ctx.Done():
						errC <- p.ctx.Err()

						break outer
					case localBuf <- localEntry:
					}
				}

				endFn := time.Since(startFn)
				endIter := time.Since(startIter) - endFn

				for _, opt := range p.opts {
					err := opt.OnSplitterOutput(input.Details, splitter.mainStep.Details, endIter, endFn)
					if err != nil {
						errC <- errors.Wrap(err, "unable to run before merger function")
					}
				}
			}
		}
	}()
	p.errcList.add(decoratedError)

	return splitter, nil
}
