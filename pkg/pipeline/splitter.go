package pipeline

import (
	"context"
	"sync"
	"time"

	"github.com/pkg/errors"

	"github.com/askiada/go-pipeline/pkg/pipeline/model"
)

// Splitter is a step that splits the input into multiple outputs.
type Splitter[I any] struct {
	mu            sync.Mutex
	currIdx       int
	mainStep      *model.Step[I]
	splittedSteps []*model.Step[I]
	bufferSize    int
	Total         int
}

// Get returns the next splitted step.
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

func prepareSplitter[I any](pipe *Pipeline, name string, input *model.Step[I], total int, opts ...SplitterOption[I]) (*Splitter[I], error) {
	if pipe == nil {
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

	splitter.splittedSteps = make([]*model.Step[I], total)

	if splitter.bufferSize == 0 {
		splitter.bufferSize = 1
	}

	for i := range total {
		step := model.Step[I]{
			Details: &model.StepInfo{
				Type: model.SplitterStepType,
				Name: name,
			},
			Output: make(chan I),
		}
		splitter.splittedSteps[i] = &step
	}

	for _, opt := range pipe.opts {
		err := opt.PrepareSplitter(input.Details, splitter.mainStep.Details)
		if err != nil {
			return nil, errors.Wrap(err, "unable to run before step function")
		}
	}

	return splitter, nil
}

func runSplitter[I any](
	pipe *Pipeline,
	splitter *Splitter[I],
	input *model.Step[I],
	splitterBuffer []chan I,
	errC chan error,
	wgrp *sync.WaitGroup,
) {
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
		case <-pipe.ctx.Done():
			errC <- pipe.ctx.Err()

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
				case <-pipe.ctx.Done():
					errC <- pipe.ctx.Err()

					break outer
				case localBuf <- localEntry:
				}
			}

			endFn := time.Since(startFn)
			endIter := time.Since(startIter) - endFn

			for _, opt := range pipe.opts {
				err := opt.OnSplitterOutput(input.Details, splitter.mainStep.Details, endIter, endFn)
				if err != nil {
					errC <- errors.Wrap(err, "unable to run before merger function")
				}
			}
		}
	}
}

// AddSplitter adds a splitter step to the pipeline. It will split the input into multiple outputs based on the total.
func AddSplitter[I any](pipe *Pipeline, name string, input *model.Step[I], total int, opts ...SplitterOption[I]) (*Splitter[I], error) {
	errC := make(chan error, 1)
	decoratedError := newErrorChan(name, errC)

	splitter, err := prepareSplitter(pipe, name, input, total, opts...)
	if err != nil {
		return nil, err
	}

	splitterBuffer := make([]chan I, total)

	for i := range splitterBuffer {
		splitterBuffer[i] = make(chan I, splitter.bufferSize)
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
				case <-pipe.ctx.Done():
					errC <- pipe.ctx.Err()

					break outer
				}
			}
			close(splitter.splittedSteps[localI].Output)
		}()
	}

	go func() {
		runSplitter(pipe, splitter, input, splitterBuffer, errC, wgrp)
	}()

	pipe.errcList.add(decoratedError)

	return splitter, nil
}

// SplitterFn is a function that returns wether to keep the input or not.
type SplitterFn[I any] func(ctx context.Context, input I) (bool, error)

// AddSplitterFn adds a splitter step to the pipeline. It will split the input into multiple outputs based on the provided functions.
func AddSplitterFn[I any](
	pipe *Pipeline,
	name string,
	input *model.Step[I],
	fns []SplitterFn[I],
	opts ...SplitterOption[I],
) (*Splitter[I], error) {
	total := len(fns)
	errC := make(chan error, 1)
	decoratedError := newErrorChan(name, errC)

	splitter, err := prepareSplitter(pipe, name, input, total, opts...)
	if err != nil {
		return nil, err
	}

	splitterBuffer := make([]chan I, total)

	for i := range splitterBuffer {
		splitterBuffer[i] = make(chan I, splitter.bufferSize)
	}

	wgrp := &sync.WaitGroup{}
	wgrp.Add(len(splitterBuffer))

	for i, buf := range splitterBuffer {
		localBuf := buf
		localI := i

		go func() {
			defer func() {
				close(splitter.splittedSteps[localI].Output)
				wgrp.Done()
			}()
		outer:
			for {
				select {
				case <-pipe.ctx.Done():
					errC <- pipe.ctx.Err()

					break outer

				case elem, ok := <-localBuf:
					if !ok {
						break outer
					}
					ok, err := fns[localI](pipe.ctx, elem)
					if err != nil {
						errC <- errors.Wrap(err, "unable to run splitter function")
					}
					if !ok {
						continue
					}

					splitter.splittedSteps[localI].Output <- elem
				}
			}
		}()
	}

	go func() {
		runSplitter(pipe, splitter, input, splitterBuffer, errC, wgrp)
	}()

	pipe.errcList.add(decoratedError)

	return splitter, nil
}
