package pipeline

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/pkg/errors"

	"github.com/askiada/go-pipeline/v2/pkg/pipeline/model"
)

// Splitter is a step that splits the input into multiple outputs.
type Splitter[I any] struct {
	mu            sync.Mutex
	currIdx       int
	mainStep      *Step[I]
	splittedSteps []*Step[I]
	bufferSize    int
	Total         int
}

// Get returns the next splitted step.
func (s *Splitter[I]) Get() (*Step[I], bool) {
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

func prepareSplitter[I any](pipe *Pipeline, name string, input *Step[I], total int, opts ...SplitterOption[I]) (*Splitter[I], error) {
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
		mainStep: &Step[I]{
			Details: &StepInfo{
				Type:       model.SplitterStepType,
				Name:       name,
				Concurrent: 1,
			},
		},
	}

	applySplitterDefaults(pipe, splitter)

	for _, opt := range opts {
		opt(splitter)
	}

	splitter.splittedSteps = make([]*Step[I], total)

	if splitter.bufferSize == 0 {
		splitter.bufferSize = 1
	}

	inputConcurrent := 1
	if input.Details != nil && input.Details.Concurrent > 0 {
		inputConcurrent = input.Details.Concurrent
	}

	warnSplitterBuffer(name, splitter.bufferSize, inputConcurrent)

	for idx := range total {
		step := Step[I]{
			Details: &StepInfo{
				Type:       model.SplitterStepType,
				Name:       name,
				Concurrent: 1,
			},
			Output: make(chan I),
		}
		splitter.splittedSteps[idx] = &step
	}

	for _, opt := range pipe.opts {
		err := opt.PrepareSplitter(input.Details, splitter.mainStep.Details)
		if err != nil {
			return nil, errors.Wrap(err, "unable to run before step function")
		}
	}

	return splitter, nil
}

func warnSplitterBuffer(name string, bufferSize, inputConcurrent int) {
	if bufferSize < 1 {
		return
	}

	if inputConcurrent < 1 {
		inputConcurrent = 1
	}

	if bufferSize < inputConcurrent {
		log.Printf(
			"go-pipeline: splitter %q buffer size %d is smaller than input concurrency %d; "+
				"expect upstream backpressure",
			name,
			bufferSize,
			inputConcurrent,
		)

		return
	}

	const warnFactor = 8
	if bufferSize > inputConcurrent*warnFactor {
		log.Printf(
			"go-pipeline: splitter %q buffer size %d is much larger than input concurrency %d; "+
				"large per-branch buffers can increase memory use",
			name,
			bufferSize,
			inputConcurrent,
		)
	}
}

//nolint:cyclop,gocognit,gocyclo // Branch-heavy error handling stays localised here.
func runSplitter[I any](
	ctx context.Context,
	pipe *Pipeline,
	splitter *Splitter[I],
	input *Step[I],
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

	cfg := pipe.hookConfig()

	for {
		var startIter time.Time
		if cfg.outputMetrics {
			startIter = time.Now()
		}

		select {
		case <-ctx.Done():
			errC <- ctx.Err()

			return
		case entry, ok := <-input.Output:
			if !ok {
				return
			}

			var startFn time.Time
			if cfg.outputMetrics {
				startFn = time.Now()
			}

			for _, buf := range splitterBuffer {
				localEntry := entry
				localBuf := buf

				select {
				case <-ctx.Done():
					errC <- ctx.Err()

					return
				case localBuf <- localEntry:
				}
			}

			for _, opt := range cfg.opts {
				err := opt.OnSplitterOutput(input.Details, splitter.mainStep.Details)
				if err != nil {
					errC <- errors.Wrap(err, "unable to run before merger function")
				}
			}

			if !cfg.outputMetrics {
				continue
			}

			endFn := time.Since(startFn)
			endIter := time.Since(startIter) - endFn

			for _, opt := range cfg.metricsOpts {
				err := opt.OnSplitterOutputMetrics(input.Details, splitter.mainStep.Details, endIter, endFn)
				if err != nil {
					errC <- errors.Wrap(err, "unable to run before merger function")
				}
			}
		}
	}
}

func startSplitterWorkers[I any](
	ctx context.Context,
	splitter *Splitter[I],
	splitterBuffer []chan I,
	errC chan error,
	wgrp *sync.WaitGroup,
	handle func(ctx context.Context, idx int, elem I) (bool, error),
) {
	for i, buf := range splitterBuffer {
		localBuf := buf
		localI := i

		go func() {
			defer func() {
				close(splitter.splittedSteps[localI].Output)
				wgrp.Done()
			}()

			for {
				select {
				case <-ctx.Done():
					errC <- ctx.Err()

					return
				case elem, ok := <-localBuf:
					if !ok {
						return
					}

					ok, err := handle(ctx, localI, elem)
					if err != nil {
						errC <- err
					}

					if !ok {
						continue
					}

					splitter.splittedSteps[localI].Output <- elem
				}
			}
		}()
	}
}

// Split adds a splitter step to the pipeline. It will split the input into multiple outputs based on the total.
func Split[I any](pipe *Pipeline, name string, input *Step[I], total int, opts ...SplitterOption[I]) *Splitter[I] {
	if pipe == nil {
		return nil
	}

	if pipe.buildErr != nil {
		return nil
	}

	errC := make(chan error, 1)
	decoratedError := newErrorChan(name, errC)

	splitter, err := prepareSplitter(pipe, name, input, total, opts...)
	if err != nil {
		pipe.recordErr(name, err)

		return nil
	}

	splitterBuffer := make([]chan I, total)

	for i := range splitterBuffer {
		splitterBuffer[i] = make(chan I, splitter.bufferSize)
	}

	wgrp := &sync.WaitGroup{}
	wgrp.Add(len(splitterBuffer))

	pipe.addRunner(func(ctx context.Context) {
		startSplitterWorkers(ctx, splitter, splitterBuffer, errC, wgrp, func(_ context.Context, _ int, _ I) (bool, error) {
			return true, nil
		})

		go func() {
			runSplitter(ctx, pipe, splitter, input, splitterBuffer, errC, wgrp)
		}()
	})

	pipe.errcList.add(decoratedError)

	return splitter
}

// SplitFn is a function that returns whether to keep the input or not.
type SplitFn[I any] func(ctx context.Context, input I) (bool, error)

// SplitBy adds a splitter step to the pipeline. It will split the input into multiple outputs based on the provided functions.
func SplitBy[I any](
	pipe *Pipeline,
	name string,
	input *Step[I],
	fns []SplitFn[I],
	opts ...SplitterOption[I],
) *Splitter[I] {
	if pipe == nil {
		return nil
	}

	if pipe.buildErr != nil {
		return nil
	}

	total := len(fns)
	errC := make(chan error, 1)
	decoratedError := newErrorChan(name, errC)

	splitter, err := prepareSplitter(pipe, name, input, total, opts...)
	if err != nil {
		pipe.recordErr(name, err)

		return nil
	}

	splitterBuffer := make([]chan I, total)

	for i := range splitterBuffer {
		splitterBuffer[i] = make(chan I, splitter.bufferSize)
	}

	wgrp := &sync.WaitGroup{}
	wgrp.Add(len(splitterBuffer))

	pipe.addRunner(func(ctx context.Context) {
		startSplitterWorkers(ctx, splitter, splitterBuffer, errC, wgrp, func(ctx context.Context, idx int, elem I) (bool, error) {
			ok, err := fns[idx](ctx, elem)
			if err != nil {
				return ok, errors.Wrap(err, "unable to run splitter function")
			}

			return ok, nil
		})

		go func() {
			runSplitter(ctx, pipe, splitter, input, splitterBuffer, errC, wgrp)
		}()
	})

	pipe.errcList.add(decoratedError)

	return splitter
}
