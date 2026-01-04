package pipeline

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/askiada/go-pipeline/v2/pkg/pipeline/model"
)

// Splitter fans out items into multiple branch steps.
// Use Get to retrieve each branch step in order.
type Splitter[I any] struct {
	mu            sync.Mutex
	currIdx       int
	mainStep      *Step[I]
	splittedSteps []*Step[I]
	bufferSize    int
	Total         int
}

// Get returns the next branch step in order.
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

	splitter.mainStep.Details.BufferSize = splitter.bufferSize

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
				BufferSize: splitter.bufferSize,
			},
			Output: make(chan I, splitter.bufferSize),
		}
		splitter.splittedSteps[idx] = &step
	}

	for _, opt := range pipe.opts {
		err := opt.PrepareSplitter(input.Details, splitter.mainStep.Details)
		if err != nil {
			return nil, fmt.Errorf("unable to run before step function: %w", err)
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
	errC chan error,
) {
	defer func() {
		for _, step := range splitter.splittedSteps {
			close(step.Output)
		}

		close(errC)
	}()

	cfg := pipe.hookConfig()

	for {
		var waitStart time.Time
		if cfg.outputMetrics {
			waitStart = time.Now()
		}

		select {
		case <-ctx.Done():
			errC <- ctx.Err()

			return
		case entry, ok := <-input.Output:
			if !ok {
				return
			}

			var inputWait time.Duration
			if cfg.outputMetrics {
				inputWait = time.Since(waitStart)
			}

			var startFn time.Time
			if cfg.outputMetrics {
				startFn = time.Now()
			}

			for _, step := range splitter.splittedSteps {
				select {
				case <-ctx.Done():
					errC <- ctx.Err()

					return
				case step.Output <- entry:
				}
			}

			for _, opt := range cfg.opts {
				err := opt.OnSplitterOutput(input.Details, splitter.mainStep.Details)
				if err != nil {
					errC <- fmt.Errorf("unable to run before merger function: %w", err)
				}
			}

			if !cfg.outputMetrics {
				continue
			}

			endFn := time.Since(startFn)

			for _, opt := range cfg.metricsOpts {
				err := opt.OnSplitterOutputMetrics(input.Details, splitter.mainStep.Details, inputWait, endFn)
				if err != nil {
					errC <- fmt.Errorf("unable to run before merger function: %w", err)
				}
			}
		}
	}
}

//nolint:cyclop,gocognit,gocyclo // Branch-heavy error handling stays localised here.
func runSplitBy[I any](
	ctx context.Context,
	pipe *Pipeline,
	splitter *Splitter[I],
	input *Step[I],
	errC chan error,
	fns []SplitFn[I],
) {
	defer func() {
		for _, step := range splitter.splittedSteps {
			close(step.Output)
		}

		close(errC)
	}()

	cfg := pipe.hookConfig()

	for {
		var waitStart time.Time
		if cfg.outputMetrics {
			waitStart = time.Now()
		}

		select {
		case <-ctx.Done():
			errC <- ctx.Err()

			return
		case entry, ok := <-input.Output:
			if !ok {
				return
			}

			var inputWait time.Duration
			if cfg.outputMetrics {
				inputWait = time.Since(waitStart)
			}

			var startFn time.Time
			if cfg.outputMetrics {
				startFn = time.Now()
			}

			for idx, fn := range fns {
				ok, err := fn(ctx, entry)
				if err != nil {
					errC <- fmt.Errorf("unable to run splitter function: %w", err)

					return
				}

				if !ok {
					continue
				}

				select {
				case <-ctx.Done():
					errC <- ctx.Err()

					return
				case splitter.splittedSteps[idx].Output <- entry:
				}
			}

			for _, opt := range cfg.opts {
				err := opt.OnSplitterOutput(input.Details, splitter.mainStep.Details)
				if err != nil {
					errC <- fmt.Errorf("unable to run before merger function: %w", err)
				}
			}

			if !cfg.outputMetrics {
				continue
			}

			endFn := time.Since(startFn)

			for _, opt := range cfg.metricsOpts {
				err := opt.OnSplitterOutputMetrics(input.Details, splitter.mainStep.Details, inputWait, endFn)
				if err != nil {
					errC <- fmt.Errorf("unable to run before merger function: %w", err)
				}
			}
		}
	}
}

// Split adds a splitter step that copies each item to every branch.
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

	pipe.addRunner(func(ctx context.Context) {
		go func() {
			runSplitter(ctx, pipe, splitter, input, errC)
		}()
	})

	pipe.errcList.add(decoratedError)

	return splitter
}

// SplitFn decides whether an item should be sent to a branch.
type SplitFn[I any] func(ctx context.Context, input I) (bool, error)

// SplitBy adds a splitter step that routes items to branches.
// Each function is called for each item; returning true sends the item to that branch.
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

	pipe.addRunner(func(ctx context.Context) {
		go func() {
			runSplitBy(ctx, pipe, splitter, input, errC, fns)
		}()
	})

	pipe.errcList.add(decoratedError)

	return splitter
}
