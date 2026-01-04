package pipeline

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/askiada/go-pipeline/v2/pkg/pipeline/model"
)

func prepareMerger[I any](pipe *Pipeline, output chan I, name string, steps ...*Step[I]) (*Step[I], error) {
	outputStep := &Step[I]{
		Details: &StepInfo{
			Type:       model.MergerStepType,
			Name:       name,
			Concurrent: 1,
		},
		Output: output,
	}

	stepInfos := make([]*StepInfo, len(steps))
	for i, step := range steps {
		stepInfos[i] = step.Details
	}

	for _, opt := range pipe.opts {
		err := opt.PrepareMerger(stepInfos, outputStep.Details)
		if err != nil {
			return nil, fmt.Errorf("unable to run before merger function: %w", err)
		}
	}

	return outputStep, nil
}

func reportMergerOutput[I any](
	cfg hookConfig,
	input *Step[I],
	output *Step[I],
	inputWait time.Duration,
) error {
	for _, opt := range cfg.opts {
		err := opt.OnMergerOutput(input.Details, output.Details)
		if err != nil {
			return fmt.Errorf("unable to run before merger function: %w", err)
		}
	}

	if !cfg.outputMetrics {
		return nil
	}

	for _, opt := range cfg.metricsOpts {
		err := opt.OnMergerOutputMetrics(input.Details, output.Details, inputWait)
		if err != nil {
			return fmt.Errorf("unable to run before merger function: %w", err)
		}
	}

	return nil
}

func runStepMerger[I any](ctx context.Context, pipe *Pipeline, errC chan error, step, outputStep *Step[I]) {
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
		case entry, ok := <-step.Output:
			if !ok {
				return
			}

			var inputWait time.Duration
			if cfg.outputMetrics {
				inputWait = time.Since(waitStart)
			}

			select {
			case <-ctx.Done():
				errC <- ctx.Err()
			case outputStep.Output <- entry:
				err := reportMergerOutput(cfg, step, outputStep, inputWait)
				if err != nil {
					errC <- err
				}
			}
		}
	}
}

// Merge combines the outputs of multiple steps into a single step.
// Items can arrive in any order across inputs.
func Merge[I any](pipe *Pipeline, name string, steps ...*Step[I]) *Step[I] {
	if pipe == nil {
		return nil
	}

	if pipe.buildErr != nil {
		return nil
	}

	if len(steps) == 0 {
		pipe.recordErr(name, ErrInputMustBeSet)

		return nil
	}

	for _, step := range steps {
		if step == nil {
			pipe.recordErr(name, ErrInputMustBeSet)

			return nil
		}
	}

	output := make(chan I)

	outputStep, err := prepareMerger(pipe, output, name, steps...)
	if err != nil {
		pipe.recordErr(name, err)

		return nil
	}

	errC := make(chan error, len(steps))
	decoratedError := newErrorChan(name, errC)
	wgrp := sync.WaitGroup{}
	wgrp.Add(len(steps))

	pipe.addRunner(func(ctx context.Context) {
		go func() {
			wgrp.Wait()
			close(errC)
			close(output)
		}()

		for _, step := range steps {
			go func(step *Step[I]) {
				defer wgrp.Done()

				runStepMerger(ctx, pipe, errC, step, outputStep)
			}(step)
		}
	})

	pipe.errcList.add(decoratedError)

	return outputStep
}
