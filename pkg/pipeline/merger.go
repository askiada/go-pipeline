package pipeline

import (
	"context"
	"sync"
	"time"

	"github.com/pkg/errors"

	"github.com/askiada/go-pipeline/pkg/pipeline/model"
)

func prepareMerger[I any](pipe *Pipeline, output chan I, name string, steps ...*Step[I]) (*Step[I], error) {
	outputStep := &Step[I]{
		Details: &model.StepInfo{
			Type:       model.MergerStepType,
			Name:       name,
			Concurrent: 1,
		},
		Output: output,
	}

	stepInfos := make([]*model.StepInfo, len(steps))
	for i, step := range steps {
		stepInfos[i] = step.Details
	}

	for _, opt := range pipe.opts {
		err := opt.PrepareMerger(stepInfos, outputStep.Details)
		if err != nil {
			return nil, errors.Wrap(err, "unable to run before merger function")
		}
	}

	return outputStep, nil
}

func runStepMerger[I any](ctx context.Context, pipe *Pipeline, errC chan error, step, outputStep *Step[I]) {
	for {
		startIter := time.Now()

		select {
		case <-ctx.Done():
			errC <- ctx.Err()

			return
		case entry, ok := <-step.Output:
			if !ok {
				return
			}

			select {
			case <-ctx.Done():
				errC <- ctx.Err()
			case outputStep.Output <- entry:
				endIter := time.Since(startIter)
				for _, opt := range pipe.opts {
					err := opt.OnMergerOutput(step.Details, outputStep.Details, endIter)
					if err != nil {
						errC <- errors.Wrap(err, "unable to run before merger function")
					}
				}
			}
		}
	}
}

// Merge adds a merger step to the pipeline. It will merge the output of the steps into a single channel.
func Merge[I any](pipe *Pipeline, name string, steps ...*Step[I]) *Step[I] {
	if pipe == nil {
		return nil
	}

	if pipe.buildErr != nil {
		return nil
	}

	if len(steps) == 0 {
		pipe.recordErr(ErrInputMustBeSet)

		return nil
	}

	for _, step := range steps {
		if step == nil {
			pipe.recordErr(ErrInputMustBeSet)

			return nil
		}
	}

	output := make(chan I)

	outputStep, err := prepareMerger(pipe, output, name, steps...)
	if err != nil {
		pipe.recordErr(errors.Wrap(err, "unable to prepare merger"))

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
