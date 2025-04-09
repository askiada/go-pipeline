package pipeline

import (
	"context"
	"sync"
	"time"

	"github.com/pkg/errors"

	"github.com/askiada/go-pipeline/pkg/pipeline/model"
)

func prepareMerger[I any](pipe *Pipeline, output chan I, name string, steps ...*model.Step[I]) (*model.Step[I], error) {
	outputStep := &model.Step[I]{
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

func runStepMerger[I any](ctx context.Context, pipe *Pipeline, errC chan error, step, outputStep *model.Step[I]) {
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

// AddMerger adds a merger step to the pipeline. It will merge the output of the steps into a single channel.
// The merger function will be run in a separate goroutine.
func AddMerger[I any](pipe *Pipeline, name string, steps ...*model.Step[I]) (*model.Step[I], error) {
	output := make(chan I)

	outputStep, err := prepareMerger(pipe, output, name, steps...)
	if err != nil {
		return nil, errors.Wrap(err, "unable to prepare merger")
	}

	errC := make(chan error, len(steps))
	decoratedError := newErrorChan(name, errC)
	wgrp := sync.WaitGroup{}
	wgrp.Add(len(steps))

	go func() {
		wgrp.Wait()
		close(errC)
		close(output)
	}()

	for _, step := range steps {
		pipe.goFn = append(pipe.goFn, func(ctx context.Context) {
			defer wgrp.Done()
			runStepMerger(ctx, pipe, errC, step, outputStep)
		})
	}

	pipe.errcList.add(decoratedError)

	return outputStep, nil
}
