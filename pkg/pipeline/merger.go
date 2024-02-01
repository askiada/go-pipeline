package pipeline

import (
	"sync"
	"time"

	"github.com/askiada/go-pipeline/pkg/pipeline/model"
	"github.com/pkg/errors"
)

func AddMerger[I any](pipe *Pipeline, name string, steps ...*model.Step[I]) (*model.Step[I], error) {
	errC := make(chan error, len(steps))
	decoratedError := newErrorChan(name, errC)
	output := make(chan I)
	outputStep := &model.Step[I]{
		Details: &model.StepInfo{
			Type:       model.MergeStepType,
			Name:       name,
			Concurrent: 1,
		},
		Output: output,
	}

	stepInfos := make([]*model.StepInfo, len(steps), len(steps))
	for i, step := range steps {
		stepInfos[i] = step.Details
	}

	for _, opt := range pipe.opts {
		err := opt.PrepareMerger(stepInfos, outputStep.Details)
		if err != nil {
			return nil, errors.Wrap(err, "unable to run before merger function")
		}
	}

	wgrp := sync.WaitGroup{}
	wgrp.Add(len(steps))
	go func() {
		wgrp.Wait()
		close(errC)
		close(output)
	}()

	for _, step := range steps {
		go func(step *model.Step[I]) {
			defer wgrp.Done()
		outer:
			for {
				startIter := time.Now()
				select {
				case <-pipe.ctx.Done():
					errC <- pipe.ctx.Err()

					break outer
				case entry, ok := <-step.Output:
					if !ok {
						break outer
					}
					select {
					case <-pipe.ctx.Done():
						return
					case output <- entry:
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
		}(step)
	}

	pipe.errcList.add(decoratedError)

	return outputStep, nil
}
