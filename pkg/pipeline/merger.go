package pipeline

import (
	"sync"
	"time"

	"github.com/pkg/errors"
)

func AddMerger[I any](pipe *Pipeline, name string, steps ...*Step[I]) (*Step[I], error) {
	errC := make(chan error, len(steps))
	decoratedError := newErrorChan(name, errC)
	output := make(chan I)
	outputStep := &Step[I]{
		details: &StepInfo{
			Type:       mergeStepType,
			Name:       name,
			Concurrent: 1,
		},
		Output: output,
	}

	stepInfos := make([]*StepInfo, len(steps), len(steps))
	for i, step := range steps {
		stepInfos[i] = step.details
	}

	for _, opt := range pipe.opts {
		err := opt.BeforeMerger(stepInfos, outputStep.details)
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
		go func(step *Step[I]) {
			defer wgrp.Done()
		outer:
			for {
				startChan := time.Now()
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
						if outputStep.details.Metric != nil {
							outputStep.details.Metric.AddTransportDuration(step.details.Name, time.Since(startChan))
						}
					}
				}
			}
		}(step)
	}

	pipe.errcList.add(decoratedError)

	return outputStep, nil
}
