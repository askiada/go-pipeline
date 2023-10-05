package pipeline

import (
	"context"
	"time"

	"github.com/pkg/errors"
)

func AddSink[I any](pipe *Pipeline, name string, input *Step[I], sinkFn func(ctx context.Context, input I) error) error {
	if pipe == nil {
		return ErrPipelineMustBeSet
	}
	if input == nil {
		return ErrInputMustBeSet
	}
	step := &Step[I]{
		details: &StepInfo{
			Type:       sinkStepType,
			Name:       name,
			Concurrent: 1,
		},
	}
	for _, opt := range pipe.opts {
		err := opt.BeforeSink(input.details, step.details)
		if err != nil {
			return errors.Wrap(err, "unable to run before step function")
		}
	}

	errC := make(chan error, 1)
	decoratedError := newErrorChan(name, errC)
	go func() {
		defer func() {
			close(errC)
		}()
	outer:
		for {
			startInputChan := time.Now()
			select {
			case <-pipe.ctx.Done():
				errC <- pipe.ctx.Err()

				break outer
			case in, ok := <-input.Output:
				if !ok {
					break outer
				}
				endInputChan := time.Since(startInputChan)

				startFn := time.Now()
				err := sinkFn(pipe.ctx, in)
				if err != nil {
					errC <- err
				}
				endFn := time.Since(startFn)
				if step.details.Metric != nil {
					step.details.Metric.AddDuration(endFn)
					step.details.Metric.AddTransportDuration(input.details.Name, endInputChan+endFn)
				}
			}
		}
		if step.details.Metric != nil {
			step.details.Metric.SetTotalDuration(time.Since(pipe.startTime))
		}
	}()
	pipe.errcList.add(decoratedError)

	return nil
}

func AddSinkFromChan[I any](pipe *Pipeline, name string, input *Step[I], stepFn func(ctx context.Context, input <-chan I) error) error {
	if pipe == nil {
		return ErrPipelineMustBeSet
	}
	if input == nil {
		return ErrInputMustBeSet
	}
	step := &Step[I]{
		details: &StepInfo{
			Type:       sinkStepType,
			Name:       name,
			Concurrent: 1,
		},
	}
	for _, opt := range pipe.opts {
		err := opt.BeforeSink(input.details, step.details)
		if err != nil {
			return errors.Wrap(err, "unable to run before step function")
		}
	}

	errC := make(chan error, 1)
	decoratedError := newErrorChan(name, errC)
	go func() {
		defer func() {
			close(errC)
		}()
		err := stepFn(pipe.ctx, input.Output)
		if err != nil {
			errC <- err
		}
		if step.details.Metric != nil {
			step.details.Metric.SetTotalDuration(time.Since(pipe.startTime))
		}
	}()
	pipe.errcList.add(decoratedError)

	return nil
}
