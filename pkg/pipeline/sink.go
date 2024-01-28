package pipeline

import (
	"context"
	"time"

	"github.com/askiada/go-pipeline/pkg/pipeline/model"
	"github.com/pkg/errors"
)

func AddSink[I any](pipe *Pipeline, name string, input *model.Step[I], sinkFn func(ctx context.Context, input I) error) error {
	if pipe == nil {
		return ErrPipelineMustBeSet
	}
	if input == nil {
		return ErrInputMustBeSet
	}
	step := &model.Step[I]{
		Details: &model.StepInfo{
			Type:       model.SinkStepType,
			Name:       name,
			Concurrent: 1,
		},
	}
	for _, opt := range pipe.opts {
		err := opt.PrepareSink(input.Details, step.Details)
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
			case entry, ok := <-input.Output:
				if !ok {
					break outer
				}

				startFn := time.Now()
				err := sinkFn(pipe.ctx, entry)
				if err != nil {
					errC <- err
				}
				endFn := time.Since(startFn)

				endInputChan := time.Since(startInputChan)
				for _, opt := range pipe.opts {
					err := opt.OnSinkOutput(input.Details, step.Details, endInputChan-endFn, endFn)
					if err != nil {
						errC <- errors.Wrap(err, "unable to run before step function")
					}
				}
			}
		}
		totalDuration := time.Since(pipe.startTime)
		for _, opt := range pipe.opts {
			err := opt.AfterSink(step.Details, totalDuration)
			if err != nil {
				errC <- errors.Wrap(err, "unable to run before step function")
			}
		}
	}()
	pipe.errcList.add(decoratedError)

	return nil
}

func AddSinkFromChan[I any](pipe *Pipeline, name string, input *model.Step[I], stepFn func(ctx context.Context, input <-chan I) error) error {
	if pipe == nil {
		return ErrPipelineMustBeSet
	}
	if input == nil {
		return ErrInputMustBeSet
	}
	step := &model.Step[I]{
		Details: &model.StepInfo{
			Type:       model.SinkStepType,
			Name:       name,
			Concurrent: 1,
		},
	}
	for _, opt := range pipe.opts {
		err := opt.PrepareSink(input.Details, step.Details)
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
		totalDuration := time.Since(pipe.startTime)
		for _, opt := range pipe.opts {
			err := opt.AfterSink(step.Details, totalDuration)
			if err != nil {
				errC <- errors.Wrap(err, "unable to run before step function")
			}
		}
	}()
	pipe.errcList.add(decoratedError)

	return nil
}
