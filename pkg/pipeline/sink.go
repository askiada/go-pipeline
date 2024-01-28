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
	inputPlaceholder := make(chan I)
	total := 0
	start := time.Now()
	var end time.Duration
	go func() {
		defer func() {
			close(inputPlaceholder)
		}()
	outer:
		for {
			select {
			case <-pipe.ctx.Done():
				break outer
			case entry, ok := <-input.Output:
				if !ok {
					break outer
				}
				select {
				case <-pipe.ctx.Done():
					break outer
				case inputPlaceholder <- entry:
					total++
				}
			}
		}
		end = time.Since(start)
	}()
	go func() {
		defer func() {
			close(errC)
		}()
		startStep := time.Now()
		err := stepFn(pipe.ctx, inputPlaceholder)
		if err != nil {
			errC <- err
		}
		endStep := time.Since(startStep)
		iterationDuration := time.Duration(float64(end) / float64(total))
		computaionDuration := time.Duration(float64(endStep) / float64(total))

		for _, opt := range pipe.opts {
			err := opt.OnSinkOutput(input.Details, step.Details, iterationDuration, computaionDuration)
			if err != nil {
				errC <- errors.Wrap(err, "unable to run before step function")
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
