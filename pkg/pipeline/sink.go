package pipeline

import (
	"context"
	"time"

	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"

	"github.com/askiada/go-pipeline/pkg/pipeline/model"
)

func prepareSink[I any](pipe *Pipeline, name string, input *model.Step[I], opts ...StepOption[I]) (*model.Step[I], error) {
	if pipe == nil {
		return nil, ErrPipelineMustBeSet
	}

	if input == nil {
		return nil, ErrInputMustBeSet
	}

	step := &model.Step[I]{
		Details: &model.StepInfo{
			Type:       model.SinkStepType,
			Name:       name,
			Concurrent: 1,
		},
	}

	for _, opt := range opts {
		opt(step)
	}

	for _, opt := range pipe.opts {
		err := opt.PrepareSink(input.Details, step.Details)
		if err != nil {
			return nil, errors.Wrap(err, "unable to run before step function")
		}
	}

	return step, nil
}

// AddSink adds a sink step to the pipeline. It will consume the input channel and run the sink function.
func AddSink[I any](
	pipe *Pipeline,
	name string,
	input *model.Step[I],
	sinkFn func(ctx context.Context, input I) error,
	opts ...StepOption[I],
) error {
	step, err := prepareSink(pipe, name, input, opts...)
	if err != nil {
		return errors.Wrap(err, "unable to perpare sink")
	}

	errC := make(chan error, 1)
	decoratedError := newErrorChan(name, errC)

	pipe.goFn = append(pipe.goFn, func(ctx context.Context) {
		defer func() {
			close(errC)
		}()

		if step.Details.Concurrent == 1 {
			err = sequentialSinkFn(ctx, 1, input, step, sinkFn, pipe.opts...)
			if err != nil {
				errC <- errors.Wrap(err, "unable to run sink function")
			}
		} else {
			err = concurrentSinkFn(ctx, input, step, sinkFn, pipe.opts...)
			if err != nil {
				errC <- errors.Wrap(err, "unable to run sink function")
			}
		}

		totalDuration := time.Since(pipe.startTime)

		for _, opt := range pipe.opts {
			err := opt.AfterSink(step.Details, totalDuration)
			if err != nil {
				errC <- errors.Wrap(err, "unable to run before step function")
			}
		}
	})

	pipe.errcList.add(decoratedError)

	return nil
}

func sequentialSinkFn[I any](
	ctx context.Context,
	goIdx int,
	input *model.Step[I],
	step *model.Step[I],
	sinkFn func(ctx context.Context, input I) error,
	opts ...model.PipelineOption,
) error {
	for {
		startInputChan := time.Now()
		select {
		case <-ctx.Done():
			return errors.Wrapf(ctx.Err(), "go routine %d", goIdx)
		case entry, ok := <-input.Output:
			if !ok {
				return nil
			}

			startFn := time.Now()

			err := sinkFn(ctx, entry)
			if err != nil {
				return errors.Wrapf(err, "go routine %d", goIdx)
			}

			endFn := time.Since(startFn)

			endInputChan := time.Since(startInputChan)
			for _, opt := range opts {
				err := opt.OnSinkOutput(input.Details, step.Details, endInputChan-endFn, endFn)
				if err != nil {
					return errors.Wrapf(err, "unable to run before step function (go routine %d)", goIdx)
				}
			}
		}
	}
}

func concurrentSinkFn[I any](
	ctx context.Context,
	input *model.Step[I],
	step *model.Step[I],
	sinkFn func(ctx context.Context, input I) error,
	opts ...model.PipelineOption,
) error {
	errGrp, dCtx := errgroup.WithContext(ctx)
	errGrp.SetLimit(step.Details.Concurrent)
	// starts many consumers concurrently
	// each consumer stops as soon as an error happens
	for goIdx := range step.Details.Concurrent {
		localGoIdx := goIdx

		errGrp.Go(func() error {
			return sequentialSinkFn(dCtx, localGoIdx, input, step, sinkFn, opts...)
		})
	}

	if err := errGrp.Wait(); err != nil {
		return errors.Wrap(err, "unable to wait for all go routines")
	}

	return nil
}

// AddSinkFromChan adds a sink step to the pipeline. It will consume the input channel.
func AddSinkFromChan[I any](
	pipe *Pipeline,
	name string,
	input *model.Step[I],
	stepFn func(ctx context.Context, input <-chan I) error,
) error {
	step, err := prepareSink(pipe, name, input)
	if err != nil {
		return errors.Wrap(err, "unable to perpare sink")
	}

	errC := make(chan error, 1)
	decoratedError := newErrorChan(name, errC)
	inputPlaceholder := make(chan I)
	total := 0
	start := time.Now()

	var end time.Duration

	pipe.goFn = append(pipe.goFn, func(ctx context.Context) {
		defer func() {
			close(inputPlaceholder)
		}()
	outer:
		for {
			select {
			case <-ctx.Done():
				break outer
			case entry, ok := <-input.Output:
				if !ok {
					break outer
				}
				select {
				case <-ctx.Done():
					break outer
				case inputPlaceholder <- entry:
					total++
				}
			}
		}

		end = time.Since(start)
	},
		func(ctx context.Context) {
			defer func() {
				close(errC)
			}()

			startStep := time.Now()

			err := stepFn(ctx, inputPlaceholder)
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
		})

	pipe.errcList.add(decoratedError)

	return nil
}
