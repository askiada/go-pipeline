package pipeline

import (
	"context"
	"time"

	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"

	"github.com/askiada/go-pipeline/pkg/pipeline/model"
)

func prepareSink[I any](pipe *Pipeline, name string, input *Step[I], opts ...StepOption[I]) (*Step[I], error) {
	if pipe == nil {
		return nil, ErrPipelineMustBeSet
	}

	if input == nil {
		return nil, ErrInputMustBeSet
	}

	step := &Step[I]{
		Details: &model.StepInfo{
			Type:       model.SinkStepType,
			Name:       name,
			Concurrent: 1,
		},
	}

	applyStepDefaults(pipe, step)

	for _, opt := range opts {
		opt(step)
	}

	for _, opt := range pipe.opts {
		err := opt.PrepareSink(input.Details, step.Details)
		if err != nil {
			return nil, errors.Wrap(err, "unable to run before step function")
		}
	}

	prepareStepErrorOutput(step)

	err := prepareErrorStep(pipe, step)
	if err != nil {
		return nil, err
	}

	return step, nil
}

func validateSinkFromChanOptions[I any](step *Step[I]) error {
	if step == nil {
		return nil
	}

	if step.RetryPolicy != nil {
		return ErrRetryUnsupported
	}

	if step.Timeout > 0 {
		return ErrTimeoutUnsupported
	}

	if step.RateLimitPolicy != nil {
		return ErrRateLimitUnsupported
	}

	if step.MaxInFlight > 0 {
		return ErrMaxInFlightUnsupported
	}

	if step.DropOnOutputFull || step.DropOnOutputTimeout > 0 {
		return ErrDropOutputUnsupported
	}

	if step.DropOnError {
		return ErrDropOnErrorUnsupported
	}

	if step.ErrorOutputEnabled {
		return ErrErrorRouteUnsupported
	}

	return nil
}

func sequentialSinkFn[I any](
	ctx context.Context,
	goIdx int,
	input *Step[I],
	step *Step[I],
	sinkFn func(ctx context.Context, input I) error,
	timeout time.Duration,
	limiter *rateLimiter,
	inFlight *inFlightLimiter,
	opts ...model.PipelineOption,
) error {
	for {
		start := time.Now()

		entry, ok, release, err := nextStepInput(ctx, goIdx, inFlight, input.Output)
		if err != nil {
			return err
		}

		if !ok {
			return nil
		}

		err = waitRateLimit(ctx, goIdx, limiter)
		if err != nil {
			release()

			return err
		}

		itemCtx, cancel := stepItemContext(ctx, timeout)
		_, endFn, err := executeWithRetry(itemCtx, step.RetryPolicy, func() (struct{}, error) {
			return struct{}{}, sinkFn(itemCtx, entry)
		}, func(attempt int, duration time.Duration) error {
			return reportStepRetry(opts, input.Details, step.Details, attempt, duration)
		})

		cancel()

		if err != nil {
			release()

			routeErr := routeStepError(ctx, step, entry, err, opts...)
			if routeErr != nil {
				return routeErr
			}

			if step.DropOnError {
				err = reportStepDrop(opts, step.Details, model.StepDropError)
				if err != nil {
					return err
				}

				continue
			}

			return errors.Wrapf(err, "go routine %d", goIdx)
		}

		end := time.Since(start)

		release()

		for _, opt := range opts {
			err := opt.OnSinkOutput(input.Details, step.Details, end-endFn, endFn)
			if err != nil {
				return errors.Wrap(err, "unable to run before step function")
			}
		}
	}
}

func concurrentSinkFn[I any](
	ctx context.Context,
	input *Step[I],
	step *Step[I],
	sinkFn func(ctx context.Context, input I) error,
	timeout time.Duration,
	limiter *rateLimiter,
	inFlight *inFlightLimiter,
	opts ...model.PipelineOption,
) error {
	errGrp, dCtx := errgroup.WithContext(ctx)
	errGrp.SetLimit(step.Details.Concurrent)

	for goIdx := range step.Details.Concurrent {
		errGrp.Go(func() error {
			return sequentialSinkFn(dCtx, goIdx, input, step, sinkFn, timeout, limiter, inFlight, opts...)
		})
	}

	err := errGrp.Wait()
	if err != nil {
		return errors.Wrap(err, "unable to wait for all go routines")
	}

	return nil
}

func runSink[I any](
	ctx context.Context,
	input *Step[I],
	step *Step[I],
	sinkFn func(ctx context.Context, input I) error,
	opts ...model.PipelineOption,
) error {
	if step.DropOnOutputFull || step.DropOnOutputTimeout > 0 {
		return ErrDropOutputUnsupported
	}

	if step.Details.Concurrent == 0 {
		step.Details.Concurrent = 1
	}

	limiter := newRateLimiter(step.RateLimitPolicy)
	inFlight := newInFlightLimiter(step.MaxInFlight)
	timeout := step.Timeout

	if step.Details.Concurrent == 1 {
		return sequentialSinkFn(ctx, 1, input, step, sinkFn, timeout, limiter, inFlight, opts...)
	}

	return concurrentSinkFn(ctx, input, step, sinkFn, timeout, limiter, inFlight, opts...)
}

// Sink adds a sink step to the pipeline. It will consume the input channel and run the sink function.
func Sink[I any](
	pipe *Pipeline,
	name string,
	input *Step[I],
	sinkFn func(ctx context.Context, input I) error,
	opts ...StepOption[I],
) *Step[I] {
	if pipe == nil {
		return nil
	}

	if pipe.buildErr != nil {
		return nil
	}

	step, err := prepareSink(pipe, name, input, opts...)
	if err != nil {
		pipe.recordErr(errors.Wrap(err, "unable to prepare sink"))

		return nil
	}

	errC := make(chan error, 1)
	decoratedError := newErrorChan(name, errC)

	pipe.addRunner(func(ctx context.Context) {
		go func() {
			defer func() {
				close(errC)

				if step.ErrorOutput != nil {
					close(step.ErrorOutput)
				}
			}()

			err := runSink(ctx, input, step, sinkFn, pipe.opts...)
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
	})

	pipe.errcList.add(decoratedError)

	return step
}

// SinkFromChan adds a sink step to the pipeline. It will consume the input channel.
func SinkFromChan[I any](
	pipe *Pipeline,
	name string,
	input *Step[I],
	stepFn func(ctx context.Context, input <-chan I) error,
	opts ...StepOption[I],
) *Step[I] {
	if pipe == nil {
		return nil
	}

	if pipe.buildErr != nil {
		return nil
	}

	step, err := prepareSink(pipe, name, input, opts...)
	if err != nil {
		pipe.recordErr(errors.Wrap(err, "unable to prepare sink"))

		return nil
	}

	err = validateSinkFromChanOptions(step)
	if err != nil {
		pipe.recordErr(err)

		return nil
	}

	errC := make(chan error, 1)
	decoratedError := newErrorChan(name, errC)

	pipe.addRunner(func(ctx context.Context) {
		go func() {
			defer func() {
				close(errC)

				if step.ErrorOutput != nil {
					close(step.ErrorOutput)
				}
			}()

			err := runSinkFromChan(ctx, input, step, stepFn, pipe.opts...)
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
	})

	pipe.errcList.add(decoratedError)

	return step
}

func sequentialSinkFromChanFn[I any](
	ctx context.Context,
	goIdx int,
	input *Step[I],
	step *Step[I],
	stepFn func(ctx context.Context, input <-chan I) error,
	conc int,
	opts ...model.PipelineOption,
) error {
	inputPlaceholder := make(chan I)
	total := float64(0)
	start := time.Now()

	var end time.Duration

	done := make(chan struct{}, 1)

	go func() {
		defer func() {
			close(inputPlaceholder)

			end = time.Since(start)

			done <- struct{}{}
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
	}()

	startStep := time.Now()

	err := stepFn(ctx, inputPlaceholder)
	if err != nil {
		return errors.Wrap(err, "unable to run sink function")
	}

	endStep := time.Since(startStep)

	if total == 0 {
		return nil
	}

	total = float64(conc) / total

	<-done

	for _, opt := range opts {
		err := opt.OnSinkOutput(
			input.Details,
			step.Details,
			time.Duration(float64(end)/float64(total)),
			time.Duration(float64(endStep)/float64(total)),
		)
		if err != nil {
			return errors.Wrapf(err, "go routine %d: unable to run after step function", goIdx)
		}
	}

	return nil
}

func concurrentSinkFromChanFn[I any](
	ctx context.Context,
	input *Step[I],
	step *Step[I],
	stepFn func(ctx context.Context, input <-chan I) error,
	opts ...model.PipelineOption,
) error {
	errGrp, dCtx := errgroup.WithContext(ctx)
	errGrp.SetLimit(step.Details.Concurrent)

	for goIdx := range step.Details.Concurrent {
		localGoIdx := goIdx

		errGrp.Go(func() error {
			return sequentialSinkFromChanFn(dCtx, localGoIdx, input, step, stepFn, step.Details.Concurrent, opts...)
		})
	}

	err := errGrp.Wait()
	if err != nil {
		return errors.Wrap(err, "unable to wait for all go routines")
	}

	return nil
}

func runSinkFromChan[I any](
	ctx context.Context,
	input *Step[I],
	step *Step[I],
	stepFn func(ctx context.Context, input <-chan I) error,
	opts ...model.PipelineOption,
) error {
	err := validateSinkFromChanOptions(step)
	if err != nil {
		return err
	}

	if step.Details.Concurrent == 0 {
		step.Details.Concurrent = 1
	}

	if step.Details.Concurrent == 1 {
		return sequentialSinkFromChanFn(ctx, 1, input, step, stepFn, 1, opts...)
	}

	return concurrentSinkFromChanFn(ctx, input, step, stepFn, opts...)
}
