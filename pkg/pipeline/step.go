package pipeline

import (
	"context"
	"reflect"
	"time"

	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"

	"github.com/askiada/go-pipeline/pkg/pipeline/model"
)

// OneToOneFn is a function that takes an input and produces an output.
type OneToOneFn[I, O any] func(context.Context, I) (O, error)

// OneToManyFn is a function that takes an input and produces many outputs.
type OneToManyFn[I, O any] func(context.Context, I) ([]O, error)

// StepFromChanFn is a function that takes an input channel and produces an output channel.
type StepFromChanFn[I, O any] func(ctx context.Context, input <-chan I, output chan O) error

type stepToStepFn[I, O any] func(ctx context.Context, input *Step[I], output *Step[O]) error

//nolint:ireturn,gocritic // Generic helper returns entry values; unnamed results keep call sites concise.
func acquireStepInput[I any](
	ctx context.Context,
	goIdx int,
	inFlight *inFlightLimiter,
	input <-chan I,
) (I, bool, func(), error) {
	var entry I

	err := inFlight.acquire(ctx)
	if err != nil {
		return entry, false, nil, errors.Wrapf(err, "go routine %d", goIdx)
	}

	released := false
	release := func() {
		if released {
			return
		}

		inFlight.release()

		released = true
	}

	select {
	case <-ctx.Done():
		release()

		return entry, false, nil, errors.Wrapf(ctx.Err(), "go routine %d", goIdx)
	case entry, ok := <-input:
		if !ok {
			release()

			return entry, false, nil, nil
		}

		return entry, true, release, nil
	}
}

func noopRelease() {}

//nolint:ireturn,gocritic // Keep the call sites compact for hot paths.
func nextStepInput[I any](ctx context.Context, goIdx int, inFlight *inFlightLimiter, input <-chan I) (I, bool, func(), error) {
	if inFlight != nil {
		return acquireStepInput(ctx, goIdx, inFlight, input)
	}

	var entry I

	select {
	case <-ctx.Done():
		return entry, false, nil, errors.Wrapf(ctx.Err(), "go routine %d", goIdx)
	case entry, ok := <-input:
		if !ok {
			return entry, false, nil, nil
		}

		return entry, true, noopRelease, nil
	}
}

func waitRateLimit(ctx context.Context, goIdx int, limiter *rateLimiter) error {
	err := limiter.wait(ctx)
	if err != nil {
		return errors.Wrapf(err, "go routine %d", goIdx)
	}

	return nil
}

//nolint:gocritic // Unnamed returns keep the timeout helper compact at call sites.
func stepItemContext(ctx context.Context, timeout time.Duration) (context.Context, func()) {
	if timeout <= 0 {
		return ctx, func() {}
	}

	return context.WithTimeout(ctx, timeout)
}

func sendStepOutput[I, O any](
	ctx context.Context,
	goIdx int,
	input *Step[I],
	output *Step[O],
	value O,
	start time.Time,
	fnDuration time.Duration,
	release func(),
	timer *time.Timer,
	opts ...model.PipelineOption,
) (bool, error) {
	if release != nil {
		release()
	}

	dropped, err := sendOutputWithPolicy(ctx, goIdx, output, value, timer, opts...)
	if err != nil {
		return dropped, err
	}

	if dropped {
		return true, nil
	}

	for _, opt := range opts {
		err := opt.OnStepOutput(input.Details, output.Details, time.Since(start)-fnDuration, fnDuration)
		if err != nil {
			return false, errors.Wrap(err, "unable to run before step function")
		}
	}

	return false, nil
}

func sendOneToManyOutputs[I, O any](
	ctx context.Context,
	goIdx int,
	input *Step[I],
	output *Step[O],
	values []O,
	start time.Time,
	fnDuration time.Duration,
	release func(),
	timer *time.Timer,
	opts ...model.PipelineOption,
) (bool, error) {
	if release != nil {
		release()
	}

	dropped := false
	sent := false

	for _, value := range values {
		itemDropped, err := sendOutputWithPolicy(ctx, goIdx, output, value, timer, opts...)
		if err != nil {
			return dropped, err
		}

		if itemDropped {
			dropped = true

			continue
		}

		sent = true
	}

	if !sent {
		return dropped, nil
	}

	end := time.Since(start)
	for _, opt := range opts {
		err := opt.OnStepOutput(input.Details, output.Details, end-fnDuration, fnDuration)
		if err != nil {
			return false, errors.Wrap(err, "unable to run before step function")
		}
	}

	return false, nil
}

func sequentialOneToOneFn[I any, O any](
	ctx context.Context,
	goIdx int,
	input *Step[I],
	output *Step[O],
	oneToOne OneToOneFn[I, O],
	ignoreZero bool,
	timeout time.Duration,
	limiter *rateLimiter,
	inFlight *inFlightLimiter,
	opts ...model.PipelineOption,
) error {
	var sendTimer *time.Timer
	if output.DropOnOutputTimeout > 0 {
		sendTimer = time.NewTimer(output.DropOnOutputTimeout)
		stopBatchTimer(sendTimer)
	}
	defer stopBatchTimer(sendTimer)

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
		outcome, endFn, err := executeWithRetry(itemCtx, output.RetryPolicy, func() (O, error) {
			return oneToOne(itemCtx, entry)
		}, func(attempt int, duration time.Duration) error {
			return reportStepRetry(opts, input.Details, output.Details, attempt, duration)
		})

		cancel()

		if err != nil {
			release()

			routeErr := routeStepError(ctx, output, entry, err, opts...)
			if routeErr != nil {
				return routeErr
			}

			if output.DropOnError {
				err = reportStepDrop(opts, output.Details, model.StepDropError)
				if err != nil {
					return err
				}

				continue
			}

			return errors.Wrapf(err, "go routine %d", goIdx)
		}

		out := outcome.value
		if ignoreZero && reflect.ValueOf(out).IsZero() {
			release()

			continue
		}

		_, err = sendStepOutput(ctx, goIdx, input, output, out, start, endFn, release, sendTimer, opts...)
		if err != nil {
			return err
		}
	}
}

func concurrentOneToOneFn[I any, O any](
	ctx context.Context,
	input *Step[I],
	output *Step[O],
	oneToOne OneToOneFn[I, O],
	ignoreZero bool,
	timeout time.Duration,
	limiter *rateLimiter,
	inFlight *inFlightLimiter,
	opts ...model.PipelineOption,
) error {
	errGrp, dCtx := errgroup.WithContext(ctx)
	errGrp.SetLimit(output.Details.Concurrent)
	// starts many consumers concurrently
	// each consumer stops as soon as an error happens
	for goIdx := range output.Details.Concurrent {
		localGoIdx := goIdx

		errGrp.Go(func() error {
			return sequentialOneToOneFn(dCtx, localGoIdx, input, output, oneToOne, ignoreZero, timeout, limiter, inFlight, opts...)
		})
	}

	err := errGrp.Wait()
	if err != nil {
		return errors.Wrap(err, "unable to wait for all go routines")
	}

	return nil
}

func runOneToOne[I any, O any](
	ctx context.Context,
	input *Step[I],
	output *Step[O],
	oneToOne OneToOneFn[I, O],
	ignoreZero bool,
	opts ...model.PipelineOption,
) error {
	if output.Details.Concurrent == 0 {
		output.Details.Concurrent = 1
	}

	limiter := newRateLimiter(output.RateLimitPolicy)
	inFlight := newInFlightLimiter(output.MaxInFlight)
	timeout := output.Timeout

	if output.Details.Concurrent == 1 {
		return sequentialOneToOneFn(ctx, 1, input, output, oneToOne, ignoreZero, timeout, limiter, inFlight, opts...)
	}

	return concurrentOneToOneFn(ctx, input, output, oneToOne, ignoreZero, timeout, limiter, inFlight, opts...)
}

func sequentialOneToManyFn[I any, O any](
	ctx context.Context,
	goIdx int,
	input *Step[I],
	output *Step[O],
	oneToMany OneToManyFn[I, O],
	timeout time.Duration,
	limiter *rateLimiter,
	inFlight *inFlightLimiter,
	opts ...model.PipelineOption,
) error {
	var sendTimer *time.Timer
	if output.DropOnOutputTimeout > 0 {
		sendTimer = time.NewTimer(output.DropOnOutputTimeout)
		stopBatchTimer(sendTimer)
	}
	defer stopBatchTimer(sendTimer)

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
		outcome, endFn, err := executeWithRetry(itemCtx, output.RetryPolicy, func() ([]O, error) {
			return oneToMany(itemCtx, entry)
		}, func(attempt int, duration time.Duration) error {
			return reportStepRetry(opts, input.Details, output.Details, attempt, duration)
		})

		cancel()

		if err != nil {
			release()

			routeErr := routeStepError(ctx, output, entry, err, opts...)
			if routeErr != nil {
				return routeErr
			}

			if output.DropOnError {
				err = reportStepDrop(opts, output.Details, model.StepDropError)
				if err != nil {
					return err
				}

				continue
			}

			return errors.Wrapf(err, "go routine %d", goIdx)
		}

		_, err = sendOneToManyOutputs(ctx, goIdx, input, output, outcome.value, start, endFn, release, sendTimer, opts...)
		if err != nil {
			return err
		}
	}
}

func concurrentOneToManyFn[I any, O any](
	ctx context.Context,
	input *Step[I],
	output *Step[O],
	oneToMany OneToManyFn[I, O],
	timeout time.Duration,
	limiter *rateLimiter,
	inFlight *inFlightLimiter,
	opts ...model.PipelineOption,
) error {
	errGrp, dCtx := errgroup.WithContext(ctx)
	errGrp.SetLimit(output.Details.Concurrent)

	// starts many consumers concurrently
	for goIdx := range output.Details.Concurrent {
		localGoIdx := goIdx

		errGrp.Go(func() error {
			return sequentialOneToManyFn(dCtx, localGoIdx, input, output, oneToMany, timeout, limiter, inFlight, opts...)
		})
	}

	err := errGrp.Wait()
	if err != nil {
		return errors.Wrap(err, "unable to wait for all go routines")
	}

	return nil
}

func runOneToMany[I any, O any](
	ctx context.Context,
	input *Step[I],
	output *Step[O],
	oneToMany func(context.Context, I) ([]O, error),
	opts ...model.PipelineOption,
) error {
	if output.Details.Concurrent == 0 {
		output.Details.Concurrent = 1
	}

	limiter := newRateLimiter(output.RateLimitPolicy)
	inFlight := newInFlightLimiter(output.MaxInFlight)
	timeout := output.Timeout

	if output.Details.Concurrent == 1 {
		return sequentialOneToManyFn(ctx, 1, input, output, oneToMany, timeout, limiter, inFlight, opts...)
	}

	return concurrentOneToManyFn(ctx, input, output, oneToMany, timeout, limiter, inFlight, opts...)
}

func prepareStep[I, O any](pipe *Pipeline, input *Step[I], step *Step[O]) error {
	for _, opt := range pipe.opts {
		err := opt.PrepareStep(input.Details, step.Details)
		if err != nil {
			return errors.Wrap(err, "unable to run before step function")
		}
	}

	step.Output = make(chan O, step.Details.BufferSize)
	prepareStepErrorOutput(step)

	return prepareErrorStep(pipe, step)
}

func addStep[I any, O any](
	pipe *Pipeline,
	name string,
	input *Step[I],
	stepToStep stepToStepFn[I, O],
	opts ...StepOption[O],
) *Step[O] {
	if pipe == nil {
		return nil
	}

	if pipe.buildErr != nil {
		return nil
	}

	if input == nil {
		pipe.recordErr(ErrInputMustBeSet)

		return nil
	}

	errC := make(chan error, 1)
	decoratedError := newErrorChan(name, errC)
	step := &Step[O]{
		Details: &model.StepInfo{
			Type:       model.NormalStepType,
			Name:       name,
			Concurrent: 1,
		},
	}

	applyStepDefaults(pipe, step)

	for _, opt := range opts {
		opt(step)
	}

	err := prepareStep(pipe, input, step)
	if err != nil {
		pipe.recordErr(err)

		return nil
	}

	pipe.addRunner(func(ctx context.Context) {
		go func() {
			defer func() {
				close(errC)

				if !step.KeepOpen {
					close(step.Output)
				}

				if step.ErrorOutput != nil {
					close(step.ErrorOutput)
				}
			}()

			err := stepToStep(ctx, input, step)
			if err != nil {
				errC <- err
			}
		}()
	})

	pipe.errcList.add(decoratedError)

	return step
}

func runStepFromChan[I, O any](
	ctx context.Context,
	input *Step[I],
	output *Step[O],
	stepFn StepFromChanFn[I, O],
	opts ...model.PipelineOption,
) error {
	err := validateFromChanOptions(output)
	if err != nil {
		return err
	}

	if output.Details.Concurrent == 0 {
		output.Details.Concurrent = 1
	}

	if output.Details.Concurrent == 1 {
		return sequentialStepFromChanFn(ctx, 1, input, output, stepFn, 1, opts...)
	}

	return concurrentStepFromChanFn(ctx, input, output, stepFn, opts...)
}

func validateFromChanOptions[O any](step *Step[O]) error {
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

func sequentialStepFromChanFn[I any, O any](
	ctx context.Context,
	goIdx int,
	input *Step[I],
	output *Step[O],
	stepFn StepFromChanFn[I, O],
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

	err := stepFn(ctx, inputPlaceholder, output.Output)
	if err != nil {
		return errors.Wrap(err, "unable to run step function")
	}

	endStep := time.Since(startStep)

	if total == 0 {
		return nil
	}

	total = float64(conc) / total

	<-done

	for _, opt := range opts {
		err := opt.OnStepOutput(
			input.Details,
			output.Details,
			time.Duration(float64(end)/float64(total)),
			time.Duration(float64(endStep)/float64(total)),
		)
		if err != nil {
			return errors.Wrapf(err, "go routine %d: unable to run after step function", goIdx)
		}
	}

	return nil
}

func concurrentStepFromChanFn[I any, O any](
	ctx context.Context,
	input *Step[I],
	output *Step[O],
	stepFn StepFromChanFn[I, O],
	opts ...model.PipelineOption,
) error {
	errGrp, dCtx := errgroup.WithContext(ctx)
	errGrp.SetLimit(output.Details.Concurrent)
	// starts many consumers concurrently
	// each consumer stops as soon as an error happens
	for goIdx := range output.Details.Concurrent {
		localGoIdx := goIdx

		errGrp.Go(func() error {
			return sequentialStepFromChanFn(dCtx, localGoIdx, input, output, stepFn, output.Details.Concurrent, opts...)
		})
	}

	err := errGrp.Wait()
	if err != nil {
		return errors.Wrap(err, "unable to wait for all go routines")
	}

	return nil
}

// OneToOne adds a step that takes one input and produces one output.
func OneToOne[I any, O any](
	pipe *Pipeline,
	name string,
	input *Step[I],
	oneToOne OneToOneFn[I, O],
	opts ...StepOption[O],
) *Step[O] {
	return addStep(pipe, name, input, func(ctx context.Context, in *Step[I], out *Step[O]) error {
		return runOneToOne(ctx, in, out, oneToOne, false, pipe.opts...)
	}, opts...)
}

// OneToOneOrZero adds a step that takes one input and produces one output. If the output is a zero value, it is ignored.
func OneToOneOrZero[I any, O any](
	pipe *Pipeline,
	name string,
	input *Step[I],
	oneToOne OneToOneFn[I, O],
	opts ...StepOption[O],
) *Step[O] {
	return addStep(pipe, name, input, func(ctx context.Context, in *Step[I], out *Step[O]) error {
		return runOneToOne(ctx, in, out, oneToOne, true, pipe.opts...)
	}, opts...)
}

// OneToMany adds a step that takes one input and produces many outputs.
func OneToMany[I any, O any](
	pipe *Pipeline,
	name string,
	input *Step[I],
	oneToMany OneToManyFn[I, O],
	opts ...StepOption[O],
) *Step[O] {
	return addStep(pipe, name, input, func(ctx context.Context, in *Step[I], out *Step[O]) error {
		return runOneToMany(ctx, in, out, oneToMany, pipe.opts...)
	}, opts...)
}

// FromChan adds a step that takes an input channel and produces an output channel.
func FromChan[I any, O any](
	pipe *Pipeline,
	name string,
	input *Step[I],
	stepFromChan StepFromChanFn[I, O],
	opts ...StepOption[O],
) *Step[O] {
	step := addStep(pipe, name, input, func(ctx context.Context, in *Step[I], out *Step[O]) error {
		return runStepFromChan(ctx, in, out, stepFromChan, pipe.opts...)
	}, opts...)

	if step == nil {
		return nil
	}

	err := validateFromChanOptions(step)
	if err != nil {
		pipe.recordErr(err)

		return nil
	}

	return step
}
