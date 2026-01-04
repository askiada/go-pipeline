package pipeline

import (
	"context"
	"fmt"
	"reflect"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/askiada/go-pipeline/v2/pkg/pipeline/model"
)

// OneToOneFn transforms one input item into one output item.
// Returning an error stops the run unless retries or drops are enabled.
type OneToOneFn[I, O any] func(context.Context, I) (O, error)

// OneToManyFn transforms one input item into zero or more output items.
type OneToManyFn[I, O any] func(context.Context, I) ([]O, error)

// StepFromChanFn processes items from input and writes to output.
// The output channel is owned by the pipeline and should not be closed.
type StepFromChanFn[I, O any] func(ctx context.Context, input <-chan I, output chan O) error

type stepToStepFn[I, O any] func(ctx context.Context, input *Step[I], output *Step[O]) error

//nolint:gocritic,ireturn // Generic helper returns entry values; unnamed results keep call sites concise.
func acquireStepInput[I any](
	ctx context.Context,
	goIdx int,
	inFlight *inFlightLimiter,
	input <-chan I,
	measure bool,
) (I, bool, func(), time.Duration, error) {
	var entry I

	err := inFlight.acquire(ctx)
	if err != nil {
		return entry, false, nil, 0, fmt.Errorf("go routine %d: %w", goIdx, err)
	}

	released := false
	release := func() {
		if released {
			return
		}

		inFlight.release()

		released = true
	}

	entry, ok, wait, err := receiveStepInput(ctx, goIdx, input, measure)
	if err != nil {
		release()

		return entry, false, nil, wait, err
	}

	if !ok {
		release()

		return entry, false, nil, wait, nil
	}

	return entry, true, release, wait, nil
}

func noopRelease() {}

//nolint:gocritic,ireturn // Keep the call sites compact for hot paths.
func nextStepInput[I any](
	ctx context.Context,
	goIdx int,
	inFlight *inFlightLimiter,
	input <-chan I,
	measure bool,
) (I, bool, func(), time.Duration, error) {
	if inFlight != nil {
		return acquireStepInput(ctx, goIdx, inFlight, input, measure)
	}

	entry, ok, wait, err := receiveStepInput(ctx, goIdx, input, measure)
	if err != nil {
		return entry, false, nil, wait, err
	}

	if !ok {
		return entry, false, nil, wait, nil
	}

	return entry, true, noopRelease, wait, nil
}

//nolint:gocritic,ireturn // Keep return values explicit for performance-sensitive call sites.
func receiveStepInput[I any](
	ctx context.Context,
	goIdx int,
	input <-chan I,
	measure bool,
) (I, bool, time.Duration, error) {
	var entry I
	var ok bool

	if !measure {
		select {
		case <-ctx.Done():
			return entry, false, 0, fmt.Errorf("go routine %d: %w", goIdx, ctx.Err())
		case entry, ok = <-input:
			if !ok {
				return entry, false, 0, nil
			}

			return entry, true, 0, nil
		}
	}

	start := time.Now()

	select {
	case <-ctx.Done():
		return entry, false, time.Since(start), fmt.Errorf("go routine %d: %w", goIdx, ctx.Err())
	case entry, ok = <-input:
		wait := time.Since(start)
		if !ok {
			return entry, false, wait, nil
		}

		return entry, true, wait, nil
	}
}

func waitRateLimit(ctx context.Context, goIdx int, limiter *rateLimiter) error {
	err := limiter.wait(ctx)
	if err != nil {
		return fmt.Errorf("go routine %d: %w", goIdx, err)
	}

	return nil
}

type stepProcessor[I any] func(ctx context.Context, entry I, inputWait time.Duration, release func()) error

func runSequentialStepLoop[I any](
	ctx context.Context,
	goIdx int,
	input *Step[I],
	limiter *rateLimiter,
	inFlight *inFlightLimiter,
	measure bool,
	process stepProcessor[I],
) error {
	for {
		entry, ok, release, inputWait, err := nextStepInput(ctx, goIdx, inFlight, input.Output, measure)
		if err != nil {
			return err
		}

		if !ok {
			return nil
		}

		err = waitRateLimit(ctx, goIdx, limiter)
		if err != nil {
			if release != nil {
				release()
			}

			return err
		}

		err = process(ctx, entry, inputWait, release)
		if release != nil {
			release()
		}

		if err != nil {
			return err
		}
	}
}

func handleStepError[I any, O any](
	ctx context.Context,
	goIdx int,
	step *Step[O],
	entry I,
	err error,
	cfg hookConfig,
) (bool, error) {
	routeErr := routeStepError(ctx, step, entry, err, cfg.errorRoute, cfg.opts...)
	if routeErr != nil {
		return false, routeErr
	}

	if step.DropOnError {
		if cfg.drop {
			dropErr := reportStepDrop(cfg.opts, step.Details, model.StepDropError)
			if dropErr != nil {
				return false, dropErr
			}
		}

		return true, nil
	}

	return false, fmt.Errorf("go routine %d: %w", goIdx, err)
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
	transportDuration time.Duration,
	fnDuration time.Duration,
	release func(),
	timer *time.Timer,
	cfg hookConfig,
) (bool, error) {
	if release != nil {
		release()
	}

	dropped, err := sendOutputWithPolicy(ctx, goIdx, output, value, timer, cfg.drop, cfg.opts...)
	if err != nil {
		return dropped, err
	}

	if dropped {
		return true, nil
	}

	for _, opt := range cfg.opts {
		err := opt.OnStepOutput(input.Details, output.Details)
		if err != nil {
			return false, fmt.Errorf("unable to run before step function: %w", err)
		}
	}

	if cfg.outputMetrics {
		for _, opt := range cfg.metricsOpts {
			err := opt.OnStepOutputMetrics(input.Details, output.Details, transportDuration, fnDuration)
			if err != nil {
				return false, fmt.Errorf("unable to run before step function: %w", err)
			}
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
	transportDuration time.Duration,
	fnDuration time.Duration,
	release func(),
	timer *time.Timer,
	cfg hookConfig,
) (bool, error) {
	if release != nil {
		release()
	}

	dropped := false
	sent := false

	for _, value := range values {
		itemDropped, err := sendOutputWithPolicy(ctx, goIdx, output, value, timer, cfg.drop, cfg.opts...)
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

	for _, opt := range cfg.opts {
		err := opt.OnStepOutput(input.Details, output.Details)
		if err != nil {
			return false, fmt.Errorf("unable to run before step function: %w", err)
		}
	}

	if cfg.outputMetrics {
		for _, opt := range cfg.metricsOpts {
			err := opt.OnStepOutputMetrics(input.Details, output.Details, transportDuration, fnDuration)
			if err != nil {
				return false, fmt.Errorf("unable to run before step function: %w", err)
			}
		}
	}

	return false, nil
}

type retryFn func(attempt int, duration time.Duration) error

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
	cfg hookConfig,
) error {
	var sendTimer *time.Timer
	if output.DropOnOutputTimeout > 0 {
		sendTimer = time.NewTimer(output.DropOnOutputTimeout)
		stopBatchTimer(sendTimer)
	}
	defer stopBatchTimer(sendTimer)

	var reportRetry retryFn

	if cfg.retry {
		reportRetry = func(attempt int, duration time.Duration) error {
			return reportStepRetry(cfg.opts, input.Details, output.Details, attempt, duration)
		}
	}

	return runSequentialStepLoop(ctx, goIdx, input, limiter, inFlight, cfg.outputMetrics, func(
		ctx context.Context,
		entry I,
		inputWait time.Duration,
		release func(),
	) error {
		itemCtx, cancel := stepItemContext(ctx, timeout)

		outcome, endFn, err := executeWithRetry(itemCtx, output.RetryPolicy, func() (O, error) {
			return oneToOne(itemCtx, entry)
		}, reportRetry, cfg.timing)

		cancel() // cancel timeout context. Noop if no timeout set.

		if err != nil {
			if release != nil {
				release()
			}

			dropped, handleErr := handleStepError(ctx, goIdx, output, entry, err, cfg)
			if handleErr != nil {
				return handleErr
			}

			if dropped {
				return nil
			}
		}

		if ignoreZero && reflect.ValueOf(outcome).IsZero() {
			if release != nil {
				release()
			}

			return nil
		}

		_, err = sendStepOutput(ctx, goIdx, input, output, outcome, inputWait, endFn, release, sendTimer, cfg)
		if err != nil {
			return err
		}

		return nil
	})
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
	cfg hookConfig,
) error {
	errGrp, dCtx := errgroup.WithContext(ctx)
	errGrp.SetLimit(output.Details.Concurrent)
	// starts many consumers concurrently
	// each consumer stops as soon as an error happens
	for goIdx := range output.Details.Concurrent {
		errGrp.Go(func() error {
			return sequentialOneToOneFn(dCtx, goIdx, input, output, oneToOne, ignoreZero, timeout, limiter, inFlight, cfg)
		})
	}

	err := errGrp.Wait()
	if err != nil {
		return fmt.Errorf("unable to wait for all go routines: %w", err)
	}

	return nil
}

func runOneToOne[I any, O any](
	ctx context.Context,
	input *Step[I],
	output *Step[O],
	oneToOne OneToOneFn[I, O],
	ignoreZero bool,
	cfg hookConfig,
) error {
	if output.Details.Concurrent == 0 {
		output.Details.Concurrent = 1
	}

	// setup rate limiter and in flight limiter
	// they will be nil if not configured
	limiter := newRateLimiter(output.RateLimitPolicy)
	inFlight := newInFlightLimiter(output.MaxInFlight)
	// timeout for each item processed
	timeout := output.Timeout

	if output.Details.Concurrent == 1 {
		return sequentialOneToOneFn(ctx, 1, input, output, oneToOne, ignoreZero, timeout, limiter, inFlight, cfg)
	}

	return concurrentOneToOneFn(ctx, input, output, oneToOne, ignoreZero, timeout, limiter, inFlight, cfg)
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
	cfg hookConfig,
) error {
	var sendTimer *time.Timer
	if output.DropOnOutputTimeout > 0 {
		sendTimer = time.NewTimer(output.DropOnOutputTimeout)
		stopBatchTimer(sendTimer)
	}
	defer stopBatchTimer(sendTimer)

	var reportRetry retryFn

	if cfg.retry {
		reportRetry = func(attempt int, duration time.Duration) error {
			return reportStepRetry(cfg.opts, input.Details, output.Details, attempt, duration)
		}
	}

	return runSequentialStepLoop(ctx, goIdx, input, limiter, inFlight, cfg.outputMetrics, func(
		ctx context.Context,
		entry I,
		inputWait time.Duration,
		release func(),
	) error {
		itemCtx, cancel := stepItemContext(ctx, timeout)

		outcome, endFn, err := executeWithRetry(itemCtx, output.RetryPolicy, func() ([]O, error) {
			return oneToMany(itemCtx, entry)
		}, reportRetry, cfg.timing)

		cancel()

		if err != nil {
			if release != nil {
				release()
			}

			dropped, handleErr := handleStepError(ctx, goIdx, output, entry, err, cfg)
			if handleErr != nil {
				return handleErr
			}

			if dropped {
				return nil
			}
		}

		_, err = sendOneToManyOutputs(ctx, goIdx, input, output, outcome, inputWait, endFn, release, sendTimer, cfg)
		if err != nil {
			return err
		}

		return nil
	})
}

func concurrentOneToManyFn[I any, O any](
	ctx context.Context,
	input *Step[I],
	output *Step[O],
	oneToMany OneToManyFn[I, O],
	timeout time.Duration,
	limiter *rateLimiter,
	inFlight *inFlightLimiter,
	cfg hookConfig,
) error {
	errGrp, dCtx := errgroup.WithContext(ctx)
	errGrp.SetLimit(output.Details.Concurrent)

	// starts many consumers concurrently
	for goIdx := range output.Details.Concurrent {
		localGoIdx := goIdx

		errGrp.Go(func() error {
			return sequentialOneToManyFn(dCtx, localGoIdx, input, output, oneToMany, timeout, limiter, inFlight, cfg)
		})
	}

	err := errGrp.Wait()
	if err != nil {
		return fmt.Errorf("unable to wait for all go routines: %w", err)
	}

	return nil
}

func runOneToMany[I any, O any](
	ctx context.Context,
	input *Step[I],
	output *Step[O],
	oneToMany func(context.Context, I) ([]O, error),
	cfg hookConfig,
) error {
	if output.Details.Concurrent == 0 {
		output.Details.Concurrent = 1
	}

	limiter := newRateLimiter(output.RateLimitPolicy)
	inFlight := newInFlightLimiter(output.MaxInFlight)
	timeout := output.Timeout

	if output.Details.Concurrent == 1 {
		return sequentialOneToManyFn(ctx, 1, input, output, oneToMany, timeout, limiter, inFlight, cfg)
	}

	return concurrentOneToManyFn(ctx, input, output, oneToMany, timeout, limiter, inFlight, cfg)
}

func prepareStep[I, O any](pipe *Pipeline, input *Step[I], step *Step[O]) error {
	for _, opt := range pipe.opts {
		err := opt.PrepareStep(input.Details, step.Details)
		if err != nil {
			return fmt.Errorf("unable to run before step function: %w", err)
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
		pipe.recordErr(name, ErrInputMustBeSet)

		return nil
	}

	errC := make(chan error, 1)
	decoratedError := newErrorChan(name, errC)
	step := &Step[O]{
		Details: &StepInfo{
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
		pipe.recordErr(name, err)

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
	cfg hookConfig,
) error {
	err := validateFromChanOptions(output)
	if err != nil {
		return err
	}

	if output.Details.Concurrent == 0 {
		output.Details.Concurrent = 1
	}

	if output.Details.Concurrent == 1 {
		return sequentialStepFromChanFn(ctx, 1, input, output, stepFn, cfg)
	}

	return concurrentStepFromChanFn(ctx, input, output, stepFn, cfg)
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

//nolint:gocognit,cyclop,gocyclo // channel plumbing and accounting make this verbose.
func sequentialStepFromChanFn[I any, O any](
	ctx context.Context,
	goIdx int,
	input *Step[I],
	output *Step[O],
	stepFn StepFromChanFn[I, O],
	cfg hookConfig,
) error {
	inputPlaceholder := make(chan I)
	total := float64(0)
	var waitTotal time.Duration

	done := make(chan struct{}, 1)

	go func() {
		defer func() {
			close(inputPlaceholder)

			done <- struct{}{}
		}()

	outer:
		for {
			var waitStart time.Time
			if cfg.outputMetrics {
				waitStart = time.Now()
			}

			select {
			case <-ctx.Done():
				break outer
			case entry, ok := <-input.Output:
				if !ok {
					break outer
				}

				if cfg.outputMetrics {
					waitTotal += time.Since(waitStart)
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

	var startStep time.Time
	if cfg.outputMetrics {
		startStep = time.Now()
	}

	err := stepFn(ctx, inputPlaceholder, output.Output)
	if err != nil {
		return fmt.Errorf("unable to run step function: %w", err)
	}

	var endStep time.Duration
	if cfg.outputMetrics {
		endStep = time.Since(startStep)
	}

	if total == 0 {
		return nil
	}

	<-done

	for _, opt := range cfg.opts {
		err := opt.OnStepOutput(input.Details, output.Details)
		if err != nil {
			return fmt.Errorf("go routine %d: unable to run after step function: %w", goIdx, err)
		}
	}

	if cfg.outputMetrics {
		iterDuration := time.Duration(float64(waitTotal) / total)
		compDuration := time.Duration(float64(endStep) / total)

		for _, opt := range cfg.metricsOpts {
			err := opt.OnStepOutputMetrics(input.Details, output.Details, iterDuration, compDuration)
			if err != nil {
				return fmt.Errorf("go routine %d: unable to run after step function: %w", goIdx, err)
			}
		}
	}

	return nil
}

func concurrentStepFromChanFn[I any, O any](
	ctx context.Context,
	input *Step[I],
	output *Step[O],
	stepFn StepFromChanFn[I, O],
	cfg hookConfig,
) error {
	errGrp, dCtx := errgroup.WithContext(ctx)
	errGrp.SetLimit(output.Details.Concurrent)
	// starts many consumers concurrently
	// each consumer stops as soon as an error happens
	for goIdx := range output.Details.Concurrent {
		localGoIdx := goIdx

		errGrp.Go(func() error {
			return sequentialStepFromChanFn(dCtx, localGoIdx, input, output, stepFn, cfg)
		})
	}

	err := errGrp.Wait()
	if err != nil {
		return fmt.Errorf("unable to wait for all go routines: %w", err)
	}

	return nil
}

// OneToOne adds a step that turns each input into one output.
func OneToOne[I any, O any](
	pipe *Pipeline,
	name string,
	input *Step[I],
	oneToOne OneToOneFn[I, O],
	opts ...StepOption[O],
) *Step[O] {
	return addStep(pipe, name, input, func(ctx context.Context, in *Step[I], out *Step[O]) error {
		return runOneToOne(ctx, in, out, oneToOne, false, pipe.hookConfig())
	}, opts...)
}

// OneToOneOrZero adds a step that turns each input into one output.
// If the output is the zero value, it is skipped.
func OneToOneOrZero[I any, O any](
	pipe *Pipeline,
	name string,
	input *Step[I],
	oneToOne OneToOneFn[I, O],
	opts ...StepOption[O],
) *Step[O] {
	return addStep(pipe, name, input, func(ctx context.Context, in *Step[I], out *Step[O]) error {
		return runOneToOne(ctx, in, out, oneToOne, true, pipe.hookConfig())
	}, opts...)
}

// OneToMany adds a step that turns each input into many outputs.
func OneToMany[I any, O any](
	pipe *Pipeline,
	name string,
	input *Step[I],
	oneToMany OneToManyFn[I, O],
	opts ...StepOption[O],
) *Step[O] {
	return addStep(pipe, name, input, func(ctx context.Context, in *Step[I], out *Step[O]) error {
		return runOneToMany(ctx, in, out, oneToMany, pipe.hookConfig())
	}, opts...)
}

// FromChan adds a step that owns its own input loop.
// Use it when you already have a channel-based worker.
func FromChan[I any, O any](
	pipe *Pipeline,
	name string,
	input *Step[I],
	stepFromChan StepFromChanFn[I, O],
	opts ...StepOption[O],
) *Step[O] {
	step := addStep(pipe, name, input, func(ctx context.Context, in *Step[I], out *Step[O]) error {
		return runStepFromChan(ctx, in, out, stepFromChan, pipe.hookConfig())
	}, opts...)

	if step == nil {
		return nil
	}

	err := validateFromChanOptions(step)
	if err != nil {
		pipe.recordErr(name, err)

		return nil
	}

	return step
}
