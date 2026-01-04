package pipeline

import (
	"context"
	"fmt"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/askiada/go-pipeline/v2/pkg/pipeline/model"
)

func validateBatchPolicy(policy *model.BatchPolicy) error {
	if policy == nil || policy.MaxSize < 1 {
		return ErrBatchPolicyMustBeSet
	}

	return nil
}

func validateBatchOptions[O any](step *Step[O]) error {
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

	if step.DropOnError {
		return ErrDropOnErrorUnsupported
	}

	if step.ErrorOutputEnabled {
		return ErrErrorRouteUnsupported
	}

	return nil
}

func stopBatchTimer(timer *time.Timer) {
	if timer == nil {
		return
	}

	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
}

type batchState[I any] struct {
	goIdx      int
	input      *Step[I]
	output     *Step[[]I]
	cfg        hookConfig
	maxSize    int
	maxWait    time.Duration
	batch      []I
	batchStart time.Time
	waitTotal  time.Duration
	timer      *time.Timer
	timerC     <-chan time.Time
	sendTimer  *time.Timer
}

func newBatchState[I any](
	goIdx int,
	input *Step[I],
	output *Step[[]I],
	policy *model.BatchPolicy,
	cfg hookConfig,
) *batchState[I] {
	state := &batchState[I]{
		goIdx:   goIdx,
		input:   input,
		output:  output,
		cfg:     cfg,
		maxSize: policy.MaxSize,
		maxWait: policy.MaxWait,
	}

	if output.DropOnOutputTimeout > 0 {
		state.sendTimer = time.NewTimer(output.DropOnOutputTimeout)
		stopBatchTimer(state.sendTimer)
	}

	return state
}

func (bs *batchState[I]) resetTimer() {
	bs.timer, bs.timerC = resetBatchTimer(bs.timer, bs.maxWait)
}

func (bs *batchState[I]) clearTimer() {
	stopBatchTimer(bs.timer)
	bs.timer = nil
	bs.timerC = nil
}

func (bs *batchState[I]) flush(ctx context.Context) error {
	if len(bs.batch) == 0 {
		bs.clearTimer()

		return nil
	}

	batchToSend := bs.batch

	bs.batch = nil
	bs.batchStart = time.Time{}
	waitTotal := bs.waitTotal
	bs.waitTotal = 0
	bs.clearTimer()

	var sendDuration time.Duration
	measure := bs.cfg.outputMetrics
	var sendStart time.Time

	if measure {
		sendStart = time.Now()
	}

	dropped, err := sendOutputWithPolicy(ctx, bs.goIdx, bs.output, batchToSend, bs.sendTimer, bs.cfg.drop, bs.cfg.opts...)

	if measure {
		sendDuration = time.Since(sendStart)
	}

	if err != nil {
		return err
	}

	if dropped {
		return nil
	}

	return reportBatchOutput(bs.cfg, bs.input, bs.output, len(batchToSend), waitTotal, sendDuration)
}

func (bs *batchState[I]) handleEntry(ctx context.Context, entry I, inputWait time.Duration) error {
	if bs.maxWait > 0 && !bs.batchStart.IsZero() {
		if time.Since(bs.batchStart) >= bs.maxWait {
			err := bs.flush(ctx)
			if err != nil {
				return err
			}
		}
	}

	if len(bs.batch) == 0 {
		bs.batchStart = time.Now()
		bs.resetTimer()
	}

	if bs.cfg.outputMetrics {
		bs.waitTotal += inputWait
	}

	bs.batch = append(bs.batch, entry)
	if len(bs.batch) >= bs.maxSize {
		return bs.flush(ctx)
	}

	return nil
}

func sequentialBatchFn[I any](
	ctx context.Context,
	goIdx int,
	input *Step[I],
	output *Step[[]I],
	cfg hookConfig,
) error {
	policy := output.BatchPolicy

	err := validateBatchPolicy(policy)
	if err != nil {
		return err
	}

	state := newBatchState(goIdx, input, output, policy, cfg)

	for {
		var waitStart time.Time
		if cfg.outputMetrics {
			waitStart = time.Now()
		}

		select {
		case <-ctx.Done():
			return fmt.Errorf("go routine %d: %w", goIdx, ctx.Err())
		case <-state.timerC:
			err := state.flush(ctx)
			if err != nil {
				return err
			}
		case entry, ok := <-input.Output:
			if !ok {
				return state.flush(ctx)
			}

			var inputWait time.Duration
			if cfg.outputMetrics {
				inputWait = time.Since(waitStart)
			}

			err := state.handleEntry(ctx, entry, inputWait)
			if err != nil {
				return err
			}
		}
	}
}

func concurrentBatchFn[I any](
	ctx context.Context,
	input *Step[I],
	output *Step[[]I],
	cfg hookConfig,
) error {
	errGrp, dCtx := errgroup.WithContext(ctx)
	errGrp.SetLimit(output.Details.Concurrent)

	for goIdx := range output.Details.Concurrent {
		localGoIdx := goIdx

		errGrp.Go(func() error {
			return sequentialBatchFn(dCtx, localGoIdx, input, output, cfg)
		})
	}

	err := errGrp.Wait()
	if err != nil {
		return fmt.Errorf("unable to wait for all go routines: %w", err)
	}

	return nil
}

func runBatch[I any](
	ctx context.Context,
	input *Step[I],
	output *Step[[]I],
	cfg hookConfig,
) error {
	err := validateBatchPolicy(output.BatchPolicy)
	if err != nil {
		return err
	}

	if output.Details.Concurrent == 0 {
		output.Details.Concurrent = 1
	}

	if output.Details.Concurrent == 1 {
		return sequentialBatchFn(ctx, 1, input, output, cfg)
	}

	return concurrentBatchFn(ctx, input, output, cfg)
}

// Batch groups incoming items into slices before emitting them downstream.
// MaxSize must be at least 1. MaxWait <= 0 means no time flush.
func Batch[I any](
	pipe *Pipeline,
	name string,
	input *Step[I],
	policy BatchPolicy,
	opts ...StepOption[[]I],
) *Step[[]I] {
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
	step := &Step[[]I]{
		Details: &StepInfo{
			Type:       model.NormalStepType,
			Name:       name,
			Concurrent: 1,
		},
	}

	applyStepDefaults(pipe, step)

	policyCopy := policy
	if policyCopy.MaxWait < 0 {
		policyCopy.MaxWait = 0
	}

	step.BatchPolicy = &policyCopy

	for _, opt := range opts {
		opt(step)
	}

	err := validateBatchOptions(step)
	if err != nil {
		pipe.recordErr(name, err)

		return nil
	}

	err = validateBatchPolicy(step.BatchPolicy)
	if err != nil {
		pipe.recordErr(name, err)

		return nil
	}

	err = prepareStep(pipe, input, step)
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

			err := runBatch(ctx, input, step, pipe.hookConfig())
			if err != nil {
				errC <- err
			}
		}()
	})

	pipe.errcList.add(decoratedError)

	return step
}
