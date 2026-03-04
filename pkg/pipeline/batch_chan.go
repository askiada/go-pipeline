package pipeline

import (
	"context"
	"fmt"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/askiada/go-pipeline/v2/pkg/pipeline/model"
)

type batchChanState[I any] struct {
	goIdx     int
	input     *Step[I]
	output    *Step[<-chan I]
	cfg       hookConfig
	tracker   *batchTracker
	sendTotal time.Duration
	batchSend time.Duration
	batchCh   chan I
	sendTimer *time.Timer
}

func newBatchChanState[I any](
	goIdx int,
	input *Step[I],
	output *Step[<-chan I],
	policy *model.BatchPolicy,
	cfg hookConfig,
) *batchChanState[I] {
	state := &batchChanState[I]{
		goIdx:   goIdx,
		input:   input,
		output:  output,
		cfg:     cfg,
		tracker: newBatchTracker(policy.MaxSize, policy.MaxWait),
	}

	if output.DropOnOutputTimeout > 0 {
		state.sendTimer = time.NewTimer(output.DropOnOutputTimeout)
		stopBatchTimer(state.sendTimer)
	}

	return state
}

func (bs *batchChanState[I]) startBatch(ctx context.Context) (bool, error) {
	if bs.batchCh != nil {
		return false, nil
	}

	bs.batchCh = make(chan I)
	bs.tracker.startBatch()

	var dropped bool
	var err error

	if bs.cfg.outputMetrics {
		sendStart := time.Now()
		dropped, err = sendOutputWithPolicy(ctx, bs.goIdx, bs.output, bs.batchCh, bs.sendTimer, bs.cfg.drop, bs.cfg.opts...)
		bs.batchSend = time.Since(sendStart)
	} else {
		dropped, err = sendOutputWithPolicy(ctx, bs.goIdx, bs.output, bs.batchCh, bs.sendTimer, bs.cfg.drop, bs.cfg.opts...)
	}

	if err != nil {
		close(bs.batchCh)
		bs.batchCh = nil
		bs.batchSend = 0
		bs.tracker.reset()

		return dropped, err
	}

	if dropped {
		close(bs.batchCh)
		bs.batchCh = nil
		bs.batchSend = 0
		bs.tracker.reset()

		return true, nil
	}

	return false, nil
}

func (bs *batchChanState[I]) closeBatch() error {
	if bs.batchCh == nil {
		bs.tracker.reset()

		return nil
	}

	close(bs.batchCh)
	bs.batchCh = nil
	batchCount, waitTotal := bs.tracker.snapshotAndReset()
	sendTotal := bs.sendTotal + bs.batchSend
	bs.sendTotal = 0
	bs.batchSend = 0

	return reportBatchOutput(bs.cfg, bs.input, bs.output, batchCount, waitTotal, sendTotal)
}

func (bs *batchChanState[I]) handleEntry(ctx context.Context, entry I, inputWait time.Duration) error {
	if bs.tracker.shouldFlushForTime() {
		err := bs.closeBatch()
		if err != nil {
			return err
		}
	}

	dropped, err := bs.startBatch(ctx)
	if err != nil {
		return err
	}

	if dropped {
		return nil
	}

	if bs.cfg.outputMetrics {
		bs.tracker.recordWait(inputWait)
	}

	var sendStart time.Time
	if bs.cfg.outputMetrics {
		sendStart = time.Now()
	}

	select {
	case <-ctx.Done():
		return fmt.Errorf("go routine %d: %w", bs.goIdx, ctx.Err())
	case bs.batchCh <- entry:
	}

	if bs.cfg.outputMetrics {
		bs.sendTotal += time.Since(sendStart)
	}

	bs.tracker.recordItem()

	if bs.tracker.shouldFlushForSize() {
		return bs.closeBatch()
	}

	return nil
}

func sequentialBatchChanFn[I any](
	ctx context.Context,
	goIdx int,
	input *Step[I],
	output *Step[<-chan I],
	cfg hookConfig,
) error {
	policy := output.BatchPolicy

	err := validateBatchPolicy(policy)
	if err != nil {
		return err
	}

	state := newBatchChanState(goIdx, input, output, policy, cfg)

	for {
		var waitStart time.Time
		if cfg.outputMetrics {
			waitStart = time.Now()
		}

		select {
		case <-ctx.Done():
			_ = state.closeBatch()

			return fmt.Errorf("go routine %d: %w", goIdx, ctx.Err())
		case <-state.tracker.timerC:
			err := state.closeBatch()
			if err != nil {
				return err
			}
		case entry, ok := <-input.Output:
			if !ok {
				return state.closeBatch()
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

func concurrentBatchChanFn[I any](
	ctx context.Context,
	input *Step[I],
	output *Step[<-chan I],
	cfg hookConfig,
) error {
	errGrp, dCtx := errgroup.WithContext(ctx)
	errGrp.SetLimit(output.Details.Concurrent)

	for goIdx := range output.Details.Concurrent {
		localGoIdx := goIdx

		errGrp.Go(func() error {
			return sequentialBatchChanFn(dCtx, localGoIdx, input, output, cfg)
		})
	}

	err := errGrp.Wait()
	if err != nil {
		return fmt.Errorf("unable to wait for all go routines: %w", err)
	}

	return nil
}

func runBatchChan[I any](
	ctx context.Context,
	input *Step[I],
	output *Step[<-chan I],
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
		return sequentialBatchChanFn(ctx, 1, input, output, cfg)
	}

	return concurrentBatchChanFn(ctx, input, output, cfg)
}

// BatchChan groups incoming items into channels before emitting them downstream.
// MaxSize must be at least 1. MaxWait <= 0 means no time flush.
func BatchChan[I any](
	pipe *Pipeline,
	name string,
	input *Step[I],
	policy BatchPolicy,
	opts ...StepOption[<-chan I],
) *Step[<-chan I] {
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
	step := &Step[<-chan I]{
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

			err := runBatchChan(ctx, input, step, pipe.hookConfig())
			if err != nil {
				errC <- err
			}
		}()
	})

	pipe.errcList.add(decoratedError)

	return step
}
