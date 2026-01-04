package pipeline

import (
	"context"
	"fmt"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/askiada/go-pipeline/v2/pkg/pipeline/model"
)

type batchChanState[I any] struct {
	goIdx      int
	input      *Step[I]
	output     *Step[<-chan I]
	cfg        hookConfig
	maxSize    int
	maxWait    time.Duration
	batchCount int
	batchStart time.Time
	waitTotal  time.Duration
	sendTotal  time.Duration
	batchSend  time.Duration
	timer      *time.Timer
	timerC     <-chan time.Time
	batchCh    chan I
	sendTimer  *time.Timer
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
		maxSize: policy.MaxSize,
		maxWait: policy.MaxWait,
	}

	if output.DropOnOutputTimeout > 0 {
		state.sendTimer = time.NewTimer(output.DropOnOutputTimeout)
		stopBatchTimer(state.sendTimer)
	}

	return state
}

func (bs *batchChanState[I]) resetTimer() {
	if bs.maxWait <= 0 {
		return
	}

	if bs.timer == nil {
		bs.timer = time.NewTimer(bs.maxWait)
		bs.timerC = bs.timer.C

		return
	}

	stopBatchTimer(bs.timer)
	bs.timer.Reset(bs.maxWait)
	bs.timerC = bs.timer.C
}

func (bs *batchChanState[I]) clearTimer() {
	stopBatchTimer(bs.timer)
	bs.timer = nil
	bs.timerC = nil
}

func (bs *batchChanState[I]) startBatch(ctx context.Context) (bool, error) {
	if bs.batchCh != nil {
		return false, nil
	}

	bs.batchCh = make(chan I)
	bs.batchStart = time.Now()
	bs.resetTimer()

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
		bs.batchStart = time.Time{}
		bs.batchSend = 0
		bs.clearTimer()

		return dropped, err
	}

	if dropped {
		close(bs.batchCh)
		bs.batchCh = nil
		bs.batchStart = time.Time{}
		bs.batchSend = 0
		bs.clearTimer()

		return true, nil
	}

	return false, nil
}

func (bs *batchChanState[I]) closeBatch() error {
	if bs.batchCh == nil {
		bs.clearTimer()

		return nil
	}

	close(bs.batchCh)
	bs.batchCh = nil
	batchCount := bs.batchCount
	bs.batchCount = 0
	bs.batchStart = time.Time{}
	waitTotal := bs.waitTotal
	sendTotal := bs.sendTotal + bs.batchSend
	bs.waitTotal = 0
	bs.sendTotal = 0
	bs.batchSend = 0
	bs.clearTimer()

	for _, opt := range bs.cfg.opts {
		err := opt.OnStepOutput(bs.input.Details, bs.output.Details)
		if err != nil {
			return fmt.Errorf("unable to run before step function: %w", err)
		}
	}

	if bs.cfg.outputMetrics {
		avgWait := time.Duration(0)
		avgCompute := time.Duration(0)

		if batchCount > 0 {
			avgWait = waitTotal / time.Duration(batchCount)
			avgCompute = sendTotal / time.Duration(batchCount)
		}

		for _, opt := range bs.cfg.metricsOpts {
			err := opt.OnStepOutputMetrics(bs.input.Details, bs.output.Details, avgWait, avgCompute)
			if err != nil {
				return fmt.Errorf("unable to run before step function: %w", err)
			}
		}
	}

	return nil
}

func (bs *batchChanState[I]) handleEntry(ctx context.Context, entry I, inputWait time.Duration) error {
	if bs.maxWait > 0 && !bs.batchStart.IsZero() {
		if time.Since(bs.batchStart) >= bs.maxWait {
			err := bs.closeBatch()
			if err != nil {
				return err
			}
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
		bs.waitTotal += inputWait
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

	bs.batchCount++
	if bs.batchCount >= bs.maxSize {
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
		case <-state.timerC:
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
