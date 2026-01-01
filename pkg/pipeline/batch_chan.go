package pipeline

import (
	"context"
	"time"

	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"

	"github.com/askiada/go-pipeline/pkg/pipeline/model"
)

type batchChanState[I any] struct {
	goIdx      int
	input      *Step[I]
	output     *Step[<-chan I]
	opts       []model.PipelineOption
	maxSize    int
	maxWait    time.Duration
	batchCount int
	batchStart time.Time
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
	opts []model.PipelineOption,
) *batchChanState[I] {
	state := &batchChanState[I]{
		goIdx:   goIdx,
		input:   input,
		output:  output,
		opts:    opts,
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

	dropped, err := sendOutputWithPolicy(ctx, bs.goIdx, bs.output, bs.batchCh, bs.sendTimer, bs.opts...)
	if err != nil {
		close(bs.batchCh)
		bs.batchCh = nil
		bs.batchStart = time.Time{}
		bs.clearTimer()

		return dropped, err
	}

	if dropped {
		close(bs.batchCh)
		bs.batchCh = nil
		bs.batchStart = time.Time{}
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

	elapsed := time.Since(bs.batchStart)

	close(bs.batchCh)
	bs.batchCh = nil
	bs.batchCount = 0
	bs.batchStart = time.Time{}
	bs.clearTimer()

	for _, opt := range bs.opts {
		err := opt.OnStepOutput(bs.input.Details, bs.output.Details, elapsed, elapsed)
		if err != nil {
			return errors.Wrap(err, "unable to run before step function")
		}
	}

	return nil
}

func (bs *batchChanState[I]) handleEntry(ctx context.Context, entry I) error {
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

	select {
	case <-ctx.Done():
		return errors.Wrapf(ctx.Err(), "go routine %d", bs.goIdx)
	case bs.batchCh <- entry:
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
	opts ...model.PipelineOption,
) error {
	policy := output.BatchPolicy

	err := validateBatchPolicy(policy)
	if err != nil {
		return err
	}

	state := newBatchChanState(goIdx, input, output, policy, opts)

	for {
		select {
		case <-ctx.Done():
			_ = state.closeBatch()

			return errors.Wrapf(ctx.Err(), "go routine %d", goIdx)
		case <-state.timerC:
			err := state.closeBatch()
			if err != nil {
				return err
			}
		case entry, ok := <-input.Output:
			if !ok {
				return state.closeBatch()
			}

			err := state.handleEntry(ctx, entry)
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
	opts ...model.PipelineOption,
) error {
	errGrp, dCtx := errgroup.WithContext(ctx)
	errGrp.SetLimit(output.Details.Concurrent)

	for goIdx := range output.Details.Concurrent {
		localGoIdx := goIdx

		errGrp.Go(func() error {
			return sequentialBatchChanFn(dCtx, localGoIdx, input, output, opts...)
		})
	}

	err := errGrp.Wait()
	if err != nil {
		return errors.Wrap(err, "unable to wait for all go routines")
	}

	return nil
}

func runBatchChan[I any](
	ctx context.Context,
	input *Step[I],
	output *Step[<-chan I],
	opts ...model.PipelineOption,
) error {
	err := validateBatchPolicy(output.BatchPolicy)
	if err != nil {
		return err
	}

	if output.Details.Concurrent == 0 {
		output.Details.Concurrent = 1
	}

	if output.Details.Concurrent == 1 {
		return sequentialBatchChanFn(ctx, 1, input, output, opts...)
	}

	return concurrentBatchChanFn(ctx, input, output, opts...)
}

// BatchChan adds a step that groups incoming items into channels before emitting them downstream.
// The batch policy is required (MaxSize must be at least 1).
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
		pipe.recordErr(ErrInputMustBeSet)

		return nil
	}

	errC := make(chan error, 1)
	decoratedError := newErrorChan(name, errC)
	step := &Step[<-chan I]{
		Details: &model.StepInfo{
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
		pipe.recordErr(err)

		return nil
	}

	err = validateBatchPolicy(step.BatchPolicy)
	if err != nil {
		pipe.recordErr(err)

		return nil
	}

	err = prepareStep(pipe, input, step)
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

			err := runBatchChan(ctx, input, step, pipe.opts...)
			if err != nil {
				errC <- err
			}
		}()
	})

	pipe.errcList.add(decoratedError)

	return step
}
