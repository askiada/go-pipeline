package pipeline

import (
	"context"
	"fmt"

	"github.com/askiada/go-pipeline/v2/pkg/pipeline/model"
)

func prepareRootStep[O any](pipe *Pipeline, step *Step[O]) error {
	for _, opt := range pipe.opts {
		err := opt.PrepareStep(model.StartStep.Details, step.Details)
		if err != nil {
			return fmt.Errorf("unable to run before step function: %w", err)
		}
	}

	step.Output = make(chan O, step.Details.BufferSize)

	return nil
}

// Root adds a root step to the pipeline. It will run the step function.
func Root[O any](
	pipe *Pipeline,
	name string,
	stepFn func(ctx context.Context, rootChan chan<- O) error,
	opts ...StepOption[O],
) *Step[O] {
	if pipe == nil {
		return nil
	}

	if pipe.buildErr != nil {
		return nil
	}

	errC := make(chan error, 1)
	decoratedError := newErrorChan(name, errC)

	step := &Step[O]{
		Details: &StepInfo{
			Type:       model.RootStepType,
			Name:       name,
			Concurrent: 1,
		},
	}

	applyStepDefaults(pipe, step)

	for _, opt := range opts {
		opt(step)
	}

	if step.RetryPolicy != nil {
		pipe.recordErr(name, ErrRetryUnsupported)

		return nil
	}

	if step.Timeout > 0 {
		pipe.recordErr(name, ErrTimeoutUnsupported)

		return nil
	}

	if step.RateLimitPolicy != nil {
		pipe.recordErr(name, ErrRateLimitUnsupported)

		return nil
	}

	if step.MaxInFlight > 0 {
		pipe.recordErr(name, ErrMaxInFlightUnsupported)

		return nil
	}

	if step.DropOnOutputFull || step.DropOnOutputTimeout > 0 {
		pipe.recordErr(name, ErrDropOutputUnsupported)

		return nil
	}

	if step.DropOnError {
		pipe.recordErr(name, ErrDropOnErrorUnsupported)

		return nil
	}

	if step.ErrorOutputEnabled {
		pipe.recordErr(name, ErrErrorRouteUnsupported)

		return nil
	}

	err := prepareRootStep(pipe, step)
	if err != nil {
		pipe.recordErr(name, err)

		return nil
	}

	pipe.addRunner(func(ctx context.Context) {
		go func() {
			defer func() {
				if !step.KeepOpen {
					close(step.Output)
				}

				close(errC)
			}()

			err := stepFn(ctx, step.Output)
			if err != nil {
				errC <- err
			}
		}()
	})

	pipe.errcList.add(decoratedError)

	return step
}
