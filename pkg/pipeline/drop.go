package pipeline

import (
	"context"
	"time"

	"github.com/pkg/errors"

	"github.com/askiada/go-pipeline/v2/pkg/pipeline/model"
)

func prepareStepErrorOutput[O any](step *Step[O]) {
	if step == nil || !step.ErrorOutputEnabled || step.ErrorOutput != nil {
		return
	}

	step.ErrorOutput = make(chan model.StepError, step.ErrorOutputBufferSize)
}

func prepareErrorStep[O any](pipe *Pipeline, step *Step[O]) error {
	if pipe == nil || step == nil || step.ErrorStep == nil {
		return nil
	}

	if step.Details == nil || step.ErrorStep.Details == nil {
		return nil
	}

	for _, opt := range pipe.opts {
		err := opt.PrepareStep(step.Details, step.ErrorStep.Details)
		if err != nil {
			return errors.Wrap(err, "unable to prepare error step")
		}
	}

	return nil
}

func reportStepDrop(opts []model.PipelineOption, step *StepInfo, kind model.StepDropKind) error {
	if step == nil {
		return nil
	}

	for _, opt := range opts {
		observer, ok := opt.(model.StepDropObserver)
		if !ok {
			continue
		}

		err := observer.OnStepDrop(step, kind)
		if err != nil {
			return errors.Wrap(err, "unable to report step drop")
		}
	}

	return nil
}

func reportStepErrorRoute(opts []model.PipelineOption, step *StepInfo) error {
	if step == nil {
		return nil
	}

	for _, opt := range opts {
		observer, ok := opt.(model.StepErrorRouteObserver)
		if !ok {
			continue
		}

		err := observer.OnStepErrorRoute(step)
		if err != nil {
			return errors.Wrap(err, "unable to report step error route")
		}
	}

	return nil
}

func routeStepError[O any](
	ctx context.Context,
	step *Step[O],
	item any,
	err error,
	errorRouteEnabled bool,
	opts ...model.PipelineOption,
) error {
	if step == nil || step.ErrorOutput == nil {
		return nil
	}

	if ctx == nil {
		return ErrContextMustBeSet
	}

	stepName := ""
	if step.Details != nil {
		stepName = step.Details.Name
	}

	if err == nil {
		return nil
	}

	payload := model.StepError{
		StepName: stepName,
		Item:     item,
		Err:      err,
	}

	sendErr := sendStepError(ctx, step.ErrorOutput, payload)
	if sendErr != nil {
		return sendErr
	}

	if !errorRouteEnabled {
		return nil
	}

	reportErr := reportStepErrorRoute(opts, step.Details)
	if reportErr == nil {
		return nil
	}

	payload.Err = reportErr

	sendErr = sendStepError(ctx, step.ErrorOutput, payload)
	if sendErr != nil {
		return errors.Wrap(sendErr, "unable to route step error route failure")
	}

	return reportErr
}

func sendStepError(ctx context.Context, out chan<- model.StepError, payload model.StepError) error {
	select {
	case <-ctx.Done():
		return errors.Wrap(ctx.Err(), "unable to route step error")
	case out <- payload:
		return nil
	}
}

func sendOutputWithPolicy[O any](
	ctx context.Context,
	goIdx int,
	step *Step[O],
	value O,
	timer *time.Timer,
	dropEnabled bool,
	opts ...model.PipelineOption,
) (bool, error) {
	if step == nil {
		return false, nil
	}

	if step.DropOnOutputFull {
		select {
		case <-ctx.Done():
			return false, errors.Wrapf(ctx.Err(), "go routine %d", goIdx)
		case step.Output <- value:
			return false, nil
		default:
			if dropEnabled {
				return true, reportStepDrop(opts, step.Details, model.StepDropBufferFull)
			}

			return true, nil
		}
	}

	if step.DropOnOutputTimeout > 0 {
		if timer == nil {
			timer = time.NewTimer(step.DropOnOutputTimeout)
		} else {
			stopBatchTimer(timer)
			timer.Reset(step.DropOnOutputTimeout)
		}

		select {
		case <-ctx.Done():
			stopBatchTimer(timer)

			return false, errors.Wrapf(ctx.Err(), "go routine %d", goIdx)
		case step.Output <- value:
			stopBatchTimer(timer)

			return false, nil
		case <-timer.C:
			stopBatchTimer(timer)

			if dropEnabled {
				return true, reportStepDrop(opts, step.Details, model.StepDropSendTimeout)
			}

			return true, nil
		}
	}

	select {
	case <-ctx.Done():
		return false, errors.Wrapf(ctx.Err(), "go routine %d", goIdx)
	case step.Output <- value:
		return false, nil
	}
}
