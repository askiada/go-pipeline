package pipeline

import (
	"context"

	"github.com/pkg/errors"

	"github.com/askiada/go-pipeline/pkg/pipeline/model"
)

func prepareRootStep[O any](pipe *Pipeline, step *model.Step[O], opts ...StepOption[O]) error {
	for _, opt := range pipe.opts {
		err := opt.PrepareStep(model.StartStep.Details, step.Details)
		if err != nil {
			return errors.Wrap(err, "unable to run before step function")
		}
	}

	for _, opt := range opts {
		opt(step)
	}

	return nil
}

// AddRootStep adds a root step to the pipeline. It will run the step function.
func AddRootStep[O any](
	pipe *Pipeline,
	name string,
	stepFn func(ctx context.Context, rootChan chan<- O) error,
	opts ...StepOption[O],
) (*model.Step[O], error) {
	if pipe == nil {
		return nil, ErrPipelineMustBeSet
	}

	errC := make(chan error, 1)
	decoratedError := newErrorChan(name, errC)
	output := make(chan O)

	step := &model.Step[O]{
		Details: &model.StepInfo{
			Type: model.RootStepType,
			Name: name,
		},
		Output: output,
	}

	err := prepareRootStep(pipe, step, opts...)
	if err != nil {
		return nil, err
	}

	go func() {
		defer func() {
			if !step.KeepOpen {
				close(output)
			}

			close(errC)
		}()

		err := stepFn(pipe.ctx, output)
		if err != nil {
			errC <- err
		}
	}()

	pipe.errcList.add(decoratedError)

	return step, nil
}
