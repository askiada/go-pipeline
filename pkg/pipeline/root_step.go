package pipeline

import (
	"context"

	"github.com/pkg/errors"
)

func prepareRootStep[O any](pipe *Pipeline, step *Step[O], opts ...StepOption[O]) error {
	for _, opt := range pipe.opts {
		err := opt.BeforeStep(startStep.details, step.details)
		if err != nil {
			return errors.Wrap(err, "unable to run before step function")
		}
	}
	for _, opt := range opts {
		opt(step)
	}
	return nil
}

func AddRootStep[O any](p *Pipeline, name string, stepFn func(ctx context.Context, rootChan chan<- O) error, opts ...StepOption[O]) (*Step[O], error) {
	if p == nil {
		return nil, ErrPipelineMustBeSet
	}

	errC := make(chan error, 1)
	decoratedError := newErrorChan(name, errC)
	output := make(chan O)
	step := &Step[O]{
		details: &StepInfo{
			Type: rootStepType,
			Name: name,
		},
		Output: output,
	}
	err := prepareRootStep(p, step, opts...)
	if err != nil {
		return nil, err
	}
	go func() {
		defer func() {
			if !step.keepOpen {
				close(output)
			}
			close(errC)
		}()
		err := stepFn(p.ctx, output)
		if err != nil {
			errC <- err
		}
	}()
	p.errcList.add(decoratedError)

	return step, nil
}
