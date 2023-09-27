package pipeline

import "context"

func prepareRootStep[O any](pipe *Pipeline, step *Step[O], opts ...StepOption[O]) error {
	if pipe.drawer != nil {
		err := pipe.drawer.AddStep(step.Name)
		if err != nil {
			return err
		}
		err = pipe.drawer.AddLink(startStepName, step.Name)
		if err != nil {
			return err
		}
	}
	if pipe.measure != nil {
		mt := pipe.measure.AddMetric(step.Name, 1)
		step.metric = mt
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
		Type:   rootStepType,
		Name:   name,
		Output: output,
	}
	err := prepareRootStep(p, step, opts...)
	if err != nil {
		return nil, err
	}
	go func() {
		defer func() {
			if !step.noClose {
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
