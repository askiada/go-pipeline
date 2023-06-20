package pipeline

import "context"

func prepareRootStep[O any](p *Pipeline, step *Step[O]) error {
	if p.drawer != nil {
		err := p.drawer.addStep(step.Name)
		if err != nil {
			return err
		}
		err = p.drawer.addLink("start", step.Name)
		if err != nil {
			return err
		}
	}
	if p.measure != nil {
		mt := p.measure.addStep(step.Name, 1)
		step.metric = mt
	}
	return nil
}

func AddRootStep[O any](p *Pipeline, name string, stepFn func(ctx context.Context, rootChan chan<- O) error) (*Step[O], error) {
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
	err := prepareRootStep(p, step)
	if err != nil {
		return nil, err
	}
	go func() {
		defer func() {
			close(output)
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
