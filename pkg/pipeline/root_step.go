package pipeline

import "context"

func prepareRootStep[O any](pipe *Pipeline, step *Step[O]) error {
	if pipe.drawer != nil {
		err := pipe.drawer.addStep(step.Name)
		if err != nil {
			return err
		}
		err = pipe.drawer.addLink("start", step.Name)
		if err != nil {
			return err
		}
	}
	if pipe.measure != nil {
		mt := pipe.measure.addStep(step.Name, 1)
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
