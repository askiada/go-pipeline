package pipeline

import (
	"context"
	"time"
)

func AddSink[I any](p *Pipeline, name string, input *Step[I], sinkFn func(ctx context.Context, input I) error) error {
	if p == nil {
		return ErrPipelineMustBeSet
	}
	if input == nil {
		return ErrInputMustBeSet
	}
	step := Step[I]{
		Type: sinkStepType,
		Name: name,
	}
	if p.drawer != nil {
		err := p.drawer.AddStep(step.Name)
		if err != nil {
			return err
		}
		err = p.drawer.AddLink(input.Name, step.Name)
		if err != nil {
			return err
		}
		err = p.drawer.AddLink(step.Name, endStepName)
		if err != nil {
			return err
		}
	}
	if p.measure != nil {
		mt := p.measure.AddMetric(step.Name, 1)
		step.metric = mt
	}

	errC := make(chan error, 1)
	decoratedError := newErrorChan(name, errC)
	go func() {
		defer func() {
			close(errC)
		}()
	outer:
		for {
			startInputChan := time.Now()
			select {
			case <-p.ctx.Done():
				errC <- p.ctx.Err()

				break outer
			case in, ok := <-input.Output:
				if !ok {
					break outer
				}
				endInputChan := time.Since(startInputChan)

				startFn := time.Now()
				err := sinkFn(p.ctx, in)
				if err != nil {
					errC <- err
				}
				endFn := time.Since(startFn)
				if step.metric != nil {
					step.metric.AddDuration(endFn)
					step.metric.AddTransportDuration(input.Name, endInputChan+endFn)
				}
			}
		}
		if step.metric != nil {
			step.metric.SetTotalDuration(time.Since(p.startTime))
		}
	}()
	p.errcList.add(decoratedError)

	return nil
}

func AddSinkFromChan[I any](p *Pipeline, name string, input *Step[I], stepFn func(ctx context.Context, input <-chan I) error) error {
	if p == nil {
		return ErrPipelineMustBeSet
	}
	if input == nil {
		return ErrInputMustBeSet
	}
	step := Step[I]{
		Type: sinkStepType,
		Name: name,
	}
	if p.drawer != nil {
		err := p.drawer.AddStep(step.Name)
		if err != nil {
			return err
		}
		err = p.drawer.AddLink(input.Name, step.Name)
		if err != nil {
			return err
		}
		err = p.drawer.AddLink(step.Name, endStepName)
		if err != nil {
			return err
		}
	}
	if p.measure != nil {
		mt := p.measure.AddMetric(step.Name, 1)
		step.metric = mt
	}

	errC := make(chan error, 1)
	decoratedError := newErrorChan(name, errC)
	go func() {
		defer func() {
			close(errC)
		}()
		err := stepFn(p.ctx, input.Output)
		if err != nil {
			errC <- err
		}
		if step.metric != nil {
			step.metric.SetTotalDuration(time.Since(p.startTime))
		}
	}()
	p.errcList.add(decoratedError)

	return nil
}
