package pipeline

import (
	"context"
	"time"

	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

type Step[O any] struct {
	Type       stepType
	Name       string
	Output     chan O
	concurrent int
	metric     *metric
}

func sequentialOneToOneFn[I any, O any](ctx context.Context, goIdx int, input *Step[I], output *Step[O], oneToOneFn func(context.Context, I) (O, error)) error {
outer:
	for {
		start := time.Now()
		select {
		case <-ctx.Done():
			return errors.Wrapf(ctx.Err(), "go routine %d:", goIdx)
		case in, ok := <-input.Output:
			if !ok {
				break outer
			}
			startFn := time.Now()
			out, err := oneToOneFn(ctx, in)
			if err != nil {
				return errors.Wrapf(err, "go routine %d:", goIdx)
			}
			endFn := time.Since(startFn)

			// we check the context again to make sure all go routines currently running
			// stop to add new elements to the pipeline
			select {
			case <-ctx.Done():
				return errors.Wrapf(ctx.Err(), "go routine %d:", goIdx)
			case output.Output <- out:
				if output.metric != nil {
					output.metric.add(endFn)
					output.metric.addChannel(input.Name, (time.Since(start)))
				}
			}
		}
	}

	return nil
}

func concurrentOneToOneFn[I any, O any](ctx context.Context, input *Step[I], output *Step[O], oneToOneFn func(context.Context, I) (O, error)) error {
	errGrp, dCtx := errgroup.WithContext(ctx)
	errGrp.SetLimit(output.concurrent)
	// starts many consumers concurrently
	// each consumer stops as soon as an error happens
	for goIdx := 0; goIdx < output.concurrent; goIdx++ {
		localGoIdx := goIdx
		errGrp.Go(func() error {
			return sequentialOneToOneFn(dCtx, localGoIdx, input, output, oneToOneFn)
		})
	}
	return errGrp.Wait()
}

func oneToOne[I any, O any](ctx context.Context, input *Step[I], output *Step[O], oneToOneFn func(context.Context, I) (O, error)) error {
	if output.concurrent == 0 {
		output.concurrent = 1
	}
	if output.concurrent == 1 {
		return sequentialOneToOneFn(ctx, 1, input, output, oneToOneFn)
	}
	return concurrentOneToOneFn(ctx, input, output, oneToOneFn)
}

func sequentialOneToManyFn[I any, O any](ctx context.Context, goIdx int, input *Step[I], output *Step[O], oneToManyFn func(context.Context, I) ([]O, error)) error {
outer:
	for {
		start := time.Now()
		select {
		case <-ctx.Done():
			return errors.Wrapf(ctx.Err(), "go routine %d:", goIdx)
		case in, ok := <-input.Output:
			if !ok {
				break outer
			}
			startFn := time.Now()
			outs, err := oneToManyFn(ctx, in)
			if err != nil {
				return errors.Wrapf(err, "go routine %d:", goIdx)
			}
			endFn := time.Since(startFn)
			for _, out := range outs {
				// we check the context again to make sure all go routines currently running
				// stop to add new elements to the pipeline
				select {
				case <-ctx.Done():
					return errors.Wrapf(ctx.Err(), "go routine %d:", goIdx)
				case output.Output <- out:
					if output.metric != nil {
						output.metric.add(endFn)
						output.metric.addChannel(input.Name, (time.Since(start)-endFn)/time.Duration(output.concurrent))
					}
				}
			}
		}
	}

	return nil
}

func concurrentOneToManyFn[I any, O any](ctx context.Context, input *Step[I], output *Step[O], oneToManyFn func(context.Context, I) ([]O, error)) error {
	errGrp, dCtx := errgroup.WithContext(ctx)
	errGrp.SetLimit(output.concurrent)
	for goIdx := 0; goIdx < output.concurrent; goIdx++ {
		localGoIdx := goIdx
		errGrp.Go(func() error {
			return sequentialOneToManyFn(dCtx, localGoIdx, input, output, oneToManyFn)
		})
	}
	return errGrp.Wait()
}

func oneToMany[I any, O any](ctx context.Context, input *Step[I], output *Step[O], oneToManyFn func(context.Context, I) ([]O, error)) error {
	if output.concurrent == 0 {
		output.concurrent = 1
	}
	if output.concurrent == 1 {
		return sequentialOneToManyFn(ctx, 1, input, output, oneToManyFn)
	}
	return concurrentOneToManyFn(ctx, input, output, oneToManyFn)
}

func prepareStep[I, O any](p *Pipeline, name string, input *Step[I], opts ...StepOption[O]) (*Step[O], error) {
	output := make(chan O)
	step := &Step[O]{
		Type:   normalStepType,
		Name:   name,
		Output: output,
	}
	for _, opt := range opts {
		opt(step)
	}

	err := p.feature.addStep(step.Name)
	if err != nil {
		return nil, err
	}
	err = p.feature.addLink(input.Name, step.Name)
	if err != nil {
		return nil, err
	}

	if p.feature.measure != nil {
		mt := p.feature.measure.addStep(step.Name, step.concurrent)
		step.metric = mt
	}
	return step, nil
}

func addStep[I any, O any](p *Pipeline, name string, input *Step[I], step *Step[O], stepToStepFn func(ctx context.Context, input *Step[I], output *Step[O]) error, opts ...StepOption[O]) (*Step[O], error) {
	if p == nil {
		return nil, ErrPipelineMustBeSet
	}
	if input == nil {
		return nil, ErrInputMustBeSet
	}

	errC := make(chan error, 1)
	decoratedError := newErrorChan(name, errC)

	go func() {
		defer func() {
			close(errC)
			if step.Output != nil {
				close(step.Output)
			}
		}()
		err := stepToStepFn(p.ctx, input, step)
		if err != nil {
			errC <- err
		}
	}()
	p.errcList.add(decoratedError)

	return step, nil
}

func AddStepOneToOne[I any, O any](p *Pipeline, name string, input *Step[I], oneToOneFn func(context.Context, I) (O, error), opts ...StepOption[O]) (*Step[O], error) {
	step, err := prepareStep(p, name, input, opts...)
	if err != nil {
		return nil, err
	}
	return addStep(p, name, input, step, func(ctx context.Context, in *Step[I], out *Step[O]) error {
		return oneToOne(ctx, in, out, oneToOneFn)
	}, opts...)
}

func AddStepOneToMany[I any, O any](p *Pipeline, name string, input *Step[I], oneToManyFn func(context.Context, I) ([]O, error), opts ...StepOption[O]) (*Step[O], error) {
	step, err := prepareStep(p, name, input, opts...)
	if err != nil {
		return nil, err
	}
	return addStep(p, name, input, step, func(ctx context.Context, in *Step[I], out *Step[O]) error {
		return oneToMany(ctx, in, out, oneToManyFn)
	})
}
