package pipeline

import (
	"context"
	"time"

	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

func validateSink[I any](p *Pipeline, input *Step[I]) error {
	if p == nil {
		return ErrPipelineMustBeSet
	}
	if input == nil {
		return ErrInputMustBeSet
	}
	return nil
}

func prepareSink[I any](p *Pipeline, name string, input *Step[I], opts ...StepOption[I]) (*Step[I], error) {
	step := &Step[I]{
		Type: sinkStepType,
		Name: name,
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

		err := p.feature.addLink(step.Name, "end")
		if err != nil {
			return nil, err
		}
	}
	return step, nil
}

func AddSink[I any](p *Pipeline, name string, input *Step[I], oneToZeroFn func(ctx context.Context, input I) error, opts ...StepOption[I]) error {
	err := validateSink(p, input)
	if err != nil {
		return err
	}
	step, err := prepareSink(p, name, input, opts...)
	if err != nil {
		return err
	}

	_, err = addStep(p, name, input, step, func(ctx context.Context, in *Step[I], out *Step[I]) error {
		err := oneToZero(ctx, in, out, oneToZeroFn)
		if err != nil {
			return err
		}
		if step.metric != nil {
			step.metric.end(time.Since(p.startTime))
		}
		return nil
	}, opts...)
	if err != nil {
		return err
	}

	return nil
}

func oneToZero[I any](ctx context.Context, input *Step[I], output *Step[I], oneToZeroFn func(context.Context, I) error) error {
	if output.concurrent == 0 {
		output.concurrent = 1
	}
	if output.concurrent == 1 {
		return sequentialOneToZeroFn(ctx, 1, input, output, oneToZeroFn)
	}
	return concurrentOneToZeroFn(ctx, input, output, oneToZeroFn)
}

func sequentialOneToZeroFn[I any](ctx context.Context, goIdx int, input *Step[I], output *Step[I], oneToZeroFn func(context.Context, I) error) error {
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
			err := oneToZeroFn(ctx, in)
			if err != nil {
				return errors.Wrapf(err, "go routine %d:", goIdx)
			}
			endFn := time.Since(startFn)
			if output.metric != nil {
				output.metric.add(endFn)
				output.metric.addChannel(input.Name, time.Since(start))
			}
		}
	}

	return nil
}

func concurrentOneToZeroFn[I any](ctx context.Context, input *Step[I], output *Step[I], oneToZeroFn func(context.Context, I) error) error {
	errGrp, dCtx := errgroup.WithContext(ctx)
	errGrp.SetLimit(output.concurrent)
	// starts many consumers concurrently
	// each consumer stops as soon as an error happens
	for goIdx := 0; goIdx < output.concurrent; goIdx++ {
		localGoIdx := goIdx
		errGrp.Go(func() error {
			return sequentialOneToZeroFn(dCtx, localGoIdx, input, output, oneToZeroFn)
		})
	}
	return errGrp.Wait()
}
