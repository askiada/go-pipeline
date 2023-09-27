package pipeline

import (
	"context"
	"reflect"
	"time"

	"github.com/askiada/go-pipeline/pkg/pipeline/measure"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

type Step[O any] struct {
	Type       stepType
	Name       string
	Output     chan O
	concurrent int
	noClose    bool
	metric     measure.Metric
}

type OneToOneFn[I, O any] func(context.Context, I) (O, error)

type OneToManyFn[I, O any] func(context.Context, I) ([]O, error)

type StepFromChanFn[I, O any] func(ctx context.Context, input <-chan I, output chan O) error

type stepToStepFn[I, O any] func(ctx context.Context, input *Step[I], output *Step[O]) error

func sequentialOneToOneFn[I any, O any](ctx context.Context, goIdx int, input *Step[I], output *Step[O], oneToOne OneToOneFn[I, O], ignoreZero bool) error {
outer:
	for {
		start := time.Now()
		select {
		case <-ctx.Done():
			return errors.Wrapf(ctx.Err(), "go routine %d", goIdx)
		case in, ok := <-input.Output:
			if !ok {
				break outer
			}
			startFn := time.Now()
			out, err := oneToOne(ctx, in)
			if err != nil {
				return errors.Wrapf(err, "go routine %d", goIdx)
			}
			endFn := time.Since(startFn)
			if ignoreZero && reflect.ValueOf(out).IsZero() {
				continue
			}

			// we check the context again to make sure all go routines currently running
			// stop to add new elements to the pipeline
			select {
			case <-ctx.Done():
				return errors.Wrapf(ctx.Err(), "go routine %d:", goIdx)
			case output.Output <- out:
				if output.metric != nil {
					output.metric.AddDuration(endFn)
					output.metric.AddTransportDuration(input.Name, time.Since(start)-endFn)
				}
			}
		}
	}

	return nil
}

func concurrentOneToOneFn[I any, O any](
	ctx context.Context,
	input *Step[I],
	output *Step[O],
	oneToOne OneToOneFn[I, O],
	ignoreZero bool,
) error {
	errGrp, dCtx := errgroup.WithContext(ctx)
	errGrp.SetLimit(output.concurrent)
	// starts many consumers concurrently
	// each consumer stops as soon as an error happens
	for goIdx := 0; goIdx < output.concurrent; goIdx++ {
		localGoIdx := goIdx
		errGrp.Go(func() error {
			return sequentialOneToOneFn(dCtx, localGoIdx, input, output, oneToOne, ignoreZero)
		})
	}
	return errGrp.Wait()
}

func runOneToOne[I any, O any](ctx context.Context, input *Step[I], output *Step[O], oneToOne OneToOneFn[I, O], ignoreZero bool) error {
	if output.concurrent == 0 {
		output.concurrent = 1
	}
	if output.concurrent == 1 {
		return sequentialOneToOneFn(ctx, 1, input, output, oneToOne, ignoreZero)
	}
	return concurrentOneToOneFn(ctx, input, output, oneToOne, ignoreZero)
}

func sequentialOneToManyFn[I any, O any](
	ctx context.Context,
	goIdx int,
	input *Step[I],
	output *Step[O],
	oneToMany OneToManyFn[I, O],
) error {
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
			outs, err := oneToMany(ctx, in)
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
						output.metric.AddDuration(endFn)
						output.metric.AddTransportDuration(input.Name, time.Since(start)-endFn)
					}
				}
			}
		}
	}

	return nil
}

func concurrentOneToManyFn[I any, O any](ctx context.Context, input *Step[I], output *Step[O], oneToMany OneToManyFn[I, O]) error {
	errGrp, dCtx := errgroup.WithContext(ctx)
	errGrp.SetLimit(output.concurrent)
	for goIdx := 0; goIdx < output.concurrent; goIdx++ {
		localGoIdx := goIdx
		errGrp.Go(func() error {
			return sequentialOneToManyFn(dCtx, localGoIdx, input, output, oneToMany)
		})
	}
	return errGrp.Wait()
}

func runOneToMany[I any, O any](
	ctx context.Context,
	input *Step[I],
	output *Step[O],
	oneToMany func(context.Context, I) ([]O, error),
) error {
	if output.concurrent == 0 {
		output.concurrent = 1
	}
	if output.concurrent == 1 {
		return sequentialOneToManyFn(ctx, 1, input, output, oneToMany)
	}
	return concurrentOneToManyFn(ctx, input, output, oneToMany)
}

func prepareStep[I, O any](pipe *Pipeline, input *Step[I], step *Step[O]) error {
	if pipe.drawer != nil {
		err := pipe.drawer.AddStep(step.Name)
		if err != nil {
			return err
		}
		err = pipe.drawer.AddLink(input.Name, step.Name)
		if err != nil {
			return err
		}
	}

	if pipe.measure != nil {
		mt := pipe.measure.AddMetric(step.Name, step.concurrent)
		step.metric = mt
	}
	return nil
}

func addStep[I any, O any](
	pipe *Pipeline,
	name string,
	input *Step[I],
	stepToStep stepToStepFn[I, O],
	opts ...StepOption[O],
) (*Step[O], error) {
	if pipe == nil {
		return nil, ErrPipelineMustBeSet
	}
	if input == nil {
		return nil, ErrInputMustBeSet
	}

	errC := make(chan error, 1)
	decoratedError := newErrorChan(name, errC)
	output := make(chan O)
	step := &Step[O]{
		Type:   normalStepType,
		Name:   name,
		Output: output,
	}
	for _, opt := range opts {
		opt(step)
	}
	err := prepareStep(pipe, input, step)
	if err != nil {
		return nil, err
	}
	go func() {
		defer func() {
			close(errC)
			if !step.noClose {
				close(output)
			}
		}()
		err := stepToStep(pipe.ctx, input, step)
		if err != nil {
			errC <- err
		}
	}()
	pipe.errcList.add(decoratedError)

	return step, nil
}

func runStepFromChan[I, O any](
	ctx context.Context,
	input *Step[I],
	output *Step[O],
	stepFn StepFromChanFn[I, O],
	ignoreZero bool,
) error {
	if output.concurrent == 0 {
		output.concurrent = 1
	}
	if output.concurrent == 1 {
		return sequentialStepFromChanFn(ctx, 1, input, output, stepFn, ignoreZero)
	}
	return concurrentStepFromChanFn(ctx, input, output, stepFn, ignoreZero)
}

func sequentialStepFromChanFn[I any, O any](
	ctx context.Context,
	goIdx int,
	input *Step[I],
	output *Step[O],
	stepFn StepFromChanFn[I, O],
	ignoreZero bool,
) error {
	inputPlaceholder := make(chan I)
	go func() {
	outer:
		for {
			start := time.Now()
			select {
			case <-ctx.Done():
				break outer
			case entry, ok := <-input.Output:
				if !ok {
					break outer
				}
				select {
				case <-ctx.Done():
					break outer
				case inputPlaceholder <- entry:
					output.metric.AddTransportDuration(input.Name, time.Since(start))
				}
			}
		}
		close(inputPlaceholder)
	}()

	return stepFn(ctx, inputPlaceholder, output.Output)
}

func concurrentStepFromChanFn[I any, O any](
	ctx context.Context,
	input *Step[I],
	output *Step[O],
	stepFn StepFromChanFn[I, O],
	ignoreZero bool,
) error {
	errGrp, dCtx := errgroup.WithContext(ctx)
	errGrp.SetLimit(output.concurrent)
	// starts many consumers concurrently
	// each consumer stops as soon as an error happens
	for goIdx := 0; goIdx < output.concurrent; goIdx++ {
		localGoIdx := goIdx
		errGrp.Go(func() error {
			return sequentialStepFromChanFn(dCtx, localGoIdx, input, output, stepFn, ignoreZero)
		})
	}
	return errGrp.Wait()
}

func AddStepOneToOne[I any, O any](
	pipe *Pipeline,
	name string,
	input *Step[I],
	oneToOne OneToOneFn[I, O],
	opts ...StepOption[O],
) (*Step[O], error) {
	return addStep(pipe, name, input, func(ctx context.Context, in *Step[I], out *Step[O]) error {
		return runOneToOne(ctx, in, out, oneToOne, false)
	}, opts...)
}

func AddStepOneToOneOrZero[I any, O any](
	pipe *Pipeline,
	name string,
	input *Step[I],
	oneToOne OneToOneFn[I, O],
	opts ...StepOption[O],
) (*Step[O], error) {
	return addStep(pipe, name, input, func(ctx context.Context, in *Step[I], out *Step[O]) error {
		return runOneToOne(ctx, in, out, oneToOne, true)
	}, opts...)
}

func AddStepOneToMany[I any, O any](
	pipe *Pipeline,
	name string,
	input *Step[I],
	oneToMany OneToManyFn[I, O],
	opts ...StepOption[O],
) (*Step[O], error) {
	return addStep(pipe, name, input, func(ctx context.Context, in *Step[I], out *Step[O]) error {
		return runOneToMany(ctx, in, out, oneToMany)
	}, opts...)
}

func AddStepFromChan[I any, O any](
	pipe *Pipeline,
	name string,
	input *Step[I],
	stepFromChan StepFromChanFn[I, O],
	opts ...StepOption[O],
) (*Step[O], error) {
	return addStep(pipe, name, input, func(ctx context.Context, in *Step[I], out *Step[O]) error {
		return runStepFromChan(ctx, in, out, stepFromChan, false)
	}, opts...)
}
