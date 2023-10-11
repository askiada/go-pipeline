package pipeline

import (
	"context"
	"fmt"
	"reflect"
	"time"

	"github.com/askiada/go-pipeline/pkg/pipeline/model"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

type OneToOneFn[I, O any] func(context.Context, I) (O, error)

type OneToManyFn[I, O any] func(context.Context, I) ([]O, error)

type StepFromChanFn[I, O any] func(ctx context.Context, input <-chan I, output chan O) error

type stepToStepFn[I, O any] func(ctx context.Context, input *model.Step[I], output *model.Step[O]) error

func sequentialOneToOneFn[I any, O any](ctx context.Context, goIdx int, input *model.Step[I], output *model.Step[O], oneToOne OneToOneFn[I, O], ignoreZero bool, opts ...model.PipelineOption) error {
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
				for _, opt := range opts {
					err := opt.OnStepOutput(input.Details, output.Details, time.Since(start)-endFn, endFn)
					if err != nil {
						return errors.Wrap(err, "unable to run before step function")
					}
				}
			}
		}
	}

	return nil
}

func concurrentOneToOneFn[I any, O any](
	ctx context.Context,
	input *model.Step[I],
	output *model.Step[O],
	oneToOne OneToOneFn[I, O],
	ignoreZero bool,
	opts ...model.PipelineOption,
) error {
	errGrp, dCtx := errgroup.WithContext(ctx)
	errGrp.SetLimit(output.Details.Concurrent)
	// starts many consumers concurrently
	// each consumer stops as soon as an error happens
	for goIdx := 0; goIdx < output.Details.Concurrent; goIdx++ {
		localGoIdx := goIdx
		errGrp.Go(func() error {
			return sequentialOneToOneFn(dCtx, localGoIdx, input, output, oneToOne, ignoreZero, opts...)
		})
	}
	return errGrp.Wait()
}

func runOneToOne[I any, O any](ctx context.Context, input *model.Step[I], output *model.Step[O], oneToOne OneToOneFn[I, O], ignoreZero bool, opts ...model.PipelineOption) error {
	if output.Details.Concurrent == 0 {
		output.Details.Concurrent = 1
	}
	if output.Details.Concurrent == 1 {
		return sequentialOneToOneFn(ctx, 1, input, output, oneToOne, ignoreZero, opts...)
	}
	return concurrentOneToOneFn(ctx, input, output, oneToOne, ignoreZero, opts...)
}

func sequentialOneToManyFn[I any, O any](
	ctx context.Context,
	goIdx int,
	input *model.Step[I],
	output *model.Step[O],
	oneToMany OneToManyFn[I, O],
	opts ...model.PipelineOption,
) error {
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
			outs, err := oneToMany(ctx, in)
			if err != nil {
				return errors.Wrapf(err, "go routine %d", goIdx)
			}
			endFn := time.Since(startFn)
			for _, out := range outs {
				// we check the context again to make sure all go routines currently running
				// stop to add new elements to the pipeline
				select {
				case <-ctx.Done():
					return errors.Wrapf(ctx.Err(), "go routine %d", goIdx)
				case output.Output <- out:
					end := time.Since(start)
					for _, opt := range opts {
						err := opt.OnStepOutput(input.Details, output.Details, end-endFn, endFn)
						if err != nil {
							return errors.Wrap(err, "unable to run before step function")
						}
					}
				}
			}
		}
	}

	return nil
}

func concurrentOneToManyFn[I any, O any](ctx context.Context, input *model.Step[I], output *model.Step[O], oneToMany OneToManyFn[I, O], opts ...model.PipelineOption) error {
	errGrp, dCtx := errgroup.WithContext(ctx)
	errGrp.SetLimit(output.Details.Concurrent)
	for goIdx := 0; goIdx < output.Details.Concurrent; goIdx++ {
		localGoIdx := goIdx
		errGrp.Go(func() error {
			return sequentialOneToManyFn(dCtx, localGoIdx, input, output, oneToMany, opts...)
		})
	}
	return errGrp.Wait()
}

func runOneToMany[I any, O any](
	ctx context.Context,
	input *model.Step[I],
	output *model.Step[O],
	oneToMany func(context.Context, I) ([]O, error),
	opts ...model.PipelineOption,
) error {
	if output.Details.Concurrent == 0 {
		output.Details.Concurrent = 1
	}
	if output.Details.Concurrent == 1 {
		return sequentialOneToManyFn(ctx, 1, input, output, oneToMany, opts...)
	}
	return concurrentOneToManyFn(ctx, input, output, oneToMany, opts...)
}

func prepareStep[I, O any](pipe *Pipeline, input *model.Step[I], step *model.Step[O]) error {
	for _, opt := range pipe.opts {
		err := opt.PrepareStep(input.Details, step.Details)
		if err != nil {
			return errors.Wrap(err, "unable to run before step function")
		}
	}
	return nil
}

func addStep[I any, O any](
	pipe *Pipeline,
	name string,
	input *model.Step[I],
	stepToStep stepToStepFn[I, O],
	opts ...StepOption[O],
) (*model.Step[O], error) {
	if pipe == nil {
		return nil, ErrPipelineMustBeSet
	}
	if input == nil {
		return nil, ErrInputMustBeSet
	}

	errC := make(chan error, 1)
	decoratedError := newErrorChan(name, errC)
	output := make(chan O)
	step := &model.Step[O]{
		Details: &model.StepInfo{
			Type: model.NormalStepType,
			Name: name,
		},
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
			if !step.KeepOpen {
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
	input *model.Step[I],
	output *model.Step[O],
	stepFn StepFromChanFn[I, O],
	ignoreZero bool,
	opts ...model.PipelineOption,
) error {
	if output.Details.Concurrent == 0 {
		output.Details.Concurrent = 1
	}
	if output.Details.Concurrent == 1 {
		return sequentialStepFromChanFn(ctx, 1, input, output, stepFn, ignoreZero, opts...)
	}
	return concurrentStepFromChanFn(ctx, input, output, stepFn, ignoreZero, opts...)
}

func sequentialStepFromChanFn[I any, O any](
	ctx context.Context,
	goIdx int,
	input *model.Step[I],
	output *model.Step[O],
	stepFn StepFromChanFn[I, O],
	ignoreZero bool,
	opts ...model.PipelineOption,
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

					for _, opt := range opts {
						err := opt.OnStepOutput(input.Details, output.Details, time.Since(start), 0)
						if err != nil {
							// TODO: fix me to return an error
							fmt.Println(errors.Wrap(err, "unable to run before step function"))
						}
					}
				}
			}
		}
		close(inputPlaceholder)
	}()

	return stepFn(ctx, inputPlaceholder, output.Output)
}

func concurrentStepFromChanFn[I any, O any](
	ctx context.Context,
	input *model.Step[I],
	output *model.Step[O],
	stepFn StepFromChanFn[I, O],
	ignoreZero bool,
	opts ...model.PipelineOption,
) error {
	errGrp, dCtx := errgroup.WithContext(ctx)
	errGrp.SetLimit(output.Details.Concurrent)
	// starts many consumers concurrently
	// each consumer stops as soon as an error happens
	for goIdx := 0; goIdx < output.Details.Concurrent; goIdx++ {
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
	input *model.Step[I],
	oneToOne OneToOneFn[I, O],
	opts ...StepOption[O],
) (*model.Step[O], error) {
	return addStep(pipe, name, input, func(ctx context.Context, in *model.Step[I], out *model.Step[O]) error {
		return runOneToOne(ctx, in, out, oneToOne, false, pipe.opts...)
	}, opts...)
}

func AddStepOneToOneOrZero[I any, O any](
	pipe *Pipeline,
	name string,
	input *model.Step[I],
	oneToOne OneToOneFn[I, O],
	opts ...StepOption[O],
) (*model.Step[O], error) {
	return addStep(pipe, name, input, func(ctx context.Context, in *model.Step[I], out *model.Step[O]) error {
		return runOneToOne(ctx, in, out, oneToOne, true, pipe.opts...)
	}, opts...)
}

func AddStepOneToMany[I any, O any](
	pipe *Pipeline,
	name string,
	input *model.Step[I],
	oneToMany OneToManyFn[I, O],
	opts ...StepOption[O],
) (*model.Step[O], error) {
	return addStep(pipe, name, input, func(ctx context.Context, in *model.Step[I], out *model.Step[O]) error {
		return runOneToMany(ctx, in, out, oneToMany, pipe.opts...)
	}, opts...)
}

func AddStepFromChan[I any, O any](
	pipe *Pipeline,
	name string,
	input *model.Step[I],
	stepFromChan StepFromChanFn[I, O],
	opts ...StepOption[O],
) (*model.Step[O], error) {
	return addStep(pipe, name, input, func(ctx context.Context, in *model.Step[I], out *model.Step[O]) error {
		return runStepFromChan(ctx, in, out, stepFromChan, false)
	}, opts...)
}
