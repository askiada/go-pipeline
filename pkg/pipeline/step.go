package pipeline

import (
	"context"
	"reflect"
	"time"

	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"

	"github.com/askiada/go-pipeline/pkg/pipeline/model"
)

// OneToOneFn is a function that takes an input and produces an output.
type OneToOneFn[I, O any] func(context.Context, I) (O, error)

// OneToManyFn is a function that takes an input and produces many outputs.
type OneToManyFn[I, O any] func(context.Context, I) ([]O, error)

// StepFromChanFn is a function that takes an input channel and produces an output channel.
type StepFromChanFn[I, O any] func(ctx context.Context, input <-chan I, output chan O) error

type stepToStepFn[I, O any] func(ctx context.Context, input *model.Step[I], output *model.Step[O]) error

func sequentialOneToOneFn[I any, O any](
	ctx context.Context,
	goIdx int,
	input *model.Step[I],
	output *model.Step[O],
	oneToOne OneToOneFn[I, O],
	ignoreZero bool,
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
				return errors.Wrapf(ctx.Err(), "go routine: %d", goIdx)
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
	for goIdx := range output.Details.Concurrent {
		localGoIdx := goIdx

		errGrp.Go(func() error {
			return sequentialOneToOneFn(dCtx, localGoIdx, input, output, oneToOne, ignoreZero, opts...)
		})
	}

	if err := errGrp.Wait(); err != nil {
		return errors.Wrap(err, "unable to wait for all go routines")
	}

	return nil
}

func runOneToOne[I any, O any](
	ctx context.Context,
	input *model.Step[I],
	output *model.Step[O],
	oneToOne OneToOneFn[I, O],
	ignoreZero bool,
	opts ...model.PipelineOption,
) error {
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
				}
			}
			end := time.Since(start)
			for _, opt := range opts {
				err := opt.OnStepOutput(input.Details, output.Details, end-endFn, endFn)
				if err != nil {
					return errors.Wrap(err, "unable to run before step function")
				}
			}
		}
	}

	return nil
}

func concurrentOneToManyFn[I any, O any](
	ctx context.Context,
	input *model.Step[I],
	output *model.Step[O],
	oneToMany OneToManyFn[I, O],
	opts ...model.PipelineOption,
) error {
	errGrp, dCtx := errgroup.WithContext(ctx)
	errGrp.SetLimit(output.Details.Concurrent)

	// starts many consumers concurrently
	for goIdx := range output.Details.Concurrent {
		localGoIdx := goIdx

		errGrp.Go(func() error {
			return sequentialOneToManyFn(dCtx, localGoIdx, input, output, oneToMany, opts...)
		})
	}

	if err := errGrp.Wait(); err != nil {
		return errors.Wrap(err, "unable to wait for all go routines")
	}

	return nil
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

	step.Output = make(chan O, step.Details.BufferSize)

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
	step := &model.Step[O]{
		Details: &model.StepInfo{
			Type: model.NormalStepType,
			Name: name,
		},
	}

	for _, opt := range opts {
		opt(step)
	}

	err := prepareStep(pipe, input, step)
	if err != nil {
		return nil, err
	}

	pipe.goFn = append(pipe.goFn, func(ctx context.Context) {
		defer func() {
			close(errC)

			if !step.KeepOpen {
				close(step.Output)
			}
		}()

		err := stepToStep(ctx, input, step)
		if err != nil {
			errC <- err
		}
	})

	pipe.errcList.add(decoratedError)

	return step, nil
}

func runStepFromChan[I, O any](
	ctx context.Context,
	input *model.Step[I],
	output *model.Step[O],
	stepFn StepFromChanFn[I, O],
	opts ...model.PipelineOption,
) error {
	if output.Details.Concurrent == 0 {
		output.Details.Concurrent = 1
	}

	if output.Details.Concurrent == 1 {
		return sequentialStepFromChanFn(ctx, 1, input, output, stepFn, 1, opts...)
	}

	return concurrentStepFromChanFn(ctx, input, output, stepFn, opts...)
}

func sequentialStepFromChanFn[I any, O any](
	ctx context.Context,
	goIdx int,
	input *model.Step[I],
	output *model.Step[O],
	stepFn StepFromChanFn[I, O],
	conc int,
	opts ...model.PipelineOption,
) error {
	inputPlaceholder := make(chan I)
	total := float64(0)
	start := time.Now()

	var end time.Duration

	done := make(chan struct{})

	go func() {
		defer func() {
			close(inputPlaceholder)

			end = time.Since(start)
			done <- struct{}{}
		}()

	outer:
		for {
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
					total++
				}
			}
		}
	}()

	startStep := time.Now()

	err := stepFn(ctx, inputPlaceholder, output.Output)
	if err != nil {
		return errors.Wrap(err, "unable to run step function")
	}

	endStep := time.Since(startStep)

	if total == 0 {
		return nil
	}

	total = float64(conc) / total

	<-done

	for _, opt := range opts {
		err := opt.OnStepOutput(
			input.Details,
			output.Details,
			time.Duration(float64(end)/float64(total)),
			time.Duration(float64(endStep)/float64(total)),
		)
		if err != nil {
			return errors.Wrapf(err, "go routine %d: unable to run after step function", goIdx)
		}
	}

	return nil
}

func concurrentStepFromChanFn[I any, O any](
	ctx context.Context,
	input *model.Step[I],
	output *model.Step[O],
	stepFn StepFromChanFn[I, O],
	opts ...model.PipelineOption,
) error {
	errGrp, dCtx := errgroup.WithContext(ctx)
	errGrp.SetLimit(output.Details.Concurrent)
	// starts many consumers concurrently
	// each consumer stops as soon as an error happens
	for goIdx := range output.Details.Concurrent {
		localGoIdx := goIdx

		errGrp.Go(func() error {
			return sequentialStepFromChanFn(dCtx, localGoIdx, input, output, stepFn, output.Details.Concurrent, opts...)
		})
	}

	if err := errGrp.Wait(); err != nil {
		return errors.Wrap(err, "unable to wait for all go routines")
	}

	return nil
}

// AddStepOneToOne adds a step that takes one input and produces one output.
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

// AddStepOneToOneOrZero adds a step that takes one input and produces one output. If the output is a zero value, it is ignored.
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

// AddStepOneToMany adds a step that takes one input and produces many outputs.
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

// AddStepFromChan adds a step that takes an input channel and produces an output channel.
func AddStepFromChan[I any, O any](
	pipe *Pipeline,
	name string,
	input *model.Step[I],
	stepFromChan StepFromChanFn[I, O],
	opts ...StepOption[O],
) (*model.Step[O], error) {
	return addStep(pipe, name, input, func(ctx context.Context, in *model.Step[I], out *model.Step[O]) error {
		return runStepFromChan(ctx, in, out, stepFromChan, pipe.opts...)
	}, opts...)
}
