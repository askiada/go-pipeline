package pipeline

import (
	"context"
	"time"

	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

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
					output.metric.addChannel(input.Name, (time.Since(start))/time.Duration(output.concurrent))
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
