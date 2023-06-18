package pipeline

import "context"

func OneToOne[I any, O any](ctx context.Context, input <-chan I, output chan O, oneToOneFn func(context.Context, I) (O, error)) error {
outer:
	for {
		select {
		case <-ctx.Done():
			return nil
		case in, ok := <-input:
			if !ok {
				break outer
			}
			out, err := oneToOneFn(ctx, in)
			if err != nil {
				return err
			}
			select {
			case <-ctx.Done():
				return nil
			case output <- out:
			}
		}
	}

	return nil
}

func OneToMany[I any, O any](ctx context.Context, input <-chan I, output chan<- O, oneToOneFn func(context.Context, I) ([]O, error)) error {
outer:
	for {
		select {
		case <-ctx.Done():
			return nil
		case in, ok := <-input:
			if !ok {
				break outer
			}
			outs, err := oneToOneFn(ctx, in)
			if err != nil {
				return err
			}
			for _, out := range outs {
				select {
				case <-ctx.Done():
					return nil
				case output <- out:
				}
			}
		}
	}

	return nil
}
