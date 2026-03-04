// Package pipeline builds concurrent data pipelines from small steps.
//
// A pipeline is a graph of steps. Each step reads from a channel, does work,
// and sends results to the next step. Steps run in their own goroutines, and
// backpressure comes from channel sends.
//
// The default behaviour is fail-fast errors and blocking sends. Options let you
// change concurrency, buffers, retries, drops, and monitoring.
//
// Example:
//
//	pipe, _ := pipeline.New()
//
//	root := pipeline.Root(pipe, "root", func(ctx context.Context, out chan<- int) error {
//		for i := range 3 {
//			out <- i
//		}
//
//		return nil
//	})
//
//	doubled := pipeline.OneToOne(pipe, "double", root, func(ctx context.Context, v int) (int, error) {
//		return v * 2, nil
//	})
//
//	pipeline.Sink(pipe, "print", doubled, func(ctx context.Context, v int) error {
//		fmt.Println(v)
//
//		return nil
//	})
//
//	_ = pipe.Run(context.Background())
package pipeline
