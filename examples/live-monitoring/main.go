package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"math/rand"
	"os"
	"sync"
	"time"

	"github.com/askiada/go-pipeline/v2/pkg/pipeline"
	"github.com/askiada/go-pipeline/v2/pkg/pipeline/drawer"
	"github.com/askiada/go-pipeline/v2/pkg/pipeline/measure"
	"github.com/askiada/go-pipeline/v2/pkg/pipeline/monitor"
)

const (
	defaultTelegrafAddr = "127.0.0.1:8094"
	defaultUIBindAddr   = "127.0.0.1:8096"
	totalItems          = 300
)

type jitter struct {
	mu  sync.Mutex
	rng *rand.Rand
}

func newJitter() *jitter {
	return &jitter{
		rng: rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

func (j *jitter) duration(min, max time.Duration) time.Duration {
	if max <= min {
		return min
	}

	j.mu.Lock()
	defer j.mu.Unlock()

	delta := max - min
	offset := time.Duration(j.rng.Int63n(int64(delta)))

	return min + offset
}

func (j *jitter) sleepWithContext(ctx context.Context, min, max time.Duration) error {
	sleepFor := j.duration(min, max)

	timer := time.NewTimer(sleepFor)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func newPipeline(withDrawer bool, uiEnabled bool, uiBindAddr string, telegrafAddr string) (*pipeline.Pipeline, error) {
	cfg := monitor.Config{
		RunName:      "live-monitoring",
		Origin:       "local",
		PipelineName: "example",
		TelegrafAddr: telegrafAddr,
		TelegrafNet:  "udp",
		EnableUI:     uiEnabled,
		BindAddr:     uiBindAddr,
	}

	if !withDrawer {
		return pipeline.New(monitor.PipelineMonitor(&cfg))
	}

	msr := measure.NewDefaultMeasure()
	drw := drawer.NewSVGDrawer("examples/live-monitoring/pipeline.dot")

	return pipeline.New(
		monitor.PipelineMonitor(&cfg),
		measure.PipelineMeasure(msr),
		drawer.PipelineDrawer(drw, msr),
	)
}

func main() {
	drawerEnabled := flag.Bool("drawer", false, "write examples/live-monitoring/pipeline.dot with metrics")
	uiEnabled := flag.Bool("ui", true, "start the local monitoring UI server")
	uiBindAddr := flag.String("ui-addr", "", "bind address for the UI server")
	flag.Parse()

	telegrafAddr := os.Getenv("TELEGRAF_ADDR")
	if telegrafAddr == "" {
		telegrafAddr = defaultTelegrafAddr
	}

	bindAddr := *uiBindAddr
	if bindAddr == "" {
		bindAddr = defaultUIBindAddr
	}

	pipe, err := newPipeline(*drawerEnabled, *uiEnabled, bindAddr, telegrafAddr)
	if err != nil {
		log.Fatal(err)
	}

	j := newJitter()

	root := pipeline.Root(pipe, "root", func(ctx context.Context, out chan<- int) error {
		for i := range totalItems {
			if err := j.sleepWithContext(ctx, 20*time.Millisecond, 60*time.Millisecond); err != nil {
				return err
			}

			out <- i
		}
		return nil
	})

	parse := pipeline.OneToOne(pipe, "parse", root, func(ctx context.Context, in int) (int, error) {
		if err := j.sleepWithContext(ctx, 120*time.Millisecond, 250*time.Millisecond); err != nil {
			return 0, err
		}

		return in + 1, nil
	}, pipeline.StepConcurrency[int](4), pipeline.StepBufferSize[int](4))

	expand := pipeline.OneToMany(pipe, "expand", parse, func(ctx context.Context, in int) ([]int, error) {
		if err := j.sleepWithContext(ctx, 150*time.Millisecond, 300*time.Millisecond); err != nil {
			return nil, err
		}

		return []int{in, in * 10}, nil
	}, pipeline.StepConcurrency[int](2), pipeline.StepBufferSize[int](2))

	split := pipeline.SplitBy(pipe, "route", expand, []pipeline.SplitFn[int]{
		func(ctx context.Context, value int) (bool, error) {
			return value%2 == 0, nil
		},
		func(ctx context.Context, value int) (bool, error) {
			return value%2 != 0, nil
		},
	}, pipeline.SplitterBufferSize[int](300))

	evenInput, _ := split.Get()
	oddInput, _ := split.Get()

	evenStep := pipeline.OneToOne(pipe, "even-work", evenInput, func(ctx context.Context, in int) (int, error) {
		if err := j.sleepWithContext(ctx, 200*time.Millisecond, 350*time.Millisecond); err != nil {
			return 0, err
		}

		return in * 2, nil
	}, pipeline.StepConcurrency[int](2), pipeline.StepBufferSize[int](1), pipeline.StepDropOnFull[int]())

	oddStep := pipeline.OneToOne(pipe, "odd-work", oddInput, func(ctx context.Context, in int) (int, error) {
		if err := j.sleepWithContext(ctx, 220*time.Millisecond, 400*time.Millisecond); err != nil {
			return 0, err
		}

		return in * 3, nil
	}, pipeline.StepConcurrency[int](2), pipeline.StepBufferSize[int](1), pipeline.StepDropOnBlocked[int](25*time.Millisecond))

	merged := pipeline.Merge(pipe, "merge", evenStep, oddStep)

	batched := pipeline.Batch(pipe, "batch", merged, pipeline.BatchPolicy{
		MaxSize: 8,
		MaxWait: 40 * time.Millisecond,
	}, pipeline.StepBufferSize[[]int](1), pipeline.StepDropOnFull[[]int]())

	summarize := pipeline.OneToOne(pipe, "summarize", batched, func(ctx context.Context, input []int) (int, error) {
		if err := j.sleepWithContext(ctx, 150*time.Millisecond, 280*time.Millisecond); err != nil {
			return 0, err
		}

		sum := 0
		for _, value := range input {
			sum += value
		}

		if sum%17 == 0 {
			return 0, errors.New("transient compute error")
		}

		return sum, nil
	}, pipeline.StepBufferSize[int](1), pipeline.StepRetry[int](pipeline.RetryPolicy{
		MaxAttempts: 3,
		Backoff:     5 * time.Millisecond,
		MaxBackoff:  15 * time.Millisecond,
		Jitter:      0.2,
	}), pipeline.StepDropOnError[int](), pipeline.StepDropOnBlocked[int](30*time.Millisecond))

	pipeline.Sink(pipe, "sink", summarize, func(ctx context.Context, in int) error {
		if err := j.sleepWithContext(ctx, 180*time.Millisecond, 320*time.Millisecond); err != nil {
			return err
		}

		return nil
	})

	if err := pipe.Run(context.Background()); err != nil {
		log.Fatal(err)
	}
}
