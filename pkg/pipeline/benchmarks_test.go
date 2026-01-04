package pipeline_test

import (
	"context"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/askiada/go-pipeline/v2/pkg/pipeline"
)

const (
	benchWorkIters     = 64
	benchOverheadItems = 4096
	benchBatchSize     = 32
	benchStepWorkers   = 1
)

type benchCase struct {
	name    string
	items   int
	workers int
}

var benchCases = []benchCase{
	{name: "items=1k/conc=1", items: 1024, workers: 1},
	{name: "items=1k/conc=4", items: 1024, workers: 4},
}

var (
	benchWorkFactors = []int{1, 2, 4, 8}
	benchStepCounts  = []int{1, 2, 4, 8, 16}
)

var benchSink int64

func makeInputs(n int) []int {
	inputs := make([]int, n)
	for i := range inputs {
		inputs[i] = i
	}

	return inputs
}

func realisticWork(v int) int {
	x := uint64(v) + 0x9e3779b97f4a7c15
	for range benchWorkIters {
		x ^= x << 7
		x ^= x >> 9
		x *= 0x9e3779b97f4a7c15
	}

	return int(x)
}

func workWithFactor(factor int) func(int) int {
	if factor < 1 {
		factor = 1
	}

	return func(v int) int {
		out := v
		for range factor {
			out = realisticWork(out)
		}

		return out
	}
}

type benchFns struct {
	buildChannels func([]int, int) func(context.Context) int64
	buildPipeline func([]int, int) (func(context.Context) (int64, error), error)
}

type benchWorkFns struct {
	buildChannels func([]int, int, func(int) int) func(context.Context) int64
	buildPipeline func([]int, int, func(int) int) (func(context.Context) (int64, error), error)
}

func runBenchCase(b *testing.B, tc benchCase, inputs []int, fns benchFns) {
	b.Helper()

	b.Run(tc.name+"/channels", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()

		for range b.N {
			b.StopTimer()

			run := fns.buildChannels(inputs, tc.workers)

			b.StartTimer()

			benchSink = run(b.Context())

			b.StopTimer()
		}
	})

	b.Run(tc.name+"/pipeline", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()

		for range b.N {
			b.StopTimer()

			run, err := fns.buildPipeline(inputs, tc.workers)
			if err != nil {
				b.Fatal(err)
			}

			b.StartTimer()

			sum, err := run(b.Context())
			if err != nil {
				b.Fatal(err)
			}

			b.StopTimer()

			benchSink = sum
		}
	})
}

func runWorkSweep(b *testing.B, inputs []int, workers int, fns benchWorkFns) {
	b.Helper()

	for _, factor := range benchWorkFactors {
		workFn := workWithFactor(factor)
		caseName := "factor=" + strconv.Itoa(factor)

		b.Run(caseName+"/channels", func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()

			for range b.N {
				b.StopTimer()

				run := fns.buildChannels(inputs, workers, workFn)

				b.StartTimer()

				benchSink = run(b.Context())

				b.StopTimer()
			}
		})

		b.Run(caseName+"/pipeline", func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()

			for range b.N {
				b.StopTimer()

				run, err := fns.buildPipeline(inputs, workers, workFn)
				if err != nil {
					b.Fatal(err)
				}

				b.StartTimer()

				sum, err := run(b.Context())
				if err != nil {
					b.Fatal(err)
				}

				b.StopTimer()

				benchSink = sum
			}
		})
	}
}

func BenchmarkOneToOne(b *testing.B) {
	fns := benchFns{
		buildChannels: func(inputs []int, workers int) func(context.Context) int64 {
			return buildChannelsOneToOne(inputs, workers, realisticWork)
		},
		buildPipeline: func(inputs []int, workers int) (func(context.Context) (int64, error), error) {
			return buildPipelineOneToOne(inputs, workers, realisticWork)
		},
	}

	for _, tc := range benchCases {
		inputs := makeInputs(tc.items)
		runBenchCase(b, tc, inputs, fns)
	}
}

func BenchmarkOneToOneOrZero(b *testing.B) {
	fns := benchFns{
		buildChannels: func(inputs []int, workers int) func(context.Context) int64 {
			return buildChannelsOneToOneOrZero(inputs, workers, realisticWork)
		},
		buildPipeline: func(inputs []int, workers int) (func(context.Context) (int64, error), error) {
			return buildPipelineOneToOneOrZero(inputs, workers, realisticWork)
		},
	}

	for _, tc := range benchCases {
		inputs := makeInputs(tc.items)
		runBenchCase(b, tc, inputs, fns)
	}
}

func BenchmarkOneToMany(b *testing.B) {
	fns := benchFns{
		buildChannels: func(inputs []int, workers int) func(context.Context) int64 {
			return buildChannelsOneToMany(inputs, workers, realisticWork)
		},
		buildPipeline: func(inputs []int, workers int) (func(context.Context) (int64, error), error) {
			return buildPipelineOneToMany(inputs, workers, realisticWork)
		},
	}

	for _, tc := range benchCases {
		inputs := makeInputs(tc.items)
		runBenchCase(b, tc, inputs, fns)
	}
}

func BenchmarkFromChan(b *testing.B) {
	fns := benchFns{
		buildChannels: func(inputs []int, workers int) func(context.Context) int64 {
			return buildChannelsFromChan(inputs, workers, realisticWork)
		},
		buildPipeline: func(inputs []int, workers int) (func(context.Context) (int64, error), error) {
			return buildPipelineFromChan(inputs, workers, realisticWork)
		},
	}

	for _, tc := range benchCases {
		inputs := makeInputs(tc.items)
		runBenchCase(b, tc, inputs, fns)
	}
}

func BenchmarkSink(b *testing.B) {
	fns := benchFns{
		buildChannels: func(inputs []int, workers int) func(context.Context) int64 {
			return buildChannelsSink(inputs, workers, realisticWork)
		},
		buildPipeline: func(inputs []int, workers int) (func(context.Context) (int64, error), error) {
			return buildPipelineSink(inputs, workers, realisticWork)
		},
	}

	for _, tc := range benchCases {
		inputs := makeInputs(tc.items)
		runBenchCase(b, tc, inputs, fns)
	}
}

func BenchmarkSinkFromChan(b *testing.B) {
	fns := benchFns{
		buildChannels: func(inputs []int, workers int) func(context.Context) int64 {
			return buildChannelsSinkFromChan(inputs, workers, realisticWork)
		},
		buildPipeline: func(inputs []int, workers int) (func(context.Context) (int64, error), error) {
			return buildPipelineSinkFromChan(inputs, workers, realisticWork)
		},
	}

	for _, tc := range benchCases {
		inputs := makeInputs(tc.items)
		runBenchCase(b, tc, inputs, fns)
	}
}

func BenchmarkBatch(b *testing.B) {
	fns := benchFns{
		buildChannels: func(inputs []int, workers int) func(context.Context) int64 {
			return buildChannelsBatch(inputs, workers, benchBatchSize, realisticWork)
		},
		buildPipeline: func(inputs []int, workers int) (func(context.Context) (int64, error), error) {
			return buildPipelineBatch(inputs, workers, benchBatchSize, realisticWork)
		},
	}

	for _, tc := range benchCases {
		inputs := makeInputs(tc.items)
		runBenchCase(b, tc, inputs, fns)
	}
}

func BenchmarkBatchChan(b *testing.B) {
	fns := benchFns{
		buildChannels: func(inputs []int, workers int) func(context.Context) int64 {
			return buildChannelsBatchChan(inputs, workers, benchBatchSize, realisticWork)
		},
		buildPipeline: func(inputs []int, workers int) (func(context.Context) (int64, error), error) {
			return buildPipelineBatchChan(inputs, workers, benchBatchSize, realisticWork)
		},
	}

	for _, tc := range benchCases {
		inputs := makeInputs(tc.items)
		runBenchCase(b, tc, inputs, fns)
	}
}

func BenchmarkTwoStage(b *testing.B) {
	fns := benchFns{
		buildChannels: func(inputs []int, workers int) func(context.Context) int64 {
			return buildChannelsTwoStage(inputs, workers, realisticWork, realisticWork)
		},
		buildPipeline: func(inputs []int, workers int) (func(context.Context) (int64, error), error) {
			return buildPipelineTwoStage(inputs, workers, realisticWork, realisticWork)
		},
	}

	for _, tc := range benchCases {
		inputs := makeInputs(tc.items)
		runBenchCase(b, tc, inputs, fns)
	}
}

func BenchmarkSplitMerge(b *testing.B) {
	fns := benchFns{
		buildChannels: func(inputs []int, workers int) func(context.Context) int64 {
			return buildChannelsSplitMerge(inputs, workers, realisticWork, realisticWork)
		},
		buildPipeline: func(inputs []int, workers int) (func(context.Context) (int64, error), error) {
			return buildPipelineSplitMerge(inputs, workers, realisticWork, realisticWork)
		},
	}

	for _, tc := range benchCases {
		inputs := makeInputs(tc.items)
		runBenchCase(b, tc, inputs, fns)
	}
}

func BenchmarkSplitBy(b *testing.B) {
	fns := benchFns{
		buildChannels: func(inputs []int, workers int) func(context.Context) int64 {
			return buildChannelsSplitBy(inputs, workers, realisticWork, realisticWork)
		},
		buildPipeline: func(inputs []int, workers int) (func(context.Context) (int64, error), error) {
			return buildPipelineSplitBy(inputs, workers, realisticWork, realisticWork)
		},
	}

	for _, tc := range benchCases {
		inputs := makeInputs(tc.items)
		runBenchCase(b, tc, inputs, fns)
	}
}

func BenchmarkOverheadWorkSweep(b *testing.B) {
	inputs := makeInputs(benchOverheadItems)
	fns := benchWorkFns{
		buildChannels: func(inputs []int, workers int, workFn func(int) int) func(context.Context) int64 {
			return buildChannelsOneToOne(inputs, workers, workFn)
		},
		buildPipeline: func(inputs []int, workers int, workFn func(int) int) (func(context.Context) (int64, error), error) {
			return buildPipelineOneToOne(inputs, workers, workFn)
		},
	}

	runWorkSweep(b, inputs, benchStepWorkers, fns)
}

func BenchmarkOneToOneOrZeroWorkSweep(b *testing.B) {
	inputs := makeInputs(benchOverheadItems)
	fns := benchWorkFns{
		buildChannels: func(inputs []int, workers int, workFn func(int) int) func(context.Context) int64 {
			return buildChannelsOneToOneOrZero(inputs, workers, workFn)
		},
		buildPipeline: func(inputs []int, workers int, workFn func(int) int) (func(context.Context) (int64, error), error) {
			return buildPipelineOneToOneOrZero(inputs, workers, workFn)
		},
	}

	runWorkSweep(b, inputs, benchStepWorkers, fns)
}

func BenchmarkOneToManyWorkSweep(b *testing.B) {
	inputs := makeInputs(benchOverheadItems)
	fns := benchWorkFns{
		buildChannels: func(inputs []int, workers int, workFn func(int) int) func(context.Context) int64 {
			return buildChannelsOneToMany(inputs, workers, workFn)
		},
		buildPipeline: func(inputs []int, workers int, workFn func(int) int) (func(context.Context) (int64, error), error) {
			return buildPipelineOneToMany(inputs, workers, workFn)
		},
	}

	runWorkSweep(b, inputs, benchStepWorkers, fns)
}

func BenchmarkFromChanWorkSweep(b *testing.B) {
	inputs := makeInputs(benchOverheadItems)
	fns := benchWorkFns{
		buildChannels: func(inputs []int, workers int, workFn func(int) int) func(context.Context) int64 {
			return buildChannelsFromChan(inputs, workers, workFn)
		},
		buildPipeline: func(inputs []int, workers int, workFn func(int) int) (func(context.Context) (int64, error), error) {
			return buildPipelineFromChan(inputs, workers, workFn)
		},
	}

	runWorkSweep(b, inputs, benchStepWorkers, fns)
}

func BenchmarkSinkWorkSweep(b *testing.B) {
	inputs := makeInputs(benchOverheadItems)
	fns := benchWorkFns{
		buildChannels: func(inputs []int, workers int, workFn func(int) int) func(context.Context) int64 {
			return buildChannelsSink(inputs, workers, workFn)
		},
		buildPipeline: func(inputs []int, workers int, workFn func(int) int) (func(context.Context) (int64, error), error) {
			return buildPipelineSink(inputs, workers, workFn)
		},
	}

	runWorkSweep(b, inputs, benchStepWorkers, fns)
}

func BenchmarkSinkFromChanWorkSweep(b *testing.B) {
	inputs := makeInputs(benchOverheadItems)
	fns := benchWorkFns{
		buildChannels: func(inputs []int, workers int, workFn func(int) int) func(context.Context) int64 {
			return buildChannelsSinkFromChan(inputs, workers, workFn)
		},
		buildPipeline: func(inputs []int, workers int, workFn func(int) int) (func(context.Context) (int64, error), error) {
			return buildPipelineSinkFromChan(inputs, workers, workFn)
		},
	}

	runWorkSweep(b, inputs, benchStepWorkers, fns)
}

func BenchmarkBatchWorkSweep(b *testing.B) {
	inputs := makeInputs(benchOverheadItems)
	fns := benchWorkFns{
		buildChannels: func(inputs []int, workers int, workFn func(int) int) func(context.Context) int64 {
			return buildChannelsBatch(inputs, workers, benchBatchSize, workFn)
		},
		buildPipeline: func(inputs []int, workers int, workFn func(int) int) (func(context.Context) (int64, error), error) {
			return buildPipelineBatch(inputs, workers, benchBatchSize, workFn)
		},
	}

	runWorkSweep(b, inputs, benchStepWorkers, fns)
}

func BenchmarkBatchChanWorkSweep(b *testing.B) {
	inputs := makeInputs(benchOverheadItems)
	fns := benchWorkFns{
		buildChannels: func(inputs []int, workers int, workFn func(int) int) func(context.Context) int64 {
			return buildChannelsBatchChan(inputs, workers, benchBatchSize, workFn)
		},
		buildPipeline: func(inputs []int, workers int, workFn func(int) int) (func(context.Context) (int64, error), error) {
			return buildPipelineBatchChan(inputs, workers, benchBatchSize, workFn)
		},
	}

	runWorkSweep(b, inputs, benchStepWorkers, fns)
}

func BenchmarkSplitByWorkSweep(b *testing.B) {
	inputs := makeInputs(benchOverheadItems)
	fns := benchWorkFns{
		buildChannels: func(inputs []int, workers int, workFn func(int) int) func(context.Context) int64 {
			return buildChannelsSplitBy(inputs, workers, workFn, workFn)
		},
		buildPipeline: func(inputs []int, workers int, workFn func(int) int) (func(context.Context) (int64, error), error) {
			return buildPipelineSplitBy(inputs, workers, workFn, workFn)
		},
	}

	runWorkSweep(b, inputs, benchStepWorkers, fns)
}

func BenchmarkOverheadStepSweep(b *testing.B) {
	inputs := makeInputs(benchOverheadItems)

	for _, steps := range benchStepCounts {
		caseName := "steps=" + strconv.Itoa(steps)

		b.Run(caseName+"/channels", func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()

			for range b.N {
				b.StopTimer()

				run := buildChannelsMultiStage(inputs, steps, benchStepWorkers, realisticWork)

				b.StartTimer()

				benchSink = run(b.Context())

				b.StopTimer()
			}
		})

		b.Run(caseName+"/pipeline", func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()

			for range b.N {
				b.StopTimer()

				run, err := buildPipelineMultiStage(inputs, steps, benchStepWorkers, realisticWork)
				if err != nil {
					b.Fatal(err)
				}

				b.StartTimer()

				sum, err := run(b.Context())
				if err != nil {
					b.Fatal(err)
				}

				b.StopTimer()

				benchSink = sum
			}
		})
	}
}

func normalizeWorkers(workers int) int {
	if workers < 1 {
		return 1
	}

	return workers
}

func stepOptions[O any](workers int) []pipeline.StepOption[O] {
	return []pipeline.StepOption[O]{pipeline.StepConcurrency[O](normalizeWorkers(workers))}
}

func sendInt(ctx context.Context, ch chan<- int, value int) bool {
	select {
	case <-ctx.Done():
		return false
	case ch <- value:
		return true
	}
}

func recvInt(ctx context.Context, ch <-chan int) (int, bool) {
	select {
	case <-ctx.Done():
		return 0, false
	case value, ok := <-ch:
		return value, ok
	}
}

func sendInts(ctx context.Context, ch chan<- []int, value []int) bool {
	select {
	case <-ctx.Done():
		return false
	case ch <- value:
		return true
	}
}

func recvInts(ctx context.Context, ch <-chan []int) ([]int, bool) {
	select {
	case <-ctx.Done():
		return nil, false
	case value, ok := <-ch:
		return value, ok
	}
}

func sendChanInt(ctx context.Context, ch chan<- chan int, value chan int) bool {
	select {
	case <-ctx.Done():
		return false
	case ch <- value:
		return true
	}
}

func recvChanInt(ctx context.Context, ch <-chan chan int) (chan int, bool) {
	select {
	case <-ctx.Done():
		return nil, false
	case value, ok := <-ch:
		return value, ok
	}
}

func buildChannelsOneToOne(inputs []int, workers int, fn func(int) int) func(context.Context) int64 {
	workers = normalizeWorkers(workers)
	in := make(chan int)
	out := make(chan int)

	return func(ctx context.Context) int64 {
		var wg sync.WaitGroup
		wg.Add(workers)

		for range workers {
			go func() {
				defer wg.Done()

				for {
					v, ok := recvInt(ctx, in)
					if !ok {
						return
					}

					if !sendInt(ctx, out, fn(v)) {
						return
					}
				}
			}()
		}

		go func() {
			wg.Wait()
			close(out)
		}()

		go func() {
			defer close(in)

			for _, v := range inputs {
				if !sendInt(ctx, in, v) {
					return
				}
			}
		}()

		var sum int64

		for {
			v, ok := recvInt(ctx, out)
			if !ok {
				break
			}

			sum += int64(v)
		}

		return sum
	}
}

//nolint:gocognit // Benchmark wiring keeps channel plumbing together.
func buildChannelsOneToOneOrZero(inputs []int, workers int, fn func(int) int) func(context.Context) int64 {
	workers = normalizeWorkers(workers)
	in := make(chan int)
	out := make(chan int)

	return func(ctx context.Context) int64 {
		var wg sync.WaitGroup
		wg.Add(workers)

		for range workers {
			go func() {
				defer wg.Done()

				for {
					v, ok := recvInt(ctx, in)
					if !ok {
						return
					}

					if v%2 == 0 {
						continue
					}

					if !sendInt(ctx, out, fn(v)) {
						return
					}
				}
			}()
		}

		go func() {
			wg.Wait()
			close(out)
		}()

		go func() {
			defer close(in)

			for _, v := range inputs {
				if !sendInt(ctx, in, v) {
					return
				}
			}
		}()

		var sum int64

		for {
			v, ok := recvInt(ctx, out)
			if !ok {
				break
			}

			sum += int64(v)
		}

		return sum
	}
}

//nolint:gocognit // Benchmark wiring keeps channel plumbing together.
func buildChannelsOneToMany(inputs []int, workers int, fn func(int) int) func(context.Context) int64 {
	workers = normalizeWorkers(workers)
	in := make(chan int)
	out := make(chan int)

	return func(ctx context.Context) int64 {
		var wg sync.WaitGroup
		wg.Add(workers)

		for range workers {
			go func() {
				defer wg.Done()

				for {
					v, ok := recvInt(ctx, in)
					if !ok {
						return
					}

					outVal := fn(v)
					if !sendInt(ctx, out, outVal) {
						return
					}

					if !sendInt(ctx, out, outVal+1) {
						return
					}
				}
			}()
		}

		go func() {
			wg.Wait()
			close(out)
		}()

		go func() {
			defer close(in)

			for _, v := range inputs {
				if !sendInt(ctx, in, v) {
					return
				}
			}
		}()

		var sum int64

		for {
			v, ok := recvInt(ctx, out)
			if !ok {
				break
			}

			sum += int64(v)
		}

		return sum
	}
}

func buildChannelsFromChan(inputs []int, workers int, fn func(int) int) func(context.Context) int64 {
	workers = normalizeWorkers(workers)
	in := make(chan int)
	out := make(chan int)

	return func(ctx context.Context) int64 {
		var wg sync.WaitGroup
		wg.Add(workers)

		for range workers {
			go func() {
				defer wg.Done()

				for {
					v, ok := recvInt(ctx, in)
					if !ok {
						return
					}

					if !sendInt(ctx, out, fn(v)) {
						return
					}
				}
			}()
		}

		go func() {
			wg.Wait()
			close(out)
		}()

		go func() {
			defer close(in)

			for _, v := range inputs {
				if !sendInt(ctx, in, v) {
					return
				}
			}
		}()

		var sum int64

		for {
			v, ok := recvInt(ctx, out)
			if !ok {
				break
			}

			sum += int64(v)
		}

		return sum
	}
}

func buildChannelsSink(inputs []int, workers int, fn func(int) int) func(context.Context) int64 {
	workers = normalizeWorkers(workers)
	in := make(chan int)

	return func(ctx context.Context) int64 {
		var sum int64
		var wg sync.WaitGroup
		wg.Add(workers)

		for range workers {
			go func() {
				defer wg.Done()

				for {
					v, ok := recvInt(ctx, in)
					if !ok {
						return
					}

					atomic.AddInt64(&sum, int64(fn(v)))
				}
			}()
		}

		go func() {
			defer close(in)

			for _, v := range inputs {
				if !sendInt(ctx, in, v) {
					return
				}
			}
		}()

		wg.Wait()

		return atomic.LoadInt64(&sum)
	}
}

func buildChannelsSinkFromChan(inputs []int, workers int, fn func(int) int) func(context.Context) int64 {
	workers = normalizeWorkers(workers)
	in := make(chan int)

	return func(ctx context.Context) int64 {
		var sum int64
		var wg sync.WaitGroup
		wg.Add(workers)

		for range workers {
			go func() {
				defer wg.Done()

				for {
					v, ok := recvInt(ctx, in)
					if !ok {
						return
					}

					atomic.AddInt64(&sum, int64(fn(v)))
				}
			}()
		}

		go func() {
			defer close(in)

			for _, v := range inputs {
				if !sendInt(ctx, in, v) {
					return
				}
			}
		}()

		wg.Wait()

		return atomic.LoadInt64(&sum)
	}
}

//nolint:gocognit // Benchmark wiring keeps channel plumbing together.
func buildChannelsBatch(inputs []int, workers int, batchSize int, fn func(int) int) func(context.Context) int64 {
	workers = normalizeWorkers(workers)

	if batchSize < 1 {
		batchSize = 1
	}

	in := make(chan int)
	out := make(chan []int)

	return func(ctx context.Context) int64 {
		var wg sync.WaitGroup
		wg.Add(workers)

		for range workers {
			go func() {
				defer wg.Done()

				batch := make([]int, 0, batchSize)

				flush := func() bool {
					if len(batch) == 0 {
						return true
					}

					if !sendInts(ctx, out, batch) {
						return false
					}

					batch = make([]int, 0, batchSize)

					return true
				}

				for {
					v, ok := recvInt(ctx, in)
					if !ok {
						flush()

						return
					}

					batch = append(batch, v)
					if len(batch) >= batchSize {
						if !flush() {
							return
						}
					}
				}
			}()
		}

		go func() {
			wg.Wait()
			close(out)
		}()

		go func() {
			defer close(in)

			for _, v := range inputs {
				if !sendInt(ctx, in, v) {
					return
				}
			}
		}()

		var sum int64

		for {
			batch, ok := recvInts(ctx, out)
			if !ok {
				break
			}

			for _, v := range batch {
				sum += int64(fn(v))
			}
		}

		return sum
	}
}

//nolint:gocognit,gocyclo // Benchmark wiring keeps channel plumbing together.
func buildChannelsBatchChan(inputs []int, workers int, batchSize int, fn func(int) int) func(context.Context) int64 {
	workers = normalizeWorkers(workers)

	if batchSize < 1 {
		batchSize = 1
	}

	in := make(chan int)
	out := make(chan chan int)

	return func(ctx context.Context) int64 {
		var wg sync.WaitGroup
		wg.Add(workers)

		for range workers {
			go func() {
				defer wg.Done()

				var batchCh chan int
				batchCount := 0

				closeBatch := func() {
					if batchCh == nil {
						return
					}

					close(batchCh)
					batchCh = nil
					batchCount = 0
				}

				for {
					v, ok := recvInt(ctx, in)
					if !ok {
						closeBatch()

						return
					}

					if batchCh == nil {
						batchCh = make(chan int)
						if !sendChanInt(ctx, out, batchCh) {
							closeBatch()

							return
						}
					}

					if !sendInt(ctx, batchCh, v) {
						closeBatch()

						return
					}

					batchCount++
					if batchCount >= batchSize {
						closeBatch()
					}
				}
			}()
		}

		go func() {
			wg.Wait()
			close(out)
		}()

		go func() {
			defer close(in)

			for _, v := range inputs {
				if !sendInt(ctx, in, v) {
					return
				}
			}
		}()

		var sum int64

		for {
			batchCh, ok := recvChanInt(ctx, out)
			if !ok {
				break
			}

			for {
				v, ok := recvInt(ctx, batchCh)
				if !ok {
					break
				}

				sum += int64(fn(v))
			}
		}

		return sum
	}
}

//nolint:gocognit // Benchmark wiring keeps channel plumbing together.
func buildChannelsTwoStage(
	inputs []int,
	workers int,
	stage1 func(int) int,
	stage2 func(int) int,
) func(context.Context) int64 {
	workers = normalizeWorkers(workers)
	in := make(chan int)
	mid := make(chan int)
	out := make(chan int)

	return func(ctx context.Context) int64 {
		var wg1 sync.WaitGroup
		wg1.Add(workers)

		var wg2 sync.WaitGroup
		wg2.Add(workers)

		for range workers {
			go func() {
				defer wg1.Done()

				for {
					v, ok := recvInt(ctx, in)
					if !ok {
						return
					}

					if !sendInt(ctx, mid, stage1(v)) {
						return
					}
				}
			}()
		}

		go func() {
			wg1.Wait()
			close(mid)
		}()

		for range workers {
			go func() {
				defer wg2.Done()

				for {
					v, ok := recvInt(ctx, mid)
					if !ok {
						return
					}

					if !sendInt(ctx, out, stage2(v)) {
						return
					}
				}
			}()
		}

		go func() {
			wg2.Wait()
			close(out)
		}()

		go func() {
			defer close(in)

			for _, v := range inputs {
				if !sendInt(ctx, in, v) {
					return
				}
			}
		}()

		var sum int64

		for {
			v, ok := recvInt(ctx, out)
			if !ok {
				break
			}

			sum += int64(v)
		}

		return sum
	}
}

//nolint:gocognit,gocyclo // Benchmark wiring keeps channel plumbing together.
func buildChannelsSplitMerge(
	inputs []int,
	workers int,
	leftFn func(int) int,
	rightFn func(int) int,
) func(context.Context) int64 {
	workers = normalizeWorkers(workers)
	in := make(chan int)
	left := make(chan int)
	right := make(chan int)
	leftOut := make(chan int)
	rightOut := make(chan int)
	out := make(chan int)

	return func(ctx context.Context) int64 {
		var wgLeft sync.WaitGroup
		wgLeft.Add(workers)

		var wgRight sync.WaitGroup
		wgRight.Add(workers)

		go func() {
			for {
				v, ok := recvInt(ctx, in)
				if !ok {
					break
				}

				if !sendInt(ctx, left, v) {
					break
				}

				if !sendInt(ctx, right, v) {
					break
				}
			}

			close(left)
			close(right)
		}()

		for range workers {
			go func() {
				defer wgLeft.Done()

				for {
					v, ok := recvInt(ctx, left)
					if !ok {
						return
					}

					if !sendInt(ctx, leftOut, leftFn(v)) {
						return
					}
				}
			}()
		}

		go func() {
			wgLeft.Wait()
			close(leftOut)
		}()

		for range workers {
			go func() {
				defer wgRight.Done()

				for {
					v, ok := recvInt(ctx, right)
					if !ok {
						return
					}

					if !sendInt(ctx, rightOut, rightFn(v)) {
						return
					}
				}
			}()
		}

		go func() {
			wgRight.Wait()
			close(rightOut)
		}()

		go func() {
			var wg sync.WaitGroup
			wg.Add(2)

			go func() {
				defer wg.Done()

				for {
					v, ok := recvInt(ctx, leftOut)
					if !ok {
						return
					}

					if !sendInt(ctx, out, v) {
						return
					}
				}
			}()

			go func() {
				defer wg.Done()

				for {
					v, ok := recvInt(ctx, rightOut)
					if !ok {
						return
					}

					if !sendInt(ctx, out, v) {
						return
					}
				}
			}()

			wg.Wait()
			close(out)
		}()

		go func() {
			defer close(in)

			for _, v := range inputs {
				if !sendInt(ctx, in, v) {
					return
				}
			}
		}()

		var sum int64

		for {
			v, ok := recvInt(ctx, out)
			if !ok {
				break
			}

			sum += int64(v)
		}

		return sum
	}
}

//nolint:gocognit,gocyclo // Benchmark wiring keeps channel plumbing together.
func buildChannelsSplitBy(
	inputs []int,
	workers int,
	leftFn func(int) int,
	rightFn func(int) int,
) func(context.Context) int64 {
	workers = normalizeWorkers(workers)
	in := make(chan int)
	left := make(chan int)
	right := make(chan int)
	leftOut := make(chan int)
	rightOut := make(chan int)
	out := make(chan int)

	return func(ctx context.Context) int64 {
		var wgLeft sync.WaitGroup
		wgLeft.Add(workers)

		var wgRight sync.WaitGroup
		wgRight.Add(workers)

		go func() {
			for {
				v, ok := recvInt(ctx, in)
				if !ok {
					break
				}

				if v%2 == 0 {
					if !sendInt(ctx, left, v) {
						break
					}

					continue
				}

				if !sendInt(ctx, right, v) {
					break
				}
			}

			close(left)
			close(right)
		}()

		for range workers {
			go func() {
				defer wgLeft.Done()

				for {
					v, ok := recvInt(ctx, left)
					if !ok {
						return
					}

					if !sendInt(ctx, leftOut, leftFn(v)) {
						return
					}
				}
			}()
		}

		go func() {
			wgLeft.Wait()
			close(leftOut)
		}()

		for range workers {
			go func() {
				defer wgRight.Done()

				for {
					v, ok := recvInt(ctx, right)
					if !ok {
						return
					}

					if !sendInt(ctx, rightOut, rightFn(v)) {
						return
					}
				}
			}()
		}

		go func() {
			wgRight.Wait()
			close(rightOut)
		}()

		go func() {
			var wg sync.WaitGroup
			wg.Add(2)

			go func() {
				defer wg.Done()

				for {
					v, ok := recvInt(ctx, leftOut)
					if !ok {
						return
					}

					if !sendInt(ctx, out, v) {
						return
					}
				}
			}()

			go func() {
				defer wg.Done()

				for {
					v, ok := recvInt(ctx, rightOut)
					if !ok {
						return
					}

					if !sendInt(ctx, out, v) {
						return
					}
				}
			}()

			wg.Wait()
			close(out)
		}()

		go func() {
			defer close(in)

			for _, v := range inputs {
				if !sendInt(ctx, in, v) {
					return
				}
			}
		}()

		var sum int64

		for {
			v, ok := recvInt(ctx, out)
			if !ok {
				break
			}

			sum += int64(v)
		}

		return sum
	}
}

//nolint:gocognit // Benchmark wiring keeps channel plumbing together.
func buildChannelsMultiStage(
	inputs []int,
	steps int,
	workers int,
	fn func(int) int,
) func(context.Context) int64 {
	workers = normalizeWorkers(workers)
	first := make(chan int)
	in := first
	stages := make([]struct {
		in  <-chan int
		out chan int
	}, 0, steps)

	for range steps {
		out := make(chan int)
		stages = append(stages, struct {
			in  <-chan int
			out chan int
		}{
			in:  in,
			out: out,
		})
		in = out
	}

	return func(ctx context.Context) int64 {
		for _, stage := range stages {
			var wg sync.WaitGroup
			wg.Add(workers)

			for range workers {
				go func(in <-chan int, out chan<- int) {
					defer wg.Done()

					for {
						v, ok := recvInt(ctx, in)
						if !ok {
							return
						}

						if !sendInt(ctx, out, fn(v)) {
							return
						}
					}
				}(stage.in, stage.out)
			}

			go func(out chan int) {
				wg.Wait()
				close(out)
			}(stage.out)
		}

		go func() {
			defer close(first)

			for _, v := range inputs {
				if !sendInt(ctx, first, v) {
					return
				}
			}
		}()

		var sum int64

		for {
			v, ok := recvInt(ctx, in)
			if !ok {
				break
			}

			sum += int64(v)
		}

		return sum
	}
}

func buildPipelineOneToOne(inputs []int, workers int, fn func(int) int) (func(context.Context) (int64, error), error) {
	pipe, err := pipeline.New()
	if err != nil {
		return nil, err
	}

	root := pipeline.Root(pipe, "root", func(ctx context.Context, out chan<- int) error {
		for _, v := range inputs {
			out <- v
		}

		return nil
	})
	if root == nil {
		return nil, pipe.Err()
	}

	step := pipeline.OneToOne(pipe, "one", root, func(ctx context.Context, in int) (int, error) {
		return fn(in), nil
	}, stepOptions[int](workers)...)
	if step == nil {
		return nil, pipe.Err()
	}

	var sum int64

	sink := pipeline.Sink(pipe, "sink", step, func(ctx context.Context, in int) error {
		sum += int64(in)

		return nil
	})
	if sink == nil {
		return nil, pipe.Err()
	}

	return func(ctx context.Context) (int64, error) {
		err := pipe.Run(ctx)
		if err != nil {
			return 0, err
		}

		return sum, nil
	}, nil
}

func buildPipelineOneToOneOrZero(inputs []int, workers int, fn func(int) int) (func(context.Context) (int64, error), error) {
	pipe, err := pipeline.New()
	if err != nil {
		return nil, err
	}

	root := pipeline.Root(pipe, "root", func(ctx context.Context, out chan<- int) error {
		for _, v := range inputs {
			out <- v
		}

		return nil
	})
	if root == nil {
		return nil, pipe.Err()
	}

	step := pipeline.OneToOneOrZero(pipe, "one-or-zero", root, func(ctx context.Context, in int) (int, error) {
		if in%2 == 0 {
			return 0, nil
		}

		return fn(in), nil
	}, stepOptions[int](workers)...)
	if step == nil {
		return nil, pipe.Err()
	}

	var sum int64

	sink := pipeline.Sink(pipe, "sink", step, func(ctx context.Context, in int) error {
		sum += int64(in)

		return nil
	})
	if sink == nil {
		return nil, pipe.Err()
	}

	return func(ctx context.Context) (int64, error) {
		err := pipe.Run(ctx)
		if err != nil {
			return 0, err
		}

		return sum, nil
	}, nil
}

func buildPipelineOneToMany(inputs []int, workers int, fn func(int) int) (func(context.Context) (int64, error), error) {
	pipe, err := pipeline.New()
	if err != nil {
		return nil, err
	}

	root := pipeline.Root(pipe, "root", func(ctx context.Context, out chan<- int) error {
		for _, v := range inputs {
			out <- v
		}

		return nil
	})
	if root == nil {
		return nil, pipe.Err()
	}

	step := pipeline.OneToMany(pipe, "one-to-many", root, func(ctx context.Context, in int) ([]int, error) {
		outVal := fn(in)

		return []int{outVal, outVal + 1}, nil
	}, stepOptions[int](workers)...)
	if step == nil {
		return nil, pipe.Err()
	}

	var sum int64

	sink := pipeline.Sink(pipe, "sink", step, func(ctx context.Context, in int) error {
		sum += int64(in)

		return nil
	})
	if sink == nil {
		return nil, pipe.Err()
	}

	return func(ctx context.Context) (int64, error) {
		err := pipe.Run(ctx)
		if err != nil {
			return 0, err
		}

		return sum, nil
	}, nil
}

func buildPipelineFromChan(inputs []int, workers int, fn func(int) int) (func(context.Context) (int64, error), error) {
	pipe, err := pipeline.New()
	if err != nil {
		return nil, err
	}

	root := pipeline.Root(pipe, "root", func(ctx context.Context, out chan<- int) error {
		for _, v := range inputs {
			out <- v
		}

		return nil
	})
	if root == nil {
		return nil, pipe.Err()
	}

	step := pipeline.FromChan(pipe, "from", root, func(ctx context.Context, input <-chan int, output chan int) error {
		for v := range input {
			output <- fn(v)
		}

		return nil
	}, stepOptions[int](workers)...)
	if step == nil {
		return nil, pipe.Err()
	}

	var sum int64

	sink := pipeline.Sink(pipe, "sink", step, func(ctx context.Context, in int) error {
		sum += int64(in)

		return nil
	})
	if sink == nil {
		return nil, pipe.Err()
	}

	return func(ctx context.Context) (int64, error) {
		err := pipe.Run(ctx)
		if err != nil {
			return 0, err
		}

		return sum, nil
	}, nil
}

func buildPipelineSink(inputs []int, workers int, fn func(int) int) (func(context.Context) (int64, error), error) {
	pipe, err := pipeline.New()
	if err != nil {
		return nil, err
	}

	root := pipeline.Root(pipe, "root", func(ctx context.Context, out chan<- int) error {
		for _, v := range inputs {
			out <- v
		}

		return nil
	})
	if root == nil {
		return nil, pipe.Err()
	}

	var sum int64

	sink := pipeline.Sink(pipe, "sink", root, func(ctx context.Context, in int) error {
		atomic.AddInt64(&sum, int64(fn(in)))

		return nil
	}, stepOptions[int](workers)...)
	if sink == nil {
		return nil, pipe.Err()
	}

	return func(ctx context.Context) (int64, error) {
		err := pipe.Run(ctx)
		if err != nil {
			return 0, err
		}

		return atomic.LoadInt64(&sum), nil
	}, nil
}

func buildPipelineSinkFromChan(inputs []int, workers int, fn func(int) int) (func(context.Context) (int64, error), error) {
	pipe, err := pipeline.New()
	if err != nil {
		return nil, err
	}

	root := pipeline.Root(pipe, "root", func(ctx context.Context, out chan<- int) error {
		for _, v := range inputs {
			out <- v
		}

		return nil
	})
	if root == nil {
		return nil, pipe.Err()
	}

	var sum int64

	sink := pipeline.SinkFromChan(pipe, "sink", root, func(ctx context.Context, input <-chan int) error {
		for v := range input {
			atomic.AddInt64(&sum, int64(fn(v)))
		}

		return nil
	}, stepOptions[int](workers)...)
	if sink == nil {
		return nil, pipe.Err()
	}

	return func(ctx context.Context) (int64, error) {
		err := pipe.Run(ctx)
		if err != nil {
			return 0, err
		}

		return atomic.LoadInt64(&sum), nil
	}, nil
}

func buildPipelineBatch(
	inputs []int,
	workers int,
	batchSize int,
	fn func(int) int,
) (func(context.Context) (int64, error), error) {
	pipe, err := pipeline.New()
	if err != nil {
		return nil, err
	}

	root := pipeline.Root(pipe, "root", func(ctx context.Context, out chan<- int) error {
		for _, v := range inputs {
			out <- v
		}

		return nil
	})
	if root == nil {
		return nil, pipe.Err()
	}

	policy := pipeline.BatchPolicy{
		MaxSize: batchSize,
		MaxWait: 0,
	}

	step := pipeline.Batch(pipe, "batch", root, policy, stepOptions[[]int](workers)...)
	if step == nil {
		return nil, pipe.Err()
	}

	var sum int64

	sink := pipeline.Sink(pipe, "sink", step, func(ctx context.Context, in []int) error {
		for _, v := range in {
			sum += int64(fn(v))
		}

		return nil
	})
	if sink == nil {
		return nil, pipe.Err()
	}

	return func(ctx context.Context) (int64, error) {
		err := pipe.Run(ctx)
		if err != nil {
			return 0, err
		}

		return sum, nil
	}, nil
}

func buildPipelineBatchChan(
	inputs []int,
	workers int,
	batchSize int,
	fn func(int) int,
) (func(context.Context) (int64, error), error) {
	pipe, err := pipeline.New()
	if err != nil {
		return nil, err
	}

	root := pipeline.Root(pipe, "root", func(ctx context.Context, out chan<- int) error {
		for _, v := range inputs {
			out <- v
		}

		return nil
	})
	if root == nil {
		return nil, pipe.Err()
	}

	policy := pipeline.BatchPolicy{
		MaxSize: batchSize,
		MaxWait: 0,
	}

	step := pipeline.BatchChan(pipe, "batch", root, policy, stepOptions[<-chan int](workers)...)
	if step == nil {
		return nil, pipe.Err()
	}

	var sum int64

	sink := pipeline.Sink(pipe, "sink", step, func(ctx context.Context, in <-chan int) error {
		for v := range in {
			sum += int64(fn(v))
		}

		return nil
	})
	if sink == nil {
		return nil, pipe.Err()
	}

	return func(ctx context.Context) (int64, error) {
		err := pipe.Run(ctx)
		if err != nil {
			return 0, err
		}

		return sum, nil
	}, nil
}

func buildPipelineTwoStage(
	inputs []int,
	workers int,
	stage1 func(int) int,
	stage2 func(int) int,
) (func(context.Context) (int64, error), error) {
	pipe, err := pipeline.New()
	if err != nil {
		return nil, err
	}

	root := pipeline.Root(pipe, "root", func(ctx context.Context, out chan<- int) error {
		for _, v := range inputs {
			out <- v
		}

		return nil
	})
	if root == nil {
		return nil, pipe.Err()
	}

	step1 := pipeline.OneToOne(pipe, "stage-1", root, func(ctx context.Context, in int) (int, error) {
		return stage1(in), nil
	}, stepOptions[int](workers)...)
	if step1 == nil {
		return nil, pipe.Err()
	}

	step2 := pipeline.OneToOne(pipe, "stage-2", step1, func(ctx context.Context, in int) (int, error) {
		return stage2(in), nil
	}, stepOptions[int](workers)...)
	if step2 == nil {
		return nil, pipe.Err()
	}

	var sum int64

	sink := pipeline.Sink(pipe, "sink", step2, func(ctx context.Context, in int) error {
		sum += int64(in)

		return nil
	})
	if sink == nil {
		return nil, pipe.Err()
	}

	return func(ctx context.Context) (int64, error) {
		err := pipe.Run(ctx)
		if err != nil {
			return 0, err
		}

		return sum, nil
	}, nil
}

func buildPipelineSplitMerge(
	inputs []int,
	workers int,
	leftFn func(int) int,
	rightFn func(int) int,
) (func(context.Context) (int64, error), error) {
	pipe, err := pipeline.New()
	if err != nil {
		return nil, err
	}

	root := pipeline.Root(pipe, "root", func(ctx context.Context, out chan<- int) error {
		for _, v := range inputs {
			out <- v
		}

		return nil
	})
	if root == nil {
		return nil, pipe.Err()
	}

	splitter := pipeline.Split(pipe, "split", root, 2)
	if splitter == nil {
		return nil, pipe.Err()
	}

	left, _ := splitter.Get()
	right, _ := splitter.Get()

	leftStep := pipeline.OneToOne(pipe, "left", left, func(ctx context.Context, in int) (int, error) {
		return leftFn(in), nil
	}, stepOptions[int](workers)...)
	if leftStep == nil {
		return nil, pipe.Err()
	}

	rightStep := pipeline.OneToOne(pipe, "right", right, func(ctx context.Context, in int) (int, error) {
		return rightFn(in), nil
	}, stepOptions[int](workers)...)
	if rightStep == nil {
		return nil, pipe.Err()
	}

	merged := pipeline.Merge(pipe, "merge", leftStep, rightStep)
	if merged == nil {
		return nil, pipe.Err()
	}

	var sum int64

	sink := pipeline.Sink(pipe, "sink", merged, func(ctx context.Context, in int) error {
		sum += int64(in)

		return nil
	})
	if sink == nil {
		return nil, pipe.Err()
	}

	return func(ctx context.Context) (int64, error) {
		err := pipe.Run(ctx)
		if err != nil {
			return 0, err
		}

		return sum, nil
	}, nil
}

func buildPipelineSplitBy(
	inputs []int,
	workers int,
	leftFn func(int) int,
	rightFn func(int) int,
) (func(context.Context) (int64, error), error) {
	pipe, err := pipeline.New()
	if err != nil {
		return nil, err
	}

	root := pipeline.Root(pipe, "root", func(ctx context.Context, out chan<- int) error {
		for _, v := range inputs {
			out <- v
		}

		return nil
	})
	if root == nil {
		return nil, pipe.Err()
	}

	routes := []pipeline.SplitFn[int]{
		func(ctx context.Context, input int) (bool, error) {
			return input%2 == 0, nil
		},
		func(ctx context.Context, input int) (bool, error) {
			return input%2 != 0, nil
		},
	}

	splitter := pipeline.SplitBy(pipe, "split", root, routes)
	if splitter == nil {
		return nil, pipe.Err()
	}

	left, _ := splitter.Get()
	right, _ := splitter.Get()

	leftStep := pipeline.OneToOne(pipe, "left", left, func(ctx context.Context, in int) (int, error) {
		return leftFn(in), nil
	}, stepOptions[int](workers)...)
	if leftStep == nil {
		return nil, pipe.Err()
	}

	rightStep := pipeline.OneToOne(pipe, "right", right, func(ctx context.Context, in int) (int, error) {
		return rightFn(in), nil
	}, stepOptions[int](workers)...)
	if rightStep == nil {
		return nil, pipe.Err()
	}

	merged := pipeline.Merge(pipe, "merge", leftStep, rightStep)
	if merged == nil {
		return nil, pipe.Err()
	}

	var sum int64

	sink := pipeline.Sink(pipe, "sink", merged, func(ctx context.Context, in int) error {
		sum += int64(in)

		return nil
	})
	if sink == nil {
		return nil, pipe.Err()
	}

	return func(ctx context.Context) (int64, error) {
		err := pipe.Run(ctx)
		if err != nil {
			return 0, err
		}

		return sum, nil
	}, nil
}

func buildPipelineMultiStage(
	inputs []int,
	steps int,
	workers int,
	fn func(int) int,
) (func(context.Context) (int64, error), error) {
	pipe, err := pipeline.New()
	if err != nil {
		return nil, err
	}

	root := pipeline.Root(pipe, "root", func(ctx context.Context, out chan<- int) error {
		for _, v := range inputs {
			out <- v
		}

		return nil
	})
	if root == nil {
		return nil, pipe.Err()
	}

	prev := root

	for i := range steps {
		name := "step-" + strconv.Itoa(i+1)
		step := pipeline.OneToOne(pipe, name, prev, func(ctx context.Context, in int) (int, error) {
			return fn(in), nil
		}, stepOptions[int](workers)...)

		if step == nil {
			return nil, pipe.Err()
		}

		prev = step
	}

	var sum int64

	sink := pipeline.Sink(pipe, "sink", prev, func(ctx context.Context, in int) error {
		sum += int64(in)

		return nil
	})
	if sink == nil {
		return nil, pipe.Err()
	}

	return func(ctx context.Context) (int64, error) {
		err := pipe.Run(ctx)
		if err != nil {
			return 0, err
		}

		return sum, nil
	}, nil
}
