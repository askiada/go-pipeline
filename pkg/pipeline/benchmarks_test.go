package pipeline_test

import (
	"context"
	"strconv"
	"sync"
	"testing"

	"github.com/askiada/go-pipeline/pkg/pipeline"
)

const (
	benchItems         = 1024
	benchBatchSize     = 32
	benchWorkIters     = 64
	benchOverheadItems = 4096
	benchStepWorkers   = 1
	benchStepWorkIters = 0
)

var (
	benchWorkSweep  = []int{0, 4, 16, 64, 256}
	benchStepCounts = []int{1, 2, 4, 8}
)

type benchCase struct {
	name    string
	items   int
	workers int
}

var benchCases = []benchCase{
	{name: "items=1k/conc=1", items: 1024, workers: 1},
	{name: "items=1k/conc=4", items: 1024, workers: 4},
	{name: "items=16k/conc=4", items: 16384, workers: 4},
}

var benchSink int64

func makeInputs(n int) []int {
	inputs := make([]int, n)
	for i := range inputs {
		inputs[i] = i
	}

	return inputs
}

func simpleIdentity(v int) int {
	return v
}

func simpleAdd1(v int) int {
	return v + 1
}

func simpleMul2(v int) int {
	return v * 2
}

func simpleAdd10(v int) int {
	return v + 10
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

func realisticWorkAlt(v int) int {
	return realisticWork(v + 1)
}

func workWithIters(v int, iters int) int {
	x := uint64(v) + 0x9e3779b97f4a7c15
	for range iters {
		x ^= x << 7
		x ^= x >> 9
		x *= 0x9e3779b97f4a7c15
	}

	return int(x)
}

func makeWorkFn(iters int) func(int) int {
	return func(v int) int {
		return workWithIters(v, iters)
	}
}

func BenchmarkOneToOne(b *testing.B) {
	for _, tc := range benchCases {
		inputs := makeInputs(tc.items)

		b.Run(tc.name+"/loop", func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()

			for range b.N {
				benchSink = runLoopOneToOne(inputs, simpleAdd1)
			}
		})

		b.Run(tc.name+"/channels", func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()

			for range b.N {
				benchSink = runChannelOneToOne(inputs, tc.workers, simpleAdd1)
			}
		})

		b.Run(tc.name+"/pipeline", func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()

			for range b.N {
				sum, err := runPipelineOneToOne(inputs, tc.workers, simpleAdd1)
				if err != nil {
					b.Fatal(err)
				}

				benchSink = sum
			}
		})
	}
}

func BenchmarkOneToOneRealistic(b *testing.B) {
	for _, tc := range benchCases {
		inputs := makeInputs(tc.items)

		b.Run(tc.name+"/loop", func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()

			for range b.N {
				benchSink = runLoopOneToOne(inputs, realisticWork)
			}
		})

		b.Run(tc.name+"/channels", func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()

			for range b.N {
				benchSink = runChannelOneToOne(inputs, tc.workers, realisticWork)
			}
		})

		b.Run(tc.name+"/pipeline", func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()

			for range b.N {
				sum, err := runPipelineOneToOne(inputs, tc.workers, realisticWork)
				if err != nil {
					b.Fatal(err)
				}

				benchSink = sum
			}
		})
	}
}

func BenchmarkTwoStage(b *testing.B) {
	for _, tc := range benchCases {
		inputs := makeInputs(tc.items)

		b.Run(tc.name+"/loop", func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()

			for range b.N {
				benchSink = runLoopTwoStage(inputs, simpleAdd1, simpleMul2)
			}
		})

		b.Run(tc.name+"/channels", func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()

			for range b.N {
				benchSink = runChannelTwoStage(inputs, tc.workers, simpleAdd1, simpleMul2)
			}
		})

		b.Run(tc.name+"/pipeline", func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()

			for range b.N {
				sum, err := runPipelineTwoStage(inputs, tc.workers, simpleAdd1, simpleMul2)
				if err != nil {
					b.Fatal(err)
				}

				benchSink = sum
			}
		})
	}
}

func BenchmarkTwoStageRealistic(b *testing.B) {
	for _, tc := range benchCases {
		inputs := makeInputs(tc.items)

		b.Run(tc.name+"/loop", func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()

			for range b.N {
				benchSink = runLoopTwoStage(inputs, realisticWork, realisticWorkAlt)
			}
		})

		b.Run(tc.name+"/channels", func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()

			for range b.N {
				benchSink = runChannelTwoStage(inputs, tc.workers, realisticWork, realisticWorkAlt)
			}
		})

		b.Run(tc.name+"/pipeline", func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()

			for range b.N {
				sum, err := runPipelineTwoStage(inputs, tc.workers, realisticWork, realisticWorkAlt)
				if err != nil {
					b.Fatal(err)
				}

				benchSink = sum
			}
		})
	}
}

func BenchmarkSplitMerge(b *testing.B) {
	for _, tc := range benchCases {
		inputs := makeInputs(tc.items)

		b.Run(tc.name+"/loop", func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()

			for range b.N {
				benchSink = runLoopSplitMerge(inputs, simpleAdd1, simpleAdd10)
			}
		})

		b.Run(tc.name+"/channels", func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()

			for range b.N {
				benchSink = runChannelSplitMerge(inputs, tc.workers, simpleAdd1, simpleAdd10)
			}
		})

		b.Run(tc.name+"/pipeline", func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()

			for range b.N {
				sum, err := runPipelineSplitMerge(inputs, tc.workers, simpleAdd1, simpleAdd10)
				if err != nil {
					b.Fatal(err)
				}

				benchSink = sum
			}
		})
	}
}

func BenchmarkSplitMergeRealistic(b *testing.B) {
	for _, tc := range benchCases {
		inputs := makeInputs(tc.items)

		b.Run(tc.name+"/loop", func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()

			for range b.N {
				benchSink = runLoopSplitMerge(inputs, realisticWork, realisticWorkAlt)
			}
		})

		b.Run(tc.name+"/channels", func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()

			for range b.N {
				benchSink = runChannelSplitMerge(inputs, tc.workers, realisticWork, realisticWorkAlt)
			}
		})

		b.Run(tc.name+"/pipeline", func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()

			for range b.N {
				sum, err := runPipelineSplitMerge(inputs, tc.workers, realisticWork, realisticWorkAlt)
				if err != nil {
					b.Fatal(err)
				}

				benchSink = sum
			}
		})
	}
}

func BenchmarkOneToManyPipeline(b *testing.B) {
	inputs := makeInputs(benchItems)

	b.ReportAllocs()
	b.ResetTimer()

	for range b.N {
		sum, err := runPipelineOneToMany(inputs, 4, simpleIdentity, simpleAdd1)
		if err != nil {
			b.Fatal(err)
		}

		benchSink = sum
	}
}

func BenchmarkOneToManyPipelineRealistic(b *testing.B) {
	inputs := makeInputs(benchItems)

	b.ReportAllocs()
	b.ResetTimer()

	for range b.N {
		sum, err := runPipelineOneToMany(inputs, 4, realisticWork, realisticWorkAlt)
		if err != nil {
			b.Fatal(err)
		}

		benchSink = sum
	}
}

func BenchmarkFromChanPipeline(b *testing.B) {
	inputs := makeInputs(benchItems)

	b.ReportAllocs()
	b.ResetTimer()

	for range b.N {
		sum, err := runPipelineFromChan(inputs, simpleAdd1)
		if err != nil {
			b.Fatal(err)
		}

		benchSink = sum
	}
}

func BenchmarkFromChanPipelineRealistic(b *testing.B) {
	inputs := makeInputs(benchItems)

	b.ReportAllocs()
	b.ResetTimer()

	for range b.N {
		sum, err := runPipelineFromChan(inputs, realisticWork)
		if err != nil {
			b.Fatal(err)
		}

		benchSink = sum
	}
}

func BenchmarkSinkFromChanPipeline(b *testing.B) {
	inputs := makeInputs(benchItems)

	b.ReportAllocs()
	b.ResetTimer()

	for range b.N {
		sum, err := runPipelineSinkFromChan(inputs, simpleIdentity)
		if err != nil {
			b.Fatal(err)
		}

		benchSink = sum
	}
}

func BenchmarkSinkFromChanPipelineRealistic(b *testing.B) {
	inputs := makeInputs(benchItems)

	b.ReportAllocs()
	b.ResetTimer()

	for range b.N {
		sum, err := runPipelineSinkFromChan(inputs, realisticWork)
		if err != nil {
			b.Fatal(err)
		}

		benchSink = sum
	}
}

func BenchmarkBatchPipeline(b *testing.B) {
	inputs := makeInputs(benchItems)

	b.ReportAllocs()
	b.ResetTimer()

	for range b.N {
		sum, err := runPipelineBatch(inputs, benchBatchSize, simpleIdentity)
		if err != nil {
			b.Fatal(err)
		}

		benchSink = sum
	}
}

func BenchmarkBatchPipelineRealistic(b *testing.B) {
	inputs := makeInputs(benchItems)

	b.ReportAllocs()
	b.ResetTimer()

	for range b.N {
		sum, err := runPipelineBatch(inputs, benchBatchSize, realisticWork)
		if err != nil {
			b.Fatal(err)
		}

		benchSink = sum
	}
}

func BenchmarkBatchChanPipeline(b *testing.B) {
	inputs := makeInputs(benchItems)

	b.ReportAllocs()
	b.ResetTimer()

	for range b.N {
		sum, err := runPipelineBatchChan(inputs, benchBatchSize, simpleIdentity)
		if err != nil {
			b.Fatal(err)
		}

		benchSink = sum
	}
}

func BenchmarkBatchChanPipelineRealistic(b *testing.B) {
	inputs := makeInputs(benchItems)

	b.ReportAllocs()
	b.ResetTimer()

	for range b.N {
		sum, err := runPipelineBatchChan(inputs, benchBatchSize, realisticWork)
		if err != nil {
			b.Fatal(err)
		}

		benchSink = sum
	}
}

func BenchmarkOverheadWorkSweep(b *testing.B) {
	inputs := makeInputs(benchOverheadItems)

	for _, iters := range benchWorkSweep {
		workFn := makeWorkFn(iters)
		caseName := "iters=" + strconv.Itoa(iters)

		b.Run(caseName+"/loop", func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()

			for range b.N {
				benchSink = runLoopOneToOne(inputs, workFn)
			}
		})

		b.Run(caseName+"/pipeline", func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()

			for range b.N {
				sum, err := runPipelineOneToOne(inputs, benchStepWorkers, workFn)
				if err != nil {
					b.Fatal(err)
				}

				benchSink = sum
			}
		})
	}
}

func BenchmarkOverheadStepScaling(b *testing.B) {
	inputs := makeInputs(benchOverheadItems)
	workFn := makeWorkFn(benchStepWorkIters)

	for _, steps := range benchStepCounts {
		caseName := "steps=" + strconv.Itoa(steps)

		b.Run(caseName+"/loop", func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()

			for range b.N {
				benchSink = runLoopMultiStage(inputs, steps, workFn)
			}
		})

		b.Run(caseName+"/pipeline", func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()

			for range b.N {
				sum, err := runPipelineMultiStage(inputs, steps, benchStepWorkers, workFn)
				if err != nil {
					b.Fatal(err)
				}

				benchSink = sum
			}
		})
	}
}

func runLoopOneToOne(inputs []int, fn func(int) int) int64 {
	var sum int64
	for _, v := range inputs {
		sum += int64(fn(v))
	}

	return sum
}

func runLoopTwoStage(inputs []int, stage1 func(int) int, stage2 func(int) int) int64 {
	var sum int64
	for _, v := range inputs {
		sum += int64(stage2(stage1(v)))
	}

	return sum
}

func runLoopSplitMerge(inputs []int, leftFn func(int) int, rightFn func(int) int) int64 {
	var sum int64
	for _, v := range inputs {
		sum += int64(leftFn(v))
		sum += int64(rightFn(v))
	}

	return sum
}

func runLoopMultiStage(inputs []int, steps int, fn func(int) int) int64 {
	var sum int64

	for _, v := range inputs {
		value := v
		for range steps {
			value = fn(value)
		}

		sum += int64(value)
	}

	return sum
}

func runChannelOneToOne(inputs []int, workers int, fn func(int) int) int64 {
	in := make(chan int)
	out := make(chan int)

	var wg sync.WaitGroup
	wg.Add(workers)

	for range workers {
		go func() {
			defer wg.Done()

			for v := range in {
				out <- fn(v)
			}
		}()
	}

	go func() {
		wg.Wait()
		close(out)
	}()

	go func() {
		for _, v := range inputs {
			in <- v
		}

		close(in)
	}()

	var sum int64
	for v := range out {
		sum += int64(v)
	}

	return sum
}

func runChannelTwoStage(inputs []int, workers int, stage1 func(int) int, stage2 func(int) int) int64 {
	in := make(chan int)
	mid := make(chan int)
	out := make(chan int)

	var wg1 sync.WaitGroup
	wg1.Add(workers)

	for range workers {
		go func() {
			defer wg1.Done()

			for v := range in {
				mid <- stage1(v)
			}
		}()
	}

	go func() {
		wg1.Wait()
		close(mid)
	}()

	var wg2 sync.WaitGroup
	wg2.Add(workers)

	for range workers {
		go func() {
			defer wg2.Done()

			for v := range mid {
				out <- stage2(v)
			}
		}()
	}

	go func() {
		wg2.Wait()
		close(out)
	}()

	go func() {
		for _, v := range inputs {
			in <- v
		}

		close(in)
	}()

	var sum int64
	for v := range out {
		sum += int64(v)
	}

	return sum
}

func sendToBoth(left chan int, right chan int, value int) {
	left <- value

	right <- value
}

func runChannelSplitMerge(inputs []int, workers int, leftFn func(int) int, rightFn func(int) int) int64 {
	in := make(chan int)
	left := make(chan int)
	right := make(chan int)
	leftOut := make(chan int)
	rightOut := make(chan int)
	out := make(chan int)

	go func() {
		for v := range in {
			sendToBoth(left, right, v)
		}

		close(left)
		close(right)
	}()

	var wgLeft sync.WaitGroup
	wgLeft.Add(workers)

	for range workers {
		go func() {
			defer wgLeft.Done()

			for v := range left {
				leftOut <- leftFn(v)
			}
		}()
	}

	go func() {
		wgLeft.Wait()
		close(leftOut)
	}()

	var wgRight sync.WaitGroup
	wgRight.Add(workers)

	for range workers {
		go func() {
			defer wgRight.Done()

			for v := range right {
				rightOut <- rightFn(v)
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

			for v := range leftOut {
				out <- v
			}
		}()

		go func() {
			defer wg.Done()

			for v := range rightOut {
				out <- v
			}
		}()

		wg.Wait()
		close(out)
	}()

	go func() {
		for _, v := range inputs {
			in <- v
		}

		close(in)
	}()

	var sum int64
	for v := range out {
		sum += int64(v)
	}

	return sum
}

func runPipelineOneToOne(inputs []int, workers int, fn func(int) int) (int64, error) {
	pipe, err := pipeline.New()
	if err != nil {
		return 0, err
	}

	root := pipeline.Root(pipe, "root", func(ctx context.Context, out chan<- int) error {
		for _, v := range inputs {
			out <- v
		}

		return nil
	})
	if root == nil {
		return 0, pipe.Err()
	}

	step := pipeline.OneToOne(pipe, "one", root, func(ctx context.Context, in int) (int, error) {
		return fn(in), nil
	}, pipeline.StepConcurrency[int](workers))
	if step == nil {
		return 0, pipe.Err()
	}

	var sum int64
	sink := pipeline.Sink(pipe, "sink", step, func(ctx context.Context, in int) error {
		sum += int64(in)

		return nil
	})

	if sink == nil {
		return 0, pipe.Err()
	}

	err = pipe.Run(context.Background())
	if err != nil {
		return 0, err
	}

	return sum, nil
}

func runPipelineTwoStage(inputs []int, workers int, stage1 func(int) int, stage2 func(int) int) (int64, error) {
	pipe, err := pipeline.New()
	if err != nil {
		return 0, err
	}

	root := pipeline.Root(pipe, "root", func(ctx context.Context, out chan<- int) error {
		for _, v := range inputs {
			out <- v
		}

		return nil
	})
	if root == nil {
		return 0, pipe.Err()
	}

	step1 := pipeline.OneToOne(pipe, "stage-1", root, func(ctx context.Context, in int) (int, error) {
		return stage1(in), nil
	}, pipeline.StepConcurrency[int](workers))
	if step1 == nil {
		return 0, pipe.Err()
	}

	step2 := pipeline.OneToOne(pipe, "stage-2", step1, func(ctx context.Context, in int) (int, error) {
		return stage2(in), nil
	}, pipeline.StepConcurrency[int](workers))
	if step2 == nil {
		return 0, pipe.Err()
	}

	var sum int64
	sink := pipeline.Sink(pipe, "sink", step2, func(ctx context.Context, in int) error {
		sum += int64(in)

		return nil
	})

	if sink == nil {
		return 0, pipe.Err()
	}

	err = pipe.Run(context.Background())
	if err != nil {
		return 0, err
	}

	return sum, nil
}

func runPipelineSplitMerge(inputs []int, workers int, leftFn func(int) int, rightFn func(int) int) (int64, error) {
	pipe, err := pipeline.New()
	if err != nil {
		return 0, err
	}

	root := pipeline.Root(pipe, "root", func(ctx context.Context, out chan<- int) error {
		for _, v := range inputs {
			out <- v
		}

		return nil
	})
	if root == nil {
		return 0, pipe.Err()
	}

	splitter := pipeline.Split(pipe, "split", root, 2)
	if splitter == nil {
		return 0, pipe.Err()
	}

	left, _ := splitter.Get()
	right, _ := splitter.Get()

	leftStep := pipeline.OneToOne(pipe, "left", left, func(ctx context.Context, in int) (int, error) {
		return leftFn(in), nil
	}, pipeline.StepConcurrency[int](workers))
	if leftStep == nil {
		return 0, pipe.Err()
	}

	rightStep := pipeline.OneToOne(pipe, "right", right, func(ctx context.Context, in int) (int, error) {
		return rightFn(in), nil
	}, pipeline.StepConcurrency[int](workers))
	if rightStep == nil {
		return 0, pipe.Err()
	}

	merged := pipeline.Merge(pipe, "merge", leftStep, rightStep)
	if merged == nil {
		return 0, pipe.Err()
	}

	var sum int64
	sink := pipeline.Sink(pipe, "sink", merged, func(ctx context.Context, in int) error {
		sum += int64(in)

		return nil
	})

	if sink == nil {
		return 0, pipe.Err()
	}

	err = pipe.Run(context.Background())
	if err != nil {
		return 0, err
	}

	return sum, nil
}

func runPipelineOneToMany(inputs []int, workers int, leftFn func(int) int, rightFn func(int) int) (int64, error) {
	pipe, err := pipeline.New()
	if err != nil {
		return 0, err
	}

	root := pipeline.Root(pipe, "root", func(ctx context.Context, out chan<- int) error {
		for _, v := range inputs {
			out <- v
		}

		return nil
	})
	if root == nil {
		return 0, pipe.Err()
	}

	step := pipeline.OneToMany(pipe, "many", root, func(ctx context.Context, in int) ([]int, error) {
		return []int{leftFn(in), rightFn(in)}, nil
	}, pipeline.StepConcurrency[int](workers))
	if step == nil {
		return 0, pipe.Err()
	}

	var sum int64
	sink := pipeline.Sink(pipe, "sink", step, func(ctx context.Context, in int) error {
		sum += int64(in)

		return nil
	})

	if sink == nil {
		return 0, pipe.Err()
	}

	err = pipe.Run(context.Background())
	if err != nil {
		return 0, err
	}

	return sum, nil
}

func runPipelineFromChan(inputs []int, fn func(int) int) (int64, error) {
	pipe, err := pipeline.New()
	if err != nil {
		return 0, err
	}

	root := pipeline.Root(pipe, "root", func(ctx context.Context, out chan<- int) error {
		for _, v := range inputs {
			out <- v
		}

		return nil
	})
	if root == nil {
		return 0, pipe.Err()
	}

	step := pipeline.FromChan(pipe, "from-chan", root, func(ctx context.Context, input <-chan int, output chan int) error {
		for v := range input {
			output <- fn(v)
		}

		return nil
	})
	if step == nil {
		return 0, pipe.Err()
	}

	var sum int64
	sink := pipeline.Sink(pipe, "sink", step, func(ctx context.Context, in int) error {
		sum += int64(in)

		return nil
	})

	if sink == nil {
		return 0, pipe.Err()
	}

	err = pipe.Run(context.Background())
	if err != nil {
		return 0, err
	}

	return sum, nil
}

func runPipelineSinkFromChan(inputs []int, fn func(int) int) (int64, error) {
	pipe, err := pipeline.New()
	if err != nil {
		return 0, err
	}

	root := pipeline.Root(pipe, "root", func(ctx context.Context, out chan<- int) error {
		for _, v := range inputs {
			out <- v
		}

		return nil
	})
	if root == nil {
		return 0, pipe.Err()
	}

	var sum int64
	sink := pipeline.SinkFromChan(pipe, "sink", root, func(ctx context.Context, input <-chan int) error {
		for v := range input {
			sum += int64(fn(v))
		}

		return nil
	})

	if sink == nil {
		return 0, pipe.Err()
	}

	err = pipe.Run(context.Background())
	if err != nil {
		return 0, err
	}

	return sum, nil
}

func runPipelineBatch(inputs []int, batchSize int, fn func(int) int) (int64, error) {
	pipe, err := pipeline.New()
	if err != nil {
		return 0, err
	}

	root := pipeline.Root(pipe, "root", func(ctx context.Context, out chan<- int) error {
		for _, v := range inputs {
			out <- v
		}

		return nil
	})
	if root == nil {
		return 0, pipe.Err()
	}

	batch := pipeline.Batch(pipe, "batch", root, pipeline.BatchPolicy{MaxSize: batchSize})
	if batch == nil {
		return 0, pipe.Err()
	}

	var sum int64
	sink := pipeline.Sink(pipe, "sink", batch, func(ctx context.Context, batch []int) error {
		for _, v := range batch {
			sum += int64(fn(v))
		}

		return nil
	})

	if sink == nil {
		return 0, pipe.Err()
	}

	err = pipe.Run(context.Background())
	if err != nil {
		return 0, err
	}

	return sum, nil
}

func runPipelineBatchChan(inputs []int, batchSize int, fn func(int) int) (int64, error) {
	pipe, err := pipeline.New()
	if err != nil {
		return 0, err
	}

	root := pipeline.Root(pipe, "root", func(ctx context.Context, out chan<- int) error {
		for _, v := range inputs {
			out <- v
		}

		return nil
	})
	if root == nil {
		return 0, pipe.Err()
	}

	batch := pipeline.BatchChan(pipe, "batch", root, pipeline.BatchPolicy{MaxSize: batchSize})
	if batch == nil {
		return 0, pipe.Err()
	}

	var sum int64
	sink := pipeline.Sink(pipe, "sink", batch, func(ctx context.Context, batch <-chan int) error {
		for v := range batch {
			sum += int64(fn(v))
		}

		return nil
	})

	if sink == nil {
		return 0, pipe.Err()
	}

	err = pipe.Run(context.Background())
	if err != nil {
		return 0, err
	}

	return sum, nil
}

func runPipelineMultiStage(inputs []int, steps int, workers int, fn func(int) int) (int64, error) {
	pipe, err := pipeline.New()
	if err != nil {
		return 0, err
	}

	root := pipeline.Root(pipe, "root", func(ctx context.Context, out chan<- int) error {
		for _, v := range inputs {
			out <- v
		}

		return nil
	})
	if root == nil {
		return 0, pipe.Err()
	}

	prev := root

	for i := range steps {
		name := "step-" + strconv.Itoa(i+1)
		step := pipeline.OneToOne(pipe, name, prev, func(ctx context.Context, in int) (int, error) {
			return fn(in), nil
		}, pipeline.StepConcurrency[int](workers))

		if step == nil {
			return 0, pipe.Err()
		}

		prev = step
	}

	var sum int64
	sink := pipeline.Sink(pipe, "sink", prev, func(ctx context.Context, in int) error {
		sum += int64(in)

		return nil
	})

	if sink == nil {
		return 0, pipe.Err()
	}

	err = pipe.Run(context.Background())
	if err != nil {
		return 0, err
	}

	return sum, nil
}
