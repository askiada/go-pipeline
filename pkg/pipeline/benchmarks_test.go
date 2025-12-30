package pipeline_test

import (
	"context"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/askiada/go-pipeline/pkg/pipeline"
)

const (
	benchItems         = 1024
	benchBatchSize     = 32
	benchCompositeFan  = 2
	benchWorkIters     = 64
	benchOverheadItems = 4096
	benchStepWorkers   = 1
	benchStepWorkIters = 0
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

var benchWorkSweep = []int{0, 4, 16, 64, 256, 1024, 4096}

var benchStepCounts = []int{1, 2, 4, 8, 16, 32, 64}

var benchSink int64

func makeInputs(n int) []int {
	inputs := make([]int, n)
	for i := range inputs {
		inputs[i] = i
	}

	return inputs
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

func maybeZero(v int) int {
	if v%4 == 0 {
		return 0
	}

	return v + 1
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

type baselineFns struct {
	serial   func([]int) int64
	workers  func([]int, int) int64
	channels func([]int, int) int64
	pipeline func([]int, int) (int64, error)
}

func runBenchCase(b *testing.B, tc benchCase, inputs []int, fns baselineFns) {
	b.Helper()

	b.Run(tc.name+"/loop-serial", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()

		for range b.N {
			benchSink = fns.serial(inputs)
		}
	})

	b.Run(tc.name+"/loop-workers", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()

		for range b.N {
			benchSink = fns.workers(inputs, tc.workers)
		}
	})

	b.Run(tc.name+"/channels", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()

		for range b.N {
			benchSink = fns.channels(inputs, tc.workers)
		}
	})

	b.Run(tc.name+"/pipeline", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()

		for range b.N {
			sum, err := fns.pipeline(inputs, tc.workers)
			if err != nil {
				b.Fatal(err)
			}

			benchSink = sum
		}
	})
}

func BenchmarkOneToOne(b *testing.B) {
	benchmarkOneToOne(b, simpleAdd1, false)
}

func BenchmarkOneToOneRealistic(b *testing.B) {
	benchmarkOneToOne(b, realisticWork, false)
}

func BenchmarkOneToOneOrZero(b *testing.B) {
	benchmarkOneToOne(b, maybeZero, true)
}

func BenchmarkTwoStage(b *testing.B) {
	benchmarkTwoStage(b, simpleAdd1, simpleMul2)
}

func BenchmarkTwoStageRealistic(b *testing.B) {
	benchmarkTwoStage(b, realisticWork, realisticWorkAlt)
}

func BenchmarkSplitMerge(b *testing.B) {
	benchmarkSplitMerge(b, simpleAdd1, simpleAdd10)
}

func BenchmarkSplitMergeRealistic(b *testing.B) {
	benchmarkSplitMerge(b, realisticWork, realisticWorkAlt)
}

func BenchmarkSplitBy(b *testing.B) {
	benchmarkSplitBy(b, simpleAdd1, simpleAdd10)
}

func BenchmarkOneToMany(b *testing.B) {
	benchmarkOneToMany(b, simpleAdd1, simpleAdd10)
}

func BenchmarkFromChan(b *testing.B) {
	benchmarkFromChan(b, simpleAdd1)
}

func BenchmarkSinkFromChan(b *testing.B) {
	benchmarkSinkFromChan(b, simpleAdd1)
}

func BenchmarkBatch(b *testing.B) {
	benchmarkBatch(b, benchBatchSize, simpleAdd1)
}

func BenchmarkBatchChan(b *testing.B) {
	benchmarkBatchChan(b, benchBatchSize, simpleAdd1)
}

func BenchmarkOverheadWorkSweep(b *testing.B) {
	inputs := makeInputs(benchOverheadItems)

	for _, iters := range benchWorkSweep {
		workFn := makeWorkFn(iters)
		caseName := "iters=" + strconv.Itoa(iters)

		b.Run(caseName+"/loop-serial", func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()

			for range b.N {
				benchSink = runLoopSerial(inputs, func(v int) int64 {
					return int64(workFn(v))
				})
			}
		})

		b.Run(caseName+"/pipeline", func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()

			for range b.N {
				sum, err := runPipelineOneToOne(inputs, benchStepWorkers, workFn, false)
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

		b.Run(caseName+"/loop-serial", func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()

			for range b.N {
				benchSink = runLoopMultiStage(inputs, steps, func(v int) int64 {
					return int64(workFn(v))
				})
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

func BenchmarkOverheadCompositeOneToMany(b *testing.B) {
	inputs := makeInputs(benchOverheadItems)
	oneToManyFn := func(v int) []int {
		return []int{simpleAdd1(v), simpleAdd10(v)}
	}

	stage := func(values []int) []int {
		out := make([]int, 0, len(values))

		for _, v := range values {
			out = append(out, reduceOutputs(oneToManyFn(v)))
		}

		return out
	}

	for _, stages := range benchStepCounts {
		caseName := "stages=" + strconv.Itoa(stages)

		b.Run(caseName+"/loop-serial", func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()

			for range b.N {
				benchSink = runLoopCompositeStages(inputs, stages, stage)
			}
		})

		b.Run(caseName+"/pipeline", func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()

			for range b.N {
				sum, err := runPipelineCompositeOneToMany(inputs, stages, benchStepWorkers, oneToManyFn)
				if err != nil {
					b.Fatal(err)
				}

				benchSink = sum
			}
		})
	}
}

func BenchmarkOverheadCompositeBatch(b *testing.B) {
	inputs := makeInputs(benchOverheadItems)
	stage := compositeBatchStage(benchBatchSize)

	for _, stages := range benchStepCounts {
		caseName := "stages=" + strconv.Itoa(stages)

		b.Run(caseName+"/loop-serial", func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()

			for range b.N {
				benchSink = runLoopCompositeStages(inputs, stages, stage)
			}
		})

		b.Run(caseName+"/pipeline", func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()

			for range b.N {
				sum, err := runPipelineCompositeBatch(inputs, stages, benchStepWorkers, benchBatchSize)
				if err != nil {
					b.Fatal(err)
				}

				benchSink = sum
			}
		})
	}
}

func BenchmarkOverheadCompositeBatchChan(b *testing.B) {
	inputs := makeInputs(benchOverheadItems)
	stage := compositeBatchStage(benchBatchSize)

	for _, stages := range benchStepCounts {
		caseName := "stages=" + strconv.Itoa(stages)

		b.Run(caseName+"/loop-serial", func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()

			for range b.N {
				benchSink = runLoopCompositeStages(inputs, stages, stage)
			}
		})

		b.Run(caseName+"/pipeline", func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()

			for range b.N {
				sum, err := runPipelineCompositeBatchChan(inputs, stages, benchStepWorkers, benchBatchSize)
				if err != nil {
					b.Fatal(err)
				}

				benchSink = sum
			}
		})
	}
}

func benchmarkOneToOne(b *testing.B, fn func(int) int, dropZero bool) {
	b.Helper()

	fns := baselineFns{
		serial: func(inputs []int) int64 {
			return runLoopSerial(inputs, func(v int) int64 {
				res := fn(v)

				return int64(res)
			})
		},
		workers: func(inputs []int, workers int) int64 {
			return runLoopWorkers(inputs, workers, func(v int) int64 {
				res := fn(v)

				return int64(res)
			})
		},
		channels: func(inputs []int, workers int) int64 {
			return runChannelsOneToOne(inputs, workers, fn, dropZero)
		},
		pipeline: func(inputs []int, workers int) (int64, error) {
			return runPipelineOneToOne(inputs, workers, fn, dropZero)
		},
	}

	for _, tc := range benchCases {
		inputs := makeInputs(tc.items)
		runBenchCase(b, tc, inputs, fns)
	}
}

func benchmarkTwoStage(b *testing.B, stage1 func(int) int, stage2 func(int) int) {
	b.Helper()

	fns := baselineFns{
		serial: func(inputs []int) int64 {
			return runLoopSerial(inputs, func(v int) int64 {
				return int64(stage2(stage1(v)))
			})
		},
		workers: func(inputs []int, workers int) int64 {
			return runLoopWorkers(inputs, workers, func(v int) int64 {
				return int64(stage2(stage1(v)))
			})
		},
		channels: func(inputs []int, workers int) int64 {
			return runChannelsTwoStage(inputs, workers, stage1, stage2)
		},
		pipeline: func(inputs []int, workers int) (int64, error) {
			return runPipelineTwoStage(inputs, workers, stage1, stage2)
		},
	}

	for _, tc := range benchCases {
		inputs := makeInputs(tc.items)
		runBenchCase(b, tc, inputs, fns)
	}
}

func benchmarkSplitMerge(b *testing.B, leftFn func(int) int, rightFn func(int) int) {
	b.Helper()

	fns := baselineFns{
		serial: func(inputs []int) int64 {
			return runLoopSerial(inputs, func(v int) int64 {
				return int64(leftFn(v) + rightFn(v))
			})
		},
		workers: func(inputs []int, workers int) int64 {
			return runLoopWorkers(inputs, workers, func(v int) int64 {
				return int64(leftFn(v) + rightFn(v))
			})
		},
		channels: func(inputs []int, workers int) int64 {
			return runChannelsSplitMerge(inputs, workers, leftFn, rightFn)
		},
		pipeline: func(inputs []int, workers int) (int64, error) {
			return runPipelineSplitMerge(inputs, workers, leftFn, rightFn)
		},
	}

	for _, tc := range benchCases {
		inputs := makeInputs(tc.items)
		runBenchCase(b, tc, inputs, fns)
	}
}

func benchmarkSplitBy(b *testing.B, leftFn func(int) int, rightFn func(int) int) {
	b.Helper()

	fns := baselineFns{
		serial: func(inputs []int) int64 {
			return runLoopSerial(inputs, func(v int) int64 {
				if v%2 == 0 {
					return int64(leftFn(v))
				}

				return int64(rightFn(v))
			})
		},
		workers: func(inputs []int, workers int) int64 {
			return runLoopWorkers(inputs, workers, func(v int) int64 {
				if v%2 == 0 {
					return int64(leftFn(v))
				}

				return int64(rightFn(v))
			})
		},
		channels: func(inputs []int, workers int) int64 {
			return runChannelsSplitBy(inputs, workers, leftFn, rightFn)
		},
		pipeline: func(inputs []int, workers int) (int64, error) {
			return runPipelineSplitBy(inputs, workers, leftFn, rightFn)
		},
	}

	for _, tc := range benchCases {
		inputs := makeInputs(tc.items)
		runBenchCase(b, tc, inputs, fns)
	}
}

func benchmarkOneToMany(b *testing.B, leftFn func(int) int, rightFn func(int) int) {
	b.Helper()

	oneToManyFn := func(v int) []int {
		return []int{leftFn(v), rightFn(v)}
	}

	fns := baselineFns{
		serial: func(inputs []int) int64 {
			return runLoopSerial(inputs, func(v int) int64 {
				return sumOutputs(oneToManyFn(v))
			})
		},
		workers: func(inputs []int, workers int) int64 {
			return runLoopWorkers(inputs, workers, func(v int) int64 {
				return sumOutputs(oneToManyFn(v))
			})
		},
		channels: func(inputs []int, workers int) int64 {
			return runChannelsOneToMany(inputs, workers, oneToManyFn)
		},
		pipeline: func(inputs []int, workers int) (int64, error) {
			return runPipelineOneToMany(inputs, workers, oneToManyFn)
		},
	}

	for _, tc := range benchCases {
		inputs := makeInputs(tc.items)
		runBenchCase(b, tc, inputs, fns)
	}
}

func benchmarkFromChan(b *testing.B, fn func(int) int) {
	b.Helper()

	fns := baselineFns{
		serial: func(inputs []int) int64 {
			return runLoopSerial(inputs, func(v int) int64 {
				return int64(fn(v))
			})
		},
		workers: func(inputs []int, workers int) int64 {
			return runLoopWorkers(inputs, workers, func(v int) int64 {
				return int64(fn(v))
			})
		},
		channels: func(inputs []int, workers int) int64 {
			return runChannelsFromChan(inputs, workers, fn)
		},
		pipeline: func(inputs []int, workers int) (int64, error) {
			return runPipelineFromChan(inputs, workers, fn)
		},
	}

	for _, tc := range benchCases {
		inputs := makeInputs(tc.items)
		runBenchCase(b, tc, inputs, fns)
	}
}

func benchmarkSinkFromChan(b *testing.B, fn func(int) int) {
	b.Helper()

	fns := baselineFns{
		serial: func(inputs []int) int64 {
			return runLoopSerial(inputs, func(v int) int64 {
				return int64(fn(v))
			})
		},
		workers: func(inputs []int, workers int) int64 {
			return runLoopWorkers(inputs, workers, func(v int) int64 {
				return int64(fn(v))
			})
		},
		channels: func(inputs []int, workers int) int64 {
			return runChannelsSinkFromChan(inputs, workers, fn)
		},
		pipeline: func(inputs []int, workers int) (int64, error) {
			return runPipelineSinkFromChan(inputs, workers, fn)
		},
	}

	for _, tc := range benchCases {
		inputs := makeInputs(tc.items)
		runBenchCase(b, tc, inputs, fns)
	}
}

func benchmarkBatch(b *testing.B, batchSize int, fn func(int) int) {
	b.Helper()

	fns := baselineFns{
		serial: func(inputs []int) int64 {
			return runLoopSerialBatch(inputs, batchSize, fn)
		},
		workers: func(inputs []int, workers int) int64 {
			return runLoopWorkersBatch(inputs, workers, batchSize, fn)
		},
		channels: func(inputs []int, workers int) int64 {
			return runChannelsBatch(inputs, workers, batchSize, fn)
		},
		pipeline: func(inputs []int, workers int) (int64, error) {
			return runPipelineBatch(inputs, workers, batchSize, fn)
		},
	}

	for _, tc := range benchCases {
		inputs := makeInputs(tc.items)
		runBenchCase(b, tc, inputs, fns)
	}
}

func benchmarkBatchChan(b *testing.B, batchSize int, fn func(int) int) {
	b.Helper()

	fns := baselineFns{
		serial: func(inputs []int) int64 {
			return runLoopSerialBatchChan(inputs, batchSize, fn)
		},
		workers: func(inputs []int, workers int) int64 {
			return runLoopWorkersBatchChan(inputs, workers, batchSize, fn)
		},
		channels: func(inputs []int, workers int) int64 {
			return runChannelsBatchChan(inputs, workers, batchSize, fn)
		},
		pipeline: func(inputs []int, workers int) (int64, error) {
			return runPipelineBatchChan(inputs, workers, batchSize, fn)
		},
	}

	for _, tc := range benchCases {
		inputs := makeInputs(tc.items)
		runBenchCase(b, tc, inputs, fns)
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

func runLoopSerial(inputs []int, perItem func(int) int64) int64 {
	var sum int64

	for _, v := range inputs {
		sum += perItem(v)
	}

	return sum
}

func runLoopWorkers(inputs []int, workers int, perItem func(int) int64) int64 {
	workers = normalizeWorkers(workers)
	if workers == 1 {
		return runLoopSerial(inputs, perItem)
	}

	var idx atomic.Int64
	sums := make([]int64, workers)
	n := int64(len(inputs))

	var wg sync.WaitGroup
	wg.Add(workers)

	for w := range workers {
		worker := w

		go func() {
			defer wg.Done()

			var local int64

			for {
				i := idx.Add(1) - 1
				if i >= n {
					break
				}

				local += perItem(inputs[int(i)])
			}

			sums[worker] = local
		}()
	}

	wg.Wait()

	var sum int64
	for _, value := range sums {
		sum += value
	}

	return sum
}

func runLoopMultiStage(inputs []int, steps int, perItem func(int) int64) int64 {
	var sum int64

	for _, v := range inputs {
		value := v
		for range steps {
			value = int(perItem(value))
		}

		sum += int64(value)
	}

	return sum
}

func runLoopCompositeStages(inputs []int, stages int, stage func([]int) []int) int64 {
	values := inputs

	for range stages {
		values = stage(values)
	}

	var sum int64

	for _, v := range values {
		sum += int64(v)
	}

	return sum
}

func sumOutputs(outputs []int) int64 {
	var sum int64

	for _, v := range outputs {
		sum += int64(v)
	}

	return sum
}

func reduceOutputs(outputs []int) int {
	var sum int

	for _, v := range outputs {
		sum += v
	}

	return sum
}

func compositeBatchStage(batchSize int) func([]int) []int {
	return func(values []int) []int {
		out := make([]int, 0, len(values))
		batch := make([]int, 0, batchSize)

		flush := func() {
			if len(batch) == 0 {
				return
			}

			out = append(out, batch...)
			batch = batch[:0]
		}

		for _, v := range values {
			batch = append(batch, v)
			if len(batch) >= batchSize {
				flush()
			}
		}

		flush()

		return out
	}
}

func runLoopSerialBatch(inputs []int, batchSize int, fn func(int) int) int64 {
	batch := make([]int, 0, batchSize)
	var sum int64

	flush := func() {
		for _, v := range batch {
			sum += int64(fn(v))
		}

		batch = nil
	}

	for _, v := range inputs {
		batch = append(batch, v)
		if len(batch) >= batchSize {
			flush()
		}
	}

	if len(batch) > 0 {
		flush()
	}

	return sum
}

func runLoopWorkersBatch(inputs []int, workers int, batchSize int, fn func(int) int) int64 {
	workers = normalizeWorkers(workers)
	if workers == 1 {
		return runLoopSerialBatch(inputs, batchSize, fn)
	}

	var idx atomic.Int64
	sums := make([]int64, workers)
	n := int64(len(inputs))

	var wg sync.WaitGroup
	wg.Add(workers)

	for w := range workers {
		worker := w

		go func() {
			defer wg.Done()

			batch := make([]int, 0, batchSize)
			var local int64

			flush := func() {
				for _, v := range batch {
					local += int64(fn(v))
				}

				batch = nil
			}

			for {
				i := idx.Add(1) - 1
				if i >= n {
					break
				}

				batch = append(batch, inputs[int(i)])
				if len(batch) >= batchSize {
					flush()
				}
			}

			if len(batch) > 0 {
				flush()
			}

			sums[worker] = local
		}()
	}

	wg.Wait()

	var sum int64
	for _, value := range sums {
		sum += value
	}

	return sum
}

func runLoopSerialBatchChan(inputs []int, batchSize int, fn func(int) int) int64 {
	batch := make([]int, 0, batchSize)
	var sum int64

	flush := func() {
		for _, v := range batch {
			sum += int64(fn(v))
		}

		batch = nil
	}

	for _, v := range inputs {
		batch = append(batch, v)
		if len(batch) >= batchSize {
			flush()
		}
	}

	if len(batch) > 0 {
		flush()
	}

	return sum
}

func runLoopWorkersBatchChan(inputs []int, workers int, batchSize int, fn func(int) int) int64 {
	return runLoopWorkersBatch(inputs, workers, batchSize, fn)
}

func runChannelsOneToOne(inputs []int, workers int, fn func(int) int, dropZero bool) int64 {
	workers = normalizeWorkers(workers)
	in := make(chan int)
	out := make(chan int)

	var wg sync.WaitGroup
	wg.Add(workers)

	for range workers {
		go func() {
			defer wg.Done()

			for v := range in {
				res := fn(v)
				if dropZero && res == 0 {
					continue
				}

				out <- res
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

func runChannelsTwoStage(inputs []int, workers int, stage1 func(int) int, stage2 func(int) int) int64 {
	workers = normalizeWorkers(workers)
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

func runChannelsSplitMerge(inputs []int, workers int, leftFn func(int) int, rightFn func(int) int) int64 {
	workers = normalizeWorkers(workers)
	in := make(chan int)
	left := make(chan int)
	right := make(chan int)
	leftOut := make(chan int)
	rightOut := make(chan int)
	out := make(chan int)

	go func() {
		for v := range in {
			left <- v

			right <- v
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

func runChannelsSplitBy(inputs []int, workers int, leftFn func(int) int, rightFn func(int) int) int64 {
	workers = normalizeWorkers(workers)
	in := make(chan int)
	left := make(chan int)
	right := make(chan int)
	leftOut := make(chan int)
	rightOut := make(chan int)
	out := make(chan int)

	go func() {
		for v := range in {
			if v%2 == 0 {
				left <- v
			} else {
				right <- v
			}
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

func runChannelsOneToMany(inputs []int, workers int, fn func(int) []int) int64 {
	workers = normalizeWorkers(workers)
	in := make(chan int)
	out := make(chan int)

	var wg sync.WaitGroup
	wg.Add(workers)

	for range workers {
		go func() {
			defer wg.Done()

			for v := range in {
				for _, outValue := range fn(v) {
					out <- outValue
				}
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

func runChannelsFromChan(inputs []int, workers int, fn func(int) int) int64 {
	return runChannelsOneToOne(inputs, workers, fn, false)
}

func runChannelsSinkFromChan(inputs []int, workers int, fn func(int) int) int64 {
	workers = normalizeWorkers(workers)
	in := make(chan int)
	sums := make([]int64, workers)

	var wg sync.WaitGroup
	wg.Add(workers)

	for w := range workers {
		worker := w

		go func() {
			defer wg.Done()

			var local int64

			for v := range in {
				local += int64(fn(v))
			}

			sums[worker] = local
		}()
	}

	go func() {
		for _, v := range inputs {
			in <- v
		}

		close(in)
	}()

	wg.Wait()

	var sum int64
	for _, value := range sums {
		sum += value
	}

	return sum
}

func runChannelsBatch(inputs []int, workers int, batchSize int, fn func(int) int) int64 {
	workers = normalizeWorkers(workers)
	in := make(chan int)
	out := make(chan []int)

	var wg sync.WaitGroup
	wg.Add(workers)

	for range workers {
		go func() {
			defer wg.Done()

			batch := make([]int, 0, batchSize)

			flush := func() {
				if len(batch) == 0 {
					return
				}

				out <- batch

				batch = nil
			}

			for v := range in {
				batch = append(batch, v)
				if len(batch) >= batchSize {
					flush()
				}
			}

			flush()
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

	for batch := range out {
		for _, v := range batch {
			sum += int64(fn(v))
		}
	}

	return sum
}

func runChannelsBatchChan(inputs []int, workers int, batchSize int, fn func(int) int) int64 {
	workers = normalizeWorkers(workers)
	in := make(chan int)
	out := make(chan chan int)

	var wg sync.WaitGroup
	wg.Add(workers)

	for range workers {
		go func() {
			defer wg.Done()

			var batchCh chan int
			batchCount := 0

			flush := func() {
				if batchCh == nil {
					return
				}

				close(batchCh)
				batchCh = nil
				batchCount = 0
			}

			for v := range in {
				if batchCh == nil {
					batchCh = make(chan int)
					out <- batchCh
				}

				batchCh <- v

				batchCount++

				if batchCount >= batchSize {
					flush()
				}
			}

			flush()
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

	for batch := range out {
		for v := range batch {
			sum += int64(fn(v))
		}
	}

	return sum
}

func runPipelineOneToOne(inputs []int, workers int, fn func(int) int, dropZero bool) (int64, error) {
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

	var step *pipeline.Step[int]
	if dropZero {
		step = pipeline.OneToOneOrZero(pipe, "one", root, func(ctx context.Context, in int) (int, error) {
			return fn(in), nil
		}, stepOptions[int](workers)...)
	} else {
		step = pipeline.OneToOne(pipe, "one", root, func(ctx context.Context, in int) (int, error) {
			return fn(in), nil
		}, stepOptions[int](workers)...)
	}

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
	}, stepOptions[int](workers)...)
	if step1 == nil {
		return 0, pipe.Err()
	}

	step2 := pipeline.OneToOne(pipe, "stage-2", step1, func(ctx context.Context, in int) (int, error) {
		return stage2(in), nil
	}, stepOptions[int](workers)...)
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
	}, stepOptions[int](workers)...)
	if leftStep == nil {
		return 0, pipe.Err()
	}

	rightStep := pipeline.OneToOne(pipe, "right", right, func(ctx context.Context, in int) (int, error) {
		return rightFn(in), nil
	}, stepOptions[int](workers)...)
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

func runPipelineSplitBy(inputs []int, workers int, leftFn func(int) int, rightFn func(int) int) (int64, error) {
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

	splitFns := []pipeline.SplitFn[int]{
		func(ctx context.Context, input int) (bool, error) {
			return input%2 == 0, nil
		},
		func(ctx context.Context, input int) (bool, error) {
			return input%2 != 0, nil
		},
	}

	splitter := pipeline.SplitBy(pipe, "split", root, splitFns)
	if splitter == nil {
		return 0, pipe.Err()
	}

	left, _ := splitter.Get()
	right, _ := splitter.Get()

	leftStep := pipeline.OneToOne(pipe, "left", left, func(ctx context.Context, in int) (int, error) {
		return leftFn(in), nil
	}, stepOptions[int](workers)...)
	if leftStep == nil {
		return 0, pipe.Err()
	}

	rightStep := pipeline.OneToOne(pipe, "right", right, func(ctx context.Context, in int) (int, error) {
		return rightFn(in), nil
	}, stepOptions[int](workers)...)
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

func runPipelineOneToMany(inputs []int, workers int, fn func(int) []int) (int64, error) {
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
		return fn(in), nil
	}, stepOptions[int](workers)...)
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

func runPipelineFromChan(inputs []int, workers int, fn func(int) int) (int64, error) {
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
	}, stepOptions[int](workers)...)
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

func runPipelineSinkFromChan(inputs []int, workers int, fn func(int) int) (int64, error) {
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

	var sum atomic.Int64
	sink := pipeline.SinkFromChan(pipe, "sink", root, func(ctx context.Context, input <-chan int) error {
		for v := range input {
			sum.Add(int64(fn(v)))
		}

		return nil
	}, stepOptions[int](workers)...)

	if sink == nil {
		return 0, pipe.Err()
	}

	err = pipe.Run(context.Background())
	if err != nil {
		return 0, err
	}

	return sum.Load(), nil
}

func runPipelineBatch(inputs []int, workers int, batchSize int, fn func(int) int) (int64, error) {
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

	batch := pipeline.Batch(pipe, "batch", root, pipeline.BatchPolicy{MaxSize: batchSize}, stepOptions[[]int](workers)...)
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

func runPipelineBatchChan(inputs []int, workers int, batchSize int, fn func(int) int) (int64, error) {
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

	batch := pipeline.BatchChan(pipe, "batch", root, pipeline.BatchPolicy{MaxSize: batchSize}, stepOptions[<-chan int](workers)...)
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

func runPipelineCompositeOneToMany(inputs []int, stages int, workers int, fn func(int) []int) (int64, error) {
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

	for i := range stages {
		stageName := "many-" + strconv.Itoa(i+1)

		many := pipeline.OneToMany(pipe, stageName, prev, func(ctx context.Context, in int) ([]int, error) {
			return fn(in), nil
		}, stepOptions[int](workers)...)
		if many == nil {
			return 0, pipe.Err()
		}

		batched := pipeline.Batch(
			pipe,
			stageName+"-batch",
			many,
			pipeline.BatchPolicy{MaxSize: benchCompositeFan},
			stepOptions[[]int](workers)...,
		)
		if batched == nil {
			return 0, pipe.Err()
		}

		reduce := pipeline.OneToOne(pipe, stageName+"-reduce", batched, func(ctx context.Context, payload []int) (int, error) {
			return reduceOutputs(payload), nil
		}, stepOptions[int](workers)...)
		if reduce == nil {
			return 0, pipe.Err()
		}

		prev = reduce
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

func runPipelineCompositeBatch(inputs []int, stages int, workers int, batchSize int) (int64, error) {
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

	for i := range stages {
		stageName := "batch-" + strconv.Itoa(i+1)

		batch := pipeline.Batch(pipe, stageName, prev, pipeline.BatchPolicy{MaxSize: batchSize}, stepOptions[[]int](workers)...)
		if batch == nil {
			return 0, pipe.Err()
		}

		unbatch := pipeline.OneToMany(pipe, stageName+"-unbatch", batch, func(ctx context.Context, payload []int) ([]int, error) {
			return payload, nil
		}, stepOptions[int](workers)...)
		if unbatch == nil {
			return 0, pipe.Err()
		}

		prev = unbatch
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

func runPipelineCompositeBatchChan(inputs []int, stages int, workers int, batchSize int) (int64, error) {
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

	for i := range stages {
		stageName := "batch-chan-" + strconv.Itoa(i+1)

		batch := pipeline.BatchChan(
			pipe,
			stageName,
			prev,
			pipeline.BatchPolicy{MaxSize: batchSize},
			stepOptions[<-chan int](workers)...,
		)
		if batch == nil {
			return 0, pipe.Err()
		}

		flatten := pipeline.FromChan(pipe, stageName+"-flatten", batch, func(ctx context.Context, input <-chan <-chan int, output chan int) error {
			for ch := range input {
				for v := range ch {
					output <- v
				}
			}

			return nil
		}, stepOptions[int](workers)...)
		if flatten == nil {
			return 0, pipe.Err()
		}

		prev = flatten
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
		}, stepOptions[int](workers)...)

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
