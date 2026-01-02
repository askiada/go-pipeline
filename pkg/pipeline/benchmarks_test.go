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
	benchBatchSize       = 32
	benchCompositeFanOut = 2
	benchWorkIters       = 64
	benchOverheadItems   = 4096
	benchStepWorkers     = 1
	benchStepWorkIters   = 0
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

var (
	benchWorkSweep            = []int{0, 4, 16, 64, 256, 1024, 4096}
	benchStepCounts           = []int{1, 2, 4, 8, 16, 32, 64}
	benchCompositeStageCounts = []int{1, 2, 4, 8, 12}
)

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
	// run-only benchmarks that exclude setup work from timing.
	channelsRun func([]int, int) func(context.Context) int64
	pipelineRun func([]int, int) (func(context.Context) (int64, error), error)
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
			if fns.channelsRun == nil {
				benchSink = fns.channels(inputs, tc.workers)

				continue
			}

			b.StopTimer()

			run := fns.channelsRun(inputs, tc.workers)

			b.StartTimer()

			benchSink = run(b.Context())

			b.StopTimer()
		}
	})

	b.Run(tc.name+"/pipeline", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()

		for range b.N {
			if fns.pipelineRun == nil {
				sum, err := fns.pipeline(inputs, tc.workers)
				if err != nil {
					b.Fatal(err)
				}

				benchSink = sum

				continue
			}

			b.StopTimer()

			run, err := fns.pipelineRun(inputs, tc.workers)
			if err != nil {
				b.Fatal(err)
			}

			b.StartTimer()

			sum, err := run(b.Context())

			b.StopTimer()

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

		b.Run(caseName+"/channels", func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()

			for range b.N {
				benchSink = runChannelsOneToOne(inputs, benchStepWorkers, workFn, false)
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

		b.Run(caseName+"/channels", func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()

			for range b.N {
				benchSink = runChannelsMultiStage(b.Context(), inputs, steps, benchStepWorkers, workFn)
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
	oneToManyFn := func(v int) []int {
		return []int{simpleAdd1(v), simpleAdd10(v)}
	}

	stage := func(values []int) []int {
		out := make([]int, 0, len(values)*benchCompositeFanOut)

		for _, v := range values {
			out = append(out, oneToManyFn(v)...)
		}

		return out
	}

	for _, stages := range benchCompositeStageCounts {
		inputs := makeInputs(compositeInputCount(benchOverheadItems, stages, benchCompositeFanOut))
		caseName := "stages=" + strconv.Itoa(stages)

		b.Run(caseName+"/loop-serial", func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()

			for range b.N {
				benchSink = runLoopCompositeStages(inputs, stages, stage)
			}
		})

		b.Run(caseName+"/channels", func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()

			for range b.N {
				benchSink = runChannelsCompositeOneToMany(b.Context(), inputs, stages, benchStepWorkers, oneToManyFn)
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

	for _, stages := range benchCompositeStageCounts {
		caseName := "stages=" + strconv.Itoa(stages)

		b.Run(caseName+"/loop-serial", func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()

			for range b.N {
				benchSink = runLoopCompositeStages(inputs, stages, stage)
			}
		})

		b.Run(caseName+"/channels", func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()

			for range b.N {
				benchSink = runChannelsCompositeBatch(b.Context(), inputs, stages, benchStepWorkers, benchBatchSize)
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

	for _, stages := range benchCompositeStageCounts {
		caseName := "stages=" + strconv.Itoa(stages)

		b.Run(caseName+"/loop-serial", func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()

			for range b.N {
				benchSink = runLoopCompositeStages(inputs, stages, stage)
			}
		})

		b.Run(caseName+"/channels", func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()

			for range b.N {
				benchSink = runChannelsCompositeBatchChan(b.Context(), inputs, stages, benchStepWorkers, benchBatchSize)
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
		channelsRun: func(inputs []int, workers int) func(context.Context) int64 {
			return buildChannelsOneToOne(inputs, workers, fn, dropZero)
		},
		pipelineRun: func(inputs []int, workers int) (func(context.Context) (int64, error), error) {
			return buildPipelineOneToOne(inputs, workers, fn, dropZero)
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
		channelsRun: func(inputs []int, workers int) func(context.Context) int64 {
			return buildChannelsTwoStage(inputs, workers, stage1, stage2)
		},
		pipelineRun: func(inputs []int, workers int) (func(context.Context) (int64, error), error) {
			return buildPipelineTwoStage(inputs, workers, stage1, stage2)
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
		channelsRun: func(inputs []int, workers int) func(context.Context) int64 {
			return buildChannelsSplitMerge(inputs, workers, leftFn, rightFn)
		},
		pipelineRun: func(inputs []int, workers int) (func(context.Context) (int64, error), error) {
			return buildPipelineSplitMerge(inputs, workers, leftFn, rightFn)
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
		channelsRun: func(inputs []int, workers int) func(context.Context) int64 {
			return buildChannelsSplitBy(inputs, workers, leftFn, rightFn)
		},
		pipelineRun: func(inputs []int, workers int) (func(context.Context) (int64, error), error) {
			return buildPipelineSplitBy(inputs, workers, leftFn, rightFn)
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
		channelsRun: func(inputs []int, workers int) func(context.Context) int64 {
			return buildChannelsOneToMany(inputs, workers, oneToManyFn)
		},
		pipelineRun: func(inputs []int, workers int) (func(context.Context) (int64, error), error) {
			return buildPipelineOneToMany(inputs, workers, oneToManyFn)
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
		channelsRun: func(inputs []int, workers int) func(context.Context) int64 {
			return buildChannelsFromChan(inputs, workers, fn)
		},
		pipelineRun: func(inputs []int, workers int) (func(context.Context) (int64, error), error) {
			return buildPipelineFromChan(inputs, workers, fn)
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
		channelsRun: func(inputs []int, workers int) func(context.Context) int64 {
			return buildChannelsSinkFromChan(inputs, workers, fn)
		},
		pipelineRun: func(inputs []int, workers int) (func(context.Context) (int64, error), error) {
			return buildPipelineSinkFromChan(inputs, workers, fn)
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
		channelsRun: func(inputs []int, workers int) func(context.Context) int64 {
			return buildChannelsBatch(inputs, workers, batchSize, fn)
		},
		pipelineRun: func(inputs []int, workers int) (func(context.Context) (int64, error), error) {
			return buildPipelineBatch(inputs, workers, batchSize, fn)
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
		channelsRun: func(inputs []int, workers int) func(context.Context) int64 {
			return buildChannelsBatchChan(inputs, workers, batchSize, fn)
		},
		pipelineRun: func(inputs []int, workers int) (func(context.Context) (int64, error), error) {
			return buildPipelineBatchChan(inputs, workers, batchSize, fn)
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

func compositeInputCount(items int, stages int, fanOut int) int {
	if items < 1 {
		return 1
	}

	if stages < 1 || fanOut < 2 {
		return items
	}

	inputs := items
	for range stages {
		inputs /= fanOut
		if inputs < 1 {
			return 1
		}
	}

	return inputs
}

func stepOptions[O any](workers int) []pipeline.StepOption[O] {
	return []pipeline.StepOption[O]{pipeline.StepConcurrency[O](normalizeWorkers(workers))}
}

func sendWithContext[T any](ctx context.Context, ch chan<- T, value T) bool {
	select {
	case <-ctx.Done():
		return false
	case ch <- value:
		return true
	}
}

//nolint:ireturn // Benchmark helpers return generic values directly.
func recvWithContext[T any](ctx context.Context, ch <-chan T) (T, bool) {
	select {
	case <-ctx.Done():
		var zero T

		return zero, false
	case value, ok := <-ch:
		return value, ok
	}
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

//nolint:gocognit // Benchmark wiring trades clarity for direct channel plumbing.
func buildChannelsOneToOne(inputs []int, workers int, fn func(int) int, dropZero bool) func(context.Context) int64 {
	workers = normalizeWorkers(workers)
	in := make(chan int)
	out := make(chan int)

	var wg sync.WaitGroup
	wg.Add(workers)

	return func(ctx context.Context) int64 {
		for range workers {
			go func() {
				defer wg.Done()

				for {
					v, ok := recvWithContext(ctx, in)
					if !ok {
						return
					}

					res := fn(v)
					if dropZero && res == 0 {
						continue
					}

					if !sendWithContext(ctx, out, res) {
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
				if !sendWithContext(ctx, in, v) {
					return
				}
			}
		}()

		var sum int64

		for {
			v, ok := recvWithContext(ctx, out)
			if !ok {
				break
			}

			sum += int64(v)
		}

		return sum
	}
}

func runChannelsOneToOne(inputs []int, workers int, fn func(int) int, dropZero bool) int64 {
	return buildChannelsOneToOne(inputs, workers, fn, dropZero)(context.Background())
}

//nolint:gocognit // Benchmark wiring trades clarity for direct channel plumbing.
func buildChannelsTwoStage(inputs []int, workers int, stage1 func(int) int, stage2 func(int) int) func(context.Context) int64 {
	workers = normalizeWorkers(workers)
	in := make(chan int)
	mid := make(chan int)
	out := make(chan int)

	var wg1 sync.WaitGroup
	wg1.Add(workers)

	var wg2 sync.WaitGroup
	wg2.Add(workers)

	return func(ctx context.Context) int64 {
		for range workers {
			go func() {
				defer wg1.Done()

				for {
					v, ok := recvWithContext(ctx, in)
					if !ok {
						return
					}

					if !sendWithContext(ctx, mid, stage1(v)) {
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
					v, ok := recvWithContext(ctx, mid)
					if !ok {
						return
					}

					if !sendWithContext(ctx, out, stage2(v)) {
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
				if !sendWithContext(ctx, in, v) {
					return
				}
			}
		}()

		var sum int64

		for {
			v, ok := recvWithContext(ctx, out)
			if !ok {
				break
			}

			sum += int64(v)
		}

		return sum
	}
}

func runChannelsTwoStage(inputs []int, workers int, stage1 func(int) int, stage2 func(int) int) int64 {
	return buildChannelsTwoStage(inputs, workers, stage1, stage2)(context.Background())
}

func runChannelsMultiStage(ctx context.Context, inputs []int, steps int, workers int, fn func(int) int) int64 {
	workers = normalizeWorkers(workers)
	first := make(chan int)
	in := first

	for range steps {
		out := make(chan int)
		var wg sync.WaitGroup
		wg.Add(workers)

		for range workers {
			go func(in <-chan int, out chan<- int) {
				defer wg.Done()

				for {
					v, ok := recvWithContext(ctx, in)
					if !ok {
						return
					}

					if !sendWithContext(ctx, out, fn(v)) {
						return
					}
				}
			}(in, out)
		}

		go func(out chan int) {
			wg.Wait()
			close(out)
		}(out)

		in = out
	}

	go func() {
		defer close(first)

		for _, v := range inputs {
			if !sendWithContext(ctx, first, v) {
				return
			}
		}
	}()

	var sum int64

	for {
		v, ok := recvWithContext(ctx, in)
		if !ok {
			break
		}

		sum += int64(v)
	}

	return sum
}

//nolint:gocognit,gocyclo // Benchmark wiring trades clarity for direct channel plumbing.
func buildChannelsSplitMerge(inputs []int, workers int, leftFn func(int) int, rightFn func(int) int) func(context.Context) int64 {
	workers = normalizeWorkers(workers)
	in := make(chan int)
	left := make(chan int)
	right := make(chan int)
	leftOut := make(chan int)
	rightOut := make(chan int)
	out := make(chan int)

	var wgLeft sync.WaitGroup
	wgLeft.Add(workers)

	var wgRight sync.WaitGroup
	wgRight.Add(workers)

	return func(ctx context.Context) int64 {
		go func() {
			for {
				v, ok := recvWithContext(ctx, in)
				if !ok {
					break
				}

				if !sendWithContext(ctx, left, v) {
					break
				}

				if !sendWithContext(ctx, right, v) {
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
					v, ok := recvWithContext(ctx, left)
					if !ok {
						return
					}

					if !sendWithContext(ctx, leftOut, leftFn(v)) {
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
					v, ok := recvWithContext(ctx, right)
					if !ok {
						return
					}

					if !sendWithContext(ctx, rightOut, rightFn(v)) {
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
					v, ok := recvWithContext(ctx, leftOut)
					if !ok {
						return
					}

					if !sendWithContext(ctx, out, v) {
						return
					}
				}
			}()

			go func() {
				defer wg.Done()

				for {
					v, ok := recvWithContext(ctx, rightOut)
					if !ok {
						return
					}

					if !sendWithContext(ctx, out, v) {
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
				if !sendWithContext(ctx, in, v) {
					return
				}
			}
		}()

		var sum int64

		for {
			v, ok := recvWithContext(ctx, out)
			if !ok {
				break
			}

			sum += int64(v)
		}

		return sum
	}
}

func runChannelsSplitMerge(inputs []int, workers int, leftFn func(int) int, rightFn func(int) int) int64 {
	return buildChannelsSplitMerge(inputs, workers, leftFn, rightFn)(context.Background())
}

//nolint:gocognit,gocyclo // benchmark plumbing keeps channel wiring together.
func buildChannelsSplitBy(inputs []int, workers int, leftFn func(int) int, rightFn func(int) int) func(context.Context) int64 {
	workers = normalizeWorkers(workers)
	in := make(chan int)
	left := make(chan int)
	right := make(chan int)
	leftOut := make(chan int)
	rightOut := make(chan int)
	out := make(chan int)

	var wgLeft sync.WaitGroup
	wgLeft.Add(workers)

	var wgRight sync.WaitGroup
	wgRight.Add(workers)

	return func(ctx context.Context) int64 {
		go func() {
			for {
				v, ok := recvWithContext(ctx, in)
				if !ok {
					break
				}

				if v%2 == 0 {
					if !sendWithContext(ctx, left, v) {
						break
					}
				} else {
					if !sendWithContext(ctx, right, v) {
						break
					}
				}
			}

			close(left)
			close(right)
		}()

		for range workers {
			go func() {
				defer wgLeft.Done()

				for {
					v, ok := recvWithContext(ctx, left)
					if !ok {
						return
					}

					if !sendWithContext(ctx, leftOut, leftFn(v)) {
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
					v, ok := recvWithContext(ctx, right)
					if !ok {
						return
					}

					if !sendWithContext(ctx, rightOut, rightFn(v)) {
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
					v, ok := recvWithContext(ctx, leftOut)
					if !ok {
						return
					}

					if !sendWithContext(ctx, out, v) {
						return
					}
				}
			}()

			go func() {
				defer wg.Done()

				for {
					v, ok := recvWithContext(ctx, rightOut)
					if !ok {
						return
					}

					if !sendWithContext(ctx, out, v) {
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
				if !sendWithContext(ctx, in, v) {
					return
				}
			}
		}()

		var sum int64

		for {
			v, ok := recvWithContext(ctx, out)
			if !ok {
				break
			}

			sum += int64(v)
		}

		return sum
	}
}

func runChannelsSplitBy(inputs []int, workers int, leftFn func(int) int, rightFn func(int) int) int64 {
	return buildChannelsSplitBy(inputs, workers, leftFn, rightFn)(context.Background())
}

//nolint:gocognit // Benchmark wiring trades clarity for direct channel plumbing.
func runChannelsCompositeOneToMany(ctx context.Context, inputs []int, stages int, workers int, fn func(int) []int) int64 {
	workers = normalizeWorkers(workers)
	first := make(chan int)
	in := first

	for range stages {
		out := make(chan int)
		var wg sync.WaitGroup
		wg.Add(workers)

		for range workers {
			go func(in <-chan int, out chan<- int) {
				defer wg.Done()

				for {
					v, ok := recvWithContext(ctx, in)
					if !ok {
						return
					}

					for _, outV := range fn(v) {
						if !sendWithContext(ctx, out, outV) {
							return
						}
					}
				}
			}(in, out)
		}

		go func(out chan int) {
			wg.Wait()
			close(out)
		}(out)

		in = out
	}

	go func() {
		defer close(first)

		for _, v := range inputs {
			if !sendWithContext(ctx, first, v) {
				return
			}
		}
	}()

	var sum int64

	for {
		v, ok := recvWithContext(ctx, in)
		if !ok {
			break
		}

		sum += int64(v)
	}

	return sum
}

//nolint:gocognit // Benchmark wiring trades clarity for direct channel plumbing.
func runChannelsCompositeBatch(ctx context.Context, inputs []int, stages int, workers int, batchSize int) int64 {
	workers = normalizeWorkers(workers)
	first := make(chan int)
	in := first

	for range stages {
		out := make(chan int)
		var wg sync.WaitGroup
		wg.Add(workers)

		for range workers {
			go func(in <-chan int, out chan<- int) {
				defer wg.Done()

				batch := make([]int, 0, batchSize)
				flush := func() bool {
					for _, v := range batch {
						if !sendWithContext(ctx, out, v) {
							return false
						}
					}

					batch = batch[:0]

					return true
				}

				for {
					v, ok := recvWithContext(ctx, in)
					if !ok {
						break
					}

					batch = append(batch, v)
					if len(batch) >= batchSize {
						if !flush() {
							return
						}
					}
				}

				if len(batch) > 0 {
					_ = flush()
				}
			}(in, out)
		}

		go func(out chan int) {
			wg.Wait()
			close(out)
		}(out)

		in = out
	}

	go func() {
		defer close(first)

		for _, v := range inputs {
			if !sendWithContext(ctx, first, v) {
				return
			}
		}
	}()

	var sum int64

	for {
		v, ok := recvWithContext(ctx, in)
		if !ok {
			break
		}

		sum += int64(v)
	}

	return sum
}

func runChannelsCompositeBatchChan(ctx context.Context, inputs []int, stages int, workers int, batchSize int) int64 {
	workers = normalizeWorkers(workers)
	first := make(chan int)
	var in <-chan int = first

	for range stages {
		in = runChannelsCompositeBatchChanStage(ctx, in, workers, batchSize)
	}

	go func() {
		defer close(first)

		for _, v := range inputs {
			if !sendWithContext(ctx, first, v) {
				return
			}
		}
	}()

	var sum int64

	for {
		v, ok := recvWithContext(ctx, in)
		if !ok {
			break
		}

		sum += int64(v)
	}

	return sum
}

func runChannelsCompositeBatchChanStage(ctx context.Context, in <-chan int, workers int, batchSize int) <-chan int {
	out := make(chan int)
	batches := make(chan chan int)

	var wg sync.WaitGroup
	wg.Add(workers)

	for range workers {
		go runChannelsCompositeBatchChanWorker(ctx, in, batches, batchSize, &wg)
	}

	go func() {
		wg.Wait()
		close(batches)
	}()

	go func(out chan<- int, batches <-chan chan int) {
		for {
			select {
			case <-ctx.Done():
				close(out)

				return
			case batch, ok := <-batches:
				if !ok {
					close(out)

					return
				}

				for {
					v, ok := recvWithContext(ctx, batch)
					if !ok {
						break
					}

					if !sendWithContext(ctx, out, v) {
						close(out)

						return
					}
				}
			}
		}
	}(out, batches)

	return out
}

func runChannelsCompositeBatchChanWorker(
	ctx context.Context,
	in <-chan int,
	batches chan<- chan int,
	batchSize int,
	wg *sync.WaitGroup,
) {
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

	for {
		v, ok := recvWithContext(ctx, in)
		if !ok {
			break
		}

		if batchCh == nil {
			batchCh = make(chan int)
			if !sendWithContext(ctx, batches, batchCh) {
				flush()

				return
			}
		}

		if !sendWithContext(ctx, batchCh, v) {
			flush()

			return
		}

		batchCount += 1

		if batchCount >= batchSize {
			flush()
		}
	}

	flush()
}

//nolint:gocognit // Benchmark wiring trades clarity for direct channel plumbing.
func buildChannelsOneToMany(inputs []int, workers int, fn func(int) []int) func(context.Context) int64 {
	workers = normalizeWorkers(workers)
	in := make(chan int)
	out := make(chan int)

	var wg sync.WaitGroup
	wg.Add(workers)

	return func(ctx context.Context) int64 {
		for range workers {
			go func() {
				defer wg.Done()

				for {
					v, ok := recvWithContext(ctx, in)
					if !ok {
						return
					}

					for _, outValue := range fn(v) {
						if !sendWithContext(ctx, out, outValue) {
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
				if !sendWithContext(ctx, in, v) {
					return
				}
			}
		}()

		var sum int64

		for {
			v, ok := recvWithContext(ctx, out)
			if !ok {
				break
			}

			sum += int64(v)
		}

		return sum
	}
}

func runChannelsOneToMany(inputs []int, workers int, fn func(int) []int) int64 {
	return buildChannelsOneToMany(inputs, workers, fn)(context.Background())
}

func buildChannelsFromChan(inputs []int, workers int, fn func(int) int) func(context.Context) int64 {
	return buildChannelsOneToOne(inputs, workers, fn, false)
}

func runChannelsFromChan(inputs []int, workers int, fn func(int) int) int64 {
	return buildChannelsFromChan(inputs, workers, fn)(context.Background())
}

func buildChannelsSinkFromChan(inputs []int, workers int, fn func(int) int) func(context.Context) int64 {
	workers = normalizeWorkers(workers)
	in := make(chan int)
	sums := make([]int64, workers)

	var wg sync.WaitGroup
	wg.Add(workers)

	return func(ctx context.Context) int64 {
		for w := range workers {
			worker := w

			go func() {
				defer wg.Done()

				var local int64

				for {
					v, ok := recvWithContext(ctx, in)
					if !ok {
						break
					}

					local += int64(fn(v))
				}

				sums[worker] = local
			}()
		}

		go func() {
			defer close(in)

			for _, v := range inputs {
				if !sendWithContext(ctx, in, v) {
					return
				}
			}
		}()

		wg.Wait()

		var sum int64
		for _, value := range sums {
			sum += value
		}

		return sum
	}
}

func runChannelsSinkFromChan(inputs []int, workers int, fn func(int) int) int64 {
	return buildChannelsSinkFromChan(inputs, workers, fn)(context.Background())
}

//nolint:gocognit // Benchmark wiring trades clarity for direct channel plumbing.
func buildChannelsBatch(inputs []int, workers int, batchSize int, fn func(int) int) func(context.Context) int64 {
	workers = normalizeWorkers(workers)
	in := make(chan int)
	out := make(chan []int)

	var wg sync.WaitGroup
	wg.Add(workers)

	return func(ctx context.Context) int64 {
		for range workers {
			go func() {
				defer wg.Done()

				batch := make([]int, 0, batchSize)

				flush := func() bool {
					if len(batch) == 0 {
						return true
					}

					if !sendWithContext(ctx, out, batch) {
						return false
					}

					batch = nil

					return true
				}

				for {
					v, ok := recvWithContext(ctx, in)
					if !ok {
						break
					}

					batch = append(batch, v)
					if len(batch) >= batchSize {
						if !flush() {
							return
						}
					}
				}

				_ = flush()
			}()
		}

		go func() {
			wg.Wait()
			close(out)
		}()

		go func() {
			defer close(in)

			for _, v := range inputs {
				if !sendWithContext(ctx, in, v) {
					return
				}
			}
		}()

		var sum int64

		for {
			batch, ok := recvWithContext(ctx, out)
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

func runChannelsBatch(inputs []int, workers int, batchSize int, fn func(int) int) int64 {
	return buildChannelsBatch(inputs, workers, batchSize, fn)(context.Background())
}

//nolint:gocognit // Benchmark wiring trades clarity for direct channel plumbing.
func buildChannelsBatchChan(inputs []int, workers int, batchSize int, fn func(int) int) func(context.Context) int64 {
	workers = normalizeWorkers(workers)
	in := make(chan int)
	out := make(chan chan int)

	var wg sync.WaitGroup
	wg.Add(workers)

	return func(ctx context.Context) int64 {
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

				for {
					v, ok := recvWithContext(ctx, in)
					if !ok {
						break
					}

					if batchCh == nil {
						batchCh = make(chan int)
						if !sendWithContext(ctx, out, batchCh) {
							flush()

							return
						}
					}

					if !sendWithContext(ctx, batchCh, v) {
						flush()

						return
					}

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
			defer close(in)

			for _, v := range inputs {
				if !sendWithContext(ctx, in, v) {
					return
				}
			}
		}()

		var sum int64

		for {
			batch, ok := recvWithContext(ctx, out)
			if !ok {
				break
			}

			for {
				v, ok := recvWithContext(ctx, batch)
				if !ok {
					break
				}

				sum += int64(fn(v))
			}
		}

		return sum
	}
}

func runChannelsBatchChan(inputs []int, workers int, batchSize int, fn func(int) int) int64 {
	return buildChannelsBatchChan(inputs, workers, batchSize, fn)(context.Background())
}

func buildPipelineOneToOne(inputs []int, workers int, fn func(int) int, dropZero bool) (func(context.Context) (int64, error), error) {
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
		err = pipe.Run(ctx)
		if err != nil {
			return 0, err
		}

		return sum, nil
	}, nil
}

func runPipelineOneToOne(inputs []int, workers int, fn func(int) int, dropZero bool) (int64, error) {
	run, err := buildPipelineOneToOne(inputs, workers, fn, dropZero)
	if err != nil {
		return 0, err
	}

	return run(context.Background())
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
		err = pipe.Run(ctx)
		if err != nil {
			return 0, err
		}

		return sum, nil
	}, nil
}

func runPipelineTwoStage(inputs []int, workers int, stage1 func(int) int, stage2 func(int) int) (int64, error) {
	run, err := buildPipelineTwoStage(inputs, workers, stage1, stage2)
	if err != nil {
		return 0, err
	}

	return run(context.Background())
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
		err = pipe.Run(ctx)
		if err != nil {
			return 0, err
		}

		return sum, nil
	}, nil
}

func runPipelineSplitMerge(inputs []int, workers int, leftFn func(int) int, rightFn func(int) int) (int64, error) {
	run, err := buildPipelineSplitMerge(inputs, workers, leftFn, rightFn)
	if err != nil {
		return 0, err
	}

	return run(context.Background())
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
		err = pipe.Run(ctx)
		if err != nil {
			return 0, err
		}

		return sum, nil
	}, nil
}

func runPipelineSplitBy(inputs []int, workers int, leftFn func(int) int, rightFn func(int) int) (int64, error) {
	run, err := buildPipelineSplitBy(inputs, workers, leftFn, rightFn)
	if err != nil {
		return 0, err
	}

	return run(context.Background())
}

func buildPipelineOneToMany(inputs []int, workers int, fn func(int) []int) (func(context.Context) (int64, error), error) {
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

	step := pipeline.OneToMany(pipe, "many", root, func(ctx context.Context, in int) ([]int, error) {
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
		err = pipe.Run(ctx)
		if err != nil {
			return 0, err
		}

		return sum, nil
	}, nil
}

func runPipelineOneToMany(inputs []int, workers int, fn func(int) []int) (int64, error) {
	run, err := buildPipelineOneToMany(inputs, workers, fn)
	if err != nil {
		return 0, err
	}

	return run(context.Background())
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

	step := pipeline.FromChan(pipe, "from-chan", root, func(ctx context.Context, input <-chan int, output chan int) error {
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
		err = pipe.Run(ctx)
		if err != nil {
			return 0, err
		}

		return sum, nil
	}, nil
}

func runPipelineFromChan(inputs []int, workers int, fn func(int) int) (int64, error) {
	run, err := buildPipelineFromChan(inputs, workers, fn)
	if err != nil {
		return 0, err
	}

	return run(context.Background())
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

	var sum atomic.Int64
	sink := pipeline.SinkFromChan(pipe, "sink", root, func(ctx context.Context, input <-chan int) error {
		for v := range input {
			sum.Add(int64(fn(v)))
		}

		return nil
	}, stepOptions[int](workers)...)

	if sink == nil {
		return nil, pipe.Err()
	}

	return func(ctx context.Context) (int64, error) {
		err = pipe.Run(ctx)
		if err != nil {
			return 0, err
		}

		return sum.Load(), nil
	}, nil
}

func runPipelineSinkFromChan(inputs []int, workers int, fn func(int) int) (int64, error) {
	run, err := buildPipelineSinkFromChan(inputs, workers, fn)
	if err != nil {
		return 0, err
	}

	return run(context.Background())
}

func buildPipelineBatch(inputs []int, workers int, batchSize int, fn func(int) int) (func(context.Context) (int64, error), error) {
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

	batch := pipeline.Batch(pipe, "batch", root, pipeline.BatchPolicy{MaxSize: batchSize}, stepOptions[[]int](workers)...)
	if batch == nil {
		return nil, pipe.Err()
	}

	var sum int64
	sink := pipeline.Sink(pipe, "sink", batch, func(ctx context.Context, batch []int) error {
		for _, v := range batch {
			sum += int64(fn(v))
		}

		return nil
	})

	if sink == nil {
		return nil, pipe.Err()
	}

	return func(ctx context.Context) (int64, error) {
		err = pipe.Run(ctx)
		if err != nil {
			return 0, err
		}

		return sum, nil
	}, nil
}

func runPipelineBatch(inputs []int, workers int, batchSize int, fn func(int) int) (int64, error) {
	run, err := buildPipelineBatch(inputs, workers, batchSize, fn)
	if err != nil {
		return 0, err
	}

	return run(context.Background())
}

func buildPipelineBatchChan(inputs []int, workers int, batchSize int, fn func(int) int) (func(context.Context) (int64, error), error) {
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

	batch := pipeline.BatchChan(pipe, "batch", root, pipeline.BatchPolicy{MaxSize: batchSize}, stepOptions[<-chan int](workers)...)
	if batch == nil {
		return nil, pipe.Err()
	}

	var sum int64
	sink := pipeline.Sink(pipe, "sink", batch, func(ctx context.Context, batch <-chan int) error {
		for v := range batch {
			sum += int64(fn(v))
		}

		return nil
	})

	if sink == nil {
		return nil, pipe.Err()
	}

	return func(ctx context.Context) (int64, error) {
		err = pipe.Run(ctx)
		if err != nil {
			return 0, err
		}

		return sum, nil
	}, nil
}

func runPipelineBatchChan(inputs []int, workers int, batchSize int, fn func(int) int) (int64, error) {
	run, err := buildPipelineBatchChan(inputs, workers, batchSize, fn)
	if err != nil {
		return 0, err
	}

	return run(context.Background())
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

		prev = many
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

		flatten := pipeline.FromChan(
			pipe,
			stageName+"-flatten",
			batch,
			func(ctx context.Context, input <-chan <-chan int, output chan int) error {
				for ch := range input {
					for v := range ch {
						output <- v
					}
				}

				return nil
			},
			stepOptions[int](workers)...)
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
