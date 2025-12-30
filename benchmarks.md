# Benchmarks

This document tracks overhead benchmarks comparing go-pipeline with baseline implementations.

## Scenarios
- Single-stage one-to-one: baseline loop vs ad-hoc channels vs go-pipeline.
- Two-stage chain: baseline loop vs ad-hoc channels vs go-pipeline.
- Fan-out/fan-in (split/merge): baseline loop vs ad-hoc channels vs go-pipeline.
- Realistic variants (suffix `Realistic`) add CPU work per item using `realisticWork` (see `benchWorkIters = 64`).
- Pipeline-only coverage for step types: OneToMany, FromChan, SinkFromChan, Batch, BatchChan (plus Realistic variants).
- Overhead sweeps: work intensity sweep and step-count sweep (conc=1).

## How to run
Run the full suite with allocations (fast iteration uses `-count 1`):
```bash
go test ./pkg/pipeline -run '^$' -bench Benchmark -benchmem -count 1
```

For more stable numbers, increase the count and take medians:
```bash
go test ./pkg/pipeline -run '^$' -bench Benchmark -benchmem -count 5
```

Optional: pin CPU count for consistent runs:
```bash
GOMAXPROCS=1 go test ./pkg/pipeline -run '^$' -bench Benchmark -benchmem -count 1
GOMAXPROCS=4 go test ./pkg/pipeline -run '^$' -bench Benchmark -benchmem -count 1
```

To regenerate the overhead model tables from a run:
```bash
go test ./pkg/pipeline -run '^$' -bench Benchmark -benchmem -count 1 > /tmp/bench.txt
python3 scripts/bench_overhead_summary.py /tmp/bench.txt
```

## Results
Record results below with environment details so comparisons stay meaningful.

## Summary (single run, count=1)
### Simple work
| Scenario | Loop (ns/op) | Channels (ns/op) | Pipeline (ns/op) | Pipeline/Loop | Pipeline/Channels |
| --- | ---: | ---: | ---: | ---: | ---: |
| OneToOne items=1k conc=1 | 793.9 | 550,598.0 | 2,255,983.0 | 2841.6x | 4.1x |
| OneToOne items=1k conc=4 | 680.3 | 638,533.0 | 2,452,563.0 | 3605.1x | 3.8x |
| OneToOne items=16k conc=4 | 11,139.0 | 9,546,340.0 | 38,538,407.0 | 3459.8x | 4.0x |
| TwoStage items=1k conc=1 | 1,006.0 | 994,345.0 | 3,217,371.0 | 3198.2x | 3.2x |
| TwoStage items=1k conc=4 | 1,031.0 | 933,317.0 | 4,229,425.0 | 4102.3x | 4.5x |
| TwoStage items=16k conc=4 | 18,807.0 | 16,152,540.0 | 58,926,113.0 | 3133.2x | 3.6x |
| SplitMerge items=1k conc=1 | 1,066.0 | 1,641,073.0 | 9,167,580.0 | 8600.0x | 5.6x |
| SplitMerge items=1k conc=4 | 1,019.0 | 1,544,646.0 | 8,208,736.0 | 8055.7x | 5.3x |
| SplitMerge items=16k conc=4 | 17,005.0 | 25,518,934.0 | 130,505,287.0 | 7674.5x | 5.1x |

### Realistic work
| Scenario | Loop (ns/op) | Channels (ns/op) | Pipeline (ns/op) | Pipeline/Loop | Pipeline/Channels |
| --- | ---: | ---: | ---: | ---: | ---: |
| OneToOne items=1k conc=1 | 114,704.0 | 679,530.0 | 2,548,695.0 | 22.2x | 3.8x |
| OneToOne items=1k conc=4 | 114,016.0 | 728,344.0 | 2,520,078.0 | 22.1x | 3.5x |
| OneToOne items=16k conc=4 | 1,860,727.0 | 11,487,400.0 | 40,734,086.0 | 21.9x | 3.5x |
| TwoStage items=1k conc=1 | 290,012.0 | 1,438,369.0 | 3,570,883.0 | 12.3x | 2.5x |
| TwoStage items=1k conc=4 | 292,369.0 | 1,051,071.0 | 3,821,716.0 | 13.1x | 3.6x |
| TwoStage items=16k conc=4 | 4,458,318.0 | 17,700,450.0 | 65,149,612.0 | 14.6x | 3.7x |
| SplitMerge items=1k conc=1 | 283,577.0 | 1,975,245.0 | 9,207,839.0 | 32.5x | 4.7x |
| SplitMerge items=1k conc=4 | 258,244.0 | 1,818,779.0 | 8,545,557.0 | 33.1x | 4.7x |
| SplitMerge items=16k conc=4 | 3,957,529.0 | 27,802,797.0 | 146,606,250.0 | 37.0x | 5.3x |

Pipeline-only (single run):
- OneToManyPipeline: 3,780,725.0 ns/op
- FromChanPipeline: 2,320,030.0 ns/op
- SinkFromChanPipeline: 780,137.0 ns/op
- BatchPipeline: 467,761.0 ns/op
- BatchChanPipeline: 928,772.0 ns/op
- OneToManyPipelineRealistic: 4,477,737.0 ns/op
- FromChanPipelineRealistic: 2,096,347.0 ns/op
- SinkFromChanPipelineRealistic: 946,131.0 ns/op
- BatchPipelineRealistic: 578,139.0 ns/op
- BatchChanPipelineRealistic: 1,190,538.0 ns/op

## Overhead model
The pipeline cost is well-approximated as a fixed per-item overhead plus a per-step overhead.

### Work sweep (conc=1, items=4096)
| Work iters | Loop per item (ns) | Pipeline per item (ns) | Overhead per item (ns) | Pipeline/Loop |
| ---: | ---: | ---: | ---: | ---: |
| 0 | 3.8 | 2,090.7 | 2,086.9 | 555.1x |
| 4 | 7.0 | 1,908.8 | 1,901.8 | 272.9x |
| 16 | 19.3 | 1,933.9 | 1,914.7 | 100.5x |
| 64 | 116.4 | 2,029.8 | 1,913.4 | 17.4x |
| 256 | 629.1 | 2,614.8 | 1,985.7 | 4.2x |

Overhead per item stays roughly flat (~1.9-2.1 us) while work per item grows, so the overhead ratio shrinks as work increases.

### Step-count sweep (conc=1, items=4096, work iters=0)
| Steps | Loop per item (ns) | Pipeline per item (ns) | Overhead per item (ns) | Overhead per step (ns) |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 5.3 | 1,967.6 | 1,962.3 | 1,962.3 |
| 2 | 7.9 | 3,269.3 | 3,261.4 | 1,630.7 |
| 4 | 15.0 | 5,187.6 | 5,172.6 | 1,293.2 |
| 8 | 29.5 | 12,788.5 | 12,758.9 | 1,594.9 |

Per-step overhead clusters around ~1.3-1.9 us in this run (count=1).

Linear fit on overhead per item vs steps (conc=1):
- Overhead per item ≈ 0 ns + 1,544.9 ns * steps (fit intercept is ~-4.4 ns, within noise).

### When is it worth it?
Use the overhead per item to decide if the pipeline cost is acceptable. A simple rule of thumb is to aim for work per item that is at least 10x the overhead per item (keeps overhead under ~10%).

| Steps | Overhead per item (ns) | Work per item for <10% overhead (ns) |
| ---: | ---: | ---: |
| 1 | 1,962.3 | 19,622.8 |
| 2 | 3,261.4 | 32,613.9 |
| 4 | 5,172.6 | 51,726.2 |
| 8 | 12,758.9 | 127,589.3 |

These thresholds are hardware- and configuration-specific (concurrency, buffers, retries, metrics, etc.). Re-run the benchmarks on your target environment to calibrate.

## Decisions
- Keep serial baselines in the main summary tables alongside worker baselines.
- Treat loop worker baselines as unbuffered; buffer effects are reserved for option-level benchmarks.
- Target loop + channel + pipeline baselines for every step type; document any exceptions explicitly.
- Use count=1 for fast iteration during development and count=5 for median reporting.
- Keep overhead sweeps at conc=1 with items=4096 to isolate per-item and per-step costs.
- Track realistic work using `benchWorkIters=64` to show how overhead ratios shrink as work grows.
- Next option-level benchmarks to add: `StepBufferSize` and `StepMaxInFlight`.

```
Date: 2025-12-30
Machine: VirtualApple @ 2.50GHz
OS/Arch: darwin/amd64
Go version: go1.25.0
Commit: c7dbda45f5898932feb138ac131a583b585a6459
Command: go test ./pkg/pipeline -run '^$' -bench Benchmark -benchmem -count 1
Notes: Default GOMAXPROCS. Overhead sweeps use items=4096.

goos: darwin
goarch: amd64
pkg: github.com/askiada/go-pipeline/pkg/pipeline
cpu: VirtualApple @ 2.50GHz
BenchmarkOneToOne/items=1k/conc=1/loop-8   	 1755778	       793.9 ns/op	       0 B/op	       0 allocs/op
BenchmarkOneToOne/items=1k/conc=1/channels-8         	    2371	    550598 ns/op	     366 B/op	       6 allocs/op
BenchmarkOneToOne/items=1k/conc=1/pipeline-8         	     591	   2255983 ns/op	    2335 B/op	      48 allocs/op
BenchmarkOneToOne/items=1k/conc=4/loop-8             	 1734610	       680.3 ns/op	       0 B/op	       0 allocs/op
BenchmarkOneToOne/items=1k/conc=4/channels-8         	    1722	    638533 ns/op	     524 B/op	       9 allocs/op
BenchmarkOneToOne/items=1k/conc=4/pipeline-8         	     481	   2452563 ns/op	    3598 B/op	      63 allocs/op
BenchmarkOneToOne/items=16k/conc=4/loop-8            	  105920	     11139 ns/op	       0 B/op	       0 allocs/op
BenchmarkOneToOne/items=16k/conc=4/channels-8        	     127	   9546340 ns/op	     504 B/op	       9 allocs/op
BenchmarkOneToOne/items=16k/conc=4/pipeline-8        	      27	  38538407 ns/op	    3552 B/op	      63 allocs/op
BenchmarkOneToOneRealistic/items=1k/conc=1/loop-8    	    9067	    114704 ns/op	       0 B/op	       0 allocs/op
BenchmarkOneToOneRealistic/items=1k/conc=1/channels-8         	    1674	    679530 ns/op	     360 B/op	       6 allocs/op
BenchmarkOneToOneRealistic/items=1k/conc=1/pipeline-8         	     571	   2548695 ns/op	    2306 B/op	      48 allocs/op
BenchmarkOneToOneRealistic/items=1k/conc=4/loop-8             	    8988	    114016 ns/op	       0 B/op	       0 allocs/op
BenchmarkOneToOneRealistic/items=1k/conc=4/channels-8         	    1610	    728344 ns/op	     512 B/op	       9 allocs/op
BenchmarkOneToOneRealistic/items=1k/conc=4/pipeline-8         	     424	   2520078 ns/op	    3575 B/op	      63 allocs/op
BenchmarkOneToOneRealistic/items=16k/conc=4/loop-8            	     648	   1860727 ns/op	       0 B/op	       0 allocs/op
BenchmarkOneToOneRealistic/items=16k/conc=4/channels-8        	     103	  11487400 ns/op	     504 B/op	       9 allocs/op
BenchmarkOneToOneRealistic/items=16k/conc=4/pipeline-8        	      27	  40734086 ns/op	    3571 B/op	      63 allocs/op
BenchmarkTwoStage/items=1k/conc=1/loop-8                      	 1000000	      1006 ns/op	       0 B/op	       0 allocs/op
BenchmarkTwoStage/items=1k/conc=1/channels-8                  	    1426	    994345 ns/op	     561 B/op	      10 allocs/op
BenchmarkTwoStage/items=1k/conc=1/pipeline-8                  	     362	   3217371 ns/op	    2908 B/op	      59 allocs/op
BenchmarkTwoStage/items=1k/conc=4/loop-8                      	 1000000	      1031 ns/op	       0 B/op	       0 allocs/op
BenchmarkTwoStage/items=1k/conc=4/channels-8                  	    1377	    933317 ns/op	     856 B/op	      16 allocs/op
BenchmarkTwoStage/items=1k/conc=4/pipeline-8                  	     243	   4229425 ns/op	    5171 B/op	      87 allocs/op
BenchmarkTwoStage/items=16k/conc=4/loop-8                     	   65400	     18807 ns/op	       0 B/op	       0 allocs/op
BenchmarkTwoStage/items=16k/conc=4/channels-8                 	      81	  16152540 ns/op	     856 B/op	      16 allocs/op
BenchmarkTwoStage/items=16k/conc=4/pipeline-8                 	      18	  58926113 ns/op	    5128 B/op	      87 allocs/op
BenchmarkTwoStageRealistic/items=1k/conc=1/loop-8             	    4066	    290012 ns/op	       0 B/op	       0 allocs/op
BenchmarkTwoStageRealistic/items=1k/conc=1/channels-8         	     999	   1438369 ns/op	     560 B/op	      10 allocs/op
BenchmarkTwoStageRealistic/items=1k/conc=1/pipeline-8         	     320	   3570883 ns/op	    2888 B/op	      59 allocs/op
BenchmarkTwoStageRealistic/items=1k/conc=4/loop-8             	    3748	    292369 ns/op	       0 B/op	       0 allocs/op
BenchmarkTwoStageRealistic/items=1k/conc=4/channels-8         	    1088	   1051071 ns/op	     863 B/op	      16 allocs/op
BenchmarkTwoStageRealistic/items=1k/conc=4/pipeline-8         	     304	   3821716 ns/op	    5136 B/op	      87 allocs/op
BenchmarkTwoStageRealistic/items=16k/conc=4/loop-8            	     273	   4458318 ns/op	       0 B/op	       0 allocs/op
BenchmarkTwoStageRealistic/items=16k/conc=4/channels-8        	      58	  17700450 ns/op	     848 B/op	      16 allocs/op
BenchmarkTwoStageRealistic/items=16k/conc=4/pipeline-8        	      19	  65149612 ns/op	    5129 B/op	      87 allocs/op
BenchmarkSplitMerge/items=1k/conc=1/loop-8                    	 1144452	      1066 ns/op	       0 B/op	       0 allocs/op
BenchmarkSplitMerge/items=1k/conc=1/channels-8                	     691	   1641073 ns/op	    1040 B/op	      18 allocs/op
BenchmarkSplitMerge/items=1k/conc=1/pipeline-8                	     121	   9167580 ns/op	    5367 B/op	     100 allocs/op
BenchmarkSplitMerge/items=1k/conc=4/loop-8                    	 1000000	      1019 ns/op	       0 B/op	       0 allocs/op
BenchmarkSplitMerge/items=1k/conc=4/channels-8                	     752	   1544646 ns/op	    1337 B/op	      24 allocs/op
BenchmarkSplitMerge/items=1k/conc=4/pipeline-8                	     138	   8208736 ns/op	    7594 B/op	     128 allocs/op
BenchmarkSplitMerge/items=16k/conc=4/loop-8                   	   70898	     17005 ns/op	       0 B/op	       0 allocs/op
BenchmarkSplitMerge/items=16k/conc=4/channels-8               	      42	  25518934 ns/op	    1366 B/op	      24 allocs/op
BenchmarkSplitMerge/items=16k/conc=4/pipeline-8               	       9	 130505287 ns/op	    8609 B/op	     139 allocs/op
BenchmarkSplitMergeRealistic/items=1k/conc=1/loop-8           	    4692	    283577 ns/op	       0 B/op	       0 allocs/op
BenchmarkSplitMergeRealistic/items=1k/conc=1/channels-8       	     585	   1975245 ns/op	    1053 B/op	      18 allocs/op
BenchmarkSplitMergeRealistic/items=1k/conc=1/pipeline-8       	     128	   9207839 ns/op	    5326 B/op	     100 allocs/op
BenchmarkSplitMergeRealistic/items=1k/conc=4/loop-8           	    4856	    258244 ns/op	       0 B/op	       0 allocs/op
BenchmarkSplitMergeRealistic/items=1k/conc=4/channels-8       	     669	   1818779 ns/op	    1337 B/op	      24 allocs/op
BenchmarkSplitMergeRealistic/items=1k/conc=4/pipeline-8       	     146	   8545557 ns/op	    7549 B/op	     128 allocs/op
BenchmarkSplitMergeRealistic/items=16k/conc=4/loop-8          	     320	   3957529 ns/op	       0 B/op	       0 allocs/op
BenchmarkSplitMergeRealistic/items=16k/conc=4/channels-8      	      40	  27802797 ns/op	    1328 B/op	      24 allocs/op
BenchmarkSplitMergeRealistic/items=16k/conc=4/pipeline-8      	       8	 146606250 ns/op	    8362 B/op	     136 allocs/op
BenchmarkOneToManyPipeline-8                                  	     313	   3780725 ns/op	   19891 B/op	    1087 allocs/op
BenchmarkOneToManyPipelineRealistic-8                         	     260	   4477737 ns/op	   19882 B/op	    1087 allocs/op
BenchmarkFromChanPipeline-8                                   	     622	   2320030 ns/op	    2644 B/op	      52 allocs/op
BenchmarkFromChanPipelineRealistic-8                          	     561	   2096347 ns/op	    2648 B/op	      52 allocs/op
BenchmarkSinkFromChanPipeline-8                               	    1518	    780137 ns/op	    2000 B/op	      39 allocs/op
BenchmarkSinkFromChanPipelineRealistic-8                      	    1264	    946131 ns/op	    2000 B/op	      39 allocs/op
BenchmarkBatchPipeline-8                                      	    2656	    467761 ns/op	   18415 B/op	     239 allocs/op
BenchmarkBatchPipelineRealistic-8                             	    2392	    578139 ns/op	   18414 B/op	     239 allocs/op
BenchmarkBatchChanPipeline-8                                  	    1291	    928772 ns/op	    5865 B/op	      79 allocs/op
BenchmarkBatchChanPipelineRealistic-8                         	    1131	   1190538 ns/op	    5868 B/op	      79 allocs/op
BenchmarkOverheadWorkSweep/iters=0/loop-8                     	   74928	     15428 ns/op	       0 B/op	       0 allocs/op
BenchmarkOverheadWorkSweep/iters=0/pipeline-8                 	     152	   8563378 ns/op	    2332 B/op	      48 allocs/op
BenchmarkOverheadWorkSweep/iters=4/loop-8                     	   49297	     28649 ns/op	       0 B/op	       0 allocs/op
BenchmarkOverheadWorkSweep/iters=4/pipeline-8                 	     157	   7818462 ns/op	    2304 B/op	      48 allocs/op
BenchmarkOverheadWorkSweep/iters=16/loop-8                    	   14149	     78851 ns/op	       0 B/op	       0 allocs/op
BenchmarkOverheadWorkSweep/iters=16/pipeline-8                	     152	   7921364 ns/op	    2304 B/op	      48 allocs/op
BenchmarkOverheadWorkSweep/iters=64/loop-8                    	    2187	    476786 ns/op	       0 B/op	       0 allocs/op
BenchmarkOverheadWorkSweep/iters=64/pipeline-8                	     142	   8313949 ns/op	    2304 B/op	      48 allocs/op
BenchmarkOverheadWorkSweep/iters=256/loop-8                   	     393	   2576757 ns/op	       0 B/op	       0 allocs/op
BenchmarkOverheadWorkSweep/iters=256/pipeline-8               	     100	  10710138 ns/op	    2304 B/op	      48 allocs/op
BenchmarkOverheadStepScaling/steps=1/loop-8                   	   71354	     21760 ns/op	       0 B/op	       0 allocs/op
BenchmarkOverheadStepScaling/steps=1/pipeline-8               	     153	   8059267 ns/op	    2313 B/op	      49 allocs/op
BenchmarkOverheadStepScaling/steps=2/loop-8                   	   39301	     32337 ns/op	       0 B/op	       0 allocs/op
BenchmarkOverheadStepScaling/steps=2/pipeline-8               	      93	  13390985 ns/op	    2955 B/op	      61 allocs/op
BenchmarkOverheadStepScaling/steps=4/loop-8                   	   19148	     61443 ns/op	       0 B/op	       0 allocs/op
BenchmarkOverheadStepScaling/steps=4/pipeline-8               	      51	  21248508 ns/op	    4243 B/op	      87 allocs/op
BenchmarkOverheadStepScaling/steps=8/loop-8                   	   10132	    120932 ns/op	       0 B/op	       0 allocs/op
BenchmarkOverheadStepScaling/steps=8/pipeline-8               	      26	  52381526 ns/op	    6874 B/op	     137 allocs/op
PASS
ok  	github.com/askiada/go-pipeline/pkg/pipeline	129.251s
```
