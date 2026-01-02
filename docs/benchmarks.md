# Benchmarks

This document tracks overhead benchmarks comparing go-pipeline with baseline implementations.

## Scenarios
- Baselines: loop-serial, loop-workers (no channels), channels, and go-pipeline.
- Single-stage one-to-one, two-stage chain, fan-out/fan-in (split/merge), and split-by routing.
- Realistic variants (suffix `Realistic`) add CPU work per item using `realisticWork` (see `benchWorkIters = 64`).
- Step-level coverage: OneToOneOrZero, OneToMany, FromChan, SinkFromChan, Batch, BatchChan.
- Overhead sweeps: work intensity sweep (iters up to 4096) and step-count sweep (steps up to 64), conc=1.
- Composite step-count sweeps for fan-out/batch steps with inputs scaled so total outputs stay near 4096.

## How to run
Run the full suite with allocations (fast iteration uses `-count 1`):
```bash
go test ./pkg/pipeline -run '^$' -bench Benchmark -benchmem -benchtime=100ms -count 1 -timeout 700s
```

For more stable numbers, increase the count and take medians:
```bash
go test ./pkg/pipeline -run '^$' -bench Benchmark -benchmem -benchtime=100ms -count 5 -timeout 700s
```

Optional: pin CPU count for consistent runs:
```bash
GOMAXPROCS=1 go test ./pkg/pipeline -run '^$' -bench Benchmark -benchmem -count 1
GOMAXPROCS=4 go test ./pkg/pipeline -run '^$' -bench Benchmark -benchmem -count 1
```

To regenerate the overhead model tables from a run:
```bash
go test ./pkg/pipeline -run '^$' -bench Benchmark -benchmem -benchtime=100ms -count 5 -timeout 700s > /tmp/bench.txt
python3 scripts/bench_overhead_summary.py /tmp/bench.txt > /tmp/bench_summary.md
```

## Results
Record results below with environment details so comparisons stay meaningful.

## Summary (median of 5 runs)
### Simple work
| Scenario | Loop serial (ns/op) | Loop workers (ns/op) | Channels (ns/op) | Pipeline (ns/op) | Pipeline/Loop workers | Pipeline/Channels |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| OneToOne items=1k/conc=1 | 2,506 | 8,337 | 1,123,053 | 1,217,177 | 146.0x | 1.1x |
| OneToOne items=1k/conc=4 | 2,449 | 19,488 | 1,690,323 | 2,250,479 | 115.5x | 1.3x |
| OneToOne items=16k/conc=4 | 39,040 | 418,109 | 32,086,916 | 36,612,014 | 87.6x | 1.1x |
| TwoStage items=1k/conc=1 | 5,353 | 7,615 | 2,207,015 | 2,059,946 | 270.5x | 0.9x |
| TwoStage items=1k/conc=4 | 5,341 | 19,577 | 3,950,660 | 4,184,100 | 213.7x | 1.1x |
| TwoStage items=16k/conc=4 | 86,429 | 439,735 | 81,166,812 | 58,146,312 | 132.2x | 0.7x |
| SplitMerge items=1k/conc=1 | 5,489 | 9,714 | 5,043,505 | 8,828,447 | 908.8x | 1.8x |
| SplitMerge items=1k/conc=4 | 5,365 | 23,152 | 9,087,182 | 9,512,222 | 410.9x | 1.0x |
| SplitMerge items=16k/conc=4 | 86,654 | 469,341 | 155,310,250 | 152,558,708 | 325.0x | 1.0x |
| SplitBy items=1k/conc=1 | 3,769 | 12,217 | 3,628,231 | 5,166,255 | 422.9x | 1.4x |
| SplitBy items=1k/conc=4 | 3,942 | 18,256 | 3,997,618 | 5,224,730 | 286.2x | 1.3x |
| SplitBy items=16k/conc=4 | 60,643 | 427,920 | 66,186,626 | 89,461,834 | 209.1x | 1.4x |

### Realistic work
| Scenario | Loop serial (ns/op) | Loop workers (ns/op) | Channels (ns/op) | Pipeline (ns/op) | Pipeline/Loop workers | Pipeline/Channels |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| OneToOne items=1k/conc=1 | 118,528 | 124,545 | 1,365,954 | 1,373,910 | 11.0x | 1.0x |
| OneToOne items=1k/conc=4 | 119,431 | 82,956 | 2,165,673 | 2,385,722 | 28.8x | 1.1x |
| OneToOne items=16k/conc=4 | 1,891,245 | 749,287 | 37,020,153 | 40,069,237 | 53.5x | 1.1x |
| TwoStage items=1k/conc=1 | 278,441 | 282,639 | 2,239,984 | 2,354,873 | 8.3x | 1.1x |
| TwoStage items=1k/conc=4 | 282,709 | 152,329 | 4,090,062 | 4,906,130 | 32.2x | 1.2x |
| TwoStage items=16k/conc=4 | 4,449,328 | 1,292,339 | 78,381,645 | 69,699,020 | 53.9x | 0.9x |
| SplitMerge items=1k/conc=1 | 237,280 | 241,026 | 5,555,399 | 8,659,094 | 35.9x | 1.6x |
| SplitMerge items=1k/conc=4 | 237,144 | 128,043 | 8,282,991 | 10,367,875 | 81.0x | 1.3x |
| SplitMerge items=16k/conc=4 | 3,928,246 | 1,129,529 | 160,171,042 | 154,490,792 | 136.8x | 1.0x |

### Step-level snapshot (items=1k, conc=4)
| Step | Loop serial (ns/op) | Loop workers (ns/op) | Channels (ns/op) | Pipeline (ns/op) | Pipeline/Loop workers | Pipeline/Channels |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| OneToOneOrZero | 2,443 | 21,061 | 1,605,872 | 1,967,226 | 93.4x | 1.2x |
| OneToMany | 24,079 | 47,042 | 3,043,683 | 3,623,744 | 77.0x | 1.2x |
| FromChan | 2,494 | 18,697 | 1,755,009 | 2,258,575 | 120.8x | 1.3x |
| SinkFromChan | 2,422 | 17,938 | 842,495 | 1,522,314 | 84.9x | 1.8x |
| Batch | 10,116 | 29,687 | 979,724 | 857,349 | 28.9x | 0.9x |
| BatchChan | 10,096 | 30,671 | 1,272,293 | 958,452 | 31.2x | 0.8x |

## Step-level notes
- OneToOne/OneToOneOrZero: best for pure transforms; `OneToOneOrZero` avoids downstream traffic for zero outputs.
- OneToMany: allocates output slices per input and fans out writes; overhead increases with output count.
- FromChan: good when upstream already produces channels; pipeline adds orchestration/metrics overhead on top of channel work.
- SinkFromChan: use when sink work is heavy and benefits from sink-level concurrency; overhead is mostly coordination.
- Batch: amortizes per-item overhead at the cost of batching latency (benchmarks use MaxSize only).
- BatchChan: higher overhead due to per-batch channel creation/close; use when downstream needs streaming per batch.
- Split/SplitBy/Merge: split adds routing and merge adds extra channel hops; overhead grows with branch count.

## Overhead model
The pipeline cost is well-approximated as a fixed per-item overhead plus a per-step overhead.
Overhead columns use loop-serial as the baseline; channels are shown for comparison.
### Work sweep (conc=1, items=4096)
| Work iters | Loop per item (ns) | Channels per item (ns) | Pipeline per item (ns) | Overhead per item (ns) | Pipeline/Loop | Pipeline/Channels |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 0 | 4.1 | 821.2 | 1,114.0 | 1,109.9 | 269.0x | 1.4x |
| 4 | 6.3 | 843.4 | 1,137.4 | 1,131.1 | 180.8x | 1.3x |
| 16 | 19.9 | 817.5 | 1,223.9 | 1,204.0 | 61.5x | 1.5x |
| 64 | 119.6 | 1,025.7 | 1,382.5 | 1,262.9 | 11.6x | 1.3x |
| 256 | 609.3 | 1,654.5 | 1,935.5 | 1,326.2 | 3.2x | 1.2x |
| 1024 | 2,450.4 | 4,256.0 | 4,330.6 | 1,880.1 | 1.8x | 1.0x |
| 4096 | 9,990.9 | 13,067.1 | 13,285.6 | 3,294.6 | 1.3x | 1.0x |

Overhead per item stays roughly flat while work per item grows, so the overhead ratio shrinks as work increases.

### Step-count sweep (conc=1, items=4096, work iters=0)
| Steps | Loop per item (ns) | Channels per item (ns) | Pipeline per item (ns) | Overhead per item (ns) | Overhead per step (ns) | Pipeline/Channels |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 4.0 | 1,114.7 | 1,232.2 | 1,228.2 | 1,228.2 | 1.1x |
| 2 | 7.5 | 1,750.5 | 1,822.7 | 1,815.2 | 907.6 | 1.0x |
| 4 | 14.9 | 3,313.7 | 4,232.1 | 4,217.2 | 1,054.3 | 1.3x |
| 8 | 29.9 | 6,362.8 | 7,387.0 | 7,357.2 | 919.6 | 1.2x |
| 16 | 67.6 | 14,790.4 | 21,372.3 | 21,304.8 | 1,331.5 | 1.4x |
| 32 | 122.8 | 41,502.1 | 43,559.3 | 43,436.5 | 1,357.4 | 1.0x |
| 64 | 248.3 | 77,313.1 | 84,577.2 | 84,328.9 | 1,317.6 | 1.1x |

Per-step overhead is roughly linear in the number of steps in this run.

Linear fit on overhead per item vs steps (conc=1):
- Overhead per item approx -976.4 ns + 1,342.7 ns * steps

### When is it worth it?
Aim for work per item that is at least 10x the overhead per item (keeps overhead under ~10%).

| Steps | Overhead per item (ns) | Work per item for <10% overhead (ns) |
| ---: | ---: | ---: |
| 1 | 1,228.2 | 12,281.8 |
| 2 | 1,815.2 | 18,152.1 |
| 4 | 4,217.2 | 42,172.0 |
| 8 | 7,357.2 | 73,571.7 |
| 16 | 21,304.8 | 213,047.6 |
| 32 | 43,436.5 | 434,364.7 |
| 64 | 84,328.9 | 843,289.3 |

### Total overhead estimates (conc=1, steps)
| Steps | Overhead per item (ns) | Total @1M items (s) | Total @1B items (s) |
| ---: | ---: | ---: | ---: |
| 1 | 1,228.2 | 1.2 | 1,228.2 |
| 2 | 1,815.2 | 1.8 | 1,815.2 |
| 4 | 4,217.2 | 4.2 | 4,217.2 |
| 8 | 7,357.2 | 7.4 | 7,357.2 |
| 16 | 21,304.8 | 21.3 | 21,304.8 |
| 32 | 43,436.5 | 43.4 | 43,436.5 |
| 64 | 84,328.9 | 84.3 | 84,328.9 |

Composite sweeps scale inputs by `fanOut^stages` for OneToMany so total outputs stay near 4096; batch composites keep inputs fixed. All composite sweeps run with conc=1.

### Composite step-count sweep: OneToMany (expand -> reduce) (conc=1, items=4096)
| Stages | Loop per item (ns) | Channels per item (ns) | Pipeline per item (ns) | Overhead per item (ns) | Overhead per stage (ns) | Pipeline/Channels |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 5.4 | 844.1 | 885.9 | 880.6 | 880.6 | 1.0x |
| 2 | 7.7 | 1,095.7 | 1,201.3 | 1,193.6 | 596.8 | 1.1x |
| 4 | 9.3 | 1,444.6 | 1,518.4 | 1,509.1 | 377.3 | 1.1x |
| 8 | 9.6 | 1,478.7 | 1,646.8 | 1,637.3 | 204.7 | 1.1x |
| 12 | 9.7 | 1,419.7 | 1,579.7 | 1,570.0 | 130.8 | 1.1x |

Linear fit on overhead per item vs stages (conc=1) for OneToMany (expand -> reduce):
- Overhead per item approx 1,061.3 ns + 55.0 ns * stages

### Total overhead estimates (conc=1, stages)
| Stages | Overhead per item (ns) | Total @1M items (s) | Total @1B items (s) |
| ---: | ---: | ---: | ---: |
| 1 | 880.6 | 0.9 | 880.6 |
| 2 | 1,193.6 | 1.2 | 1,193.6 |
| 4 | 1,509.1 | 1.5 | 1,509.1 |
| 8 | 1,637.3 | 1.6 | 1,637.3 |
| 12 | 1,570.0 | 1.6 | 1,570.0 |

### Composite step-count sweep: Batch (batch -> unbatch) (conc=1, items=4096)
| Stages | Loop per item (ns) | Channels per item (ns) | Pipeline per item (ns) | Overhead per item (ns) | Overhead per stage (ns) | Pipeline/Channels |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 3.0 | 842.5 | 1,299.9 | 1,296.9 | 1,296.9 | 1.5x |
| 2 | 6.0 | 1,522.5 | 2,322.6 | 2,316.6 | 1,158.3 | 1.5x |
| 4 | 11.2 | 3,174.8 | 4,094.6 | 4,083.4 | 1,020.8 | 1.3x |
| 8 | 22.0 | 6,663.8 | 8,482.5 | 8,460.5 | 1,057.6 | 1.3x |
| 12 | 36.9 | 10,158.1 | 11,808.0 | 11,771.1 | 980.9 | 1.2x |

Linear fit on overhead per item vs stages (conc=1) for Batch (batch -> unbatch):
- Overhead per item approx 368.3 ns + 966.2 ns * stages

### Total overhead estimates (conc=1, stages)
| Stages | Overhead per item (ns) | Total @1M items (s) | Total @1B items (s) |
| ---: | ---: | ---: | ---: |
| 1 | 1,296.9 | 1.3 | 1,296.9 |
| 2 | 2,316.6 | 2.3 | 2,316.6 |
| 4 | 4,083.4 | 4.1 | 4,083.4 |
| 8 | 8,460.5 | 8.5 | 8,460.5 |
| 12 | 11,771.1 | 11.8 | 11,771.1 |

### Composite step-count sweep: BatchChan (batch -> flatten) (conc=1, items=4096)
| Stages | Loop per item (ns) | Channels per item (ns) | Pipeline per item (ns) | Overhead per item (ns) | Overhead per stage (ns) | Pipeline/Channels |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 3.2 | 3,849.2 | 1,590.8 | 1,587.6 | 1,587.6 | 0.4x |
| 2 | 5.8 | 3,318.3 | 2,892.5 | 2,886.7 | 1,443.4 | 0.9x |
| 4 | 11.6 | 7,515.3 | 6,429.9 | 6,418.3 | 1,604.6 | 0.9x |
| 8 | 21.4 | 15,552.4 | 11,894.4 | 11,873.1 | 1,484.1 | 0.8x |
| 12 | 32.9 | 27,778.9 | 19,745.3 | 19,712.3 | 1,642.7 | 0.7x |

Linear fit on overhead per item vs stages (conc=1) for BatchChan (batch -> flatten):
- Overhead per item approx -278.5 ns + 1,624.8 ns * stages

### Total overhead estimates (conc=1, stages)
| Stages | Overhead per item (ns) | Total @1M items (s) | Total @1B items (s) |
| ---: | ---: | ---: | ---: |
| 1 | 1,587.6 | 1.6 | 1,587.6 |
| 2 | 2,886.7 | 2.9 | 2,886.7 |
| 4 | 6,418.3 | 6.4 | 6,418.3 |
| 8 | 11,873.1 | 11.9 | 11,873.1 |
| 12 | 19,712.3 | 19.7 | 19,712.3 |

## Decisions
- Keep serial baselines in the main summary tables alongside worker baselines.
- Treat loop-worker baselines as unbuffered; buffer effects are reserved for option-level benchmarks.
- Target loop + channel + pipeline baselines for every step type; document any exceptions explicitly.
- Use count=1 during development; increase counts when publishing medians.
- Baseline channels/pipeline benches stop timers during setup and start at execution to isolate run overhead.
- Keep overhead sweeps at conc=1 with items=4096 to isolate per-item and per-step costs.
- Composite fan-out sweeps cap stages at 12 and scale inputs by `fanOut^stages` to keep total outputs near 4096.
- Composite batch sweeps reuse the composite stage list but keep input counts fixed (no fan-out).
- Composite stage list: 1, 2, 4, 8, 12 to keep runtime bounded.
- Composite sweeps assume conc=1 ordering.
- Track realistic work using `benchWorkIters=64` to show how overhead ratios shrink as work grows.
- Next option-level benchmarks to add: `StepBufferSize` and `StepMaxInFlight`.

```
Date: 2026-01-02
Machine: VirtualApple @ 2.50GHz
OS/Arch: darwin/amd64
Go version: go1.25.0
Commit: 1df5aec3bc5aad76c5ae7a26e226aa444affda2d
Command: GOCACHE=$(pwd)/.cache/go-build go test ./pkg/pipeline -run '^$' -bench Benchmark -benchmem -benchtime=100ms -count 5 -timeout 700s
Notes: Default GOMAXPROCS. Run-only timing (setup excluded). Overhead sweeps use items=4096 (iters up to 4096, steps up to 64). Composite stages: 1, 2, 4, 8, 12. benchtime=100ms. Summary tables use medians of 5 runs.

goos: darwin
goarch: amd64
pkg: github.com/askiada/go-pipeline/v2/pkg/pipeline
cpu: VirtualApple @ 2.50GHz
BenchmarkOneToOne/items=1k/conc=1/loop-serial-8         	   43982	      2506 ns/op	       0 B/op	       0 allocs/op
BenchmarkOneToOne/items=1k/conc=1/loop-serial-8         	   45752	      2440 ns/op	       0 B/op	       0 allocs/op
BenchmarkOneToOne/items=1k/conc=1/loop-serial-8         	   49054	      2460 ns/op	       0 B/op	       0 allocs/op
BenchmarkOneToOne/items=1k/conc=1/loop-serial-8         	   46778	      2744 ns/op	       0 B/op	       0 allocs/op
BenchmarkOneToOne/items=1k/conc=1/loop-serial-8         	   45080	      2789 ns/op	       0 B/op	       0 allocs/op
BenchmarkOneToOne/items=1k/conc=1/loop-workers-8        	   13500	      8596 ns/op	      16 B/op	       1 allocs/op
BenchmarkOneToOne/items=1k/conc=1/loop-workers-8        	   14130	      9240 ns/op	      16 B/op	       1 allocs/op
BenchmarkOneToOne/items=1k/conc=1/loop-workers-8        	   14215	      8249 ns/op	      16 B/op	       1 allocs/op
BenchmarkOneToOne/items=1k/conc=1/loop-workers-8        	   14322	      8335 ns/op	      16 B/op	       1 allocs/op
BenchmarkOneToOne/items=1k/conc=1/loop-workers-8        	   14437	      8337 ns/op	      16 B/op	       1 allocs/op
BenchmarkOneToOne/items=1k/conc=1/channels-8            	      94	   1123053 ns/op	     167 B/op	       3 allocs/op
BenchmarkOneToOne/items=1k/conc=1/channels-8            	     100	   1095734 ns/op	     192 B/op	       3 allocs/op
BenchmarkOneToOne/items=1k/conc=1/channels-8            	      99	   1063138 ns/op	     192 B/op	       3 allocs/op
BenchmarkOneToOne/items=1k/conc=1/channels-8            	     102	   1152547 ns/op	     179 B/op	       3 allocs/op
BenchmarkOneToOne/items=1k/conc=1/channels-8            	     104	   1133681 ns/op	     180 B/op	       3 allocs/op
BenchmarkOneToOne/items=1k/conc=1/pipeline-8            	      39	   4524426 ns/op	     948 B/op	      16 allocs/op
BenchmarkOneToOne/items=1k/conc=1/pipeline-8            	      94	   1217177 ns/op	     830 B/op	      16 allocs/op
BenchmarkOneToOne/items=1k/conc=1/pipeline-8            	      80	   1452161 ns/op	     796 B/op	      16 allocs/op
BenchmarkOneToOne/items=1k/conc=1/pipeline-8            	     103	   1181492 ns/op	     820 B/op	      16 allocs/op
BenchmarkOneToOne/items=1k/conc=1/pipeline-8            	      90	   1177124 ns/op	     823 B/op	      16 allocs/op
BenchmarkOneToOne/items=1k/conc=4/loop-serial-8         	   47364	      2449 ns/op	       0 B/op	       0 allocs/op
BenchmarkOneToOne/items=1k/conc=4/loop-serial-8         	   42271	      2566 ns/op	       0 B/op	       0 allocs/op
BenchmarkOneToOne/items=1k/conc=4/loop-serial-8         	   48476	      2460 ns/op	       0 B/op	       0 allocs/op
BenchmarkOneToOne/items=1k/conc=4/loop-serial-8         	   45630	      2435 ns/op	       0 B/op	       0 allocs/op
BenchmarkOneToOne/items=1k/conc=4/loop-serial-8         	   49047	      2439 ns/op	       0 B/op	       0 allocs/op
BenchmarkOneToOne/items=1k/conc=4/loop-workers-8        	    6132	     19488 ns/op	     460 B/op	       8 allocs/op
BenchmarkOneToOne/items=1k/conc=4/loop-workers-8        	    9120	     18285 ns/op	     456 B/op	       8 allocs/op
BenchmarkOneToOne/items=1k/conc=4/loop-workers-8        	    6517	     20439 ns/op	     457 B/op	       8 allocs/op
BenchmarkOneToOne/items=1k/conc=4/loop-workers-8        	    8997	     17990 ns/op	     456 B/op	       8 allocs/op
BenchmarkOneToOne/items=1k/conc=4/loop-workers-8        	    5702	     19700 ns/op	     456 B/op	       8 allocs/op
BenchmarkOneToOne/items=1k/conc=4/channels-8            	      63	   1751810 ns/op	     371 B/op	       6 allocs/op
BenchmarkOneToOne/items=1k/conc=4/channels-8            	      85	   1471910 ns/op	     398 B/op	       6 allocs/op
BenchmarkOneToOne/items=1k/conc=4/channels-8            	     100	   1648659 ns/op	     412 B/op	       6 allocs/op
BenchmarkOneToOne/items=1k/conc=4/channels-8            	      76	   1690323 ns/op	     440 B/op	       7 allocs/op
BenchmarkOneToOne/items=1k/conc=4/channels-8            	      64	   2142775 ns/op	     345 B/op	       6 allocs/op
BenchmarkOneToOne/items=1k/conc=4/pipeline-8            	      50	   2032151 ns/op	    2333 B/op	      33 allocs/op
BenchmarkOneToOne/items=1k/conc=4/pipeline-8            	      52	   2250479 ns/op	    2175 B/op	      31 allocs/op
BenchmarkOneToOne/items=1k/conc=4/pipeline-8            	      56	   3282650 ns/op	    2297 B/op	      32 allocs/op
BenchmarkOneToOne/items=1k/conc=4/pipeline-8            	      52	   2214921 ns/op	    2177 B/op	      31 allocs/op
BenchmarkOneToOne/items=1k/conc=4/pipeline-8            	      68	   2319164 ns/op	    2165 B/op	      31 allocs/op
BenchmarkOneToOne/items=16k/conc=4/loop-serial-8        	    3132	     39244 ns/op	       0 B/op	       0 allocs/op
BenchmarkOneToOne/items=16k/conc=4/loop-serial-8        	    3148	     38937 ns/op	       0 B/op	       0 allocs/op
BenchmarkOneToOne/items=16k/conc=4/loop-serial-8        	    3030	     38970 ns/op	       0 B/op	       0 allocs/op
BenchmarkOneToOne/items=16k/conc=4/loop-serial-8        	    3007	     39040 ns/op	       0 B/op	       0 allocs/op
BenchmarkOneToOne/items=16k/conc=4/loop-serial-8        	    3003	     39136 ns/op	       0 B/op	       0 allocs/op
BenchmarkOneToOne/items=16k/conc=4/loop-workers-8       	     265	    414285 ns/op	     456 B/op	       8 allocs/op
BenchmarkOneToOne/items=16k/conc=4/loop-workers-8       	     284	    411455 ns/op	     456 B/op	       8 allocs/op
BenchmarkOneToOne/items=16k/conc=4/loop-workers-8       	     254	    457701 ns/op	     456 B/op	       8 allocs/op
BenchmarkOneToOne/items=16k/conc=4/loop-workers-8       	     247	    509184 ns/op	     456 B/op	       8 allocs/op
BenchmarkOneToOne/items=16k/conc=4/loop-workers-8       	     294	    418109 ns/op	     456 B/op	       8 allocs/op
BenchmarkOneToOne/items=16k/conc=4/channels-8           	       5	  33482467 ns/op	     462 B/op	       7 allocs/op
BenchmarkOneToOne/items=16k/conc=4/channels-8           	       4	  29449802 ns/op	     612 B/op	       8 allocs/op
BenchmarkOneToOne/items=16k/conc=4/channels-8           	       4	  36416188 ns/op	     372 B/op	       6 allocs/op
BenchmarkOneToOne/items=16k/conc=4/channels-8           	       4	  30225073 ns/op	     468 B/op	       7 allocs/op
BenchmarkOneToOne/items=16k/conc=4/channels-8           	       4	  32086916 ns/op	     372 B/op	       6 allocs/op
BenchmarkOneToOne/items=16k/conc=4/pipeline-8           	       3	  39033680 ns/op	    2250 B/op	      32 allocs/op
BenchmarkOneToOne/items=16k/conc=4/pipeline-8           	       3	  41565681 ns/op	    2250 B/op	      32 allocs/op
BenchmarkOneToOne/items=16k/conc=4/pipeline-8           	       3	  36612014 ns/op	    2314 B/op	      32 allocs/op
BenchmarkOneToOne/items=16k/conc=4/pipeline-8           	       3	  36221125 ns/op	    2250 B/op	      32 allocs/op
BenchmarkOneToOne/items=16k/conc=4/pipeline-8           	       4	  32003458 ns/op	    2724 B/op	      37 allocs/op
BenchmarkOneToOneRealistic/items=1k/conc=1/loop-serial-8         	     955	    118239 ns/op	       0 B/op	       0 allocs/op
BenchmarkOneToOneRealistic/items=1k/conc=1/loop-serial-8         	    1000	    118173 ns/op	       0 B/op	       0 allocs/op
BenchmarkOneToOneRealistic/items=1k/conc=1/loop-serial-8         	     956	    118889 ns/op	       0 B/op	       0 allocs/op
BenchmarkOneToOneRealistic/items=1k/conc=1/loop-serial-8         	     994	    118528 ns/op	       0 B/op	       0 allocs/op
BenchmarkOneToOneRealistic/items=1k/conc=1/loop-serial-8         	    1011	    118709 ns/op	       0 B/op	       0 allocs/op
BenchmarkOneToOneRealistic/items=1k/conc=1/loop-workers-8        	     908	    124274 ns/op	      16 B/op	       1 allocs/op
BenchmarkOneToOneRealistic/items=1k/conc=1/loop-workers-8        	     960	    124826 ns/op	      16 B/op	       1 allocs/op
BenchmarkOneToOneRealistic/items=1k/conc=1/loop-workers-8        	     914	    124545 ns/op	      16 B/op	       1 allocs/op
BenchmarkOneToOneRealistic/items=1k/conc=1/loop-workers-8        	     945	    124181 ns/op	      16 B/op	       1 allocs/op
BenchmarkOneToOneRealistic/items=1k/conc=1/loop-workers-8        	     925	    127700 ns/op	      16 B/op	       1 allocs/op
BenchmarkOneToOneRealistic/items=1k/conc=1/channels-8            	      82	   1243364 ns/op	     163 B/op	       3 allocs/op
BenchmarkOneToOneRealistic/items=1k/conc=1/channels-8            	     100	   1365954 ns/op	     169 B/op	       3 allocs/op
BenchmarkOneToOneRealistic/items=1k/conc=1/channels-8            	      84	   1392993 ns/op	     157 B/op	       3 allocs/op
BenchmarkOneToOneRealistic/items=1k/conc=1/channels-8            	      82	   1366095 ns/op	     153 B/op	       3 allocs/op
BenchmarkOneToOneRealistic/items=1k/conc=1/channels-8            	      79	   1330186 ns/op	     153 B/op	       3 allocs/op
BenchmarkOneToOneRealistic/items=1k/conc=1/pipeline-8            	      72	   1500703 ns/op	     821 B/op	      16 allocs/op
BenchmarkOneToOneRealistic/items=1k/conc=1/pipeline-8            	      84	   1373910 ns/op	     756 B/op	      16 allocs/op
BenchmarkOneToOneRealistic/items=1k/conc=1/pipeline-8            	      85	   1359724 ns/op	     782 B/op	      16 allocs/op
BenchmarkOneToOneRealistic/items=1k/conc=1/pipeline-8            	      73	   1400788 ns/op	     757 B/op	      16 allocs/op
BenchmarkOneToOneRealistic/items=1k/conc=1/pipeline-8            	      82	   1370889 ns/op	     769 B/op	      16 allocs/op
BenchmarkOneToOneRealistic/items=1k/conc=4/loop-serial-8         	     990	    119935 ns/op	       0 B/op	       0 allocs/op
BenchmarkOneToOneRealistic/items=1k/conc=4/loop-serial-8         	     962	    118559 ns/op	       0 B/op	       0 allocs/op
BenchmarkOneToOneRealistic/items=1k/conc=4/loop-serial-8         	     997	    119198 ns/op	       0 B/op	       0 allocs/op
BenchmarkOneToOneRealistic/items=1k/conc=4/loop-serial-8         	     986	    119431 ns/op	       0 B/op	       0 allocs/op
BenchmarkOneToOneRealistic/items=1k/conc=4/loop-serial-8         	     981	    123994 ns/op	       0 B/op	       0 allocs/op
BenchmarkOneToOneRealistic/items=1k/conc=4/loop-workers-8        	    1484	     90340 ns/op	     462 B/op	       8 allocs/op
BenchmarkOneToOneRealistic/items=1k/conc=4/loop-workers-8        	    1230	     81464 ns/op	     456 B/op	       8 allocs/op
BenchmarkOneToOneRealistic/items=1k/conc=4/loop-workers-8        	     904	    111126 ns/op	     456 B/op	       8 allocs/op
BenchmarkOneToOneRealistic/items=1k/conc=4/loop-workers-8        	    1504	     79521 ns/op	     456 B/op	       8 allocs/op
BenchmarkOneToOneRealistic/items=1k/conc=4/loop-workers-8        	    1456	     82956 ns/op	     456 B/op	       8 allocs/op
BenchmarkOneToOneRealistic/items=1k/conc=4/channels-8            	      49	   2165673 ns/op	     454 B/op	       7 allocs/op
BenchmarkOneToOneRealistic/items=1k/conc=4/channels-8            	      49	   2130961 ns/op	     381 B/op	       6 allocs/op
BenchmarkOneToOneRealistic/items=1k/conc=4/channels-8            	      48	   4391455 ns/op	     384 B/op	       6 allocs/op
BenchmarkOneToOneRealistic/items=1k/conc=4/channels-8            	      55	   2055838 ns/op	     396 B/op	       6 allocs/op
BenchmarkOneToOneRealistic/items=1k/conc=4/channels-8            	      56	   2510813 ns/op	     402 B/op	       6 allocs/op
BenchmarkOneToOneRealistic/items=1k/conc=4/pipeline-8            	      49	   2527799 ns/op	    2211 B/op	      31 allocs/op
BenchmarkOneToOneRealistic/items=1k/conc=4/pipeline-8            	      46	   2316622 ns/op	    2192 B/op	      31 allocs/op
BenchmarkOneToOneRealistic/items=1k/conc=4/pipeline-8            	      50	   2392897 ns/op	    2298 B/op	      32 allocs/op
BenchmarkOneToOneRealistic/items=1k/conc=4/pipeline-8            	      46	   2345114 ns/op	    2232 B/op	      32 allocs/op
BenchmarkOneToOneRealistic/items=1k/conc=4/pipeline-8            	      42	   2385722 ns/op	    2356 B/op	      33 allocs/op
BenchmarkOneToOneRealistic/items=16k/conc=4/loop-serial-8        	      60	   1885364 ns/op	       0 B/op	       0 allocs/op
BenchmarkOneToOneRealistic/items=16k/conc=4/loop-serial-8        	      62	   1891129 ns/op	       0 B/op	       0 allocs/op
BenchmarkOneToOneRealistic/items=16k/conc=4/loop-serial-8        	      62	   1907940 ns/op	       0 B/op	       0 allocs/op
BenchmarkOneToOneRealistic/items=16k/conc=4/loop-serial-8        	      62	   1891245 ns/op	       0 B/op	       0 allocs/op
BenchmarkOneToOneRealistic/items=16k/conc=4/loop-serial-8        	      52	   1925201 ns/op	       0 B/op	       0 allocs/op
BenchmarkOneToOneRealistic/items=16k/conc=4/loop-workers-8       	     153	    749287 ns/op	     456 B/op	       8 allocs/op
BenchmarkOneToOneRealistic/items=16k/conc=4/loop-workers-8       	     126	    803299 ns/op	     456 B/op	       8 allocs/op
BenchmarkOneToOneRealistic/items=16k/conc=4/loop-workers-8       	     130	    897888 ns/op	     456 B/op	       8 allocs/op
BenchmarkOneToOneRealistic/items=16k/conc=4/loop-workers-8       	     175	    715909 ns/op	     456 B/op	       8 allocs/op
BenchmarkOneToOneRealistic/items=16k/conc=4/loop-workers-8       	     156	    734324 ns/op	     456 B/op	       8 allocs/op
BenchmarkOneToOneRealistic/items=16k/conc=4/channels-8           	       3	  39050430 ns/op	     381 B/op	       6 allocs/op
BenchmarkOneToOneRealistic/items=16k/conc=4/channels-8           	       4	  29129292 ns/op	     372 B/op	       6 allocs/op
BenchmarkOneToOneRealistic/items=16k/conc=4/channels-8           	       1	 124585001 ns/op	     456 B/op	       7 allocs/op
BenchmarkOneToOneRealistic/items=16k/conc=4/channels-8           	       3	  34726611 ns/op	     989 B/op	      12 allocs/op
BenchmarkOneToOneRealistic/items=16k/conc=4/channels-8           	       3	  37020153 ns/op	     605 B/op	       8 allocs/op
BenchmarkOneToOneRealistic/items=16k/conc=4/pipeline-8           	       3	  39283667 ns/op	    2346 B/op	      33 allocs/op
BenchmarkOneToOneRealistic/items=16k/conc=4/pipeline-8           	       3	  40069237 ns/op	    2826 B/op	      38 allocs/op
BenchmarkOneToOneRealistic/items=16k/conc=4/pipeline-8           	       3	  38875305 ns/op	    2506 B/op	      34 allocs/op
BenchmarkOneToOneRealistic/items=16k/conc=4/pipeline-8           	       3	  40918458 ns/op	    2442 B/op	      34 allocs/op
BenchmarkOneToOneRealistic/items=16k/conc=4/pipeline-8           	       3	  41260569 ns/op	    2346 B/op	      33 allocs/op
BenchmarkOneToOneOrZero/items=1k/conc=1/loop-serial-8            	   44768	      2410 ns/op	       0 B/op	       0 allocs/op
BenchmarkOneToOneOrZero/items=1k/conc=1/loop-serial-8            	   49754	      2428 ns/op	       0 B/op	       0 allocs/op
BenchmarkOneToOneOrZero/items=1k/conc=1/loop-serial-8            	   46386	      2443 ns/op	       0 B/op	       0 allocs/op
BenchmarkOneToOneOrZero/items=1k/conc=1/loop-serial-8            	   47046	      2435 ns/op	       0 B/op	       0 allocs/op
BenchmarkOneToOneOrZero/items=1k/conc=1/loop-serial-8            	   45531	      2460 ns/op	       0 B/op	       0 allocs/op
BenchmarkOneToOneOrZero/items=1k/conc=1/loop-workers-8           	   13443	      8840 ns/op	      16 B/op	       1 allocs/op
BenchmarkOneToOneOrZero/items=1k/conc=1/loop-workers-8           	   13454	      8941 ns/op	      16 B/op	       1 allocs/op
BenchmarkOneToOneOrZero/items=1k/conc=1/loop-workers-8           	   12940	      8879 ns/op	      16 B/op	       1 allocs/op
BenchmarkOneToOneOrZero/items=1k/conc=1/loop-workers-8           	   13273	      9392 ns/op	      16 B/op	       1 allocs/op
BenchmarkOneToOneOrZero/items=1k/conc=1/loop-workers-8           	   13400	      8840 ns/op	      16 B/op	       1 allocs/op
BenchmarkOneToOneOrZero/items=1k/conc=1/channels-8               	     112	    999674 ns/op	     159 B/op	       3 allocs/op
BenchmarkOneToOneOrZero/items=1k/conc=1/channels-8               	     100	   1016823 ns/op	     153 B/op	       3 allocs/op
BenchmarkOneToOneOrZero/items=1k/conc=1/channels-8               	     100	   1026799 ns/op	     153 B/op	       3 allocs/op
BenchmarkOneToOneOrZero/items=1k/conc=1/channels-8               	     117	    973733 ns/op	     157 B/op	       3 allocs/op
BenchmarkOneToOneOrZero/items=1k/conc=1/channels-8               	     100	   1028521 ns/op	     156 B/op	       3 allocs/op
BenchmarkOneToOneOrZero/items=1k/conc=1/pipeline-8               	     106	   1077312 ns/op	     775 B/op	      16 allocs/op
BenchmarkOneToOneOrZero/items=1k/conc=1/pipeline-8               	     103	   1086101 ns/op	     795 B/op	      16 allocs/op
BenchmarkOneToOneOrZero/items=1k/conc=1/pipeline-8               	     106	   1115836 ns/op	     756 B/op	      16 allocs/op
BenchmarkOneToOneOrZero/items=1k/conc=1/pipeline-8               	     102	   1048974 ns/op	     807 B/op	      16 allocs/op
BenchmarkOneToOneOrZero/items=1k/conc=1/pipeline-8               	     103	   1061978 ns/op	     792 B/op	      16 allocs/op
BenchmarkOneToOneOrZero/items=1k/conc=4/loop-serial-8            	   39998	      2617 ns/op	       0 B/op	       0 allocs/op
BenchmarkOneToOneOrZero/items=1k/conc=4/loop-serial-8            	   49525	      2425 ns/op	       0 B/op	       0 allocs/op
BenchmarkOneToOneOrZero/items=1k/conc=4/loop-serial-8            	   45693	      2432 ns/op	       0 B/op	       0 allocs/op
BenchmarkOneToOneOrZero/items=1k/conc=4/loop-serial-8            	   49179	      2443 ns/op	       0 B/op	       0 allocs/op
BenchmarkOneToOneOrZero/items=1k/conc=4/loop-serial-8            	   46980	      7721 ns/op	       0 B/op	       0 allocs/op
BenchmarkOneToOneOrZero/items=1k/conc=4/loop-workers-8           	    4977	     21850 ns/op	     456 B/op	       8 allocs/op
BenchmarkOneToOneOrZero/items=1k/conc=4/loop-workers-8           	    5169	     20628 ns/op	     456 B/op	       8 allocs/op
BenchmarkOneToOneOrZero/items=1k/conc=4/loop-workers-8           	   10000	     20716 ns/op	     456 B/op	       8 allocs/op
BenchmarkOneToOneOrZero/items=1k/conc=4/loop-workers-8           	    8140	     21082 ns/op	     456 B/op	       8 allocs/op
BenchmarkOneToOneOrZero/items=1k/conc=4/loop-workers-8           	    8463	     21061 ns/op	     456 B/op	       8 allocs/op
BenchmarkOneToOneOrZero/items=1k/conc=4/channels-8               	      72	   1506239 ns/op	     345 B/op	       6 allocs/op
BenchmarkOneToOneOrZero/items=1k/conc=4/channels-8               	      70	   1605872 ns/op	     349 B/op	       6 allocs/op
BenchmarkOneToOneOrZero/items=1k/conc=4/channels-8               	      61	   2045230 ns/op	     388 B/op	       6 allocs/op
BenchmarkOneToOneOrZero/items=1k/conc=4/channels-8               	      74	   1830751 ns/op	     376 B/op	       6 allocs/op
BenchmarkOneToOneOrZero/items=1k/conc=4/channels-8               	      67	   1531804 ns/op	     362 B/op	       6 allocs/op
BenchmarkOneToOneOrZero/items=1k/conc=4/pipeline-8               	      58	   1815531 ns/op	    2233 B/op	      32 allocs/op
BenchmarkOneToOneOrZero/items=1k/conc=4/pipeline-8               	      57	   1876190 ns/op	    2201 B/op	      31 allocs/op
BenchmarkOneToOneOrZero/items=1k/conc=4/pipeline-8               	      55	   2012596 ns/op	    2295 B/op	      32 allocs/op
BenchmarkOneToOneOrZero/items=1k/conc=4/pipeline-8               	      85	   1967226 ns/op	    2196 B/op	      31 allocs/op
BenchmarkOneToOneOrZero/items=1k/conc=4/pipeline-8               	      61	   1973098 ns/op	    2215 B/op	      31 allocs/op
BenchmarkOneToOneOrZero/items=16k/conc=4/loop-serial-8           	    2715	     40709 ns/op	       0 B/op	       0 allocs/op
BenchmarkOneToOneOrZero/items=16k/conc=4/loop-serial-8           	    3019	     38579 ns/op	       0 B/op	       0 allocs/op
BenchmarkOneToOneOrZero/items=16k/conc=4/loop-serial-8           	    2994	     38898 ns/op	       0 B/op	       0 allocs/op
BenchmarkOneToOneOrZero/items=16k/conc=4/loop-serial-8           	    2984	     39587 ns/op	       0 B/op	       0 allocs/op
BenchmarkOneToOneOrZero/items=16k/conc=4/loop-serial-8           	    2978	     38789 ns/op	       0 B/op	       0 allocs/op
BenchmarkOneToOneOrZero/items=16k/conc=4/loop-workers-8          	     238	    533678 ns/op	     456 B/op	       8 allocs/op
BenchmarkOneToOneOrZero/items=16k/conc=4/loop-workers-8          	     225	    571859 ns/op	     456 B/op	       8 allocs/op
BenchmarkOneToOneOrZero/items=16k/conc=4/loop-workers-8          	     208	    523153 ns/op	     456 B/op	       8 allocs/op
BenchmarkOneToOneOrZero/items=16k/conc=4/loop-workers-8          	     262	    468772 ns/op	     456 B/op	       8 allocs/op
BenchmarkOneToOneOrZero/items=16k/conc=4/loop-workers-8          	     236	    460982 ns/op	     456 B/op	       8 allocs/op
BenchmarkOneToOneOrZero/items=16k/conc=4/channels-8              	       6	  32162500 ns/op	     506 B/op	       7 allocs/op
BenchmarkOneToOneOrZero/items=16k/conc=4/channels-8              	       6	  25369326 ns/op	     394 B/op	       6 allocs/op
BenchmarkOneToOneOrZero/items=16k/conc=4/channels-8              	       4	  31208687 ns/op	     372 B/op	       6 allocs/op
BenchmarkOneToOneOrZero/items=16k/conc=4/channels-8              	       5	  30159417 ns/op	     366 B/op	       6 allocs/op
BenchmarkOneToOneOrZero/items=16k/conc=4/channels-8              	       5	  30509000 ns/op	     366 B/op	       6 allocs/op
BenchmarkOneToOneOrZero/items=16k/conc=4/pipeline-8              	       4	  31501302 ns/op	    2676 B/op	      36 allocs/op
BenchmarkOneToOneOrZero/items=16k/conc=4/pipeline-8              	       4	  28046281 ns/op	    2772 B/op	      37 allocs/op
BenchmarkOneToOneOrZero/items=16k/conc=4/pipeline-8              	       4	  29328208 ns/op	    2484 B/op	      34 allocs/op
BenchmarkOneToOneOrZero/items=16k/conc=4/pipeline-8              	       4	  30488562 ns/op	    2364 B/op	      33 allocs/op
BenchmarkOneToOneOrZero/items=16k/conc=4/pipeline-8              	       4	  31958052 ns/op	    2700 B/op	      36 allocs/op
BenchmarkTwoStage/items=1k/conc=1/loop-serial-8                  	   21568	      5363 ns/op	       0 B/op	       0 allocs/op
BenchmarkTwoStage/items=1k/conc=1/loop-serial-8                  	   22226	      5353 ns/op	       0 B/op	       0 allocs/op
BenchmarkTwoStage/items=1k/conc=1/loop-serial-8                  	   22269	      5347 ns/op	       0 B/op	       0 allocs/op
BenchmarkTwoStage/items=1k/conc=1/loop-serial-8                  	   21073	      5379 ns/op	       0 B/op	       0 allocs/op
BenchmarkTwoStage/items=1k/conc=1/loop-serial-8                  	   21740	      5353 ns/op	       0 B/op	       0 allocs/op
BenchmarkTwoStage/items=1k/conc=1/loop-workers-8                 	   15451	      7615 ns/op	      24 B/op	       1 allocs/op
BenchmarkTwoStage/items=1k/conc=1/loop-workers-8                 	   15554	      7549 ns/op	      24 B/op	       1 allocs/op
BenchmarkTwoStage/items=1k/conc=1/loop-workers-8                 	   15735	      7517 ns/op	      24 B/op	       1 allocs/op
BenchmarkTwoStage/items=1k/conc=1/loop-workers-8                 	   15597	      7841 ns/op	      24 B/op	       1 allocs/op
BenchmarkTwoStage/items=1k/conc=1/loop-workers-8                 	   15264	      7619 ns/op	      24 B/op	       1 allocs/op
BenchmarkTwoStage/items=1k/conc=1/channels-8                     	      36	   5207402 ns/op	     272 B/op	       5 allocs/op
BenchmarkTwoStage/items=1k/conc=1/channels-8                     	      60	   4483027 ns/op	     248 B/op	       5 allocs/op
BenchmarkTwoStage/items=1k/conc=1/channels-8                     	      74	   2207015 ns/op	     244 B/op	       5 allocs/op
BenchmarkTwoStage/items=1k/conc=1/channels-8                     	      62	   1849419 ns/op	     252 B/op	       5 allocs/op
BenchmarkTwoStage/items=1k/conc=1/channels-8                     	      66	   1883627 ns/op	     241 B/op	       5 allocs/op
BenchmarkTwoStage/items=1k/conc=1/pipeline-8                     	      57	   1973352 ns/op	     895 B/op	      18 allocs/op
BenchmarkTwoStage/items=1k/conc=1/pipeline-8                     	      54	   2078647 ns/op	     922 B/op	      18 allocs/op
BenchmarkTwoStage/items=1k/conc=1/pipeline-8                     	      57	   2150330 ns/op	     870 B/op	      18 allocs/op
BenchmarkTwoStage/items=1k/conc=1/pipeline-8                     	      49	   2059946 ns/op	     906 B/op	      18 allocs/op
BenchmarkTwoStage/items=1k/conc=1/pipeline-8                     	      57	   2041841 ns/op	     890 B/op	      18 allocs/op
BenchmarkTwoStage/items=1k/conc=4/loop-serial-8                  	   21776	      5367 ns/op	       0 B/op	       0 allocs/op
BenchmarkTwoStage/items=1k/conc=4/loop-serial-8                  	   22035	      5340 ns/op	       0 B/op	       0 allocs/op
BenchmarkTwoStage/items=1k/conc=4/loop-serial-8                  	   22170	      5323 ns/op	       0 B/op	       0 allocs/op
BenchmarkTwoStage/items=1k/conc=4/loop-serial-8                  	   22082	      5346 ns/op	       0 B/op	       0 allocs/op
BenchmarkTwoStage/items=1k/conc=4/loop-serial-8                  	   22218	      5341 ns/op	       0 B/op	       0 allocs/op
BenchmarkTwoStage/items=1k/conc=4/loop-workers-8                 	    4318	     32699 ns/op	     464 B/op	       8 allocs/op
BenchmarkTwoStage/items=1k/conc=4/loop-workers-8                 	    4879	     21190 ns/op	     464 B/op	       8 allocs/op
BenchmarkTwoStage/items=1k/conc=4/loop-workers-8                 	    5900	     19577 ns/op	     464 B/op	       8 allocs/op
BenchmarkTwoStage/items=1k/conc=4/loop-workers-8                 	    6538	     19176 ns/op	     464 B/op	       8 allocs/op
BenchmarkTwoStage/items=1k/conc=4/loop-workers-8                 	    6806	     18053 ns/op	     464 B/op	       8 allocs/op
BenchmarkTwoStage/items=1k/conc=4/channels-8                     	      30	   4489432 ns/op	     838 B/op	      13 allocs/op
BenchmarkTwoStage/items=1k/conc=4/channels-8                     	      37	   3950660 ns/op	     800 B/op	      11 allocs/op
BenchmarkTwoStage/items=1k/conc=4/channels-8                     	      38	   3277020 ns/op	     629 B/op	      11 allocs/op
BenchmarkTwoStage/items=1k/conc=4/channels-8                     	      26	   4975322 ns/op	     702 B/op	      11 allocs/op
BenchmarkTwoStage/items=1k/conc=4/channels-8                     	      32	   3461856 ns/op	     693 B/op	      11 allocs/op
BenchmarkTwoStage/items=1k/conc=4/pipeline-8                     	      30	   4184100 ns/op	    3615 B/op	      48 allocs/op
BenchmarkTwoStage/items=1k/conc=4/pipeline-8                     	      28	   3927549 ns/op	    3568 B/op	      48 allocs/op
BenchmarkTwoStage/items=1k/conc=4/pipeline-8                     	      31	   4111191 ns/op	    3486 B/op	      47 allocs/op
BenchmarkTwoStage/items=1k/conc=4/pipeline-8                     	      34	   4258708 ns/op	    3379 B/op	      46 allocs/op
BenchmarkTwoStage/items=1k/conc=4/pipeline-8                     	      27	   4637245 ns/op	    3611 B/op	      48 allocs/op
BenchmarkTwoStage/items=16k/conc=4/loop-serial-8                 	    1284	     88062 ns/op	       0 B/op	       0 allocs/op
BenchmarkTwoStage/items=16k/conc=4/loop-serial-8                 	    1329	     86359 ns/op	       0 B/op	       0 allocs/op
BenchmarkTwoStage/items=16k/conc=4/loop-serial-8                 	    1286	     88140 ns/op	       0 B/op	       0 allocs/op
BenchmarkTwoStage/items=16k/conc=4/loop-serial-8                 	    1354	     86429 ns/op	       0 B/op	       0 allocs/op
BenchmarkTwoStage/items=16k/conc=4/loop-serial-8                 	    1284	     85917 ns/op	       0 B/op	       0 allocs/op
BenchmarkTwoStage/items=16k/conc=4/loop-workers-8                	     258	    431466 ns/op	     464 B/op	       8 allocs/op
BenchmarkTwoStage/items=16k/conc=4/loop-workers-8                	     262	    439735 ns/op	     464 B/op	       8 allocs/op
BenchmarkTwoStage/items=16k/conc=4/loop-workers-8                	     254	    439527 ns/op	     464 B/op	       8 allocs/op
BenchmarkTwoStage/items=16k/conc=4/loop-workers-8                	     237	    518611 ns/op	     464 B/op	       8 allocs/op
BenchmarkTwoStage/items=16k/conc=4/loop-workers-8                	     232	    491370 ns/op	     464 B/op	       8 allocs/op
BenchmarkTwoStage/items=16k/conc=4/channels-8                    	       3	  54662389 ns/op	     821 B/op	      13 allocs/op
BenchmarkTwoStage/items=16k/conc=4/channels-8                    	       2	  84257604 ns/op	     776 B/op	      12 allocs/op
BenchmarkTwoStage/items=16k/conc=4/channels-8                    	       2	  81166812 ns/op	     872 B/op	      13 allocs/op
BenchmarkTwoStage/items=16k/conc=4/channels-8                    	       3	  66700139 ns/op	     661 B/op	      11 allocs/op
BenchmarkTwoStage/items=16k/conc=4/channels-8                    	       2	  87183146 ns/op	    1112 B/op	      16 allocs/op
BenchmarkTwoStage/items=16k/conc=4/pipeline-8                    	       2	  51415042 ns/op	    4024 B/op	      52 allocs/op
BenchmarkTwoStage/items=16k/conc=4/pipeline-8                    	       2	  53404333 ns/op	    3832 B/op	      50 allocs/op
BenchmarkTwoStage/items=16k/conc=4/pipeline-8                    	       2	  65612958 ns/op	    4936 B/op	      62 allocs/op
BenchmarkTwoStage/items=16k/conc=4/pipeline-8                    	       2	  59014021 ns/op	    4264 B/op	      55 allocs/op
BenchmarkTwoStage/items=16k/conc=4/pipeline-8                    	       2	  58146312 ns/op	    3928 B/op	      51 allocs/op
BenchmarkTwoStageRealistic/items=1k/conc=1/loop-serial-8         	     403	    277588 ns/op	       0 B/op	       0 allocs/op
BenchmarkTwoStageRealistic/items=1k/conc=1/loop-serial-8         	     418	    279971 ns/op	       0 B/op	       0 allocs/op
BenchmarkTwoStageRealistic/items=1k/conc=1/loop-serial-8         	     387	    286733 ns/op	       0 B/op	       0 allocs/op
BenchmarkTwoStageRealistic/items=1k/conc=1/loop-serial-8         	     410	    278441 ns/op	       0 B/op	       0 allocs/op
BenchmarkTwoStageRealistic/items=1k/conc=1/loop-serial-8         	     415	    277736 ns/op	       0 B/op	       0 allocs/op
BenchmarkTwoStageRealistic/items=1k/conc=1/loop-workers-8        	     394	    829548 ns/op	      24 B/op	       1 allocs/op
BenchmarkTwoStageRealistic/items=1k/conc=1/loop-workers-8        	     397	    282639 ns/op	      24 B/op	       1 allocs/op
BenchmarkTwoStageRealistic/items=1k/conc=1/loop-workers-8        	     410	    280967 ns/op	      24 B/op	       1 allocs/op
BenchmarkTwoStageRealistic/items=1k/conc=1/loop-workers-8        	     414	    282760 ns/op	      24 B/op	       1 allocs/op
BenchmarkTwoStageRealistic/items=1k/conc=1/loop-workers-8        	     421	    282120 ns/op	      24 B/op	       1 allocs/op
BenchmarkTwoStageRealistic/items=1k/conc=1/channels-8            	      49	   2293983 ns/op	     271 B/op	       5 allocs/op
BenchmarkTwoStageRealistic/items=1k/conc=1/channels-8            	      67	   2239984 ns/op	     241 B/op	       5 allocs/op
BenchmarkTwoStageRealistic/items=1k/conc=1/channels-8            	      51	   2152301 ns/op	     242 B/op	       5 allocs/op
BenchmarkTwoStageRealistic/items=1k/conc=1/channels-8            	      51	   2259029 ns/op	     255 B/op	       5 allocs/op
BenchmarkTwoStageRealistic/items=1k/conc=1/channels-8            	      57	   2205141 ns/op	     314 B/op	       5 allocs/op
BenchmarkTwoStageRealistic/items=1k/conc=1/pipeline-8            	      46	   2413247 ns/op	     872 B/op	      18 allocs/op
BenchmarkTwoStageRealistic/items=1k/conc=1/pipeline-8            	      44	   2354873 ns/op	     905 B/op	      18 allocs/op
BenchmarkTwoStageRealistic/items=1k/conc=1/pipeline-8            	      48	   2274507 ns/op	     919 B/op	      18 allocs/op
BenchmarkTwoStageRealistic/items=1k/conc=1/pipeline-8            	      48	   2394142 ns/op	     871 B/op	      18 allocs/op
BenchmarkTwoStageRealistic/items=1k/conc=1/pipeline-8            	      85	   2341429 ns/op	     895 B/op	      18 allocs/op
BenchmarkTwoStageRealistic/items=1k/conc=4/loop-serial-8         	     408	    284952 ns/op	       0 B/op	       0 allocs/op
BenchmarkTwoStageRealistic/items=1k/conc=4/loop-serial-8         	     410	    286230 ns/op	       0 B/op	       0 allocs/op
BenchmarkTwoStageRealistic/items=1k/conc=4/loop-serial-8         	     429	    279631 ns/op	       0 B/op	       0 allocs/op
BenchmarkTwoStageRealistic/items=1k/conc=4/loop-serial-8         	     418	    279749 ns/op	       0 B/op	       0 allocs/op
BenchmarkTwoStageRealistic/items=1k/conc=4/loop-serial-8         	     427	    282709 ns/op	       0 B/op	       0 allocs/op
BenchmarkTwoStageRealistic/items=1k/conc=4/loop-workers-8        	     904	    128614 ns/op	     464 B/op	       8 allocs/op
BenchmarkTwoStageRealistic/items=1k/conc=4/loop-workers-8        	     708	    149656 ns/op	     464 B/op	       8 allocs/op
BenchmarkTwoStageRealistic/items=1k/conc=4/loop-workers-8        	     930	    152329 ns/op	     464 B/op	       8 allocs/op
BenchmarkTwoStageRealistic/items=1k/conc=4/loop-workers-8        	     638	    172802 ns/op	     464 B/op	       8 allocs/op
BenchmarkTwoStageRealistic/items=1k/conc=4/loop-workers-8        	     837	    168845 ns/op	     464 B/op	       8 allocs/op
BenchmarkTwoStageRealistic/items=1k/conc=4/channels-8            	      28	   4090062 ns/op	     720 B/op	      12 allocs/op
BenchmarkTwoStageRealistic/items=1k/conc=4/channels-8            	      38	   4782734 ns/op	     839 B/op	      13 allocs/op
BenchmarkTwoStageRealistic/items=1k/conc=4/channels-8            	      45	   3979434 ns/op	     748 B/op	      12 allocs/op
BenchmarkTwoStageRealistic/items=1k/conc=4/channels-8            	      28	   5176829 ns/op	     628 B/op	      11 allocs/op
BenchmarkTwoStageRealistic/items=1k/conc=4/channels-8            	      45	   4000664 ns/op	     649 B/op	      11 allocs/op
BenchmarkTwoStageRealistic/items=1k/conc=4/pipeline-8            	      27	   4906130 ns/op	    3540 B/op	      47 allocs/op
BenchmarkTwoStageRealistic/items=1k/conc=4/pipeline-8            	      25	   4801428 ns/op	    3478 B/op	      47 allocs/op
BenchmarkTwoStageRealistic/items=1k/conc=4/pipeline-8            	      22	   4945597 ns/op	    3381 B/op	      46 allocs/op
BenchmarkTwoStageRealistic/items=1k/conc=4/pipeline-8            	      38	   4969174 ns/op	    3569 B/op	      48 allocs/op
BenchmarkTwoStageRealistic/items=1k/conc=4/pipeline-8            	      27	   4753586 ns/op	    3501 B/op	      47 allocs/op
BenchmarkTwoStageRealistic/items=16k/conc=4/loop-serial-8        	      25	   4456942 ns/op	       0 B/op	       0 allocs/op
BenchmarkTwoStageRealistic/items=16k/conc=4/loop-serial-8        	      26	   4438768 ns/op	       0 B/op	       0 allocs/op
BenchmarkTwoStageRealistic/items=16k/conc=4/loop-serial-8        	      26	   4808755 ns/op	       0 B/op	       0 allocs/op
BenchmarkTwoStageRealistic/items=16k/conc=4/loop-serial-8        	      24	   4449328 ns/op	       0 B/op	       0 allocs/op
BenchmarkTwoStageRealistic/items=16k/conc=4/loop-serial-8        	      26	   4434207 ns/op	       0 B/op	       0 allocs/op
BenchmarkTwoStageRealistic/items=16k/conc=4/loop-workers-8       	      84	   1269464 ns/op	     464 B/op	       8 allocs/op
BenchmarkTwoStageRealistic/items=16k/conc=4/loop-workers-8       	      93	   1259908 ns/op	     464 B/op	       8 allocs/op
BenchmarkTwoStageRealistic/items=16k/conc=4/loop-workers-8       	      87	   1357456 ns/op	     464 B/op	       8 allocs/op
BenchmarkTwoStageRealistic/items=16k/conc=4/loop-workers-8       	      74	   1445104 ns/op	     464 B/op	       8 allocs/op
BenchmarkTwoStageRealistic/items=16k/conc=4/loop-workers-8       	      93	   1292339 ns/op	     464 B/op	       8 allocs/op
BenchmarkTwoStageRealistic/items=16k/conc=4/channels-8           	       2	  58641833 ns/op	     968 B/op	      14 allocs/op
BenchmarkTwoStageRealistic/items=16k/conc=4/channels-8           	       1	 107041291 ns/op	    1696 B/op	      22 allocs/op
BenchmarkTwoStageRealistic/items=16k/conc=4/channels-8           	       2	  78381645 ns/op	    1400 B/op	      19 allocs/op
BenchmarkTwoStageRealistic/items=16k/conc=4/channels-8           	       2	  69865646 ns/op	    1256 B/op	      17 allocs/op
BenchmarkTwoStageRealistic/items=16k/conc=4/channels-8           	       1	 101509292 ns/op	     736 B/op	      12 allocs/op
BenchmarkTwoStageRealistic/items=16k/conc=4/pipeline-8           	       2	  55351625 ns/op	    4552 B/op	      58 allocs/op
BenchmarkTwoStageRealistic/items=16k/conc=4/pipeline-8           	       2	  69699020 ns/op	    4264 B/op	      55 allocs/op
BenchmarkTwoStageRealistic/items=16k/conc=4/pipeline-8           	       2	 160894104 ns/op	    4168 B/op	      54 allocs/op
BenchmarkTwoStageRealistic/items=16k/conc=4/pipeline-8           	       2	  65969520 ns/op	    4216 B/op	      54 allocs/op
BenchmarkTwoStageRealistic/items=16k/conc=4/pipeline-8           	       2	  74099021 ns/op	    4984 B/op	      62 allocs/op
BenchmarkSplitMerge/items=1k/conc=1/loop-serial-8                	   19189	      5602 ns/op	       0 B/op	       0 allocs/op
BenchmarkSplitMerge/items=1k/conc=1/loop-serial-8                	   21759	      5359 ns/op	       0 B/op	       0 allocs/op
BenchmarkSplitMerge/items=1k/conc=1/loop-serial-8                	   21800	      5579 ns/op	       0 B/op	       0 allocs/op
BenchmarkSplitMerge/items=1k/conc=1/loop-serial-8                	   21889	      5489 ns/op	       0 B/op	       0 allocs/op
BenchmarkSplitMerge/items=1k/conc=1/loop-serial-8                	   21922	      5397 ns/op	       0 B/op	       0 allocs/op
BenchmarkSplitMerge/items=1k/conc=1/loop-workers-8               	   12038	      9702 ns/op	      24 B/op	       1 allocs/op
BenchmarkSplitMerge/items=1k/conc=1/loop-workers-8               	   12244	     10827 ns/op	      24 B/op	       1 allocs/op
BenchmarkSplitMerge/items=1k/conc=1/loop-workers-8               	   12165	      9749 ns/op	      24 B/op	       1 allocs/op
BenchmarkSplitMerge/items=1k/conc=1/loop-workers-8               	   12261	      9714 ns/op	      24 B/op	       1 allocs/op
BenchmarkSplitMerge/items=1k/conc=1/loop-workers-8               	   12148	      9675 ns/op	      24 B/op	       1 allocs/op
BenchmarkSplitMerge/items=1k/conc=1/channels-8                   	      22	   4826047 ns/op	     605 B/op	      11 allocs/op
BenchmarkSplitMerge/items=1k/conc=1/channels-8                   	      24	   5043505 ns/op	     472 B/op	      10 allocs/op
BenchmarkSplitMerge/items=1k/conc=1/channels-8                   	      22	   5092017 ns/op	     453 B/op	      10 allocs/op
BenchmarkSplitMerge/items=1k/conc=1/channels-8                   	      22	   4806905 ns/op	     453 B/op	      10 allocs/op
BenchmarkSplitMerge/items=1k/conc=1/channels-8                   	      22	   5111053 ns/op	     536 B/op	      10 allocs/op
BenchmarkSplitMerge/items=1k/conc=1/pipeline-8                   	       8	  19481823 ns/op	    2354 B/op	      38 allocs/op
BenchmarkSplitMerge/items=1k/conc=1/pipeline-8                   	      14	   8468801 ns/op	    1619 B/op	      30 allocs/op
BenchmarkSplitMerge/items=1k/conc=1/pipeline-8                   	      12	   9394170 ns/op	    1606 B/op	      30 allocs/op
BenchmarkSplitMerge/items=1k/conc=1/pipeline-8                   	      12	   8693097 ns/op	    1758 B/op	      32 allocs/op
BenchmarkSplitMerge/items=1k/conc=1/pipeline-8                   	      18	   8828447 ns/op	    1577 B/op	      30 allocs/op
BenchmarkSplitMerge/items=1k/conc=4/loop-serial-8                	   21306	      5381 ns/op	       0 B/op	       0 allocs/op
BenchmarkSplitMerge/items=1k/conc=4/loop-serial-8                	   21898	      5365 ns/op	       0 B/op	       0 allocs/op
BenchmarkSplitMerge/items=1k/conc=4/loop-serial-8                	   22047	      5354 ns/op	       0 B/op	       0 allocs/op
BenchmarkSplitMerge/items=1k/conc=4/loop-serial-8                	   21116	      5363 ns/op	       0 B/op	       0 allocs/op
BenchmarkSplitMerge/items=1k/conc=4/loop-serial-8                	   20594	      6127 ns/op	       0 B/op	       0 allocs/op
BenchmarkSplitMerge/items=1k/conc=4/loop-workers-8               	    4928	     23152 ns/op	     464 B/op	       8 allocs/op
BenchmarkSplitMerge/items=1k/conc=4/loop-workers-8               	    6068	     19917 ns/op	     464 B/op	       8 allocs/op
BenchmarkSplitMerge/items=1k/conc=4/loop-workers-8               	    5146	     24122 ns/op	     464 B/op	       8 allocs/op
BenchmarkSplitMerge/items=1k/conc=4/loop-workers-8               	    4194	     24346 ns/op	     464 B/op	       8 allocs/op
BenchmarkSplitMerge/items=1k/conc=4/loop-workers-8               	    4674	     23149 ns/op	     464 B/op	       8 allocs/op
BenchmarkSplitMerge/items=1k/conc=4/channels-8                   	      18	   8621861 ns/op	     859 B/op	      16 allocs/op
BenchmarkSplitMerge/items=1k/conc=4/channels-8                   	      16	   9087182 ns/op	     971 B/op	      17 allocs/op
BenchmarkSplitMerge/items=1k/conc=4/channels-8                   	      14	   9066917 ns/op	    1196 B/op	      19 allocs/op
BenchmarkSplitMerge/items=1k/conc=4/channels-8                   	      14	   9700065 ns/op	    1141 B/op	      19 allocs/op
BenchmarkSplitMerge/items=1k/conc=4/channels-8                   	      14	   9553399 ns/op	    1402 B/op	      21 allocs/op
BenchmarkSplitMerge/items=1k/conc=4/pipeline-8                   	      12	   9512222 ns/op	    4198 B/op	      59 allocs/op
BenchmarkSplitMerge/items=1k/conc=4/pipeline-8                   	      10	  10174858 ns/op	    4507 B/op	      62 allocs/op
BenchmarkSplitMerge/items=1k/conc=4/pipeline-8                   	      12	  10590799 ns/op	    4318 B/op	      60 allocs/op
BenchmarkSplitMerge/items=1k/conc=4/pipeline-8                   	      12	   8934160 ns/op	    5270 B/op	      70 allocs/op
BenchmarkSplitMerge/items=1k/conc=4/pipeline-8                   	      13	   8994118 ns/op	    4172 B/op	      59 allocs/op
BenchmarkSplitMerge/items=16k/conc=4/loop-serial-8               	    1252	     86654 ns/op	       0 B/op	       0 allocs/op
BenchmarkSplitMerge/items=16k/conc=4/loop-serial-8               	    1267	     86494 ns/op	       0 B/op	       0 allocs/op
BenchmarkSplitMerge/items=16k/conc=4/loop-serial-8               	    1332	     86229 ns/op	       0 B/op	       0 allocs/op
BenchmarkSplitMerge/items=16k/conc=4/loop-serial-8               	    1305	     86695 ns/op	       0 B/op	       0 allocs/op
BenchmarkSplitMerge/items=16k/conc=4/loop-serial-8               	    1274	     86732 ns/op	       0 B/op	       0 allocs/op
BenchmarkSplitMerge/items=16k/conc=4/loop-workers-8              	     236	    441681 ns/op	     464 B/op	       8 allocs/op
BenchmarkSplitMerge/items=16k/conc=4/loop-workers-8              	     242	    469341 ns/op	     464 B/op	       8 allocs/op
BenchmarkSplitMerge/items=16k/conc=4/loop-workers-8              	     300	    501395 ns/op	     464 B/op	       8 allocs/op
BenchmarkSplitMerge/items=16k/conc=4/loop-workers-8              	     217	    598174 ns/op	     464 B/op	       8 allocs/op
BenchmarkSplitMerge/items=16k/conc=4/loop-workers-8              	     292	    432110 ns/op	     464 B/op	       8 allocs/op
BenchmarkSplitMerge/items=16k/conc=4/channels-8                  	       1	 100090750 ns/op	    1808 B/op	      26 allocs/op
BenchmarkSplitMerge/items=16k/conc=4/channels-8                  	       1	 155310250 ns/op	    3152 B/op	      40 allocs/op
BenchmarkSplitMerge/items=16k/conc=4/channels-8                  	       1	 171115750 ns/op	    1136 B/op	      19 allocs/op
BenchmarkSplitMerge/items=16k/conc=4/channels-8                  	       1	 139726958 ns/op	    1136 B/op	      19 allocs/op
BenchmarkSplitMerge/items=16k/conc=4/channels-8                  	       1	 157429708 ns/op	     944 B/op	      17 allocs/op
BenchmarkSplitMerge/items=16k/conc=4/pipeline-8                  	       1	 153915959 ns/op	    7296 B/op	      91 allocs/op
BenchmarkSplitMerge/items=16k/conc=4/pipeline-8                  	       1	 134887584 ns/op	    5952 B/op	      77 allocs/op
BenchmarkSplitMerge/items=16k/conc=4/pipeline-8                  	       1	 154678875 ns/op	    5280 B/op	      70 allocs/op
BenchmarkSplitMerge/items=16k/conc=4/pipeline-8                  	       1	 149294584 ns/op	    4608 B/op	      63 allocs/op
BenchmarkSplitMerge/items=16k/conc=4/pipeline-8                  	       1	 152558708 ns/op	    7008 B/op	      88 allocs/op
BenchmarkSplitMergeRealistic/items=1k/conc=1/loop-serial-8       	     489	    237085 ns/op	       0 B/op	       0 allocs/op
BenchmarkSplitMergeRealistic/items=1k/conc=1/loop-serial-8       	     480	    237280 ns/op	       0 B/op	       0 allocs/op
BenchmarkSplitMergeRealistic/items=1k/conc=1/loop-serial-8       	     487	    247320 ns/op	       0 B/op	       0 allocs/op
BenchmarkSplitMergeRealistic/items=1k/conc=1/loop-serial-8       	     492	    236346 ns/op	       0 B/op	       0 allocs/op
BenchmarkSplitMergeRealistic/items=1k/conc=1/loop-serial-8       	     476	    237589 ns/op	       0 B/op	       0 allocs/op
BenchmarkSplitMergeRealistic/items=1k/conc=1/loop-workers-8      	     498	    240126 ns/op	      24 B/op	       1 allocs/op
BenchmarkSplitMergeRealistic/items=1k/conc=1/loop-workers-8      	     474	    242811 ns/op	      24 B/op	       1 allocs/op
BenchmarkSplitMergeRealistic/items=1k/conc=1/loop-workers-8      	     463	    239772 ns/op	      24 B/op	       1 allocs/op
BenchmarkSplitMergeRealistic/items=1k/conc=1/loop-workers-8      	     466	    241026 ns/op	      24 B/op	       1 allocs/op
BenchmarkSplitMergeRealistic/items=1k/conc=1/loop-workers-8      	     486	    242711 ns/op	      24 B/op	       1 allocs/op
BenchmarkSplitMergeRealistic/items=1k/conc=1/channels-8          	      19	   5433278 ns/op	     453 B/op	      10 allocs/op
BenchmarkSplitMergeRealistic/items=1k/conc=1/channels-8          	      21	   5521822 ns/op	     453 B/op	      10 allocs/op
BenchmarkSplitMergeRealistic/items=1k/conc=1/channels-8          	      21	   5555399 ns/op	     453 B/op	      10 allocs/op
BenchmarkSplitMergeRealistic/items=1k/conc=1/channels-8          	      20	   5700288 ns/op	     544 B/op	      11 allocs/op
BenchmarkSplitMergeRealistic/items=1k/conc=1/channels-8          	      19	   5800395 ns/op	     489 B/op	      10 allocs/op
BenchmarkSplitMergeRealistic/items=1k/conc=1/pipeline-8          	      10	  10801767 ns/op	    1742 B/op	      31 allocs/op
BenchmarkSplitMergeRealistic/items=1k/conc=1/pipeline-8          	      10	  10272921 ns/op	    1598 B/op	      30 allocs/op
BenchmarkSplitMergeRealistic/items=1k/conc=1/pipeline-8          	      12	   8659094 ns/op	    1486 B/op	      29 allocs/op
BenchmarkSplitMergeRealistic/items=1k/conc=1/pipeline-8          	      12	   8415441 ns/op	    1798 B/op	      32 allocs/op
BenchmarkSplitMergeRealistic/items=1k/conc=1/pipeline-8          	      14	   8252800 ns/op	    1640 B/op	      30 allocs/op
BenchmarkSplitMergeRealistic/items=1k/conc=4/loop-serial-8       	     445	    242285 ns/op	       0 B/op	       0 allocs/op
BenchmarkSplitMergeRealistic/items=1k/conc=4/loop-serial-8       	     480	    235747 ns/op	       0 B/op	       0 allocs/op
BenchmarkSplitMergeRealistic/items=1k/conc=4/loop-serial-8       	     493	    237261 ns/op	       0 B/op	       0 allocs/op
BenchmarkSplitMergeRealistic/items=1k/conc=4/loop-serial-8       	     476	    235825 ns/op	       0 B/op	       0 allocs/op
BenchmarkSplitMergeRealistic/items=1k/conc=4/loop-serial-8       	     496	    237144 ns/op	       0 B/op	       0 allocs/op
BenchmarkSplitMergeRealistic/items=1k/conc=4/loop-workers-8      	     952	    128043 ns/op	     464 B/op	       8 allocs/op
BenchmarkSplitMergeRealistic/items=1k/conc=4/loop-workers-8      	     930	    125756 ns/op	     464 B/op	       8 allocs/op
BenchmarkSplitMergeRealistic/items=1k/conc=4/loop-workers-8      	    1008	    118258 ns/op	     464 B/op	       8 allocs/op
BenchmarkSplitMergeRealistic/items=1k/conc=4/loop-workers-8      	     782	    130098 ns/op	     464 B/op	       8 allocs/op
BenchmarkSplitMergeRealistic/items=1k/conc=4/loop-workers-8      	     595	    174418 ns/op	     464 B/op	       8 allocs/op
BenchmarkSplitMergeRealistic/items=1k/conc=4/channels-8          	      15	   7332142 ns/op	     999 B/op	      17 allocs/op
BenchmarkSplitMergeRealistic/items=1k/conc=4/channels-8          	      18	   7673079 ns/op	    1200 B/op	      19 allocs/op
BenchmarkSplitMergeRealistic/items=1k/conc=4/channels-8          	      12	   9126986 ns/op	     841 B/op	      16 allocs/op
BenchmarkSplitMergeRealistic/items=1k/conc=4/channels-8          	      14	  10448574 ns/op	    1374 B/op	      21 allocs/op
BenchmarkSplitMergeRealistic/items=1k/conc=4/channels-8          	      13	   8282991 ns/op	    1062 B/op	      18 allocs/op
BenchmarkSplitMergeRealistic/items=1k/conc=4/pipeline-8          	      10	  10764325 ns/op	    4833 B/op	      66 allocs/op
BenchmarkSplitMergeRealistic/items=1k/conc=4/pipeline-8          	      13	  10219638 ns/op	    4497 B/op	      62 allocs/op
BenchmarkSplitMergeRealistic/items=1k/conc=4/pipeline-8          	      10	  10367875 ns/op	    3988 B/op	      57 allocs/op
BenchmarkSplitMergeRealistic/items=1k/conc=4/pipeline-8          	      10	  10506329 ns/op	    4296 B/op	      60 allocs/op
BenchmarkSplitMergeRealistic/items=1k/conc=4/pipeline-8          	      13	  10362737 ns/op	    4519 B/op	      62 allocs/op
BenchmarkSplitMergeRealistic/items=16k/conc=4/loop-serial-8      	      30	   3766649 ns/op	       0 B/op	       0 allocs/op
BenchmarkSplitMergeRealistic/items=16k/conc=4/loop-serial-8      	      31	   3799767 ns/op	       0 B/op	       0 allocs/op
BenchmarkSplitMergeRealistic/items=16k/conc=4/loop-serial-8      	      30	   3928246 ns/op	       0 B/op	       0 allocs/op
BenchmarkSplitMergeRealistic/items=16k/conc=4/loop-serial-8      	      30	  10957235 ns/op	       0 B/op	       0 allocs/op
BenchmarkSplitMergeRealistic/items=16k/conc=4/loop-serial-8      	      20	   6364856 ns/op	       0 B/op	       0 allocs/op
BenchmarkSplitMergeRealistic/items=16k/conc=4/loop-workers-8     	     108	   1208749 ns/op	     464 B/op	       8 allocs/op
BenchmarkSplitMergeRealistic/items=16k/conc=4/loop-workers-8     	     102	   1087673 ns/op	     464 B/op	       8 allocs/op
BenchmarkSplitMergeRealistic/items=16k/conc=4/loop-workers-8     	      90	   1129529 ns/op	     464 B/op	       8 allocs/op
BenchmarkSplitMergeRealistic/items=16k/conc=4/loop-workers-8     	      98	   1094450 ns/op	     464 B/op	       8 allocs/op
BenchmarkSplitMergeRealistic/items=16k/conc=4/loop-workers-8     	      97	   1144814 ns/op	     464 B/op	       8 allocs/op
BenchmarkSplitMergeRealistic/items=16k/conc=4/channels-8         	       1	 333556834 ns/op	     944 B/op	      17 allocs/op
BenchmarkSplitMergeRealistic/items=16k/conc=4/channels-8         	       1	 157854583 ns/op	     944 B/op	      17 allocs/op
BenchmarkSplitMergeRealistic/items=16k/conc=4/channels-8         	       1	 141204667 ns/op	     944 B/op	      17 allocs/op
BenchmarkSplitMergeRealistic/items=16k/conc=4/channels-8         	       1	 177142542 ns/op	     944 B/op	      17 allocs/op
BenchmarkSplitMergeRealistic/items=16k/conc=4/channels-8         	       1	 160171042 ns/op	    1520 B/op	      23 allocs/op
BenchmarkSplitMergeRealistic/items=16k/conc=4/pipeline-8         	       1	 153075125 ns/op	    6048 B/op	      78 allocs/op
BenchmarkSplitMergeRealistic/items=16k/conc=4/pipeline-8         	       1	 154490792 ns/op	    6624 B/op	      84 allocs/op
BenchmarkSplitMergeRealistic/items=16k/conc=4/pipeline-8         	       1	 162492417 ns/op	    4320 B/op	      60 allocs/op
BenchmarkSplitMergeRealistic/items=16k/conc=4/pipeline-8         	       1	 147727208 ns/op	    4320 B/op	      60 allocs/op
BenchmarkSplitMergeRealistic/items=16k/conc=4/pipeline-8         	       1	 159398208 ns/op	    5088 B/op	      68 allocs/op
BenchmarkSplitBy/items=1k/conc=1/loop-serial-8                   	   30385	      3918 ns/op	       0 B/op	       0 allocs/op
BenchmarkSplitBy/items=1k/conc=1/loop-serial-8                   	   31166	      3769 ns/op	       0 B/op	       0 allocs/op
BenchmarkSplitBy/items=1k/conc=1/loop-serial-8                   	   30724	      3752 ns/op	       0 B/op	       0 allocs/op
BenchmarkSplitBy/items=1k/conc=1/loop-serial-8                   	   31196	      3771 ns/op	       0 B/op	       0 allocs/op
BenchmarkSplitBy/items=1k/conc=1/loop-serial-8                   	   30765	      3765 ns/op	       0 B/op	       0 allocs/op
BenchmarkSplitBy/items=1k/conc=1/loop-workers-8                  	   15618	     45679 ns/op	      24 B/op	       1 allocs/op
BenchmarkSplitBy/items=1k/conc=1/loop-workers-8                  	    8710	     11621 ns/op	      24 B/op	       1 allocs/op
BenchmarkSplitBy/items=1k/conc=1/loop-workers-8                  	    9908	     11943 ns/op	      24 B/op	       1 allocs/op
BenchmarkSplitBy/items=1k/conc=1/loop-workers-8                  	   10000	     12217 ns/op	      24 B/op	       1 allocs/op
BenchmarkSplitBy/items=1k/conc=1/loop-workers-8                  	    6900	     15395 ns/op	      24 B/op	       1 allocs/op
BenchmarkSplitBy/items=1k/conc=1/channels-8                      	      32	   6127495 ns/op	     484 B/op	      10 allocs/op
BenchmarkSplitBy/items=1k/conc=1/channels-8                      	      32	   3907247 ns/op	     559 B/op	      11 allocs/op
BenchmarkSplitBy/items=1k/conc=1/channels-8                      	      34	   3277366 ns/op	     598 B/op	      11 allocs/op
BenchmarkSplitBy/items=1k/conc=1/channels-8                      	      43	   3628231 ns/op	     450 B/op	      10 allocs/op
BenchmarkSplitBy/items=1k/conc=1/channels-8                      	      55	   3452955 ns/op	     488 B/op	      10 allocs/op
BenchmarkSplitBy/items=1k/conc=1/pipeline-8                      	      21	   5120808 ns/op	    1560 B/op	      29 allocs/op
BenchmarkSplitBy/items=1k/conc=1/pipeline-8                      	      18	   6163197 ns/op	    1508 B/op	      29 allocs/op
BenchmarkSplitBy/items=1k/conc=1/pipeline-8                      	      24	   5171672 ns/op	    1707 B/op	      31 allocs/op
BenchmarkSplitBy/items=1k/conc=1/pipeline-8                      	      24	   5166255 ns/op	    1567 B/op	      29 allocs/op
BenchmarkSplitBy/items=1k/conc=1/pipeline-8                      	      25	   4800428 ns/op	    1502 B/op	      29 allocs/op
BenchmarkSplitBy/items=1k/conc=4/loop-serial-8                   	   26784	      4854 ns/op	       0 B/op	       0 allocs/op
BenchmarkSplitBy/items=1k/conc=4/loop-serial-8                   	   29671	      3942 ns/op	       0 B/op	       0 allocs/op
BenchmarkSplitBy/items=1k/conc=4/loop-serial-8                   	   30934	      3802 ns/op	       0 B/op	       0 allocs/op
BenchmarkSplitBy/items=1k/conc=4/loop-serial-8                   	   31572	      4776 ns/op	       0 B/op	       0 allocs/op
BenchmarkSplitBy/items=1k/conc=4/loop-serial-8                   	   28711	      3825 ns/op	       0 B/op	       0 allocs/op
BenchmarkSplitBy/items=1k/conc=4/loop-workers-8                  	    7570	     17472 ns/op	     464 B/op	       8 allocs/op
BenchmarkSplitBy/items=1k/conc=4/loop-workers-8                  	    6085	     19294 ns/op	     464 B/op	       8 allocs/op
BenchmarkSplitBy/items=1k/conc=4/loop-workers-8                  	    5530	     18256 ns/op	     464 B/op	       8 allocs/op
BenchmarkSplitBy/items=1k/conc=4/loop-workers-8                  	    6171	     19632 ns/op	     464 B/op	       8 allocs/op
BenchmarkSplitBy/items=1k/conc=4/loop-workers-8                  	    6439	     18165 ns/op	     464 B/op	       8 allocs/op
BenchmarkSplitBy/items=1k/conc=4/channels-8                      	      67	   6315659 ns/op	     969 B/op	      17 allocs/op
BenchmarkSplitBy/items=1k/conc=4/channels-8                      	      33	   4238967 ns/op	     989 B/op	      17 allocs/op
BenchmarkSplitBy/items=1k/conc=4/channels-8                      	      37	   3997618 ns/op	     982 B/op	      17 allocs/op
BenchmarkSplitBy/items=1k/conc=4/channels-8                      	      40	   3499832 ns/op	    1014 B/op	      17 allocs/op
BenchmarkSplitBy/items=1k/conc=4/channels-8                      	      85	   3488975 ns/op	     916 B/op	      16 allocs/op
BenchmarkSplitBy/items=1k/conc=4/pipeline-8                      	      22	   5417000 ns/op	    4681 B/op	      64 allocs/op
BenchmarkSplitBy/items=1k/conc=4/pipeline-8                      	      19	   5409033 ns/op	    4261 B/op	      59 allocs/op
BenchmarkSplitBy/items=1k/conc=4/pipeline-8                      	      21	   5224730 ns/op	    4431 B/op	      61 allocs/op
BenchmarkSplitBy/items=1k/conc=4/pipeline-8                      	      21	   4946958 ns/op	    4266 B/op	      59 allocs/op
BenchmarkSplitBy/items=1k/conc=4/pipeline-8                      	      49	   5068891 ns/op	    4289 B/op	      60 allocs/op
BenchmarkSplitBy/items=16k/conc=4/loop-serial-8                  	    1758	     60785 ns/op	       0 B/op	       0 allocs/op
BenchmarkSplitBy/items=16k/conc=4/loop-serial-8                  	    1734	     63817 ns/op	       0 B/op	       0 allocs/op
BenchmarkSplitBy/items=16k/conc=4/loop-serial-8                  	    1689	     60359 ns/op	       0 B/op	       0 allocs/op
BenchmarkSplitBy/items=16k/conc=4/loop-serial-8                  	    1657	     60643 ns/op	       0 B/op	       0 allocs/op
BenchmarkSplitBy/items=16k/conc=4/loop-serial-8                  	    1960	     59854 ns/op	       0 B/op	       0 allocs/op
BenchmarkSplitBy/items=16k/conc=4/loop-workers-8                 	     234	    427920 ns/op	     464 B/op	       8 allocs/op
BenchmarkSplitBy/items=16k/conc=4/loop-workers-8                 	     250	    432136 ns/op	     464 B/op	       8 allocs/op
BenchmarkSplitBy/items=16k/conc=4/loop-workers-8                 	     256	    443153 ns/op	     464 B/op	       8 allocs/op
BenchmarkSplitBy/items=16k/conc=4/loop-workers-8                 	     266	    427902 ns/op	     464 B/op	       8 allocs/op
BenchmarkSplitBy/items=16k/conc=4/loop-workers-8                 	     255	    419380 ns/op	     464 B/op	       8 allocs/op
BenchmarkSplitBy/items=16k/conc=4/channels-8                     	       2	  77142833 ns/op	     888 B/op	      16 allocs/op
BenchmarkSplitBy/items=16k/conc=4/channels-8                     	       2	  66186626 ns/op	     888 B/op	      16 allocs/op
BenchmarkSplitBy/items=16k/conc=4/channels-8                     	       1	 143810250 ns/op	     944 B/op	      17 allocs/op
BenchmarkSplitBy/items=16k/conc=4/channels-8                     	       2	  56946896 ns/op	    1080 B/op	      18 allocs/op
BenchmarkSplitBy/items=16k/conc=4/channels-8                     	       2	  60838792 ns/op	    1272 B/op	      20 allocs/op
BenchmarkSplitBy/items=16k/conc=4/pipeline-8                     	       2	  83050166 ns/op	    5704 B/op	      74 allocs/op
BenchmarkSplitBy/items=16k/conc=4/pipeline-8                     	       2	  88929396 ns/op	    5080 B/op	      68 allocs/op
BenchmarkSplitBy/items=16k/conc=4/pipeline-8                     	       2	 102682230 ns/op	    4168 B/op	      58 allocs/op
BenchmarkSplitBy/items=16k/conc=4/pipeline-8                     	       1	 102974874 ns/op	    4352 B/op	      60 allocs/op
BenchmarkSplitBy/items=16k/conc=4/pipeline-8                     	       2	  89461834 ns/op	    5704 B/op	      74 allocs/op
BenchmarkOneToMany/items=1k/conc=1/loop-serial-8                 	    4722	     24317 ns/op	   16384 B/op	    1024 allocs/op
BenchmarkOneToMany/items=1k/conc=1/loop-serial-8                 	    5077	     23837 ns/op	   16384 B/op	    1024 allocs/op
BenchmarkOneToMany/items=1k/conc=1/loop-serial-8                 	    4558	     24373 ns/op	   16384 B/op	    1024 allocs/op
BenchmarkOneToMany/items=1k/conc=1/loop-serial-8                 	    4357	     24240 ns/op	   16384 B/op	    1024 allocs/op
BenchmarkOneToMany/items=1k/conc=1/loop-serial-8                 	    4918	     23916 ns/op	   16384 B/op	    1024 allocs/op
BenchmarkOneToMany/items=1k/conc=1/loop-workers-8                	    3628	     31850 ns/op	   16400 B/op	    1025 allocs/op
BenchmarkOneToMany/items=1k/conc=1/loop-workers-8                	    3721	     38866 ns/op	   16400 B/op	    1025 allocs/op
BenchmarkOneToMany/items=1k/conc=1/loop-workers-8                	    3610	     32627 ns/op	   16400 B/op	    1025 allocs/op
BenchmarkOneToMany/items=1k/conc=1/loop-workers-8                	    3666	     31990 ns/op	   16400 B/op	    1025 allocs/op
BenchmarkOneToMany/items=1k/conc=1/loop-workers-8                	    3055	    128723 ns/op	   16400 B/op	    1025 allocs/op
BenchmarkOneToMany/items=1k/conc=1/channels-8                    	      63	   1744741 ns/op	   16554 B/op	    1027 allocs/op
BenchmarkOneToMany/items=1k/conc=1/channels-8                    	      63	   1643667 ns/op	   16537 B/op	    1027 allocs/op
BenchmarkOneToMany/items=1k/conc=1/channels-8                    	      68	   1799531 ns/op	   16537 B/op	    1027 allocs/op
BenchmarkOneToMany/items=1k/conc=1/channels-8                    	      67	   1784769 ns/op	   16557 B/op	    1027 allocs/op
BenchmarkOneToMany/items=1k/conc=1/channels-8                    	      55	   1960495 ns/op	   16541 B/op	    1027 allocs/op
BenchmarkOneToMany/items=1k/conc=1/pipeline-8                    	      54	   1955777 ns/op	   17142 B/op	    1040 allocs/op
BenchmarkOneToMany/items=1k/conc=1/pipeline-8                    	      56	   2105162 ns/op	   17163 B/op	    1040 allocs/op
BenchmarkOneToMany/items=1k/conc=1/pipeline-8                    	      73	   2146034 ns/op	   17210 B/op	    1040 allocs/op
BenchmarkOneToMany/items=1k/conc=1/pipeline-8                    	      56	   1959958 ns/op	   17166 B/op	    1040 allocs/op
BenchmarkOneToMany/items=1k/conc=1/pipeline-8                    	      52	   2132345 ns/op	   17156 B/op	    1040 allocs/op
BenchmarkOneToMany/items=1k/conc=4/loop-serial-8                 	    4731	     57521 ns/op	   16384 B/op	    1024 allocs/op
BenchmarkOneToMany/items=1k/conc=4/loop-serial-8                 	    4324	     24079 ns/op	   16384 B/op	    1024 allocs/op
BenchmarkOneToMany/items=1k/conc=4/loop-serial-8                 	    4893	     23856 ns/op	   16384 B/op	    1024 allocs/op
BenchmarkOneToMany/items=1k/conc=4/loop-serial-8                 	    4248	     24643 ns/op	   16384 B/op	    1024 allocs/op
BenchmarkOneToMany/items=1k/conc=4/loop-serial-8                 	    5004	     23787 ns/op	   16384 B/op	    1024 allocs/op
BenchmarkOneToMany/items=1k/conc=4/loop-workers-8                	    2403	     47023 ns/op	   16840 B/op	    1032 allocs/op
BenchmarkOneToMany/items=1k/conc=4/loop-workers-8                	    2413	     47042 ns/op	   16840 B/op	    1032 allocs/op
BenchmarkOneToMany/items=1k/conc=4/loop-workers-8                	    2626	     47926 ns/op	   16840 B/op	    1032 allocs/op
BenchmarkOneToMany/items=1k/conc=4/loop-workers-8                	    2270	     48912 ns/op	   16840 B/op	    1032 allocs/op
BenchmarkOneToMany/items=1k/conc=4/loop-workers-8                	    2144	     46662 ns/op	   16840 B/op	    1032 allocs/op
BenchmarkOneToMany/items=1k/conc=4/channels-8                    	      37	   2927358 ns/op	   16731 B/op	    1030 allocs/op
BenchmarkOneToMany/items=1k/conc=4/channels-8                    	      36	   3162043 ns/op	   16792 B/op	    1030 allocs/op
BenchmarkOneToMany/items=1k/conc=4/channels-8                    	      43	   3277330 ns/op	   16730 B/op	    1030 allocs/op
BenchmarkOneToMany/items=1k/conc=4/channels-8                    	      42	   3043683 ns/op	   16851 B/op	    1031 allocs/op
BenchmarkOneToMany/items=1k/conc=4/channels-8                    	      43	   3012470 ns/op	   16752 B/op	    1030 allocs/op
BenchmarkOneToMany/items=1k/conc=4/pipeline-8                    	      31	   3650585 ns/op	   18580 B/op	    1056 allocs/op
BenchmarkOneToMany/items=1k/conc=4/pipeline-8                    	      32	   3623744 ns/op	   18516 B/op	    1055 allocs/op
BenchmarkOneToMany/items=1k/conc=4/pipeline-8                    	      40	   2862056 ns/op	   18457 B/op	    1055 allocs/op
BenchmarkOneToMany/items=1k/conc=4/pipeline-8                    	      36	   4653108 ns/op	   18738 B/op	    1058 allocs/op
BenchmarkOneToMany/items=1k/conc=4/pipeline-8                    	      32	   3283659 ns/op	   18459 B/op	    1055 allocs/op
BenchmarkOneToMany/items=16k/conc=4/loop-serial-8                	     264	    425040 ns/op	  262144 B/op	   16384 allocs/op
BenchmarkOneToMany/items=16k/conc=4/loop-serial-8                	     286	    378873 ns/op	  262144 B/op	   16384 allocs/op
BenchmarkOneToMany/items=16k/conc=4/loop-serial-8                	     292	    377602 ns/op	  262144 B/op	   16384 allocs/op
BenchmarkOneToMany/items=16k/conc=4/loop-serial-8                	     295	    385348 ns/op	  262144 B/op	   16384 allocs/op
BenchmarkOneToMany/items=16k/conc=4/loop-serial-8                	     294	    378984 ns/op	  262144 B/op	   16384 allocs/op
BenchmarkOneToMany/items=16k/conc=4/loop-workers-8               	     153	    725072 ns/op	  262601 B/op	   16392 allocs/op
BenchmarkOneToMany/items=16k/conc=4/loop-workers-8               	     153	    720413 ns/op	  262602 B/op	   16392 allocs/op
BenchmarkOneToMany/items=16k/conc=4/loop-workers-8               	     158	    739390 ns/op	  262602 B/op	   16392 allocs/op
BenchmarkOneToMany/items=16k/conc=4/loop-workers-8               	     158	    729148 ns/op	  262601 B/op	   16392 allocs/op
BenchmarkOneToMany/items=16k/conc=4/loop-workers-8               	     151	    713148 ns/op	  262602 B/op	   16392 allocs/op
BenchmarkOneToMany/items=16k/conc=4/channels-8                   	       3	  41860708 ns/op	  262525 B/op	   16390 allocs/op
BenchmarkOneToMany/items=16k/conc=4/channels-8                   	       3	  35021153 ns/op	  262525 B/op	   16390 allocs/op
BenchmarkOneToMany/items=16k/conc=4/channels-8                   	       3	  38490875 ns/op	  262525 B/op	   16390 allocs/op
BenchmarkOneToMany/items=16k/conc=4/channels-8                   	       2	  60297875 ns/op	  262544 B/op	   16390 allocs/op
BenchmarkOneToMany/items=16k/conc=4/channels-8                   	       3	  53995222 ns/op	  262525 B/op	   16390 allocs/op
BenchmarkOneToMany/items=16k/conc=4/pipeline-8                   	       2	  53847562 ns/op	  264392 B/op	   16416 allocs/op
BenchmarkOneToMany/items=16k/conc=4/pipeline-8                   	       2	  54292354 ns/op	  264392 B/op	   16416 allocs/op
BenchmarkOneToMany/items=16k/conc=4/pipeline-8                   	       2	  55271876 ns/op	  264488 B/op	   16417 allocs/op
BenchmarkOneToMany/items=16k/conc=4/pipeline-8                   	       2	  54612458 ns/op	  265064 B/op	   16423 allocs/op
BenchmarkOneToMany/items=16k/conc=4/pipeline-8                   	       2	  55097792 ns/op	  264488 B/op	   16417 allocs/op
BenchmarkFromChan/items=1k/conc=1/loop-serial-8                  	   46561	      2445 ns/op	       0 B/op	       0 allocs/op
BenchmarkFromChan/items=1k/conc=1/loop-serial-8                  	   47793	      2414 ns/op	       0 B/op	       0 allocs/op
BenchmarkFromChan/items=1k/conc=1/loop-serial-8                  	   46576	      2487 ns/op	       0 B/op	       0 allocs/op
BenchmarkFromChan/items=1k/conc=1/loop-serial-8                  	   46898	      2387 ns/op	       0 B/op	       0 allocs/op
BenchmarkFromChan/items=1k/conc=1/loop-serial-8                  	   47769	      2397 ns/op	       0 B/op	       0 allocs/op
BenchmarkFromChan/items=1k/conc=1/loop-workers-8                 	   29563	      3820 ns/op	      16 B/op	       1 allocs/op
BenchmarkFromChan/items=1k/conc=1/loop-workers-8                 	   30229	      3890 ns/op	      16 B/op	       1 allocs/op
BenchmarkFromChan/items=1k/conc=1/loop-workers-8                 	   29133	      3833 ns/op	      16 B/op	       1 allocs/op
BenchmarkFromChan/items=1k/conc=1/loop-workers-8                 	   30345	      3817 ns/op	      16 B/op	       1 allocs/op
BenchmarkFromChan/items=1k/conc=1/loop-workers-8                 	   27639	      4214 ns/op	      16 B/op	       1 allocs/op
BenchmarkFromChan/items=1k/conc=1/channels-8                     	      90	   1208730 ns/op	     177 B/op	       3 allocs/op
BenchmarkFromChan/items=1k/conc=1/channels-8                     	      97	   1237936 ns/op	     160 B/op	       3 allocs/op
BenchmarkFromChan/items=1k/conc=1/channels-8                     	      86	   1274474 ns/op	     153 B/op	       3 allocs/op
BenchmarkFromChan/items=1k/conc=1/channels-8                     	      94	   1210977 ns/op	     153 B/op	       3 allocs/op
BenchmarkFromChan/items=1k/conc=1/channels-8                     	     100	   1282294 ns/op	     155 B/op	       3 allocs/op
BenchmarkFromChan/items=1k/conc=1/pipeline-8                     	      66	   1659760 ns/op	    1199 B/op	      21 allocs/op
BenchmarkFromChan/items=1k/conc=1/pipeline-8                     	      69	   3553722 ns/op	    1171 B/op	      21 allocs/op
BenchmarkFromChan/items=1k/conc=1/pipeline-8                     	      75	   1490986 ns/op	    1188 B/op	      21 allocs/op
BenchmarkFromChan/items=1k/conc=1/pipeline-8                     	      85	   1592238 ns/op	    1206 B/op	      21 allocs/op
BenchmarkFromChan/items=1k/conc=1/pipeline-8                     	      56	   2600933 ns/op	    1158 B/op	      21 allocs/op
BenchmarkFromChan/items=1k/conc=4/loop-serial-8                  	   43986	      2422 ns/op	       0 B/op	       0 allocs/op
BenchmarkFromChan/items=1k/conc=4/loop-serial-8                  	   47070	      2494 ns/op	       0 B/op	       0 allocs/op
BenchmarkFromChan/items=1k/conc=4/loop-serial-8                  	   41391	      2528 ns/op	       0 B/op	       0 allocs/op
BenchmarkFromChan/items=1k/conc=4/loop-serial-8                  	   45985	      2476 ns/op	       0 B/op	       0 allocs/op
BenchmarkFromChan/items=1k/conc=4/loop-serial-8                  	   48013	      2506 ns/op	       0 B/op	       0 allocs/op
BenchmarkFromChan/items=1k/conc=4/loop-workers-8                 	    8497	     18206 ns/op	     456 B/op	       8 allocs/op
BenchmarkFromChan/items=1k/conc=4/loop-workers-8                 	    8610	     18697 ns/op	     456 B/op	       8 allocs/op
BenchmarkFromChan/items=1k/conc=4/loop-workers-8                 	    7276	     17919 ns/op	     456 B/op	       8 allocs/op
BenchmarkFromChan/items=1k/conc=4/loop-workers-8                 	    6652	     18754 ns/op	     456 B/op	       8 allocs/op
BenchmarkFromChan/items=1k/conc=4/loop-workers-8                 	    4828	     21804 ns/op	     456 B/op	       8 allocs/op
BenchmarkFromChan/items=1k/conc=4/channels-8                     	      63	   1755009 ns/op	     345 B/op	       6 allocs/op
BenchmarkFromChan/items=1k/conc=4/channels-8                     	      92	   1743885 ns/op	     437 B/op	       6 allocs/op
BenchmarkFromChan/items=1k/conc=4/channels-8                     	      58	   1808230 ns/op	     345 B/op	       6 allocs/op
BenchmarkFromChan/items=1k/conc=4/channels-8                     	      63	   1746505 ns/op	     345 B/op	       6 allocs/op
BenchmarkFromChan/items=1k/conc=4/channels-8                     	      67	   1798118 ns/op	     347 B/op	       6 allocs/op
BenchmarkFromChan/items=1k/conc=4/pipeline-8                     	      46	   2933718 ns/op	    3608 B/op	      51 allocs/op
BenchmarkFromChan/items=1k/conc=4/pipeline-8                     	     100	   2318930 ns/op	    3654 B/op	      51 allocs/op
BenchmarkFromChan/items=1k/conc=4/pipeline-8                     	      51	   2231149 ns/op	    3629 B/op	      51 allocs/op
BenchmarkFromChan/items=1k/conc=4/pipeline-8                     	      57	   2171942 ns/op	    3719 B/op	      52 allocs/op
BenchmarkFromChan/items=1k/conc=4/pipeline-8                     	      57	   2258575 ns/op	    3606 B/op	      51 allocs/op
BenchmarkFromChan/items=16k/conc=4/loop-serial-8                 	    2986	     38805 ns/op	       0 B/op	       0 allocs/op
BenchmarkFromChan/items=16k/conc=4/loop-serial-8                 	    2637	     39565 ns/op	       0 B/op	       0 allocs/op
BenchmarkFromChan/items=16k/conc=4/loop-serial-8                 	    3069	     39050 ns/op	       0 B/op	       0 allocs/op
BenchmarkFromChan/items=16k/conc=4/loop-serial-8                 	    3105	     38734 ns/op	       0 B/op	       0 allocs/op
BenchmarkFromChan/items=16k/conc=4/loop-serial-8                 	    3088	     39001 ns/op	       0 B/op	       0 allocs/op
BenchmarkFromChan/items=16k/conc=4/loop-workers-8                	     280	    492200 ns/op	     456 B/op	       8 allocs/op
BenchmarkFromChan/items=16k/conc=4/loop-workers-8                	     226	    450921 ns/op	     456 B/op	       8 allocs/op
BenchmarkFromChan/items=16k/conc=4/loop-workers-8                	     330	    432735 ns/op	     456 B/op	       8 allocs/op
BenchmarkFromChan/items=16k/conc=4/loop-workers-8                	     277	    425490 ns/op	     456 B/op	       8 allocs/op
BenchmarkFromChan/items=16k/conc=4/loop-workers-8                	     292	    427602 ns/op	     456 B/op	       8 allocs/op
BenchmarkFromChan/items=16k/conc=4/channels-8                    	       4	  26712072 ns/op	     420 B/op	       6 allocs/op
BenchmarkFromChan/items=16k/conc=4/channels-8                    	       4	  27366042 ns/op	     564 B/op	       8 allocs/op
BenchmarkFromChan/items=16k/conc=4/channels-8                    	       4	  29028375 ns/op	     540 B/op	       8 allocs/op
BenchmarkFromChan/items=16k/conc=4/channels-8                    	       4	  28192875 ns/op	     492 B/op	       7 allocs/op
BenchmarkFromChan/items=16k/conc=4/channels-8                    	       4	  27092313 ns/op	     372 B/op	       6 allocs/op
BenchmarkFromChan/items=16k/conc=4/pipeline-8                    	       3	  34336778 ns/op	    3722 B/op	      52 allocs/op
BenchmarkFromChan/items=16k/conc=4/pipeline-8                    	       4	  33569208 ns/op	    4028 B/op	      55 allocs/op
BenchmarkFromChan/items=16k/conc=4/pipeline-8                    	       4	  35312427 ns/op	    4148 B/op	      56 allocs/op
BenchmarkFromChan/items=16k/conc=4/pipeline-8                    	       4	  33982698 ns/op	    4028 B/op	      55 allocs/op
BenchmarkFromChan/items=16k/conc=4/pipeline-8                    	       4	  37389864 ns/op	    4508 B/op	      60 allocs/op
BenchmarkSinkFromChan/items=1k/conc=1/loop-serial-8              	   45423	      2445 ns/op	       0 B/op	       0 allocs/op
BenchmarkSinkFromChan/items=1k/conc=1/loop-serial-8              	   49087	      2424 ns/op	       0 B/op	       0 allocs/op
BenchmarkSinkFromChan/items=1k/conc=1/loop-serial-8              	   48865	      2416 ns/op	       0 B/op	       0 allocs/op
BenchmarkSinkFromChan/items=1k/conc=1/loop-serial-8              	   45008	      2404 ns/op	       0 B/op	       0 allocs/op
BenchmarkSinkFromChan/items=1k/conc=1/loop-serial-8              	   47128	      7199 ns/op	       0 B/op	       0 allocs/op
BenchmarkSinkFromChan/items=1k/conc=1/loop-workers-8             	   31041	      3848 ns/op	      16 B/op	       1 allocs/op
BenchmarkSinkFromChan/items=1k/conc=1/loop-workers-8             	   29773	      3838 ns/op	      16 B/op	       1 allocs/op
BenchmarkSinkFromChan/items=1k/conc=1/loop-workers-8             	   29974	      3830 ns/op	      16 B/op	       1 allocs/op
BenchmarkSinkFromChan/items=1k/conc=1/loop-workers-8             	   30072	      3845 ns/op	      16 B/op	       1 allocs/op
BenchmarkSinkFromChan/items=1k/conc=1/loop-workers-8             	   30692	      3848 ns/op	      16 B/op	       1 allocs/op
BenchmarkSinkFromChan/items=1k/conc=1/channels-8                 	     271	    458733 ns/op	     147 B/op	       2 allocs/op
BenchmarkSinkFromChan/items=1k/conc=1/channels-8                 	     247	    444394 ns/op	     144 B/op	       2 allocs/op
BenchmarkSinkFromChan/items=1k/conc=1/channels-8                 	     241	    466296 ns/op	     144 B/op	       2 allocs/op
BenchmarkSinkFromChan/items=1k/conc=1/channels-8                 	     231	    444355 ns/op	     144 B/op	       2 allocs/op
BenchmarkSinkFromChan/items=1k/conc=1/channels-8                 	     253	    442635 ns/op	     144 B/op	       2 allocs/op
BenchmarkSinkFromChan/items=1k/conc=1/pipeline-8                 	     147	    798188 ns/op	    1062 B/op	      19 allocs/op
BenchmarkSinkFromChan/items=1k/conc=1/pipeline-8                 	     144	    806751 ns/op	    1042 B/op	      19 allocs/op
BenchmarkSinkFromChan/items=1k/conc=1/pipeline-8                 	     144	    789248 ns/op	    1053 B/op	      19 allocs/op
BenchmarkSinkFromChan/items=1k/conc=1/pipeline-8                 	     142	    792977 ns/op	    1058 B/op	      19 allocs/op
BenchmarkSinkFromChan/items=1k/conc=1/pipeline-8                 	     144	    796502 ns/op	    1044 B/op	      19 allocs/op
BenchmarkSinkFromChan/items=1k/conc=4/loop-serial-8              	   45661	      2414 ns/op	       0 B/op	       0 allocs/op
BenchmarkSinkFromChan/items=1k/conc=4/loop-serial-8              	   33337	      3708 ns/op	       0 B/op	       0 allocs/op
BenchmarkSinkFromChan/items=1k/conc=4/loop-serial-8              	   45784	      2431 ns/op	       0 B/op	       0 allocs/op
BenchmarkSinkFromChan/items=1k/conc=4/loop-serial-8              	   46401	      2409 ns/op	       0 B/op	       0 allocs/op
BenchmarkSinkFromChan/items=1k/conc=4/loop-serial-8              	   47900	      2422 ns/op	       0 B/op	       0 allocs/op
BenchmarkSinkFromChan/items=1k/conc=4/loop-workers-8             	    6784	     17417 ns/op	     456 B/op	       8 allocs/op
BenchmarkSinkFromChan/items=1k/conc=4/loop-workers-8             	    6952	     18412 ns/op	     456 B/op	       8 allocs/op
BenchmarkSinkFromChan/items=1k/conc=4/loop-workers-8             	    6736	     17787 ns/op	     456 B/op	       8 allocs/op
BenchmarkSinkFromChan/items=1k/conc=4/loop-workers-8             	    6805	     17938 ns/op	     456 B/op	       8 allocs/op
BenchmarkSinkFromChan/items=1k/conc=4/loop-workers-8             	    6662	     19026 ns/op	     456 B/op	       8 allocs/op
BenchmarkSinkFromChan/items=1k/conc=4/channels-8                 	     144	    770274 ns/op	     392 B/op	       5 allocs/op
BenchmarkSinkFromChan/items=1k/conc=4/channels-8                 	     129	    933818 ns/op	     416 B/op	       5 allocs/op
BenchmarkSinkFromChan/items=1k/conc=4/channels-8                 	     146	    842018 ns/op	     384 B/op	       5 allocs/op
BenchmarkSinkFromChan/items=1k/conc=4/channels-8                 	     145	    872416 ns/op	     409 B/op	       5 allocs/op
BenchmarkSinkFromChan/items=1k/conc=4/channels-8                 	     134	    842495 ns/op	     396 B/op	       5 allocs/op
BenchmarkSinkFromChan/items=1k/conc=4/pipeline-8                 	      78	   1412578 ns/op	    3521 B/op	      49 allocs/op
BenchmarkSinkFromChan/items=1k/conc=4/pipeline-8                 	      82	   1459500 ns/op	    3621 B/op	      50 allocs/op
BenchmarkSinkFromChan/items=1k/conc=4/pipeline-8                 	      70	   1522314 ns/op	    3541 B/op	      49 allocs/op
BenchmarkSinkFromChan/items=1k/conc=4/pipeline-8                 	      91	   1537601 ns/op	    3516 B/op	      49 allocs/op
BenchmarkSinkFromChan/items=1k/conc=4/pipeline-8                 	     100	   1561163 ns/op	    3497 B/op	      49 allocs/op
BenchmarkSinkFromChan/items=16k/conc=4/loop-serial-8             	    2437	     41449 ns/op	       0 B/op	       0 allocs/op
BenchmarkSinkFromChan/items=16k/conc=4/loop-serial-8             	    3075	     39054 ns/op	       0 B/op	       0 allocs/op
BenchmarkSinkFromChan/items=16k/conc=4/loop-serial-8             	    3082	     38487 ns/op	       0 B/op	       0 allocs/op
BenchmarkSinkFromChan/items=16k/conc=4/loop-serial-8             	    3104	     38452 ns/op	       0 B/op	       0 allocs/op
BenchmarkSinkFromChan/items=16k/conc=4/loop-serial-8             	    2592	     39294 ns/op	       0 B/op	       0 allocs/op
BenchmarkSinkFromChan/items=16k/conc=4/loop-workers-8            	     267	    426529 ns/op	     456 B/op	       8 allocs/op
BenchmarkSinkFromChan/items=16k/conc=4/loop-workers-8            	     270	    421617 ns/op	     456 B/op	       8 allocs/op
BenchmarkSinkFromChan/items=16k/conc=4/loop-workers-8            	     375	    408213 ns/op	     456 B/op	       8 allocs/op
BenchmarkSinkFromChan/items=16k/conc=4/loop-workers-8            	     307	    416568 ns/op	     456 B/op	       8 allocs/op
BenchmarkSinkFromChan/items=16k/conc=4/loop-workers-8            	     248	    426429 ns/op	     456 B/op	       8 allocs/op
BenchmarkSinkFromChan/items=16k/conc=4/channels-8                	       9	  11911662 ns/op	     396 B/op	       5 allocs/op
BenchmarkSinkFromChan/items=16k/conc=4/channels-8                	       8	  13582557 ns/op	     398 B/op	       5 allocs/op
BenchmarkSinkFromChan/items=16k/conc=4/channels-8                	       8	  13034682 ns/op	     398 B/op	       5 allocs/op
BenchmarkSinkFromChan/items=16k/conc=4/channels-8                	       8	  12806808 ns/op	     398 B/op	       5 allocs/op
BenchmarkSinkFromChan/items=16k/conc=4/channels-8                	       8	  13295162 ns/op	     446 B/op	       5 allocs/op
BenchmarkSinkFromChan/items=16k/conc=4/pipeline-8                	       6	  49600833 ns/op	    4013 B/op	      54 allocs/op
BenchmarkSinkFromChan/items=16k/conc=4/pipeline-8                	       6	  22837104 ns/op	    3965 B/op	      53 allocs/op
BenchmarkSinkFromChan/items=16k/conc=4/pipeline-8                	       5	  24164750 ns/op	    4080 B/op	      55 allocs/op
BenchmarkSinkFromChan/items=16k/conc=4/pipeline-8                	       6	  23886840 ns/op	    4013 B/op	      54 allocs/op
BenchmarkSinkFromChan/items=16k/conc=4/pipeline-8                	       5	  23002450 ns/op	    3600 B/op	      50 allocs/op
BenchmarkBatch/items=1k/conc=1/loop-serial-8                     	   10935	     10082 ns/op	   15824 B/op	     184 allocs/op
BenchmarkBatch/items=1k/conc=1/loop-serial-8                     	    9930	     10222 ns/op	   15824 B/op	     184 allocs/op
BenchmarkBatch/items=1k/conc=1/loop-serial-8                     	   10000	     10153 ns/op	   15824 B/op	     184 allocs/op
BenchmarkBatch/items=1k/conc=1/loop-serial-8                     	   12038	      9739 ns/op	   15824 B/op	     184 allocs/op
BenchmarkBatch/items=1k/conc=1/loop-serial-8                     	   10000	     10334 ns/op	   15824 B/op	     184 allocs/op
BenchmarkBatch/items=1k/conc=1/loop-workers-8                    	   10958	      9732 ns/op	   15824 B/op	     184 allocs/op
BenchmarkBatch/items=1k/conc=1/loop-workers-8                    	   11624	      9999 ns/op	   15824 B/op	     184 allocs/op
BenchmarkBatch/items=1k/conc=1/loop-workers-8                    	   11816	      9960 ns/op	   15824 B/op	     184 allocs/op
BenchmarkBatch/items=1k/conc=1/loop-workers-8                    	   10000	     11161 ns/op	   15824 B/op	     184 allocs/op
BenchmarkBatch/items=1k/conc=1/loop-workers-8                    	   11701	     10808 ns/op	   15824 B/op	     184 allocs/op
BenchmarkBatch/items=1k/conc=1/channels-8                        	     192	    597649 ns/op	   16046 B/op	     190 allocs/op
BenchmarkBatch/items=1k/conc=1/channels-8                        	     181	    596854 ns/op	   16059 B/op	     190 allocs/op
BenchmarkBatch/items=1k/conc=1/channels-8                        	     195	    599600 ns/op	   16050 B/op	     190 allocs/op
BenchmarkBatch/items=1k/conc=1/channels-8                        	     188	    593998 ns/op	   16044 B/op	     190 allocs/op
BenchmarkBatch/items=1k/conc=1/channels-8                        	     188	    602600 ns/op	   16084 B/op	     190 allocs/op
BenchmarkBatch/items=1k/conc=1/pipeline-8                        	     231	    513382 ns/op	   17104 B/op	     209 allocs/op
BenchmarkBatch/items=1k/conc=1/pipeline-8                        	     230	    505783 ns/op	   17093 B/op	     209 allocs/op
BenchmarkBatch/items=1k/conc=1/pipeline-8                        	     243	    517435 ns/op	   17091 B/op	     209 allocs/op
BenchmarkBatch/items=1k/conc=1/pipeline-8                        	     100	   1186520 ns/op	   17117 B/op	     209 allocs/op
BenchmarkBatch/items=1k/conc=1/pipeline-8                        	     219	    493614 ns/op	   17092 B/op	     209 allocs/op
BenchmarkBatch/items=1k/conc=4/loop-serial-8                     	   11266	     10116 ns/op	   15824 B/op	     184 allocs/op
BenchmarkBatch/items=1k/conc=4/loop-serial-8                     	   10066	     10828 ns/op	   15824 B/op	     184 allocs/op
BenchmarkBatch/items=1k/conc=4/loop-serial-8                     	   10234	     10740 ns/op	   15824 B/op	     184 allocs/op
BenchmarkBatch/items=1k/conc=4/loop-serial-8                     	   10580	      9683 ns/op	   15824 B/op	     184 allocs/op
BenchmarkBatch/items=1k/conc=4/loop-serial-8                     	   12008	      9870 ns/op	   15824 B/op	     184 allocs/op
BenchmarkBatch/items=1k/conc=4/loop-workers-8                    	    7539	     29687 ns/op	   16651 B/op	     188 allocs/op
BenchmarkBatch/items=1k/conc=4/loop-workers-8                    	    3756	     28461 ns/op	   16644 B/op	     188 allocs/op
BenchmarkBatch/items=1k/conc=4/loop-workers-8                    	    4275	     28608 ns/op	   16600 B/op	     187 allocs/op
BenchmarkBatch/items=1k/conc=4/loop-workers-8                    	    3582	     35265 ns/op	   16616 B/op	     187 allocs/op
BenchmarkBatch/items=1k/conc=4/loop-workers-8                    	    3152	     33923 ns/op	   16707 B/op	     188 allocs/op
BenchmarkBatch/items=1k/conc=4/channels-8                        	      99	   1042504 ns/op	   15872 B/op	     186 allocs/op
BenchmarkBatch/items=1k/conc=4/channels-8                        	     123	    969273 ns/op	   15765 B/op	     185 allocs/op
BenchmarkBatch/items=1k/conc=4/channels-8                        	     116	    979724 ns/op	   15768 B/op	     185 allocs/op
BenchmarkBatch/items=1k/conc=4/channels-8                        	     120	    945684 ns/op	   15715 B/op	     184 allocs/op
BenchmarkBatch/items=1k/conc=4/channels-8                        	     122	    991901 ns/op	   15711 B/op	     184 allocs/op
BenchmarkBatch/items=1k/conc=4/pipeline-8                        	     105	   1023544 ns/op	   19005 B/op	     233 allocs/op
BenchmarkBatch/items=1k/conc=4/pipeline-8                        	     134	    906780 ns/op	   19052 B/op	     234 allocs/op
BenchmarkBatch/items=1k/conc=4/pipeline-8                        	     123	    848474 ns/op	   19038 B/op	     234 allocs/op
BenchmarkBatch/items=1k/conc=4/pipeline-8                        	     128	    857349 ns/op	   19078 B/op	     234 allocs/op
BenchmarkBatch/items=1k/conc=4/pipeline-8                        	     124	    856436 ns/op	   18987 B/op	     233 allocs/op
BenchmarkBatch/items=16k/conc=4/loop-serial-8                    	     594	    184350 ns/op	  257744 B/op	    3064 allocs/op
BenchmarkBatch/items=16k/conc=4/loop-serial-8                    	     740	    158992 ns/op	  257744 B/op	    3064 allocs/op
BenchmarkBatch/items=16k/conc=4/loop-serial-8                    	     646	    170513 ns/op	  257744 B/op	    3064 allocs/op
BenchmarkBatch/items=16k/conc=4/loop-serial-8                    	     670	    520980 ns/op	  257744 B/op	    3064 allocs/op
BenchmarkBatch/items=16k/conc=4/loop-serial-8                    	     692	    167120 ns/op	  257744 B/op	    3064 allocs/op
BenchmarkBatch/items=16k/conc=4/loop-workers-8                   	     202	    533250 ns/op	  257811 B/op	    3057 allocs/op
BenchmarkBatch/items=16k/conc=4/loop-workers-8                   	     229	    550834 ns/op	  257759 B/op	    3056 allocs/op
BenchmarkBatch/items=16k/conc=4/loop-workers-8                   	     241	    513076 ns/op	  257883 B/op	    3058 allocs/op
BenchmarkBatch/items=16k/conc=4/loop-workers-8                   	     219	    512889 ns/op	  257778 B/op	    3056 allocs/op
BenchmarkBatch/items=16k/conc=4/loop-workers-8                   	     218	    530309 ns/op	  257656 B/op	    3054 allocs/op
BenchmarkBatch/items=16k/conc=4/channels-8                       	       8	  14988161 ns/op	  257736 B/op	    3064 allocs/op
BenchmarkBatch/items=16k/conc=4/channels-8                       	       9	  14454736 ns/op	  257821 B/op	    3067 allocs/op
BenchmarkBatch/items=16k/conc=4/channels-8                       	       8	  14209177 ns/op	  257886 B/op	    3067 allocs/op
BenchmarkBatch/items=16k/conc=4/channels-8                       	       8	  14167776 ns/op	  257694 B/op	    3063 allocs/op
BenchmarkBatch/items=16k/conc=4/channels-8                       	      13	  16806074 ns/op	  257843 B/op	    3066 allocs/op
BenchmarkBatch/items=16k/conc=4/pipeline-8                       	       8	  12794864 ns/op	  260868 B/op	    3113 allocs/op
BenchmarkBatch/items=16k/conc=4/pipeline-8                       	      24	  12545208 ns/op	  261026 B/op	    3114 allocs/op
BenchmarkBatch/items=16k/conc=4/pipeline-8                       	       8	  13660974 ns/op	  261316 B/op	    3117 allocs/op
BenchmarkBatch/items=16k/conc=4/pipeline-8                       	       9	  12432342 ns/op	  261507 B/op	    3120 allocs/op
BenchmarkBatch/items=16k/conc=4/pipeline-8                       	      14	  12461357 ns/op	  261420 B/op	    3120 allocs/op
BenchmarkBatchChan/items=1k/conc=1/loop-serial-8                 	   10381	     10073 ns/op	   15824 B/op	     184 allocs/op
BenchmarkBatchChan/items=1k/conc=1/loop-serial-8                 	   11631	      9918 ns/op	   15824 B/op	     184 allocs/op
BenchmarkBatchChan/items=1k/conc=1/loop-serial-8                 	   11445	      9926 ns/op	   15824 B/op	     184 allocs/op
BenchmarkBatchChan/items=1k/conc=1/loop-serial-8                 	   10000	     10247 ns/op	   15824 B/op	     184 allocs/op
BenchmarkBatchChan/items=1k/conc=1/loop-serial-8                 	   11655	      9753 ns/op	   15824 B/op	     184 allocs/op
BenchmarkBatchChan/items=1k/conc=1/loop-workers-8                	   11028	      9840 ns/op	   15824 B/op	     184 allocs/op
BenchmarkBatchChan/items=1k/conc=1/loop-workers-8                	   10309	     10384 ns/op	   15824 B/op	     184 allocs/op
BenchmarkBatchChan/items=1k/conc=1/loop-workers-8                	   11188	     10570 ns/op	   15824 B/op	     184 allocs/op
BenchmarkBatchChan/items=1k/conc=1/loop-workers-8                	   11350	      9860 ns/op	   15824 B/op	     184 allocs/op
BenchmarkBatchChan/items=1k/conc=1/loop-workers-8                	    9774	     10391 ns/op	   15824 B/op	     184 allocs/op
BenchmarkBatchChan/items=1k/conc=1/channels-8                    	      84	   1970984 ns/op	    3760 B/op	      35 allocs/op
BenchmarkBatchChan/items=1k/conc=1/channels-8                    	      88	   1484512 ns/op	    3737 B/op	      35 allocs/op
BenchmarkBatchChan/items=1k/conc=1/channels-8                    	     100	   1618982 ns/op	    3756 B/op	      35 allocs/op
BenchmarkBatchChan/items=1k/conc=1/channels-8                    	     100	   1237182 ns/op	    3737 B/op	      35 allocs/op
BenchmarkBatchChan/items=1k/conc=1/channels-8                    	      94	   1197387 ns/op	    3739 B/op	      35 allocs/op
BenchmarkBatchChan/items=1k/conc=1/pipeline-8                    	     109	    973253 ns/op	    4499 B/op	      49 allocs/op
BenchmarkBatchChan/items=1k/conc=1/pipeline-8                    	     121	    968216 ns/op	    4519 B/op	      49 allocs/op
BenchmarkBatchChan/items=1k/conc=1/pipeline-8                    	     122	    878022 ns/op	    4511 B/op	      49 allocs/op
BenchmarkBatchChan/items=1k/conc=1/pipeline-8                    	     122	    940367 ns/op	    4509 B/op	      49 allocs/op
BenchmarkBatchChan/items=1k/conc=1/pipeline-8                    	      96	   1161043 ns/op	    4499 B/op	      49 allocs/op
BenchmarkBatchChan/items=1k/conc=4/loop-serial-8                 	   10000	     10563 ns/op	   15824 B/op	     184 allocs/op
BenchmarkBatchChan/items=1k/conc=4/loop-serial-8                 	   10000	     10112 ns/op	   15824 B/op	     184 allocs/op
BenchmarkBatchChan/items=1k/conc=4/loop-serial-8                 	   11696	     10096 ns/op	   15824 B/op	     184 allocs/op
BenchmarkBatchChan/items=1k/conc=4/loop-serial-8                 	   11174	      9655 ns/op	   15824 B/op	     184 allocs/op
BenchmarkBatchChan/items=1k/conc=4/loop-serial-8                 	   11610	      9556 ns/op	   15824 B/op	     184 allocs/op
BenchmarkBatchChan/items=1k/conc=4/loop-workers-8                	    3369	     30671 ns/op	   16676 B/op	     188 allocs/op
BenchmarkBatchChan/items=1k/conc=4/loop-workers-8                	    3415	     30686 ns/op	   16629 B/op	     187 allocs/op
BenchmarkBatchChan/items=1k/conc=4/loop-workers-8                	    3813	     28786 ns/op	   16657 B/op	     188 allocs/op
BenchmarkBatchChan/items=1k/conc=4/loop-workers-8                	    4580	     31303 ns/op	   16657 B/op	     188 allocs/op
BenchmarkBatchChan/items=1k/conc=4/loop-workers-8                	    3931	     29275 ns/op	   16680 B/op	     188 allocs/op
BenchmarkBatchChan/items=1k/conc=4/channels-8                    	     100	   1240192 ns/op	    4309 B/op	      41 allocs/op
BenchmarkBatchChan/items=1k/conc=4/channels-8                    	      82	   1237453 ns/op	    4265 B/op	      41 allocs/op
BenchmarkBatchChan/items=1k/conc=4/channels-8                    	     100	   1439820 ns/op	    4265 B/op	      41 allocs/op
BenchmarkBatchChan/items=1k/conc=4/channels-8                    	      99	   1272293 ns/op	    4276 B/op	      41 allocs/op
BenchmarkBatchChan/items=1k/conc=4/channels-8                    	      94	   1645868 ns/op	    4277 B/op	      41 allocs/op
BenchmarkBatchChan/items=1k/conc=4/pipeline-8                    	     105	    958452 ns/op	    6566 B/op	      70 allocs/op
BenchmarkBatchChan/items=1k/conc=4/pipeline-8                    	     122	    908656 ns/op	    6512 B/op	      70 allocs/op
BenchmarkBatchChan/items=1k/conc=4/pipeline-8                    	     121	    942346 ns/op	    6572 B/op	      70 allocs/op
BenchmarkBatchChan/items=1k/conc=4/pipeline-8                    	     150	   1070035 ns/op	    6508 B/op	      70 allocs/op
BenchmarkBatchChan/items=1k/conc=4/pipeline-8                    	     100	   1122548 ns/op	    6513 B/op	      70 allocs/op
BenchmarkBatchChan/items=16k/conc=4/loop-serial-8                	     601	    179887 ns/op	  257744 B/op	    3064 allocs/op
BenchmarkBatchChan/items=16k/conc=4/loop-serial-8                	     591	    169216 ns/op	  257744 B/op	    3064 allocs/op
BenchmarkBatchChan/items=16k/conc=4/loop-serial-8                	     708	    168235 ns/op	  257746 B/op	    3064 allocs/op
BenchmarkBatchChan/items=16k/conc=4/loop-serial-8                	     627	    170826 ns/op	  257746 B/op	    3064 allocs/op
BenchmarkBatchChan/items=16k/conc=4/loop-serial-8                	     706	    162694 ns/op	  257746 B/op	    3064 allocs/op
BenchmarkBatchChan/items=16k/conc=4/loop-workers-8               	     224	    549218 ns/op	  257655 B/op	    3055 allocs/op
BenchmarkBatchChan/items=16k/conc=4/loop-workers-8               	     217	    499405 ns/op	  257816 B/op	    3057 allocs/op
BenchmarkBatchChan/items=16k/conc=4/loop-workers-8               	     213	    539802 ns/op	  257708 B/op	    3056 allocs/op
BenchmarkBatchChan/items=16k/conc=4/loop-workers-8               	     220	    516293 ns/op	  257725 B/op	    3056 allocs/op
BenchmarkBatchChan/items=16k/conc=4/loop-workers-8               	     198	    544752 ns/op	  257751 B/op	    3056 allocs/op
BenchmarkBatchChan/items=16k/conc=4/channels-8                   	       7	  16426054 ns/op	   58053 B/op	     521 allocs/op
BenchmarkBatchChan/items=16k/conc=4/channels-8                   	       7	  18091595 ns/op	   58190 B/op	     522 allocs/op
BenchmarkBatchChan/items=16k/conc=4/channels-8                   	       6	  18964180 ns/op	   58106 B/op	     521 allocs/op
BenchmarkBatchChan/items=16k/conc=4/channels-8                   	       6	  18754076 ns/op	   58042 B/op	     521 allocs/op
BenchmarkBatchChan/items=16k/conc=4/channels-8                   	       8	  19106474 ns/op	   58074 B/op	     521 allocs/op
BenchmarkBatchChan/items=16k/conc=4/pipeline-8                   	       7	  14571595 ns/op	   60308 B/op	     550 allocs/op
BenchmarkBatchChan/items=16k/conc=4/pipeline-8                   	       8	  13409276 ns/op	   60506 B/op	     552 allocs/op
BenchmarkBatchChan/items=16k/conc=4/pipeline-8                   	      10	  14485542 ns/op	   60638 B/op	     553 allocs/op
BenchmarkBatchChan/items=16k/conc=4/pipeline-8                   	       7	  14366494 ns/op	   60953 B/op	     557 allocs/op
BenchmarkBatchChan/items=16k/conc=4/pipeline-8                   	       7	  45968107 ns/op	   60377 B/op	     551 allocs/op
BenchmarkOverheadWorkSweep/iters=0/loop-serial-8                 	    6885	     16692 ns/op	       0 B/op	       0 allocs/op
BenchmarkOverheadWorkSweep/iters=0/loop-serial-8                 	    7026	     16961 ns/op	       0 B/op	       0 allocs/op
BenchmarkOverheadWorkSweep/iters=0/loop-serial-8                 	    6990	     17027 ns/op	       0 B/op	       0 allocs/op
BenchmarkOverheadWorkSweep/iters=0/loop-serial-8                 	    6235	     16840 ns/op	       0 B/op	       0 allocs/op
BenchmarkOverheadWorkSweep/iters=0/loop-serial-8                 	    6943	     17044 ns/op	       0 B/op	       0 allocs/op
BenchmarkOverheadWorkSweep/iters=0/channels-8                    	      38	   3363478 ns/op	     477 B/op	       7 allocs/op
BenchmarkOverheadWorkSweep/iters=0/channels-8                    	      38	   3479130 ns/op	     474 B/op	       7 allocs/op
BenchmarkOverheadWorkSweep/iters=0/channels-8                    	      38	   3370661 ns/op	     479 B/op	       7 allocs/op
BenchmarkOverheadWorkSweep/iters=0/channels-8                    	      34	   3321430 ns/op	     474 B/op	       7 allocs/op
BenchmarkOverheadWorkSweep/iters=0/channels-8                    	      31	   3326425 ns/op	     475 B/op	       7 allocs/op
BenchmarkOverheadWorkSweep/iters=0/pipeline-8                    	      22	   4805551 ns/op	    2643 B/op	      51 allocs/op
BenchmarkOverheadWorkSweep/iters=0/pipeline-8                    	      22	   4620790 ns/op	    2613 B/op	      51 allocs/op
BenchmarkOverheadWorkSweep/iters=0/pipeline-8                    	      25	   4554212 ns/op	    2634 B/op	      51 allocs/op
BenchmarkOverheadWorkSweep/iters=0/pipeline-8                    	      24	   4563019 ns/op	    2600 B/op	      51 allocs/op
BenchmarkOverheadWorkSweep/iters=0/pipeline-8                    	      24	   4484021 ns/op	    2620 B/op	      51 allocs/op
BenchmarkOverheadWorkSweep/iters=4/loop-serial-8                 	    4435	     25598 ns/op	       0 B/op	       0 allocs/op
BenchmarkOverheadWorkSweep/iters=4/loop-serial-8                 	    4345	     25908 ns/op	       0 B/op	       0 allocs/op
BenchmarkOverheadWorkSweep/iters=4/loop-serial-8                 	    4635	     25740 ns/op	       0 B/op	       0 allocs/op
BenchmarkOverheadWorkSweep/iters=4/loop-serial-8                 	    4604	     25763 ns/op	       0 B/op	       0 allocs/op
BenchmarkOverheadWorkSweep/iters=4/loop-serial-8                 	    4605	     25867 ns/op	       0 B/op	       0 allocs/op
BenchmarkOverheadWorkSweep/iters=4/channels-8                    	      32	   3399124 ns/op	     472 B/op	       7 allocs/op
BenchmarkOverheadWorkSweep/iters=4/channels-8                    	      34	   3496104 ns/op	     472 B/op	       7 allocs/op
BenchmarkOverheadWorkSweep/iters=4/channels-8                    	      33	   3466538 ns/op	     472 B/op	       7 allocs/op
BenchmarkOverheadWorkSweep/iters=4/channels-8                    	      36	   3334439 ns/op	     472 B/op	       7 allocs/op
BenchmarkOverheadWorkSweep/iters=4/channels-8                    	      36	   3454383 ns/op	     472 B/op	       7 allocs/op
BenchmarkOverheadWorkSweep/iters=4/pipeline-8                    	      14	  20081107 ns/op	    2716 B/op	      52 allocs/op
BenchmarkOverheadWorkSweep/iters=4/pipeline-8                    	      24	   4632606 ns/op	    2620 B/op	      51 allocs/op
BenchmarkOverheadWorkSweep/iters=4/pipeline-8                    	      22	   4682061 ns/op	    2600 B/op	      51 allocs/op
BenchmarkOverheadWorkSweep/iters=4/pipeline-8                    	      24	   4548957 ns/op	    2628 B/op	      51 allocs/op
BenchmarkOverheadWorkSweep/iters=4/pipeline-8                    	      25	   4658905 ns/op	    2623 B/op	      51 allocs/op
BenchmarkOverheadWorkSweep/iters=16/loop-serial-8                	    1429	     81158 ns/op	       0 B/op	       0 allocs/op
BenchmarkOverheadWorkSweep/iters=16/loop-serial-8                	    1249	    100824 ns/op	       0 B/op	       0 allocs/op
BenchmarkOverheadWorkSweep/iters=16/loop-serial-8                	    1428	     83418 ns/op	       0 B/op	       0 allocs/op
BenchmarkOverheadWorkSweep/iters=16/loop-serial-8                	    1232	     81495 ns/op	       0 B/op	       0 allocs/op
BenchmarkOverheadWorkSweep/iters=16/loop-serial-8                	    1456	     80945 ns/op	       0 B/op	       0 allocs/op
BenchmarkOverheadWorkSweep/iters=16/channels-8                   	      30	   3515781 ns/op	     472 B/op	       7 allocs/op
BenchmarkOverheadWorkSweep/iters=16/channels-8                   	      33	   3348477 ns/op	     472 B/op	       7 allocs/op
BenchmarkOverheadWorkSweep/iters=16/channels-8                   	      31	   3398243 ns/op	     472 B/op	       7 allocs/op
BenchmarkOverheadWorkSweep/iters=16/channels-8                   	      36	   2910499 ns/op	     472 B/op	       7 allocs/op
BenchmarkOverheadWorkSweep/iters=16/channels-8                   	      40	   3012207 ns/op	     472 B/op	       7 allocs/op
BenchmarkOverheadWorkSweep/iters=16/pipeline-8                   	      34	   5329069 ns/op	    2600 B/op	      51 allocs/op
BenchmarkOverheadWorkSweep/iters=16/pipeline-8                   	      24	   5013043 ns/op	    2600 B/op	      51 allocs/op
BenchmarkOverheadWorkSweep/iters=16/pipeline-8                   	      24	   4820649 ns/op	    2616 B/op	      51 allocs/op
BenchmarkOverheadWorkSweep/iters=16/pipeline-8                   	      21	   4792135 ns/op	    2600 B/op	      51 allocs/op
BenchmarkOverheadWorkSweep/iters=16/pipeline-8                   	      25	   5749125 ns/op	    2619 B/op	      51 allocs/op
BenchmarkOverheadWorkSweep/iters=64/loop-serial-8                	     121	   1069678 ns/op	       0 B/op	       0 allocs/op
BenchmarkOverheadWorkSweep/iters=64/loop-serial-8                	     234	    489764 ns/op	       0 B/op	       0 allocs/op
BenchmarkOverheadWorkSweep/iters=64/loop-serial-8                	     235	    490387 ns/op	       0 B/op	       0 allocs/op
BenchmarkOverheadWorkSweep/iters=64/loop-serial-8                	     244	    488797 ns/op	       0 B/op	       0 allocs/op
BenchmarkOverheadWorkSweep/iters=64/loop-serial-8                	     236	    488582 ns/op	       0 B/op	       0 allocs/op
BenchmarkOverheadWorkSweep/iters=64/channels-8                   	      28	   4128013 ns/op	     472 B/op	       7 allocs/op
BenchmarkOverheadWorkSweep/iters=64/channels-8                   	      24	   4294910 ns/op	     472 B/op	       7 allocs/op
BenchmarkOverheadWorkSweep/iters=64/channels-8                   	      25	   4201420 ns/op	     472 B/op	       7 allocs/op
BenchmarkOverheadWorkSweep/iters=64/channels-8                   	      27	   4148900 ns/op	     472 B/op	       7 allocs/op
BenchmarkOverheadWorkSweep/iters=64/channels-8                   	      26	   4259766 ns/op	     479 B/op	       7 allocs/op
BenchmarkOverheadWorkSweep/iters=64/pipeline-8                   	      21	   5231798 ns/op	    2613 B/op	      51 allocs/op
BenchmarkOverheadWorkSweep/iters=64/pipeline-8                   	      21	   5662540 ns/op	    2600 B/op	      51 allocs/op
BenchmarkOverheadWorkSweep/iters=64/pipeline-8                   	      20	   5963465 ns/op	    2628 B/op	      51 allocs/op
BenchmarkOverheadWorkSweep/iters=64/pipeline-8                   	      18	   5772618 ns/op	    2600 B/op	      51 allocs/op
BenchmarkOverheadWorkSweep/iters=64/pipeline-8                   	      28	   5367528 ns/op	    2600 B/op	      51 allocs/op
BenchmarkOverheadWorkSweep/iters=256/loop-serial-8               	      45	   2460965 ns/op	       0 B/op	       0 allocs/op
BenchmarkOverheadWorkSweep/iters=256/loop-serial-8               	      48	   2495716 ns/op	       0 B/op	       0 allocs/op
BenchmarkOverheadWorkSweep/iters=256/loop-serial-8               	      42	   2499643 ns/op	       0 B/op	       0 allocs/op
BenchmarkOverheadWorkSweep/iters=256/loop-serial-8               	      44	   2430277 ns/op	       0 B/op	       0 allocs/op
BenchmarkOverheadWorkSweep/iters=256/loop-serial-8               	      48	   2533488 ns/op	       0 B/op	       0 allocs/op
BenchmarkOverheadWorkSweep/iters=256/channels-8                  	      16	   7558956 ns/op	     472 B/op	       7 allocs/op
BenchmarkOverheadWorkSweep/iters=256/channels-8                  	      18	   6705699 ns/op	     472 B/op	       7 allocs/op
BenchmarkOverheadWorkSweep/iters=256/channels-8                  	      18	   6800144 ns/op	     472 B/op	       7 allocs/op
BenchmarkOverheadWorkSweep/iters=256/channels-8                  	      16	   6560398 ns/op	     472 B/op	       7 allocs/op
BenchmarkOverheadWorkSweep/iters=256/channels-8                  	      16	   6776802 ns/op	     472 B/op	       7 allocs/op
BenchmarkOverheadWorkSweep/iters=256/pipeline-8                  	      13	   7812022 ns/op	    2600 B/op	      51 allocs/op
BenchmarkOverheadWorkSweep/iters=256/pipeline-8                  	      13	   7794660 ns/op	    2629 B/op	      51 allocs/op
BenchmarkOverheadWorkSweep/iters=256/pipeline-8                  	      14	   7927938 ns/op	    2771 B/op	      52 allocs/op
BenchmarkOverheadWorkSweep/iters=256/pipeline-8                  	      12	   8442903 ns/op	    2696 B/op	      52 allocs/op
BenchmarkOverheadWorkSweep/iters=256/pipeline-8                  	      14	   7990164 ns/op	    2675 B/op	      51 allocs/op
BenchmarkOverheadWorkSweep/iters=1024/loop-serial-8              	      10	  10046642 ns/op	       0 B/op	       0 allocs/op
BenchmarkOverheadWorkSweep/iters=1024/loop-serial-8              	      10	  10037042 ns/op	       0 B/op	       0 allocs/op
BenchmarkOverheadWorkSweep/iters=1024/loop-serial-8              	      10	  10043767 ns/op	       0 B/op	       0 allocs/op
BenchmarkOverheadWorkSweep/iters=1024/loop-serial-8              	      10	  10007362 ns/op	       0 B/op	       0 allocs/op
BenchmarkOverheadWorkSweep/iters=1024/loop-serial-8              	      12	   9973649 ns/op	       0 B/op	       0 allocs/op
BenchmarkOverheadWorkSweep/iters=1024/channels-8                 	       6	  17432736 ns/op	     472 B/op	       7 allocs/op
BenchmarkOverheadWorkSweep/iters=1024/channels-8                 	       7	  16379381 ns/op	     472 B/op	       7 allocs/op
BenchmarkOverheadWorkSweep/iters=1024/channels-8                 	       4	  55370854 ns/op	     472 B/op	       7 allocs/op
BenchmarkOverheadWorkSweep/iters=1024/channels-8                 	       6	  17693625 ns/op	     520 B/op	       7 allocs/op
BenchmarkOverheadWorkSweep/iters=1024/channels-8                 	       6	  16979826 ns/op	     472 B/op	       7 allocs/op
BenchmarkOverheadWorkSweep/iters=1024/pipeline-8                 	       6	  17737938 ns/op	    2792 B/op	      53 allocs/op
BenchmarkOverheadWorkSweep/iters=1024/pipeline-8                 	       6	  19770250 ns/op	    2744 B/op	      52 allocs/op
BenchmarkOverheadWorkSweep/iters=1024/pipeline-8                 	       6	  17335764 ns/op	    2744 B/op	      52 allocs/op
BenchmarkOverheadWorkSweep/iters=1024/pipeline-8                 	       6	  17959132 ns/op	    2600 B/op	      51 allocs/op
BenchmarkOverheadWorkSweep/iters=1024/pipeline-8                 	       6	  17536236 ns/op	    2696 B/op	      52 allocs/op
BenchmarkOverheadWorkSweep/iters=4096/loop-serial-8              	       2	 173599271 ns/op	       0 B/op	       0 allocs/op
BenchmarkOverheadWorkSweep/iters=4096/loop-serial-8              	       3	  40922931 ns/op	       0 B/op	       0 allocs/op
BenchmarkOverheadWorkSweep/iters=4096/loop-serial-8              	       3	  41978611 ns/op	       0 B/op	       0 allocs/op
BenchmarkOverheadWorkSweep/iters=4096/loop-serial-8              	       3	  40711889 ns/op	       0 B/op	       0 allocs/op
BenchmarkOverheadWorkSweep/iters=4096/loop-serial-8              	       3	  40665139 ns/op	       0 B/op	       0 allocs/op
BenchmarkOverheadWorkSweep/iters=4096/channels-8                 	       2	  52637980 ns/op	     472 B/op	       7 allocs/op
BenchmarkOverheadWorkSweep/iters=4096/channels-8                 	       2	  51296334 ns/op	     520 B/op	       7 allocs/op
BenchmarkOverheadWorkSweep/iters=4096/channels-8                 	       2	  58858833 ns/op	     664 B/op	       9 allocs/op
BenchmarkOverheadWorkSweep/iters=4096/channels-8                 	       2	  53522792 ns/op	     520 B/op	       7 allocs/op
BenchmarkOverheadWorkSweep/iters=4096/channels-8                 	       2	  55150646 ns/op	     568 B/op	       8 allocs/op
BenchmarkOverheadWorkSweep/iters=4096/pipeline-8                 	       2	  58231271 ns/op	    2888 B/op	      54 allocs/op
BenchmarkOverheadWorkSweep/iters=4096/pipeline-8                 	       2	  58130042 ns/op	    2984 B/op	      55 allocs/op
BenchmarkOverheadWorkSweep/iters=4096/pipeline-8                 	       2	  54417792 ns/op	    2984 B/op	      55 allocs/op
BenchmarkOverheadWorkSweep/iters=4096/pipeline-8                 	       2	  52048312 ns/op	    2936 B/op	      54 allocs/op
BenchmarkOverheadWorkSweep/iters=4096/pipeline-8                 	       2	  52602833 ns/op	    2888 B/op	      54 allocs/op
BenchmarkOverheadStepScaling/steps=1/loop-serial-8               	    6972	     16565 ns/op	       0 B/op	       0 allocs/op
BenchmarkOverheadStepScaling/steps=1/loop-serial-8               	    7202	     16338 ns/op	       0 B/op	       0 allocs/op
BenchmarkOverheadStepScaling/steps=1/loop-serial-8               	    7158	     16355 ns/op	       0 B/op	       0 allocs/op
BenchmarkOverheadStepScaling/steps=1/loop-serial-8               	    7228	     16284 ns/op	       0 B/op	       0 allocs/op
BenchmarkOverheadStepScaling/steps=1/loop-serial-8               	    7225	     16360 ns/op	       0 B/op	       0 allocs/op
BenchmarkOverheadStepScaling/steps=1/channels-8                  	      24	   4346144 ns/op	     428 B/op	       8 allocs/op
BenchmarkOverheadStepScaling/steps=1/channels-8                  	      25	   4222663 ns/op	     474 B/op	       8 allocs/op
BenchmarkOverheadStepScaling/steps=1/channels-8                  	      21	   4928171 ns/op	     548 B/op	       9 allocs/op
BenchmarkOverheadStepScaling/steps=1/channels-8                  	      25	   4565938 ns/op	     428 B/op	       8 allocs/op
BenchmarkOverheadStepScaling/steps=1/channels-8                  	      21	   5018534 ns/op	     475 B/op	       8 allocs/op
BenchmarkOverheadStepScaling/steps=1/pipeline-8                  	      21	   5212409 ns/op	    2632 B/op	      50 allocs/op
BenchmarkOverheadStepScaling/steps=1/pipeline-8                  	      19	   5524507 ns/op	    2595 B/op	      50 allocs/op
BenchmarkOverheadStepScaling/steps=1/pipeline-8                  	      25	   4392557 ns/op	    2554 B/op	      50 allocs/op
BenchmarkOverheadStepScaling/steps=1/pipeline-8                  	      24	   5046979 ns/op	    2554 B/op	      50 allocs/op
BenchmarkOverheadStepScaling/steps=1/pipeline-8                  	      22	   4677403 ns/op	    2555 B/op	      50 allocs/op
BenchmarkOverheadStepScaling/steps=2/loop-serial-8               	    3812	     30495 ns/op	       0 B/op	       0 allocs/op
BenchmarkOverheadStepScaling/steps=2/loop-serial-8               	    3891	     30553 ns/op	       0 B/op	       0 allocs/op
BenchmarkOverheadStepScaling/steps=2/loop-serial-8               	    3874	     30561 ns/op	       0 B/op	       0 allocs/op
BenchmarkOverheadStepScaling/steps=2/loop-serial-8               	    3742	     30787 ns/op	       0 B/op	       0 allocs/op
BenchmarkOverheadStepScaling/steps=2/loop-serial-8               	    3765	     32789 ns/op	       0 B/op	       0 allocs/op
BenchmarkOverheadStepScaling/steps=2/channels-8                  	      16	   7603247 ns/op	     679 B/op	      14 allocs/op
BenchmarkOverheadStepScaling/steps=2/channels-8                  	      15	   7170186 ns/op	     679 B/op	      14 allocs/op
BenchmarkOverheadStepScaling/steps=2/channels-8                  	      15	   7199139 ns/op	     679 B/op	      14 allocs/op
BenchmarkOverheadStepScaling/steps=2/channels-8                  	      15	   7126786 ns/op	     679 B/op	      14 allocs/op
BenchmarkOverheadStepScaling/steps=2/channels-8                  	      16	   6914893 ns/op	     727 B/op	      14 allocs/op
BenchmarkOverheadStepScaling/steps=2/pipeline-8                  	      15	   7465661 ns/op	    3210 B/op	      62 allocs/op
BenchmarkOverheadStepScaling/steps=2/pipeline-8                  	      22	   7244593 ns/op	    3210 B/op	      62 allocs/op
BenchmarkOverheadStepScaling/steps=2/pipeline-8                  	      14	   7368548 ns/op	    3211 B/op	      62 allocs/op
BenchmarkOverheadStepScaling/steps=2/pipeline-8                  	      14	  23170193 ns/op	    3211 B/op	      62 allocs/op
BenchmarkOverheadStepScaling/steps=2/pipeline-8                  	      14	  26797905 ns/op	    3212 B/op	      62 allocs/op
BenchmarkOverheadStepScaling/steps=4/loop-serial-8               	    1796	     61511 ns/op	       0 B/op	       0 allocs/op
BenchmarkOverheadStepScaling/steps=4/loop-serial-8               	    1939	     61076 ns/op	       0 B/op	       0 allocs/op
BenchmarkOverheadStepScaling/steps=4/loop-serial-8               	    1998	     62480 ns/op	       0 B/op	       0 allocs/op
BenchmarkOverheadStepScaling/steps=4/loop-serial-8               	    1957	     60959 ns/op	       0 B/op	       0 allocs/op
BenchmarkOverheadStepScaling/steps=4/loop-serial-8               	    1954	     60944 ns/op	       0 B/op	       0 allocs/op
BenchmarkOverheadStepScaling/steps=4/channels-8                  	      14	  12607905 ns/op	    1224 B/op	      26 allocs/op
BenchmarkOverheadStepScaling/steps=4/channels-8                  	       8	  13572906 ns/op	    1182 B/op	      26 allocs/op
BenchmarkOverheadStepScaling/steps=4/channels-8                  	       8	  13666271 ns/op	    1182 B/op	      26 allocs/op
BenchmarkOverheadStepScaling/steps=4/channels-8                  	      13	  14066378 ns/op	    1228 B/op	      26 allocs/op
BenchmarkOverheadStepScaling/steps=4/channels-8                  	      13	  13286516 ns/op	    1346 B/op	      27 allocs/op
BenchmarkOverheadStepScaling/steps=4/pipeline-8                  	       7	  15508750 ns/op	    4861 B/op	      90 allocs/op
BenchmarkOverheadStepScaling/steps=4/pipeline-8                  	       6	  17226785 ns/op	    4656 B/op	      88 allocs/op
BenchmarkOverheadStepScaling/steps=4/pipeline-8                  	       8	  17334719 ns/op	    4738 B/op	      88 allocs/op
BenchmarkOverheadStepScaling/steps=4/pipeline-8                  	       6	  17710931 ns/op	    4912 B/op	      90 allocs/op
BenchmarkOverheadStepScaling/steps=4/pipeline-8                  	       7	  17876774 ns/op	    5202 B/op	      93 allocs/op
BenchmarkOverheadStepScaling/steps=8/loop-serial-8               	     777	    140968 ns/op	       0 B/op	       0 allocs/op
BenchmarkOverheadStepScaling/steps=8/loop-serial-8               	     888	    119625 ns/op	       0 B/op	       0 allocs/op
BenchmarkOverheadStepScaling/steps=8/loop-serial-8               	     900	    122816 ns/op	       0 B/op	       0 allocs/op
BenchmarkOverheadStepScaling/steps=8/loop-serial-8               	     868	    121176 ns/op	       0 B/op	       0 allocs/op
BenchmarkOverheadStepScaling/steps=8/loop-serial-8               	     871	    122403 ns/op	       0 B/op	       0 allocs/op
BenchmarkOverheadStepScaling/steps=8/channels-8                  	       4	  28696823 ns/op	    2260 B/op	      51 allocs/op
BenchmarkOverheadStepScaling/steps=8/channels-8                  	       4	  29223208 ns/op	    2692 B/op	      55 allocs/op
BenchmarkOverheadStepScaling/steps=8/channels-8                  	       9	  21665898 ns/op	    2716 B/op	      55 allocs/op
BenchmarkOverheadStepScaling/steps=8/channels-8                  	       5	  24167717 ns/op	    2182 B/op	      50 allocs/op
BenchmarkOverheadStepScaling/steps=8/channels-8                  	       4	  26062073 ns/op	    2308 B/op	      51 allocs/op
BenchmarkOverheadStepScaling/steps=8/pipeline-8                  	       4	  27898542 ns/op	    8016 B/op	     143 allocs/op
BenchmarkOverheadStepScaling/steps=8/pipeline-8                  	       4	  30257354 ns/op	    7992 B/op	     142 allocs/op
BenchmarkOverheadStepScaling/steps=8/pipeline-8                  	       4	  30112333 ns/op	    8520 B/op	     148 allocs/op
BenchmarkOverheadStepScaling/steps=8/pipeline-8                  	       4	  34962250 ns/op	    7992 B/op	     142 allocs/op
BenchmarkOverheadStepScaling/steps=8/pipeline-8                  	       3	  33878305 ns/op	    8080 B/op	     143 allocs/op
BenchmarkOverheadStepScaling/steps=16/loop-serial-8              	     403	    277548 ns/op	       0 B/op	       0 allocs/op
BenchmarkOverheadStepScaling/steps=16/loop-serial-8              	     374	    271857 ns/op	       0 B/op	       0 allocs/op
BenchmarkOverheadStepScaling/steps=16/loop-serial-8              	     372	    285002 ns/op	       0 B/op	       0 allocs/op
BenchmarkOverheadStepScaling/steps=16/loop-serial-8              	     403	    276813 ns/op	       0 B/op	       0 allocs/op
BenchmarkOverheadStepScaling/steps=16/loop-serial-8              	     428	    268227 ns/op	       0 B/op	       0 allocs/op
BenchmarkOverheadStepScaling/steps=16/channels-8                 	       2	  58600104 ns/op	    5592 B/op	     113 allocs/op
BenchmarkOverheadStepScaling/steps=16/channels-8                 	       2	  67262438 ns/op	    6504 B/op	     122 allocs/op
BenchmarkOverheadStepScaling/steps=16/channels-8                 	       2	  62623375 ns/op	    6552 B/op	     123 allocs/op
BenchmarkOverheadStepScaling/steps=16/channels-8                 	       2	  60581458 ns/op	    5448 B/op	     111 allocs/op
BenchmarkOverheadStepScaling/steps=16/channels-8                 	       2	  60059271 ns/op	    5304 B/op	     110 allocs/op
BenchmarkOverheadStepScaling/steps=16/pipeline-8                 	       2	 230213625 ns/op	   14208 B/op	     245 allocs/op
BenchmarkOverheadStepScaling/steps=16/pipeline-8                 	       2	  87541104 ns/op	   13536 B/op	     238 allocs/op
BenchmarkOverheadStepScaling/steps=16/pipeline-8                 	       2	  84221188 ns/op	   15648 B/op	     260 allocs/op
BenchmarkOverheadStepScaling/steps=16/pipeline-8                 	       2	  84219354 ns/op	   14160 B/op	     245 allocs/op
BenchmarkOverheadStepScaling/steps=16/pipeline-8                 	       2	 178930375 ns/op	   13824 B/op	     241 allocs/op
BenchmarkOverheadStepScaling/steps=32/loop-serial-8              	     240	    506134 ns/op	       0 B/op	       0 allocs/op
BenchmarkOverheadStepScaling/steps=32/loop-serial-8              	     224	    502998 ns/op	       0 B/op	       0 allocs/op
BenchmarkOverheadStepScaling/steps=32/loop-serial-8              	     224	    499892 ns/op	       0 B/op	       0 allocs/op
BenchmarkOverheadStepScaling/steps=32/loop-serial-8              	     228	    519694 ns/op	       0 B/op	       0 allocs/op
BenchmarkOverheadStepScaling/steps=32/loop-serial-8              	     224	    498188 ns/op	       0 B/op	       0 allocs/op
BenchmarkOverheadStepScaling/steps=32/channels-8                 	       1	 177507000 ns/op	   12640 B/op	     241 allocs/op
BenchmarkOverheadStepScaling/steps=32/channels-8                 	       1	 173766833 ns/op	   15456 B/op	     263 allocs/op
BenchmarkOverheadStepScaling/steps=32/channels-8                 	       1	 169992791 ns/op	   12832 B/op	     243 allocs/op
BenchmarkOverheadStepScaling/steps=32/channels-8                 	       1	 152584250 ns/op	   14272 B/op	     258 allocs/op
BenchmarkOverheadStepScaling/steps=32/channels-8                 	       1	 154926792 ns/op	   12448 B/op	     239 allocs/op
BenchmarkOverheadStepScaling/steps=32/pipeline-8                 	       1	 176044834 ns/op	   28896 B/op	     472 allocs/op
BenchmarkOverheadStepScaling/steps=32/pipeline-8                 	       1	 149873000 ns/op	   30432 B/op	     488 allocs/op
BenchmarkOverheadStepScaling/steps=32/pipeline-8                 	       1	 178418792 ns/op	   53664 B/op	     565 allocs/op
BenchmarkOverheadStepScaling/steps=32/pipeline-8                 	       1	 201332875 ns/op	   26976 B/op	     452 allocs/op
BenchmarkOverheadStepScaling/steps=32/pipeline-8                 	       1	 238066292 ns/op	   27648 B/op	     459 allocs/op
BenchmarkOverheadStepScaling/steps=64/loop-serial-8              	      82	   1334957 ns/op	       0 B/op	       0 allocs/op
BenchmarkOverheadStepScaling/steps=64/loop-serial-8              	     112	    982229 ns/op	       0 B/op	       0 allocs/op
BenchmarkOverheadStepScaling/steps=64/loop-serial-8              	     115	    990530 ns/op	       0 B/op	       0 allocs/op
BenchmarkOverheadStepScaling/steps=64/loop-serial-8              	     100	   1019291 ns/op	       0 B/op	       0 allocs/op
BenchmarkOverheadStepScaling/steps=64/loop-serial-8              	     100	   1017026 ns/op	       0 B/op	       0 allocs/op
BenchmarkOverheadStepScaling/steps=64/channels-8                 	       1	 329594292 ns/op	   49024 B/op	     601 allocs/op
BenchmarkOverheadStepScaling/steps=64/channels-8                 	       1	 313303375 ns/op	   39040 B/op	     552 allocs/op
BenchmarkOverheadStepScaling/steps=64/channels-8                 	       1	 344630375 ns/op	   26912 B/op	     499 allocs/op
BenchmarkOverheadStepScaling/steps=64/channels-8                 	       1	 316674541 ns/op	   27488 B/op	     505 allocs/op
BenchmarkOverheadStepScaling/steps=64/channels-8                 	       1	 304444084 ns/op	   25664 B/op	     486 allocs/op
BenchmarkOverheadStepScaling/steps=64/pipeline-8                 	       1	 346428333 ns/op	   63040 B/op	     970 allocs/op
BenchmarkOverheadStepScaling/steps=64/pipeline-8                 	       1	 342134667 ns/op	   60928 B/op	     948 allocs/op
BenchmarkOverheadStepScaling/steps=64/pipeline-8                 	       1	 326262875 ns/op	   51712 B/op	     852 allocs/op
BenchmarkOverheadStepScaling/steps=64/pipeline-8                 	       1	 384570084 ns/op	   52192 B/op	     857 allocs/op
BenchmarkOverheadStepScaling/steps=64/pipeline-8                 	       1	 373616833 ns/op	   56704 B/op	     904 allocs/op
BenchmarkOverheadCompositeOneToMany/stages=1/loop-serial-8       	    7491	     26819 ns/op	   32768 B/op	       1 allocs/op
BenchmarkOverheadCompositeOneToMany/stages=1/loop-serial-8       	    6428	     22096 ns/op	   32768 B/op	       1 allocs/op
BenchmarkOverheadCompositeOneToMany/stages=1/loop-serial-8       	    4794	     21917 ns/op	   32768 B/op	       1 allocs/op
BenchmarkOverheadCompositeOneToMany/stages=1/loop-serial-8       	    5439	     20748 ns/op	   32768 B/op	       1 allocs/op
BenchmarkOverheadCompositeOneToMany/stages=1/loop-serial-8       	    4550	     22733 ns/op	   32768 B/op	       1 allocs/op
BenchmarkOverheadCompositeOneToMany/stages=1/channels-8          	      32	   3639810 ns/op	   33195 B/op	    2056 allocs/op
BenchmarkOverheadCompositeOneToMany/stages=1/channels-8          	      34	   3457368 ns/op	   33195 B/op	    2056 allocs/op
BenchmarkOverheadCompositeOneToMany/stages=1/channels-8          	      32	   3162958 ns/op	   33195 B/op	    2056 allocs/op
BenchmarkOverheadCompositeOneToMany/stages=1/channels-8          	      40	   9122202 ns/op	   33194 B/op	    2056 allocs/op
BenchmarkOverheadCompositeOneToMany/stages=1/channels-8          	      32	   3338046 ns/op	   33195 B/op	    2056 allocs/op
BenchmarkOverheadCompositeOneToMany/stages=1/pipeline-8          	      32	   3730016 ns/op	   35322 B/op	    2098 allocs/op
BenchmarkOverheadCompositeOneToMany/stages=1/pipeline-8          	      39	   3458546 ns/op	   35329 B/op	    2098 allocs/op
BenchmarkOverheadCompositeOneToMany/stages=1/pipeline-8          	      33	   3443307 ns/op	   35328 B/op	    2098 allocs/op
BenchmarkOverheadCompositeOneToMany/stages=1/pipeline-8          	      34	   3662870 ns/op	   35321 B/op	    2098 allocs/op
BenchmarkOverheadCompositeOneToMany/stages=1/pipeline-8          	      34	   3628848 ns/op	   35322 B/op	    2098 allocs/op
BenchmarkOverheadCompositeOneToMany/stages=2/loop-serial-8       	    2632	    147552 ns/op	   49152 B/op	       2 allocs/op
BenchmarkOverheadCompositeOneToMany/stages=2/loop-serial-8       	    3739	     31400 ns/op	   49152 B/op	       2 allocs/op
BenchmarkOverheadCompositeOneToMany/stages=2/loop-serial-8       	    3140	     33387 ns/op	   49152 B/op	       2 allocs/op
BenchmarkOverheadCompositeOneToMany/stages=2/loop-serial-8       	    3678	     31233 ns/op	   49152 B/op	       2 allocs/op
BenchmarkOverheadCompositeOneToMany/stages=2/loop-serial-8       	    3717	     29811 ns/op	   49152 B/op	       2 allocs/op
BenchmarkOverheadCompositeOneToMany/stages=2/channels-8          	      37	   3868580 ns/op	   49827 B/op	    3086 allocs/op
BenchmarkOverheadCompositeOneToMany/stages=2/channels-8          	      26	   4487926 ns/op	   49828 B/op	    3086 allocs/op
BenchmarkOverheadCompositeOneToMany/stages=2/channels-8          	      19	   5268827 ns/op	   49834 B/op	    3086 allocs/op
BenchmarkOverheadCompositeOneToMany/stages=2/channels-8          	      42	   5007615 ns/op	   49826 B/op	    3086 allocs/op
BenchmarkOverheadCompositeOneToMany/stages=2/channels-8          	      26	   4274229 ns/op	   49828 B/op	    3086 allocs/op
BenchmarkOverheadCompositeOneToMany/stages=2/pipeline-8          	      20	   5047935 ns/op	   52363 B/op	    3134 allocs/op
BenchmarkOverheadCompositeOneToMany/stages=2/pipeline-8          	      22	   4910155 ns/op	   52380 B/op	    3134 allocs/op
BenchmarkOverheadCompositeOneToMany/stages=2/pipeline-8          	      28	   5002688 ns/op	   52389 B/op	    3134 allocs/op
BenchmarkOverheadCompositeOneToMany/stages=2/pipeline-8          	      22	   4920402 ns/op	   52361 B/op	    3134 allocs/op
BenchmarkOverheadCompositeOneToMany/stages=2/pipeline-8          	      26	   4890601 ns/op	   52361 B/op	    3134 allocs/op
BenchmarkOverheadCompositeOneToMany/stages=4/loop-serial-8       	    2481	     41100 ns/op	   61440 B/op	       4 allocs/op
BenchmarkOverheadCompositeOneToMany/stages=4/loop-serial-8       	    3151	     37766 ns/op	   61440 B/op	       4 allocs/op
BenchmarkOverheadCompositeOneToMany/stages=4/loop-serial-8       	    2727	     37018 ns/op	   61440 B/op	       4 allocs/op
BenchmarkOverheadCompositeOneToMany/stages=4/loop-serial-8       	    3072	     43015 ns/op	   61440 B/op	       4 allocs/op
BenchmarkOverheadCompositeOneToMany/stages=4/loop-serial-8       	    2859	     37952 ns/op	   61440 B/op	       4 allocs/op
BenchmarkOverheadCompositeOneToMany/stages=4/channels-8          	      24	   6176059 ns/op	   62692 B/op	    3866 allocs/op
BenchmarkOverheadCompositeOneToMany/stages=4/channels-8          	      19	   5754993 ns/op	   62649 B/op	    3866 allocs/op
BenchmarkOverheadCompositeOneToMany/stages=4/channels-8          	      20	   6582300 ns/op	   62613 B/op	    3866 allocs/op
BenchmarkOverheadCompositeOneToMany/stages=4/channels-8          	      19	   5747855 ns/op	   62613 B/op	    3866 allocs/op
BenchmarkOverheadCompositeOneToMany/stages=4/channels-8          	      19	   5917197 ns/op	   62639 B/op	    3866 allocs/op
BenchmarkOverheadCompositeOneToMany/stages=4/pipeline-8          	      22	   6490597 ns/op	   66173 B/op	    3928 allocs/op
BenchmarkOverheadCompositeOneToMany/stages=4/pipeline-8          	      19	   6165191 ns/op	   66187 B/op	    3929 allocs/op
BenchmarkOverheadCompositeOneToMany/stages=4/pipeline-8          	      21	   5972587 ns/op	   66172 B/op	    3928 allocs/op
BenchmarkOverheadCompositeOneToMany/stages=4/pipeline-8          	      18	   6539465 ns/op	   66090 B/op	    3928 allocs/op
BenchmarkOverheadCompositeOneToMany/stages=4/pipeline-8          	      22	   6219402 ns/op	   66090 B/op	    3928 allocs/op
BenchmarkOverheadCompositeOneToMany/stages=8/loop-serial-8       	    2024	     53860 ns/op	   65280 B/op	       8 allocs/op
BenchmarkOverheadCompositeOneToMany/stages=8/loop-serial-8       	    2929	     39210 ns/op	   65280 B/op	       8 allocs/op
BenchmarkOverheadCompositeOneToMany/stages=8/loop-serial-8       	    1939	    189799 ns/op	   65280 B/op	       8 allocs/op
BenchmarkOverheadCompositeOneToMany/stages=8/loop-serial-8       	    2972	     39129 ns/op	   65280 B/op	       8 allocs/op
BenchmarkOverheadCompositeOneToMany/stages=8/loop-serial-8       	    2905	     39143 ns/op	   65280 B/op	       8 allocs/op
BenchmarkOverheadCompositeOneToMany/stages=8/channels-8          	      19	   6056886 ns/op	   67476 B/op	    4130 allocs/op
BenchmarkOverheadCompositeOneToMany/stages=8/channels-8          	      19	   5675171 ns/op	   67546 B/op	    4131 allocs/op
BenchmarkOverheadCompositeOneToMany/stages=8/channels-8          	      20	   5522831 ns/op	   67445 B/op	    4130 allocs/op
BenchmarkOverheadCompositeOneToMany/stages=8/channels-8          	      26	   6280462 ns/op	   67451 B/op	    4130 allocs/op
BenchmarkOverheadCompositeOneToMany/stages=8/channels-8          	      21	   6162851 ns/op	   67550 B/op	    4131 allocs/op
BenchmarkOverheadCompositeOneToMany/stages=8/pipeline-8          	      16	   6788258 ns/op	   72824 B/op	    4218 allocs/op
BenchmarkOverheadCompositeOneToMany/stages=8/pipeline-8          	      18	   6683118 ns/op	   72811 B/op	    4218 allocs/op
BenchmarkOverheadCompositeOneToMany/stages=8/pipeline-8          	      19	   6745436 ns/op	   72845 B/op	    4218 allocs/op
BenchmarkOverheadCompositeOneToMany/stages=8/pipeline-8          	      24	   7857464 ns/op	   72838 B/op	    4218 allocs/op
BenchmarkOverheadCompositeOneToMany/stages=8/pipeline-8          	      20	   6321898 ns/op	   72835 B/op	    4218 allocs/op
BenchmarkOverheadCompositeOneToMany/stages=12/loop-serial-8      	    2955	     40856 ns/op	   65520 B/op	      12 allocs/op
BenchmarkOverheadCompositeOneToMany/stages=12/loop-serial-8      	    2901	     39721 ns/op	   65520 B/op	      12 allocs/op
BenchmarkOverheadCompositeOneToMany/stages=12/loop-serial-8      	    2778	     39647 ns/op	   65520 B/op	      12 allocs/op
BenchmarkOverheadCompositeOneToMany/stages=12/loop-serial-8      	    2907	     40302 ns/op	   65520 B/op	      12 allocs/op
BenchmarkOverheadCompositeOneToMany/stages=12/loop-serial-8      	    2856	     39469 ns/op	   65520 B/op	      12 allocs/op
BenchmarkOverheadCompositeOneToMany/stages=12/channels-8         	      24	   5890918 ns/op	   68772 B/op	    4170 allocs/op
BenchmarkOverheadCompositeOneToMany/stages=12/channels-8         	      21	   5814925 ns/op	   68677 B/op	    4169 allocs/op
BenchmarkOverheadCompositeOneToMany/stages=12/channels-8         	      21	   5588746 ns/op	   68677 B/op	    4169 allocs/op
BenchmarkOverheadCompositeOneToMany/stages=12/channels-8         	      22	   5272824 ns/op	   69026 B/op	    4172 allocs/op
BenchmarkOverheadCompositeOneToMany/stages=12/channels-8         	      25	  13792293 ns/op	   68676 B/op	    4169 allocs/op
BenchmarkOverheadCompositeOneToMany/stages=12/pipeline-8         	      13	   7761888 ns/op	   75822 B/op	    4282 allocs/op
BenchmarkOverheadCompositeOneToMany/stages=12/pipeline-8         	      20	   6832073 ns/op	   75756 B/op	    4281 allocs/op
BenchmarkOverheadCompositeOneToMany/stages=12/pipeline-8         	      19	   6400311 ns/op	   75751 B/op	    4281 allocs/op
BenchmarkOverheadCompositeOneToMany/stages=12/pipeline-8         	      16	   6470495 ns/op	   75886 B/op	    4283 allocs/op
BenchmarkOverheadCompositeOneToMany/stages=12/pipeline-8         	      21	   6413571 ns/op	   75753 B/op	    4281 allocs/op
BenchmarkOverheadCompositeBatch/stages=1/loop-serial-8           	   10034	     12199 ns/op	   32768 B/op	       1 allocs/op
BenchmarkOverheadCompositeBatch/stages=1/loop-serial-8           	    8972	     12113 ns/op	   32768 B/op	       1 allocs/op
BenchmarkOverheadCompositeBatch/stages=1/loop-serial-8           	    9236	     13021 ns/op	   32768 B/op	       1 allocs/op
BenchmarkOverheadCompositeBatch/stages=1/loop-serial-8           	    9058	     13520 ns/op	   32768 B/op	       1 allocs/op
BenchmarkOverheadCompositeBatch/stages=1/loop-serial-8           	    8822	     12171 ns/op	   32768 B/op	       1 allocs/op
BenchmarkOverheadCompositeBatch/stages=1/channels-8              	      32	   3479421 ns/op	     728 B/op	       9 allocs/op
BenchmarkOverheadCompositeBatch/stages=1/channels-8              	      34	   3356447 ns/op	     683 B/op	       9 allocs/op
BenchmarkOverheadCompositeBatch/stages=1/channels-8              	      33	   3427009 ns/op	     683 B/op	       9 allocs/op
BenchmarkOverheadCompositeBatch/stages=1/channels-8              	      32	   3646384 ns/op	     683 B/op	       9 allocs/op
BenchmarkOverheadCompositeBatch/stages=1/channels-8              	      34	   3450803 ns/op	     683 B/op	       9 allocs/op
BenchmarkOverheadCompositeBatch/stages=1/pipeline-8              	      26	   4729891 ns/op	   67864 B/op	     829 allocs/op
BenchmarkOverheadCompositeBatch/stages=1/pipeline-8              	      20	  19579773 ns/op	   67864 B/op	     829 allocs/op
BenchmarkOverheadCompositeBatch/stages=1/pipeline-8              	      19	   5535603 ns/op	   67941 B/op	     829 allocs/op
BenchmarkOverheadCompositeBatch/stages=1/pipeline-8              	      24	   5324186 ns/op	   67865 B/op	     829 allocs/op
BenchmarkOverheadCompositeBatch/stages=1/pipeline-8              	      26	   5044644 ns/op	   67865 B/op	     829 allocs/op
BenchmarkOverheadCompositeBatch/stages=2/loop-serial-8           	    4962	     24780 ns/op	   65536 B/op	       2 allocs/op
BenchmarkOverheadCompositeBatch/stages=2/loop-serial-8           	    4806	     24403 ns/op	   65536 B/op	       2 allocs/op
BenchmarkOverheadCompositeBatch/stages=2/loop-serial-8           	    4971	     24398 ns/op	   65536 B/op	       2 allocs/op
BenchmarkOverheadCompositeBatch/stages=2/loop-serial-8           	    4687	     23159 ns/op	   65536 B/op	       2 allocs/op
BenchmarkOverheadCompositeBatch/stages=2/loop-serial-8           	    4568	     22337 ns/op	   65536 B/op	       2 allocs/op
BenchmarkOverheadCompositeBatch/stages=2/channels-8              	      18	   6236123 ns/op	    1190 B/op	      16 allocs/op
BenchmarkOverheadCompositeBatch/stages=2/channels-8              	      19	   6129127 ns/op	    1189 B/op	      16 allocs/op
BenchmarkOverheadCompositeBatch/stages=2/channels-8              	      21	   9970202 ns/op	    1189 B/op	      16 allocs/op
BenchmarkOverheadCompositeBatch/stages=2/channels-8              	      18	   6143012 ns/op	    1232 B/op	      16 allocs/op
BenchmarkOverheadCompositeBatch/stages=2/channels-8              	      19	   6395831 ns/op	    1189 B/op	      16 allocs/op
BenchmarkOverheadCompositeBatch/stages=2/pipeline-8              	      13	   8122756 ns/op	  133991 B/op	    1622 allocs/op
BenchmarkOverheadCompositeBatch/stages=2/pipeline-8              	      14	   9971312 ns/op	  133987 B/op	    1622 allocs/op
BenchmarkOverheadCompositeBatch/stages=2/pipeline-8              	      14	  12365750 ns/op	  133966 B/op	    1622 allocs/op
BenchmarkOverheadCompositeBatch/stages=2/pipeline-8              	      14	   9513280 ns/op	  134029 B/op	    1622 allocs/op
BenchmarkOverheadCompositeBatch/stages=2/pipeline-8              	      14	   8208518 ns/op	  134009 B/op	    1622 allocs/op
BenchmarkOverheadCompositeBatch/stages=4/loop-serial-8           	    2593	     46879 ns/op	  131072 B/op	       4 allocs/op
BenchmarkOverheadCompositeBatch/stages=4/loop-serial-8           	    2328	     45447 ns/op	  131072 B/op	       4 allocs/op
BenchmarkOverheadCompositeBatch/stages=4/loop-serial-8           	    2468	     45845 ns/op	  131072 B/op	       4 allocs/op
BenchmarkOverheadCompositeBatch/stages=4/loop-serial-8           	    2694	     44057 ns/op	  131073 B/op	       4 allocs/op
BenchmarkOverheadCompositeBatch/stages=4/loop-serial-8           	    2541	     46787 ns/op	  131073 B/op	       4 allocs/op
BenchmarkOverheadCompositeBatch/stages=4/channels-8              	       8	  13003922 ns/op	    2410 B/op	      32 allocs/op
BenchmarkOverheadCompositeBatch/stages=4/channels-8              	      10	  13187296 ns/op	    2481 B/op	      33 allocs/op
BenchmarkOverheadCompositeBatch/stages=4/channels-8              	       8	  14902526 ns/op	    2398 B/op	      32 allocs/op
BenchmarkOverheadCompositeBatch/stages=4/channels-8              	       8	  12772885 ns/op	    2362 B/op	      31 allocs/op
BenchmarkOverheadCompositeBatch/stages=4/channels-8              	       8	  12834536 ns/op	    2278 B/op	      30 allocs/op
BenchmarkOverheadCompositeBatch/stages=4/pipeline-8              	       7	  46342815 ns/op	  266365 B/op	    3208 allocs/op
BenchmarkOverheadCompositeBatch/stages=4/pipeline-8              	       7	  17342774 ns/op	  266436 B/op	    3209 allocs/op
BenchmarkOverheadCompositeBatch/stages=4/pipeline-8              	       7	  16433821 ns/op	  266365 B/op	    3208 allocs/op
BenchmarkOverheadCompositeBatch/stages=4/pipeline-8              	       7	  14808387 ns/op	  266132 B/op	    3206 allocs/op
BenchmarkOverheadCompositeBatch/stages=4/pipeline-8              	       9	  16771361 ns/op	  266222 B/op	    3206 allocs/op
BenchmarkOverheadCompositeBatch/stages=8/loop-serial-8           	    1111	     90089 ns/op	  262146 B/op	       8 allocs/op
BenchmarkOverheadCompositeBatch/stages=8/loop-serial-8           	    1270	     88747 ns/op	  262145 B/op	       8 allocs/op
BenchmarkOverheadCompositeBatch/stages=8/loop-serial-8           	     892	    415382 ns/op	  262145 B/op	       8 allocs/op
BenchmarkOverheadCompositeBatch/stages=8/loop-serial-8           	    1282	     89718 ns/op	  262146 B/op	       8 allocs/op
BenchmarkOverheadCompositeBatch/stages=8/loop-serial-8           	    1300	     93375 ns/op	  262145 B/op	       8 allocs/op
BenchmarkOverheadCompositeBatch/stages=8/channels-8              	       7	  22734434 ns/op	    4361 B/op	      59 allocs/op
BenchmarkOverheadCompositeBatch/stages=8/channels-8              	       6	  28430111 ns/op	    4290 B/op	      58 allocs/op
BenchmarkOverheadCompositeBatch/stages=8/channels-8              	       4	  28453635 ns/op	    4908 B/op	      65 allocs/op
BenchmarkOverheadCompositeBatch/stages=8/channels-8              	       4	  27294865 ns/op	    4812 B/op	      64 allocs/op
BenchmarkOverheadCompositeBatch/stages=8/channels-8              	       6	  23878917 ns/op	    4946 B/op	      65 allocs/op
BenchmarkOverheadCompositeBatch/stages=8/pipeline-8              	       3	  34463014 ns/op	  531642 B/op	    6384 allocs/op
BenchmarkOverheadCompositeBatch/stages=8/pipeline-8              	       3	  35016917 ns/op	  530976 B/op	    6377 allocs/op
BenchmarkOverheadCompositeBatch/stages=8/pipeline-8              	       4	  41225573 ns/op	  530488 B/op	    6372 allocs/op
BenchmarkOverheadCompositeBatch/stages=8/pipeline-8              	       4	  27055510 ns/op	  531116 B/op	    6378 allocs/op
BenchmarkOverheadCompositeBatch/stages=8/pipeline-8              	       3	  34744194 ns/op	  532026 B/op	    6388 allocs/op
BenchmarkOverheadCompositeBatch/stages=12/loop-serial-8          	     691	    150976 ns/op	  393216 B/op	      12 allocs/op
BenchmarkOverheadCompositeBatch/stages=12/loop-serial-8          	     847	    137273 ns/op	  393216 B/op	      12 allocs/op
BenchmarkOverheadCompositeBatch/stages=12/loop-serial-8          	     849	    140184 ns/op	  393218 B/op	      12 allocs/op
BenchmarkOverheadCompositeBatch/stages=12/loop-serial-8          	     825	    226413 ns/op	  393218 B/op	      12 allocs/op
BenchmarkOverheadCompositeBatch/stages=12/loop-serial-8          	     724	    164145 ns/op	  393219 B/op	      12 allocs/op
BenchmarkOverheadCompositeBatch/stages=12/channels-8             	       3	  40651708 ns/op	    6549 B/op	      89 allocs/op
BenchmarkOverheadCompositeBatch/stages=12/channels-8             	       3	  45929069 ns/op	    6261 B/op	      86 allocs/op
BenchmarkOverheadCompositeBatch/stages=12/channels-8             	       3	  34641722 ns/op	    6869 B/op	      92 allocs/op
BenchmarkOverheadCompositeBatch/stages=12/channels-8             	       3	  41607500 ns/op	    6933 B/op	      93 allocs/op
BenchmarkOverheadCompositeBatch/stages=12/channels-8             	       3	  52477319 ns/op	    6773 B/op	      91 allocs/op
BenchmarkOverheadCompositeBatch/stages=12/pipeline-8             	       2	  51511688 ns/op	  796584 B/op	    9559 allocs/op
BenchmarkOverheadCompositeBatch/stages=12/pipeline-8             	       3	  45820736 ns/op	  796256 B/op	    9556 allocs/op
BenchmarkOverheadCompositeBatch/stages=12/pipeline-8             	       3	  46538778 ns/op	  797034 B/op	    9564 allocs/op
BenchmarkOverheadCompositeBatch/stages=12/pipeline-8             	       3	  48365445 ns/op	  795717 B/op	    9550 allocs/op
BenchmarkOverheadCompositeBatch/stages=12/pipeline-8             	       2	  53942542 ns/op	  795680 B/op	    9550 allocs/op
BenchmarkOverheadCompositeBatchChan/stages=1/loop-serial-8       	   10000	     13504 ns/op	   32768 B/op	       1 allocs/op
BenchmarkOverheadCompositeBatchChan/stages=1/loop-serial-8       	    8652	     12334 ns/op	   32768 B/op	       1 allocs/op
BenchmarkOverheadCompositeBatchChan/stages=1/loop-serial-8       	    7752	     13150 ns/op	   32768 B/op	       1 allocs/op
BenchmarkOverheadCompositeBatchChan/stages=1/loop-serial-8       	    9262	     12496 ns/op	   32768 B/op	       1 allocs/op
BenchmarkOverheadCompositeBatchChan/stages=1/loop-serial-8       	    7494	     13891 ns/op	   32768 B/op	       1 allocs/op
BenchmarkOverheadCompositeBatchChan/stages=1/channels-8          	      15	   8407142 ns/op	   14999 B/op	     138 allocs/op
BenchmarkOverheadCompositeBatchChan/stages=1/channels-8          	       2	  52556958 ns/op	   14952 B/op	     137 allocs/op
BenchmarkOverheadCompositeBatchChan/stages=1/channels-8          	      12	  15766479 ns/op	   14905 B/op	     137 allocs/op
BenchmarkOverheadCompositeBatchChan/stages=1/channels-8          	      16	   9312896 ns/op	   14903 B/op	     137 allocs/op
BenchmarkOverheadCompositeBatchChan/stages=1/channels-8          	      13	  24628849 ns/op	   14904 B/op	     137 allocs/op
BenchmarkOverheadCompositeBatchChan/stages=1/pipeline-8          	      19	   6515851 ns/op	   18098 B/op	     194 allocs/op
BenchmarkOverheadCompositeBatchChan/stages=1/pipeline-8          	      20	   6034294 ns/op	   18107 B/op	     194 allocs/op
BenchmarkOverheadCompositeBatchChan/stages=1/pipeline-8          	      31	   5710302 ns/op	   18112 B/op	     194 allocs/op
BenchmarkOverheadCompositeBatchChan/stages=1/pipeline-8          	      20	   6802110 ns/op	   18088 B/op	     194 allocs/op
BenchmarkOverheadCompositeBatchChan/stages=1/pipeline-8          	      14	   8459967 ns/op	   18266 B/op	     195 allocs/op
BenchmarkOverheadCompositeBatchChan/stages=2/loop-serial-8       	    5112	     23371 ns/op	   65536 B/op	       2 allocs/op
BenchmarkOverheadCompositeBatchChan/stages=2/loop-serial-8       	    2886	     50181 ns/op	   65536 B/op	       2 allocs/op
BenchmarkOverheadCompositeBatchChan/stages=2/loop-serial-8       	    4537	     23294 ns/op	   65536 B/op	       2 allocs/op
BenchmarkOverheadCompositeBatchChan/stages=2/loop-serial-8       	    4818	     23664 ns/op	   65537 B/op	       2 allocs/op
BenchmarkOverheadCompositeBatchChan/stages=2/loop-serial-8       	    4750	     23823 ns/op	   65536 B/op	       2 allocs/op
BenchmarkOverheadCompositeBatchChan/stages=2/channels-8          	       7	  14394405 ns/op	   29632 B/op	     272 allocs/op
BenchmarkOverheadCompositeBatchChan/stages=2/channels-8          	       8	  13264432 ns/op	   29714 B/op	     273 allocs/op
BenchmarkOverheadCompositeBatchChan/stages=2/channels-8          	      10	  13039246 ns/op	   29675 B/op	     272 allocs/op
BenchmarkOverheadCompositeBatchChan/stages=2/channels-8          	      10	  13591729 ns/op	   29732 B/op	     273 allocs/op
BenchmarkOverheadCompositeBatchChan/stages=2/channels-8          	       8	  13713453 ns/op	   29726 B/op	     273 allocs/op
BenchmarkOverheadCompositeBatchChan/stages=2/pipeline-8          	       9	  11866380 ns/op	   34709 B/op	     355 allocs/op
BenchmarkOverheadCompositeBatchChan/stages=2/pipeline-8          	       9	  11847648 ns/op	   34421 B/op	     352 allocs/op
BenchmarkOverheadCompositeBatchChan/stages=2/pipeline-8          	       9	  11249454 ns/op	   34400 B/op	     352 allocs/op
BenchmarkOverheadCompositeBatchChan/stages=2/pipeline-8          	       9	  11658912 ns/op	   34442 B/op	     352 allocs/op
BenchmarkOverheadCompositeBatchChan/stages=2/pipeline-8          	       9	  13093611 ns/op	   34581 B/op	     353 allocs/op
BenchmarkOverheadCompositeBatchChan/stages=4/loop-serial-8       	    2264	     45042 ns/op	  131072 B/op	       4 allocs/op
BenchmarkOverheadCompositeBatchChan/stages=4/loop-serial-8       	    2527	     51411 ns/op	  131072 B/op	       4 allocs/op
BenchmarkOverheadCompositeBatchChan/stages=4/loop-serial-8       	    2496	     47610 ns/op	  131072 B/op	       4 allocs/op
BenchmarkOverheadCompositeBatchChan/stages=4/loop-serial-8       	    2337	     47232 ns/op	  131073 B/op	       4 allocs/op
BenchmarkOverheadCompositeBatchChan/stages=4/loop-serial-8       	    2350	     48115 ns/op	  131073 B/op	       4 allocs/op
BenchmarkOverheadCompositeBatchChan/stages=4/channels-8          	       4	  28895042 ns/op	   59588 B/op	     547 allocs/op
BenchmarkOverheadCompositeBatchChan/stages=4/channels-8          	       4	  30782771 ns/op	   59444 B/op	     546 allocs/op
BenchmarkOverheadCompositeBatchChan/stages=4/channels-8          	       4	  28278625 ns/op	   59084 B/op	     542 allocs/op
BenchmarkOverheadCompositeBatchChan/stages=4/channels-8          	       4	  33882000 ns/op	   59276 B/op	     544 allocs/op
BenchmarkOverheadCompositeBatchChan/stages=4/channels-8          	       2	 128429646 ns/op	   60024 B/op	     552 allocs/op
BenchmarkOverheadCompositeBatchChan/stages=4/pipeline-8          	       4	  27217250 ns/op	   67312 B/op	     669 allocs/op
BenchmarkOverheadCompositeBatchChan/stages=4/pipeline-8          	       4	  30008875 ns/op	   67408 B/op	     670 allocs/op
BenchmarkOverheadCompositeBatchChan/stages=4/pipeline-8          	       4	  25344125 ns/op	   67288 B/op	     668 allocs/op
BenchmarkOverheadCompositeBatchChan/stages=4/pipeline-8          	       4	  26258614 ns/op	   67720 B/op	     673 allocs/op
BenchmarkOverheadCompositeBatchChan/stages=4/pipeline-8          	       4	  26337042 ns/op	   67792 B/op	     674 allocs/op
BenchmarkOverheadCompositeBatchChan/stages=8/loop-serial-8       	    1322	     86386 ns/op	  262145 B/op	       8 allocs/op
BenchmarkOverheadCompositeBatchChan/stages=8/loop-serial-8       	    1309	     87548 ns/op	  262144 B/op	       8 allocs/op
BenchmarkOverheadCompositeBatchChan/stages=8/loop-serial-8       	    1442	     86225 ns/op	  262145 B/op	       8 allocs/op
BenchmarkOverheadCompositeBatchChan/stages=8/loop-serial-8       	    1290	     88792 ns/op	  262146 B/op	       8 allocs/op
BenchmarkOverheadCompositeBatchChan/stages=8/loop-serial-8       	    1285	     94758 ns/op	  262145 B/op	       8 allocs/op
BenchmarkOverheadCompositeBatchChan/stages=8/channels-8          	       2	  63702750 ns/op	  120152 B/op	    1105 allocs/op
BenchmarkOverheadCompositeBatchChan/stages=8/channels-8          	       1	 136487792 ns/op	  118048 B/op	    1083 allocs/op
BenchmarkOverheadCompositeBatchChan/stages=8/channels-8          	       2	  63860625 ns/op	  118904 B/op	    1092 allocs/op
BenchmarkOverheadCompositeBatchChan/stages=8/channels-8          	       2	  60108916 ns/op	  118088 B/op	    1083 allocs/op
BenchmarkOverheadCompositeBatchChan/stages=8/channels-8          	       2	  60086458 ns/op	  118088 B/op	    1083 allocs/op
BenchmarkOverheadCompositeBatchChan/stages=8/pipeline-8          	       2	  51481333 ns/op	  133520 B/op	    1305 allocs/op
BenchmarkOverheadCompositeBatchChan/stages=8/pipeline-8          	       1	 258447959 ns/op	  135440 B/op	    1325 allocs/op
BenchmarkOverheadCompositeBatchChan/stages=8/pipeline-8          	       3	  47218139 ns/op	  133232 B/op	    1302 allocs/op
BenchmarkOverheadCompositeBatchChan/stages=8/pipeline-8          	       3	  44740611 ns/op	  133136 B/op	    1301 allocs/op
BenchmarkOverheadCompositeBatchChan/stages=8/pipeline-8          	       3	  48719583 ns/op	  133040 B/op	    1300 allocs/op
BenchmarkOverheadCompositeBatchChan/stages=12/loop-serial-8      	     807	    134862 ns/op	  393218 B/op	      12 allocs/op
BenchmarkOverheadCompositeBatchChan/stages=12/loop-serial-8      	     724	    142618 ns/op	  393217 B/op	      12 allocs/op
BenchmarkOverheadCompositeBatchChan/stages=12/loop-serial-8      	     856	    133573 ns/op	  393218 B/op	      12 allocs/op
BenchmarkOverheadCompositeBatchChan/stages=12/loop-serial-8      	     842	    154902 ns/op	  393216 B/op	      12 allocs/op
BenchmarkOverheadCompositeBatchChan/stages=12/loop-serial-8      	     841	    133481 ns/op	  393219 B/op	      12 allocs/op
BenchmarkOverheadCompositeBatchChan/stages=12/channels-8         	       1	 123921709 ns/op	  181632 B/op	    1672 allocs/op
BenchmarkOverheadCompositeBatchChan/stages=12/channels-8         	       1	 100626250 ns/op	  181632 B/op	    1672 allocs/op
BenchmarkOverheadCompositeBatchChan/stages=12/channels-8         	       1	 113782209 ns/op	  178752 B/op	    1642 allocs/op
BenchmarkOverheadCompositeBatchChan/stages=12/channels-8         	       2	 109378916 ns/op	  177832 B/op	    1632 allocs/op
BenchmarkOverheadCompositeBatchChan/stages=12/channels-8         	       1	 122593333 ns/op	  180576 B/op	    1661 allocs/op
BenchmarkOverheadCompositeBatchChan/stages=12/pipeline-8         	       2	  94743250 ns/op	  198064 B/op	    1927 allocs/op
BenchmarkOverheadCompositeBatchChan/stages=12/pipeline-8         	       1	 201572750 ns/op	  201712 B/op	    1965 allocs/op
BenchmarkOverheadCompositeBatchChan/stages=12/pipeline-8         	       2	  80876646 ns/op	  199120 B/op	    1938 allocs/op
BenchmarkOverheadCompositeBatchChan/stages=12/pipeline-8         	       2	  71300771 ns/op	  198496 B/op	    1931 allocs/op
BenchmarkOverheadCompositeBatchChan/stages=12/pipeline-8         	       2	  79027458 ns/op	  200320 B/op	    1950 allocs/op
PASS
ok  	github.com/askiada/go-pipeline/v2/pkg/pipeline	241.898s
```
