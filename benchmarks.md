# Benchmarks

This document tracks overhead benchmarks comparing go-pipeline with baseline implementations.

## Scenarios
- Baselines: loop-serial, loop-workers (no channels), channels, and go-pipeline.
- Single-stage one-to-one, two-stage chain, fan-out/fan-in (split/merge), and split-by routing.
- Realistic variants (suffix `Realistic`) add CPU work per item using `realisticWork` (see `benchWorkIters = 64`).
- Step-level coverage: OneToOneOrZero, OneToMany, FromChan, SinkFromChan, Batch, BatchChan.
- Overhead sweeps: work intensity sweep (iters up to 4096) and step-count sweep (steps up to 64), conc=1.
- Composite step-count sweeps for fan-out/batch steps, normalized back to one output per input.

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
| Scenario | Loop serial (ns/op) | Loop workers (ns/op) | Channels (ns/op) | Pipeline (ns/op) | Pipeline/Loop workers | Pipeline/Channels |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| OneToOne items=1k conc=1 | 7,332 | 3,802 | 474,787 | 1,839,569 | 483.8x | 3.9x |
| OneToOne items=1k conc=4 | 2,366 | 20,320 | 598,942 | 2,369,720 | 116.6x | 4.0x |
| OneToOne items=16k conc=4 | 37,591 | 452,603 | 9,426,573 | 36,487,681 | 80.6x | 3.9x |
| TwoStage items=1k conc=1 | 7,354 | 8,807 | 766,092 | 3,027,000 | 343.7x | 4.0x |
| TwoStage items=1k conc=4 | 7,323 | 22,056 | 863,992 | 3,777,852 | 171.3x | 4.4x |
| TwoStage items=16k conc=4 | 119,792 | 542,392 | 13,223,156 | 58,664,354 | 108.2x | 4.4x |
| SplitMerge items=1k conc=1 | 3,335 | 5,088 | 1,675,342 | 8,352,024 | 1641.5x | 5.0x |
| SplitMerge items=1k conc=4 | 3,335 | 20,717 | 1,554,586 | 7,422,522 | 358.3x | 4.8x |
| SplitMerge items=16k conc=4 | 53,061 | 485,551 | 24,761,342 | 113,209,041 | 233.2x | 4.6x |
| SplitBy items=1k conc=1 | 2,752 | 4,215 | 1,441,890 | 5,471,210 | 1298.0x | 3.8x |
| SplitBy items=1k conc=4 | 2,621 | 21,786 | 886,702 | 5,119,057 | 235.0x | 5.8x |
| SplitBy items=16k conc=4 | 41,988 | 469,546 | 14,067,500 | 80,708,896 | 171.9x | 5.7x |

### Realistic work
| Scenario | Loop serial (ns/op) | Loop workers (ns/op) | Channels (ns/op) | Pipeline (ns/op) | Pipeline/Loop workers | Pipeline/Channels |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| OneToOne items=1k conc=1 | 118,769 | 116,626 | 637,238 | 1,980,267 | 17.0x | 3.1x |
| OneToOne items=1k conc=4 | 115,879 | 62,498 | 752,037 | 2,898,760 | 46.4x | 3.9x |
| OneToOne items=16k conc=4 | 1,949,849 | 990,727 | 11,569,713 | 38,554,986 | 38.9x | 3.3x |
| TwoStage items=1k conc=1 | 271,513 | 275,793 | 1,122,653 | 3,261,633 | 11.8x | 2.9x |
| TwoStage items=1k conc=4 | 271,450 | 106,666 | 1,001,353 | 3,656,322 | 34.3x | 3.7x |
| TwoStage items=16k conc=4 | 4,365,146 | 1,488,915 | 46,986,589 | 57,131,146 | 38.4x | 1.2x |
| SplitMerge items=1k conc=1 | 232,114 | 234,298 | 1,824,866 | 8,530,516 | 36.4x | 4.7x |
| SplitMerge items=1k conc=4 | 231,978 | 94,774 | 1,745,555 | 7,531,726 | 79.5x | 4.3x |
| SplitMerge items=16k conc=4 | 3,745,194 | 1,279,787 | 31,280,364 | 136,577,709 | 106.7x | 4.4x |

### Step-level snapshot (items=1k, conc=4)
| Step | Loop serial (ns/op) | Loop workers (ns/op) | Channels (ns/op) | Pipeline (ns/op) | Pipeline/Loop workers | Pipeline/Channels |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| OneToOneOrZero | 2,367 | 20,300 | 543,635 | 2,002,124 | 98.6x | 3.7x |
| OneToMany | 24,859 | 42,183 | 940,362 | 3,669,766 | 87.0x | 3.9x |
| FromChan | 2,347 | 20,635 | 592,645 | 1,719,651 | 83.3x | 2.9x |
| SinkFromChan | 2,841 | 22,477 | 394,496 | 1,383,246 | 61.5x | 3.5x |
| Batch | 9,830 | 27,470 | 412,267 | 742,108 | 27.0x | 1.8x |
| BatchChan | 10,613 | 26,690 | 536,215 | 830,772 | 31.1x | 1.5x |

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
| 0 | 3.9 | 469.8 | 1,806.3 | 1,802.5 | 468.5x | 3.8x |
| 4 | 6.3 | 486.1 | 1,761.4 | 1,755.0 | 278.5x | 3.6x |
| 16 | 19.8 | 497.2 | 1,842.3 | 1,822.4 | 92.9x | 3.7x |
| 64 | 119.5 | 652.7 | 1,934.2 | 1,814.8 | 16.2x | 3.0x |
| 256 | 594.0 | 1,250.1 | 2,579.5 | 1,985.5 | 4.3x | 2.1x |
| 1024 | 2,445.8 | 3,254.0 | 4,304.8 | 1,859.0 | 1.8x | 1.3x |
| 4096 | 10,025.8 | 11,235.9 | 12,487.7 | 2,462.0 | 1.2x | 1.1x |

Overhead per item stays roughly flat while work per item grows, so the overhead ratio shrinks as work increases.

### Step-count sweep (conc=1, items=4096, work iters=0)
| Steps | Loop per item (ns) | Channels per item (ns) | Pipeline per item (ns) | Overhead per item (ns) | Overhead per step (ns) | Pipeline/Channels |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 6.5 | 468.2 | 1,755.6 | 1,749.1 | 1,749.1 | 3.7x |
| 2 | 13.1 | 783.8 | 3,050.5 | 3,037.4 | 1,518.7 | 3.9x |
| 4 | 26.2 | 1,424.9 | 5,082.5 | 5,056.2 | 1,264.1 | 3.6x |
| 8 | 49.0 | 2,283.5 | 9,481.3 | 9,432.3 | 1,179.0 | 4.2x |
| 16 | 102.3 | 3,387.7 | 19,452.6 | 19,350.4 | 1,209.4 | 5.7x |
| 32 | 195.1 | 5,683.5 | 38,937.7 | 38,742.6 | 1,210.7 | 6.9x |
| 64 | 386.7 | 8,012.2 | 121,184.3 | 120,797.6 | 1,887.5 | 15.1x |

Per-step overhead is roughly linear in the number of steps in this run.

Linear fit on overhead per item vs steps (conc=1):
- Overhead per item approx -4958.6 ns + 1,833.7 ns * steps

### When is it worth it?
Aim for work per item that is at least 10x the overhead per item (keeps overhead under ~10%).

| Steps | Overhead per item (ns) | Work per item for <10% overhead (ns) |
| ---: | ---: | ---: |
| 1 | 1,749.1 | 17,491.5 |
| 2 | 3,037.4 | 30,373.6 |
| 4 | 5,056.2 | 50,562.3 |
| 8 | 9,432.3 | 94,323.2 |
| 16 | 19,350.4 | 193,503.6 |
| 32 | 38,742.6 | 387,426.5 |
| 64 | 120,797.6 | 1,207,975.6 |

These thresholds are hardware- and configuration-specific (concurrency, buffers, retries, metrics, etc.). Re-run the benchmarks on your target environment to calibrate.

### Total overhead estimates (conc=1, steps)
| Steps | Overhead per item (ns) | Total @1M items (s) | Total @1B items (s) |
| ---: | ---: | ---: | ---: |
| 1 | 1,749.1 | 1.7 | 1,749.1 |
| 2 | 3,037.4 | 3.0 | 3,037.4 |
| 4 | 5,056.2 | 5.1 | 5,056.2 |
| 8 | 9,432.3 | 9.4 | 9,432.3 |
| 16 | 19,350.4 | 19.4 | 19,350.4 |
| 32 | 38,742.6 | 38.7 | 38,742.6 |
| 64 | 120,797.6 | 120.8 | 120,797.6 |

### Composite step-count sweeps (fan-out/batch normalization)
These sweeps chain composite stages that normalize back to one output per input before the next stage:
- OneToMany: expand to 2 outputs, `Batch(MaxSize=2)`, then reduce (sum).
- Batch: `Batch(MaxSize=32)` then `OneToMany` unbatch (return the batch as outputs).
- BatchChan: `BatchChan(MaxSize=32)` then `FromChan` flatten.
All composite sweeps run with conc=1 so the grouping is deterministic.
Tables include loop/channels/pipeline baselines; overhead is pipeline minus loop.

#### OneToMany (expand -> reduce) (conc=1, items=4096)
| Stages | Loop per item (ns) | Channels per item (ns) | Pipeline per item (ns) | Overhead per item (ns) | Overhead per stage (ns) | Pipeline/Channels |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 4.2 | 537.2 | 4,641.3 | 4,637.1 | 4,637.1 | 8.6x |
| 2 | 9.7 | 844.5 | 8,465.0 | 8,455.3 | 4,227.6 | 10.0x |
| 4 | 16.1 | 1,427.8 | 18,573.4 | 18,557.3 | 4,639.3 | 13.0x |
| 8 | 29.8 | 2,247.1 | 40,460.0 | 40,430.2 | 5,053.8 | 18.0x |
| 16 | 62.1 | 3,565.0 | 65,468.1 | 65,406.0 | 4,087.9 | 18.4x |
| 32 | 114.2 | 6,434.5 | 111,656.6 | 111,542.4 | 3,485.7 | 17.4x |
| 64 | 244.4 | 24,701.1 | 213,699.8 | 213,455.4 | 3,335.2 | 8.7x |

Linear fit on overhead per item vs stages (conc=1):
- Overhead per item approx 6,842.4 ns + 3,264.5 ns * stages

### Total overhead estimates (conc=1, stages)
| Stages | Overhead per item (ns) | Total @1M items (s) | Total @1B items (s) |
| ---: | ---: | ---: | ---: |
| 1 | 4,637.1 | 4.6 | 4,637.1 |
| 2 | 8,455.3 | 8.5 | 8,455.3 |
| 4 | 18,557.3 | 18.6 | 18,557.3 |
| 8 | 40,430.2 | 40.4 | 40,430.2 |
| 16 | 65,406.0 | 65.4 | 65,406.0 |
| 32 | 111,542.4 | 111.5 | 111,542.4 |
| 64 | 213,455.4 | 213.5 | 213,455.4 |

#### Batch (batch -> unbatch) (conc=1, items=4096)
| Stages | Loop per item (ns) | Channels per item (ns) | Pipeline per item (ns) | Overhead per item (ns) | Overhead per stage (ns) | Pipeline/Channels |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 2.8 | 455.6 | 1,318.1 | 1,315.3 | 1,315.3 | 2.9x |
| 2 | 5.4 | 579.9 | 1,977.0 | 1,971.6 | 985.8 | 3.4x |
| 4 | 10.0 | 687.9 | 3,236.6 | 3,226.6 | 806.6 | 4.7x |
| 8 | 18.7 | 889.7 | 5,977.5 | 5,958.8 | 744.9 | 6.7x |
| 16 | 38.1 | 1,445.0 | 10,246.8 | 10,208.8 | 638.0 | 7.1x |
| 32 | 80.3 | 2,267.7 | 21,022.5 | 20,942.2 | 654.4 | 9.3x |
| 64 | 624.6 | 4,070.2 | 41,001.3 | 40,376.6 | 630.9 | 10.1x |

Linear fit on overhead per item vs stages (conc=1):
- Overhead per item approx 738.9 ns + 620.7 ns * stages

### Total overhead estimates (conc=1, stages)
| Stages | Overhead per item (ns) | Total @1M items (s) | Total @1B items (s) |
| ---: | ---: | ---: | ---: |
| 1 | 1,315.3 | 1.3 | 1,315.3 |
| 2 | 1,971.6 | 2.0 | 1,971.6 |
| 4 | 3,226.6 | 3.2 | 3,226.6 |
| 8 | 5,958.8 | 6.0 | 5,958.8 |
| 16 | 10,208.8 | 10.2 | 10,208.8 |
| 32 | 20,942.2 | 20.9 | 20,942.2 |
| 64 | 40,376.6 | 40.4 | 40,376.6 |

#### BatchChan (batch -> flatten) (conc=1, items=4096)
| Stages | Loop per item (ns) | Channels per item (ns) | Pipeline per item (ns) | Overhead per item (ns) | Overhead per stage (ns) | Pipeline/Channels |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 2.7 | 851.9 | 1,829.8 | 1,827.1 | 1,827.1 | 2.1x |
| 2 | 5.1 | 1,263.4 | 2,816.6 | 2,811.5 | 1,405.7 | 2.2x |
| 4 | 9.4 | 2,250.8 | 4,852.6 | 4,843.2 | 1,210.8 | 2.2x |
| 8 | 18.5 | 3,156.4 | 9,434.2 | 9,415.7 | 1,177.0 | 3.0x |
| 16 | 36.6 | 5,199.1 | 21,792.9 | 21,756.3 | 1,359.8 | 4.2x |
| 32 | 72.8 | 7,612.1 | 32,709.3 | 32,636.5 | 1,019.9 | 4.3x |
| 64 | 144.9 | 12,919.6 | 55,187.5 | 55,042.6 | 860.0 | 4.3x |

Linear fit on overhead per item vs stages (conc=1):
- Overhead per item approx 2,879.1 ns + 851.8 ns * stages

### Total overhead estimates (conc=1, stages)
| Stages | Overhead per item (ns) | Total @1M items (s) | Total @1B items (s) |
| ---: | ---: | ---: | ---: |
| 1 | 1,827.1 | 1.8 | 1,827.1 |
| 2 | 2,811.5 | 2.8 | 2,811.5 |
| 4 | 4,843.2 | 4.8 | 4,843.2 |
| 8 | 9,415.7 | 9.4 | 9,415.7 |
| 16 | 21,756.3 | 21.8 | 21,756.3 |
| 32 | 32,636.5 | 32.6 | 32,636.5 |
| 64 | 55,042.6 | 55.0 | 55,042.6 |

## Decisions
- Keep serial baselines in the main summary tables alongside worker baselines.
- Treat loop-worker baselines as unbuffered; buffer effects are reserved for option-level benchmarks.
- Target loop + channel + pipeline baselines for every step type; document any exceptions explicitly.
- Use count=1 for fast iteration during development and count=5 for median reporting.
- Keep overhead sweeps at conc=1 with items=4096 to isolate per-item and per-step costs.
- Composite sweeps normalize fan-out/batch stages (Batch/FromChan) and assume conc=1 ordering.
- Track realistic work using `benchWorkIters=64` to show how overhead ratios shrink as work grows.
- Next option-level benchmarks to add: `StepBufferSize` and `StepMaxInFlight`.

```
Date: 2025-12-30
Machine: VirtualApple @ 2.50GHz
OS/Arch: darwin/amd64
Go version: go1.25.0
Commit: da3c9d2821925d4b8736633e3bcc5ba50ce4e5bf
Command: go test ./pkg/pipeline -run '^$' -bench Benchmark -benchmem -benchtime=100ms -count 1
Notes: Default GOMAXPROCS. Overhead sweeps use items=4096 (iters up to 4096, steps up to 64). benchtime=100ms.

goos: darwin
goarch: amd64
pkg: github.com/askiada/go-pipeline/v2/pkg/pipeline
cpu: VirtualApple @ 2.50GHz
BenchmarkOneToOne/items=1k/conc=1/loop-serial-8         	   16240	      7332 ns/op	       0 B/op	       0 allocs/op
BenchmarkOneToOne/items=1k/conc=1/loop-workers-8        	   29850	      3802 ns/op	      16 B/op	       1 allocs/op
BenchmarkOneToOne/items=1k/conc=1/channels-8            	     250	    474787 ns/op	     393 B/op	       6 allocs/op
BenchmarkOneToOne/items=1k/conc=1/pipeline-8            	      66	   1839569 ns/op	    2539 B/op	      48 allocs/op
BenchmarkOneToOne/items=1k/conc=4/loop-serial-8         	   50222	      2366 ns/op	       0 B/op	       0 allocs/op
BenchmarkOneToOne/items=1k/conc=4/loop-workers-8        	    6040	     20320 ns/op	     465 B/op	       8 allocs/op
BenchmarkOneToOne/items=1k/conc=4/channels-8            	     198	    598942 ns/op	     532 B/op	       9 allocs/op
BenchmarkOneToOne/items=1k/conc=4/pipeline-8            	      51	   2369720 ns/op	    3723 B/op	      63 allocs/op
BenchmarkOneToOne/items=16k/conc=4/loop-serial-8        	    3369	     37591 ns/op	       0 B/op	       0 allocs/op
BenchmarkOneToOne/items=16k/conc=4/loop-workers-8       	     261	    452603 ns/op	     468 B/op	       8 allocs/op
BenchmarkOneToOne/items=16k/conc=4/channels-8           	      12	   9426573 ns/op	     536 B/op	       9 allocs/op
BenchmarkOneToOne/items=16k/conc=4/pipeline-8           	       3	  36487681 ns/op	    3714 B/op	      64 allocs/op
BenchmarkOneToOneRealistic/items=1k/conc=1/loop-serial-8         	    1033	    118769 ns/op	       0 B/op	       0 allocs/op
BenchmarkOneToOneRealistic/items=1k/conc=1/loop-workers-8        	    1000	    116626 ns/op	      16 B/op	       1 allocs/op
BenchmarkOneToOneRealistic/items=1k/conc=1/channels-8            	     181	    637238 ns/op	     374 B/op	       6 allocs/op
BenchmarkOneToOneRealistic/items=1k/conc=1/pipeline-8            	      62	   1980267 ns/op	    2401 B/op	      48 allocs/op
BenchmarkOneToOneRealistic/items=1k/conc=4/loop-serial-8         	    1083	    115879 ns/op	       0 B/op	       0 allocs/op
BenchmarkOneToOneRealistic/items=1k/conc=4/loop-workers-8        	    2258	     62498 ns/op	     459 B/op	       8 allocs/op
BenchmarkOneToOneRealistic/items=1k/conc=4/channels-8            	     165	    752037 ns/op	     517 B/op	       9 allocs/op
BenchmarkOneToOneRealistic/items=1k/conc=4/pipeline-8            	      38	   2898760 ns/op	    3570 B/op	      63 allocs/op
BenchmarkOneToOneRealistic/items=16k/conc=4/loop-serial-8        	      64	   1949849 ns/op	       0 B/op	       0 allocs/op
BenchmarkOneToOneRealistic/items=16k/conc=4/loop-workers-8       	     109	    990727 ns/op	     456 B/op	       8 allocs/op
BenchmarkOneToOneRealistic/items=16k/conc=4/channels-8           	       9	  11569713 ns/op	     504 B/op	       9 allocs/op
BenchmarkOneToOneRealistic/items=16k/conc=4/pipeline-8           	       3	  38554986 ns/op	    3560 B/op	      63 allocs/op
BenchmarkOneToOneOrZero/items=1k/conc=1/loop-serial-8            	   51403	      2367 ns/op	       0 B/op	       0 allocs/op
BenchmarkOneToOneOrZero/items=1k/conc=1/loop-workers-8           	   31195	      3864 ns/op	      16 B/op	       1 allocs/op
BenchmarkOneToOneOrZero/items=1k/conc=1/channels-8               	     229	    475165 ns/op	     360 B/op	       6 allocs/op
BenchmarkOneToOneOrZero/items=1k/conc=1/pipeline-8               	      68	   1627222 ns/op	    2325 B/op	      48 allocs/op
BenchmarkOneToOneOrZero/items=1k/conc=4/loop-serial-8            	   49627	      2367 ns/op	       0 B/op	       0 allocs/op
BenchmarkOneToOneOrZero/items=1k/conc=4/loop-workers-8           	    6115	     20300 ns/op	     456 B/op	       8 allocs/op
BenchmarkOneToOneOrZero/items=1k/conc=4/channels-8               	     218	    543635 ns/op	     507 B/op	       9 allocs/op
BenchmarkOneToOneOrZero/items=1k/conc=4/pipeline-8               	      58	   2002124 ns/op	    3640 B/op	      63 allocs/op
BenchmarkOneToOneOrZero/items=16k/conc=4/loop-serial-8           	    3361	     37923 ns/op	       0 B/op	       0 allocs/op
BenchmarkOneToOneOrZero/items=16k/conc=4/loop-workers-8          	     266	    463392 ns/op	     456 B/op	       8 allocs/op
BenchmarkOneToOneOrZero/items=16k/conc=4/channels-8              	      13	   8476362 ns/op	     504 B/op	       9 allocs/op
BenchmarkOneToOneOrZero/items=16k/conc=4/pipeline-8              	       4	  31829865 ns/op	    3728 B/op	      64 allocs/op
BenchmarkTwoStage/items=1k/conc=1/loop-serial-8                  	   16441	      7354 ns/op	       0 B/op	       0 allocs/op
BenchmarkTwoStage/items=1k/conc=1/loop-workers-8                 	   13675	      8807 ns/op	      24 B/op	       1 allocs/op
BenchmarkTwoStage/items=1k/conc=1/channels-8                     	     145	    766092 ns/op	     560 B/op	      10 allocs/op
BenchmarkTwoStage/items=1k/conc=1/pipeline-8                     	      37	   3027000 ns/op	    2914 B/op	      59 allocs/op
BenchmarkTwoStage/items=1k/conc=4/loop-serial-8                  	   16468	      7323 ns/op	       0 B/op	       0 allocs/op
BenchmarkTwoStage/items=1k/conc=4/loop-workers-8                 	    5562	     22056 ns/op	     464 B/op	       8 allocs/op
BenchmarkTwoStage/items=1k/conc=4/channels-8                     	     144	    863992 ns/op	     865 B/op	      16 allocs/op
BenchmarkTwoStage/items=1k/conc=4/pipeline-8                     	      27	   3777852 ns/op	    5182 B/op	      87 allocs/op
BenchmarkTwoStage/items=16k/conc=4/loop-serial-8                 	     981	    119792 ns/op	       0 B/op	       0 allocs/op
BenchmarkTwoStage/items=16k/conc=4/loop-workers-8                	     232	    542392 ns/op	     464 B/op	       8 allocs/op
BenchmarkTwoStage/items=16k/conc=4/channels-8                    	       8	  13223156 ns/op	     848 B/op	      16 allocs/op
BenchmarkTwoStage/items=16k/conc=4/pipeline-8                    	       2	  58664354 ns/op	    5144 B/op	      87 allocs/op
BenchmarkTwoStageRealistic/items=1k/conc=1/loop-serial-8         	     450	    271513 ns/op	       0 B/op	       0 allocs/op
BenchmarkTwoStageRealistic/items=1k/conc=1/loop-workers-8        	     444	    275793 ns/op	      24 B/op	       1 allocs/op
BenchmarkTwoStageRealistic/items=1k/conc=1/channels-8            	     100	   1122653 ns/op	     560 B/op	      10 allocs/op
BenchmarkTwoStageRealistic/items=1k/conc=1/pipeline-8            	      36	   3261633 ns/op	    3148 B/op	      60 allocs/op
BenchmarkTwoStageRealistic/items=1k/conc=4/loop-serial-8         	     438	    271450 ns/op	       0 B/op	       0 allocs/op
BenchmarkTwoStageRealistic/items=1k/conc=4/loop-workers-8        	    1194	    106666 ns/op	     468 B/op	       8 allocs/op
BenchmarkTwoStageRealistic/items=1k/conc=4/channels-8            	     100	   1001353 ns/op	     869 B/op	      16 allocs/op
BenchmarkTwoStageRealistic/items=1k/conc=4/pipeline-8            	      32	   3656322 ns/op	    5132 B/op	      87 allocs/op
BenchmarkTwoStageRealistic/items=16k/conc=4/loop-serial-8        	      24	   4365146 ns/op	       0 B/op	       0 allocs/op
BenchmarkTwoStageRealistic/items=16k/conc=4/loop-workers-8       	      87	   1488915 ns/op	     464 B/op	       8 allocs/op
BenchmarkTwoStageRealistic/items=16k/conc=4/channels-8           	       7	  46986589 ns/op	     946 B/op	      17 allocs/op
BenchmarkTwoStageRealistic/items=16k/conc=4/pipeline-8           	       2	  57131146 ns/op	    5424 B/op	      90 allocs/op
BenchmarkSplitMerge/items=1k/conc=1/loop-serial-8                	   35614	      3335 ns/op	       0 B/op	       0 allocs/op
BenchmarkSplitMerge/items=1k/conc=1/loop-workers-8               	   23464	      5088 ns/op	      24 B/op	       1 allocs/op
BenchmarkSplitMerge/items=1k/conc=1/channels-8                   	      70	   1675342 ns/op	    1040 B/op	      18 allocs/op
BenchmarkSplitMerge/items=1k/conc=1/pipeline-8                   	      14	   8352024 ns/op	    5496 B/op	     102 allocs/op
BenchmarkSplitMerge/items=1k/conc=4/loop-serial-8                	   35787	      3335 ns/op	       0 B/op	       0 allocs/op
BenchmarkSplitMerge/items=1k/conc=4/loop-workers-8               	    6058	     20717 ns/op	     464 B/op	       8 allocs/op
BenchmarkSplitMerge/items=1k/conc=4/channels-8                   	      75	   1554586 ns/op	    1328 B/op	      24 allocs/op
BenchmarkSplitMerge/items=1k/conc=4/pipeline-8                   	      15	   7422522 ns/op	    7958 B/op	     132 allocs/op
BenchmarkSplitMerge/items=16k/conc=4/loop-serial-8               	    2119	     53061 ns/op	       0 B/op	       0 allocs/op
BenchmarkSplitMerge/items=16k/conc=4/loop-workers-8              	     244	    485551 ns/op	     464 B/op	       8 allocs/op
BenchmarkSplitMerge/items=16k/conc=4/channels-8                  	       5	  24761342 ns/op	    1539 B/op	      26 allocs/op
BenchmarkSplitMerge/items=16k/conc=4/pipeline-8                  	       1	 113209041 ns/op	    9744 B/op	     151 allocs/op
BenchmarkSplitMergeRealistic/items=1k/conc=1/loop-serial-8       	     522	    232114 ns/op	       0 B/op	       0 allocs/op
BenchmarkSplitMergeRealistic/items=1k/conc=1/loop-workers-8      	     505	    234298 ns/op	      24 B/op	       1 allocs/op
BenchmarkSplitMergeRealistic/items=1k/conc=1/channels-8          	      58	   1824866 ns/op	    1040 B/op	      18 allocs/op
BenchmarkSplitMergeRealistic/items=1k/conc=1/pipeline-8          	      13	   8530516 ns/op	    5289 B/op	     100 allocs/op
BenchmarkSplitMergeRealistic/items=1k/conc=4/loop-serial-8       	     518	    231978 ns/op	       0 B/op	       0 allocs/op
BenchmarkSplitMergeRealistic/items=1k/conc=4/loop-workers-8      	    1256	     94774 ns/op	     464 B/op	       8 allocs/op
BenchmarkSplitMergeRealistic/items=1k/conc=4/channels-8          	      85	   1745555 ns/op	    1350 B/op	      24 allocs/op
BenchmarkSplitMergeRealistic/items=1k/conc=4/pipeline-8          	      14	   7531726 ns/op	    7859 B/op	     131 allocs/op
BenchmarkSplitMergeRealistic/items=16k/conc=4/loop-serial-8      	      33	   3745194 ns/op	       0 B/op	       0 allocs/op
BenchmarkSplitMergeRealistic/items=16k/conc=4/loop-workers-8     	      85	   1279787 ns/op	     464 B/op	       8 allocs/op
BenchmarkSplitMergeRealistic/items=16k/conc=4/channels-8         	       4	  31280364 ns/op	    1328 B/op	      24 allocs/op
BenchmarkSplitMergeRealistic/items=16k/conc=4/pipeline-8         	       1	 136577709 ns/op	   10512 B/op	     159 allocs/op
BenchmarkSplitBy/items=1k/conc=1/loop-serial-8                   	   43172	      2752 ns/op	       0 B/op	       0 allocs/op
BenchmarkSplitBy/items=1k/conc=1/loop-workers-8                  	   28420	      4215 ns/op	      24 B/op	       1 allocs/op
BenchmarkSplitBy/items=1k/conc=1/channels-8                      	      73	   1441890 ns/op	    1071 B/op	      18 allocs/op
BenchmarkSplitBy/items=1k/conc=1/pipeline-8                      	      22	   5471210 ns/op	    5653 B/op	     103 allocs/op
BenchmarkSplitBy/items=1k/conc=4/loop-serial-8                   	   45183	      2621 ns/op	       0 B/op	       0 allocs/op
BenchmarkSplitBy/items=1k/conc=4/loop-workers-8                  	    5420	     21786 ns/op	     464 B/op	       8 allocs/op
BenchmarkSplitBy/items=1k/conc=4/channels-8                      	     120	    886702 ns/op	    1348 B/op	      24 allocs/op
BenchmarkSplitBy/items=1k/conc=4/pipeline-8                      	      24	   5119057 ns/op	    7649 B/op	     129 allocs/op
BenchmarkSplitBy/items=16k/conc=4/loop-serial-8                  	    3019	     41988 ns/op	       0 B/op	       0 allocs/op
BenchmarkSplitBy/items=16k/conc=4/loop-workers-8                 	     261	    469546 ns/op	     464 B/op	       8 allocs/op
BenchmarkSplitBy/items=16k/conc=4/channels-8                     	       8	  14067500 ns/op	    1328 B/op	      24 allocs/op
BenchmarkSplitBy/items=16k/conc=4/pipeline-8                     	       2	  80708896 ns/op	    8960 B/op	     143 allocs/op
BenchmarkOneToMany/items=1k/conc=1/loop-serial-8                 	    4581	     24998 ns/op	   16384 B/op	    1024 allocs/op
BenchmarkOneToMany/items=1k/conc=1/loop-workers-8                	    4812	     27093 ns/op	   16400 B/op	    1025 allocs/op
BenchmarkOneToMany/items=1k/conc=1/channels-8                    	     154	    729172 ns/op	   16744 B/op	    1030 allocs/op
BenchmarkOneToMany/items=1k/conc=1/pipeline-8                    	      39	   2958196 ns/op	   18688 B/op	    1072 allocs/op
BenchmarkOneToMany/items=1k/conc=4/loop-serial-8                 	    5464	     24859 ns/op	   16384 B/op	    1024 allocs/op
BenchmarkOneToMany/items=1k/conc=4/loop-workers-8                	    3592	     42183 ns/op	   16840 B/op	    1032 allocs/op
BenchmarkOneToMany/items=1k/conc=4/channels-8                    	     124	    940362 ns/op	   16888 B/op	    1033 allocs/op
BenchmarkOneToMany/items=1k/conc=4/pipeline-8                    	      32	   3669766 ns/op	   19873 B/op	    1087 allocs/op
BenchmarkOneToMany/items=16k/conc=4/loop-serial-8                	     304	    391262 ns/op	  262144 B/op	   16384 allocs/op
BenchmarkOneToMany/items=16k/conc=4/loop-workers-8               	     162	    723934 ns/op	  262601 B/op	   16392 allocs/op
BenchmarkOneToMany/items=16k/conc=4/channels-8                   	       7	  15289940 ns/op	  262648 B/op	   16393 allocs/op
BenchmarkOneToMany/items=16k/conc=4/pipeline-8                   	       2	  66487916 ns/op	  265640 B/op	   16447 allocs/op
BenchmarkFromChan/items=1k/conc=1/loop-serial-8                  	   48844	      3253 ns/op	       0 B/op	       0 allocs/op
BenchmarkFromChan/items=1k/conc=1/loop-workers-8                 	   10000	     11960 ns/op	      16 B/op	       1 allocs/op
BenchmarkFromChan/items=1k/conc=1/channels-8                     	     229	    465253 ns/op	     360 B/op	       6 allocs/op
BenchmarkFromChan/items=1k/conc=1/pipeline-8                     	      60	   1808824 ns/op	    2640 B/op	      52 allocs/op
BenchmarkFromChan/items=1k/conc=4/loop-serial-8                  	   49761	      2347 ns/op	       0 B/op	       0 allocs/op
BenchmarkFromChan/items=1k/conc=4/loop-workers-8                 	    6105	     20635 ns/op	     456 B/op	       8 allocs/op
BenchmarkFromChan/items=1k/conc=4/channels-8                     	     196	    592645 ns/op	     504 B/op	       9 allocs/op
BenchmarkFromChan/items=1k/conc=4/pipeline-8                     	      68	   1719651 ns/op	    4802 B/op	      79 allocs/op
BenchmarkFromChan/items=16k/conc=4/loop-serial-8                 	    3328	     37650 ns/op	       0 B/op	       0 allocs/op
BenchmarkFromChan/items=16k/conc=4/loop-workers-8                	     260	    452058 ns/op	     461 B/op	       8 allocs/op
BenchmarkFromChan/items=16k/conc=4/channels-8                    	      13	   9700103 ns/op	     504 B/op	       9 allocs/op
BenchmarkFromChan/items=16k/conc=4/pipeline-8                    	       4	  26981771 ns/op	    4820 B/op	      79 allocs/op
BenchmarkSinkFromChan/items=1k/conc=1/loop-serial-8              	   50348	      2345 ns/op	       0 B/op	       0 allocs/op
BenchmarkSinkFromChan/items=1k/conc=1/loop-workers-8             	   22664	      5237 ns/op	      16 B/op	       1 allocs/op
BenchmarkSinkFromChan/items=1k/conc=1/channels-8                 	     505	    234195 ns/op	     248 B/op	       5 allocs/op
BenchmarkSinkFromChan/items=1k/conc=1/pipeline-8                 	     134	    862243 ns/op	    2000 B/op	      39 allocs/op
BenchmarkSinkFromChan/items=1k/conc=4/loop-serial-8              	   40398	      2841 ns/op	       0 B/op	       0 allocs/op
BenchmarkSinkFromChan/items=1k/conc=4/loop-workers-8             	    5218	     22477 ns/op	     456 B/op	       8 allocs/op
BenchmarkSinkFromChan/items=1k/conc=4/channels-8                 	     350	    394496 ns/op	     469 B/op	       8 allocs/op
BenchmarkSinkFromChan/items=1k/conc=4/pipeline-8                 	      75	   1383246 ns/op	    4146 B/op	      66 allocs/op
BenchmarkSinkFromChan/items=16k/conc=4/loop-serial-8             	    3255	     43263 ns/op	       0 B/op	       0 allocs/op
BenchmarkSinkFromChan/items=16k/conc=4/loop-workers-8            	     206	    566429 ns/op	     456 B/op	       8 allocs/op
BenchmarkSinkFromChan/items=16k/conc=4/channels-8                	      21	   5740165 ns/op	     464 B/op	       8 allocs/op
BenchmarkSinkFromChan/items=16k/conc=4/pipeline-8                	       6	  18861653 ns/op	    4130 B/op	      66 allocs/op
BenchmarkBatch/items=1k/conc=1/loop-serial-8                     	   12460	      9274 ns/op	   15824 B/op	     184 allocs/op
BenchmarkBatch/items=1k/conc=1/loop-workers-8                    	   13069	      9387 ns/op	   15824 B/op	     184 allocs/op
BenchmarkBatch/items=1k/conc=1/channels-8                        	     534	    224439 ns/op	   16243 B/op	     193 allocs/op
BenchmarkBatch/items=1k/conc=1/pipeline-8                        	     285	    411389 ns/op	   18408 B/op	     239 allocs/op
BenchmarkBatch/items=1k/conc=4/loop-serial-8                     	   12489	      9830 ns/op	   15824 B/op	     184 allocs/op
BenchmarkBatch/items=1k/conc=4/loop-workers-8                    	    4722	     27470 ns/op	   16218 B/op	     182 allocs/op
BenchmarkBatch/items=1k/conc=4/channels-8                        	     304	    412267 ns/op	   15863 B/op	     188 allocs/op
BenchmarkBatch/items=1k/conc=4/pipeline-8                        	     164	    742108 ns/op	   19758 B/op	     261 allocs/op
BenchmarkBatch/items=16k/conc=4/loop-serial-8                    	     813	    391151 ns/op	  257744 B/op	    3064 allocs/op
BenchmarkBatch/items=16k/conc=4/loop-workers-8                   	     200	    586710 ns/op	  257642 B/op	    3055 allocs/op
BenchmarkBatch/items=16k/conc=4/channels-8                       	      18	   6344792 ns/op	  257743 B/op	    3067 allocs/op
BenchmarkBatch/items=16k/conc=4/pipeline-8                       	      10	  10805000 ns/op	  261592 B/op	    3140 allocs/op
BenchmarkBatchChan/items=1k/conc=1/loop-serial-8                 	   10000	     10126 ns/op	   15824 B/op	     184 allocs/op
BenchmarkBatchChan/items=1k/conc=1/loop-workers-8                	   12123	      9482 ns/op	   15824 B/op	     184 allocs/op
BenchmarkBatchChan/items=1k/conc=1/channels-8                    	     223	    498172 ns/op	    3944 B/op	      38 allocs/op
BenchmarkBatchChan/items=1k/conc=1/pipeline-8                    	     142	    837008 ns/op	    5870 B/op	      79 allocs/op
BenchmarkBatchChan/items=1k/conc=4/loop-serial-8                 	   12813	     10613 ns/op	   15824 B/op	     184 allocs/op
BenchmarkBatchChan/items=1k/conc=4/loop-workers-8                	    4678	     26690 ns/op	   16231 B/op	     182 allocs/op
BenchmarkBatchChan/items=1k/conc=4/channels-8                    	     222	    536215 ns/op	    4437 B/op	      44 allocs/op
BenchmarkBatchChan/items=1k/conc=4/pipeline-8                    	     138	    830772 ns/op	    7265 B/op	      97 allocs/op
BenchmarkBatchChan/items=16k/conc=4/loop-serial-8                	     807	    153343 ns/op	  257744 B/op	    3064 allocs/op
BenchmarkBatchChan/items=16k/conc=4/loop-workers-8               	     240	    510864 ns/op	  257632 B/op	    3054 allocs/op
BenchmarkBatchChan/items=16k/conc=4/channels-8                   	      13	   8507641 ns/op	   58184 B/op	     524 allocs/op
BenchmarkBatchChan/items=16k/conc=4/pipeline-8                   	       8	  13159573 ns/op	   61018 B/op	     577 allocs/op
BenchmarkOverheadWorkSweep/iters=0/loop-serial-8                 	    8467	     14276 ns/op	       0 B/op	       0 allocs/op
BenchmarkOverheadWorkSweep/iters=0/pipeline-8                    	      15	   7362742 ns/op	    2305 B/op	      48 allocs/op
BenchmarkOverheadWorkSweep/iters=4/loop-serial-8                 	    5329	     23271 ns/op	       0 B/op	       0 allocs/op
BenchmarkOverheadWorkSweep/iters=4/pipeline-8                    	      14	   7159375 ns/op	    2305 B/op	      48 allocs/op
BenchmarkOverheadWorkSweep/iters=16/loop-serial-8                	    1612	     77234 ns/op	       0 B/op	       0 allocs/op
BenchmarkOverheadWorkSweep/iters=16/pipeline-8                   	      16	   7343815 ns/op	    2306 B/op	      48 allocs/op
BenchmarkOverheadWorkSweep/iters=64/loop-serial-8                	     254	    479238 ns/op	       0 B/op	       0 allocs/op
BenchmarkOverheadWorkSweep/iters=64/pipeline-8                   	      15	   7789897 ns/op	    2304 B/op	      48 allocs/op
BenchmarkOverheadWorkSweep/iters=256/loop-serial-8               	      50	   2354550 ns/op	       0 B/op	       0 allocs/op
BenchmarkOverheadWorkSweep/iters=256/pipeline-8                  	      10	  10115042 ns/op	    2305 B/op	      48 allocs/op
BenchmarkOverheadWorkSweep/iters=1024/loop-serial-8              	      12	   9840222 ns/op	       0 B/op	       0 allocs/op
BenchmarkOverheadWorkSweep/iters=1024/pipeline-8                 	       6	  17495306 ns/op	    2306 B/op	      48 allocs/op
BenchmarkOverheadWorkSweep/iters=4096/loop-serial-8              	       3	  39933042 ns/op	       0 B/op	       0 allocs/op
BenchmarkOverheadWorkSweep/iters=4096/pipeline-8                 	       3	  49491305 ns/op	    2466 B/op	      49 allocs/op
BenchmarkOverheadStepScaling/steps=1/loop-serial-8               	    6660	     18753 ns/op	       0 B/op	       0 allocs/op
BenchmarkOverheadStepScaling/steps=1/pipeline-8                  	      15	   7173756 ns/op	    2369 B/op	      49 allocs/op
BenchmarkOverheadStepScaling/steps=2/loop-serial-8               	    3463	     36459 ns/op	       0 B/op	       0 allocs/op
BenchmarkOverheadStepScaling/steps=2/pipeline-8                  	       9	  11747694 ns/op	    2906 B/op	      61 allocs/op
BenchmarkOverheadStepScaling/steps=4/loop-serial-8               	    1568	     68661 ns/op	       0 B/op	       0 allocs/op
BenchmarkOverheadStepScaling/steps=4/pipeline-8                  	       5	  20283392 ns/op	    4332 B/op	      88 allocs/op
BenchmarkOverheadStepScaling/steps=8/loop-serial-8               	     898	    129350 ns/op	       0 B/op	       0 allocs/op
BenchmarkOverheadStepScaling/steps=8/pipeline-8                  	       3	  38249375 ns/op	    7450 B/op	     143 allocs/op
BenchmarkOverheadStepScaling/steps=16/loop-serial-8              	     405	    290989 ns/op	       0 B/op	       0 allocs/op
BenchmarkOverheadStepScaling/steps=16/pipeline-8                 	       2	  77696229 ns/op	   13968 B/op	     254 allocs/op
BenchmarkOverheadStepScaling/steps=32/loop-serial-8              	     220	    538799 ns/op	       0 B/op	       0 allocs/op
BenchmarkOverheadStepScaling/steps=32/pipeline-8                 	       1	 158745750 ns/op	   28784 B/op	     493 allocs/op
BenchmarkOverheadStepScaling/steps=64/loop-serial-8              	     100	   1041244 ns/op	       0 B/op	       0 allocs/op
BenchmarkOverheadStepScaling/steps=64/pipeline-8                 	       1	 263199625 ns/op	   78128 B/op	     903 allocs/op
BenchmarkOverheadCompositeOneToMany/stages=1/loop-serial-8       	    6412	     16865 ns/op	   32768 B/op	       1 allocs/op
BenchmarkOverheadCompositeOneToMany/stages=1/pipeline-8          	       5	  20791908 ns/op	  167462 B/op	   12361 allocs/op
BenchmarkOverheadCompositeOneToMany/stages=2/loop-serial-8       	    3448	     32937 ns/op	   65536 B/op	       2 allocs/op
BenchmarkOverheadCompositeOneToMany/stages=2/pipeline-8          	       3	  34721000 ns/op	  333029 B/op	   24683 allocs/op
BenchmarkOverheadCompositeOneToMany/stages=4/loop-serial-8       	    1652	     61424 ns/op	  131072 B/op	       4 allocs/op
BenchmarkOverheadCompositeOneToMany/stages=4/pipeline-8          	       2	  77266812 ns/op	  664456 B/op	   49329 allocs/op
BenchmarkOverheadCompositeOneToMany/stages=8/loop-serial-8       	     874	    126401 ns/op	  262144 B/op	       8 allocs/op
BenchmarkOverheadCompositeOneToMany/stages=8/pipeline-8          	       1	 155394083 ns/op	 1331264 B/op	   98660 allocs/op
BenchmarkOverheadCompositeOneToMany/stages=16/loop-serial-8      	     502	    240465 ns/op	  524289 B/op	      16 allocs/op
BenchmarkOverheadCompositeOneToMany/stages=16/pipeline-8         	       1	 269819875 ns/op	 2655472 B/op	  197221 allocs/op
BenchmarkOverheadCompositeOneToMany/stages=32/loop-serial-8      	     238	    491930 ns/op	 1048576 B/op	      32 allocs/op
BenchmarkOverheadCompositeOneToMany/stages=32/pipeline-8         	       1	 453195792 ns/op	 5365856 B/op	  394581 allocs/op
BenchmarkOverheadCompositeOneToMany/stages=64/loop-serial-8      	     100	   1018089 ns/op	 2097152 B/op	      64 allocs/op
BenchmarkOverheadCompositeOneToMany/stages=64/pipeline-8         	       1	 933619000 ns/op	10732144 B/op	  789157 allocs/op
BenchmarkOverheadCompositeBatch/stages=1/loop-serial-8           	    9806	     12673 ns/op	   32768 B/op	       1 allocs/op
BenchmarkOverheadCompositeBatch/stages=1/pipeline-8              	      19	   6125732 ns/op	   67377 B/op	     827 allocs/op
BenchmarkOverheadCompositeBatch/stages=2/loop-serial-8           	    5635	     21335 ns/op	   65536 B/op	       2 allocs/op
BenchmarkOverheadCompositeBatch/stages=2/pipeline-8              	      14	   7514018 ns/op	  133272 B/op	    1620 allocs/op
BenchmarkOverheadCompositeBatch/stages=4/loop-serial-8           	    3650	    129121 ns/op	  131072 B/op	       4 allocs/op
BenchmarkOverheadCompositeBatch/stages=4/pipeline-8              	       8	  13352599 ns/op	  264736 B/op	    3201 allocs/op
BenchmarkOverheadCompositeBatch/stages=8/loop-serial-8           	    1712	    240882 ns/op	  262144 B/op	       8 allocs/op
BenchmarkOverheadCompositeBatch/stages=8/pipeline-8              	       4	  25175760 ns/op	  527872 B/op	    6363 allocs/op
BenchmarkOverheadCompositeBatch/stages=16/loop-serial-8          	     732	    159101 ns/op	  524288 B/op	      16 allocs/op
BenchmarkOverheadCompositeBatch/stages=16/pipeline-8             	       3	  40920861 ns/op	 1056848 B/op	   12696 allocs/op
BenchmarkOverheadCompositeBatch/stages=32/loop-serial-8          	     374	    320276 ns/op	 1048576 B/op	      32 allocs/op
BenchmarkOverheadCompositeBatch/stages=32/pipeline-8             	       2	  75876896 ns/op	 2108840 B/op	   25345 allocs/op
BenchmarkOverheadCompositeBatch/stages=64/loop-serial-8          	     196	    638727 ns/op	 2097154 B/op	      64 allocs/op
BenchmarkOverheadCompositeBatch/stages=64/pipeline-8             	       1	 148340125 ns/op	 4226560 B/op	   50797 allocs/op
BenchmarkOverheadCompositeBatchChan/stages=1/loop-serial-8       	   10000	     11314 ns/op	   32768 B/op	       1 allocs/op
BenchmarkOverheadCompositeBatchChan/stages=1/pipeline-8          	      15	   7452092 ns/op	   17553 B/op	     191 allocs/op
BenchmarkOverheadCompositeBatchChan/stages=2/loop-serial-8       	    7413	     21779 ns/op	   65536 B/op	       2 allocs/op
BenchmarkOverheadCompositeBatchChan/stages=2/pipeline-8          	       9	  11857398 ns/op	   33888 B/op	     350 allocs/op
BenchmarkOverheadCompositeBatchChan/stages=4/loop-serial-8       	    3026	     41380 ns/op	  131072 B/op	       4 allocs/op
BenchmarkOverheadCompositeBatchChan/stages=4/pipeline-8          	       5	  20931200 ns/op	   65686 B/op	     659 allocs/op
BenchmarkOverheadCompositeBatchChan/stages=8/loop-serial-8       	    1347	     79409 ns/op	  262144 B/op	       8 allocs/op
BenchmarkOverheadCompositeBatchChan/stages=8/pipeline-8          	       3	  38420778 ns/op	  130112 B/op	    1283 allocs/op
BenchmarkOverheadCompositeBatchChan/stages=16/loop-serial-8      	     862	    171865 ns/op	  524288 B/op	      16 allocs/op
BenchmarkOverheadCompositeBatchChan/stages=16/pipeline-8         	       2	  86774770 ns/op	  257104 B/op	    2510 allocs/op
BenchmarkOverheadCompositeBatchChan/stages=32/loop-serial-8      	     370	    320429 ns/op	 1048576 B/op	      32 allocs/op
BenchmarkOverheadCompositeBatchChan/stages=32/pipeline-8         	       1	 134666500 ns/op	  521744 B/op	    5069 allocs/op
BenchmarkOverheadCompositeBatchChan/stages=64/loop-serial-8      	     186	    624538 ns/op	 2097158 B/op	      64 allocs/op
BenchmarkOverheadCompositeBatchChan/stages=64/pipeline-8         	       1	 236162500 ns/op	 1051504 B/op	   10236 allocs/op
PASS
ok  	github.com/askiada/go-pipeline/v2/pkg/pipeline	39.975s
```
