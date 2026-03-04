# Benchmarks

This document tracks the small benchmark suite that compares raw channels with go-pipeline.

## Scenarios
- One stage (one-to-one)
- One-to-one-or-zero
- One-to-many
- FromChan
- Sink
- SinkFromChan
- Batch
- BatchChan
- Two stage chain
- Split/merge fan-out + fan-in
- SplitBy routing (two branches)

## Sweeps
- Work sweep: repeat `realisticWork` per item (factors: 1, 2, 4, 8) for each basic step.
- Step sweep: number of stages (1, 2, 4, 8, 16) for a one-to-one chain.

## How to run
Run the full suite with allocations:
```bash
go test ./pkg/pipeline -run '^$' -bench Benchmark -benchmem -benchtime=100ms -count 1 -timeout 300s
```

For more stable numbers, increase the count:
```bash
go test ./pkg/pipeline -run '^$' -bench Benchmark -benchmem -benchtime=100ms -count 5 -timeout 300s
```

## Notes
- All benchmarks use `realisticWork` as the per-item function.
- Results compare channels vs pipeline only.
- Bench timings exclude pipeline/channel build time and measure run only.
- Use `scripts/bench_overhead_summary.py` to print channel vs pipeline percent diff:
  ```bash
  go test ./pkg/pipeline -run '^$' -bench Benchmark -benchmem -benchtime=100ms -count 1 -timeout 300s | python3 scripts/bench_overhead_summary.py
  ```

## Results
Record results below with environment details.

```
Date: 2026-01-04
Machine: VirtualApple @ 2.50GHz
OS/Arch: darwin/amd64
Go version: go1.25.0
Command: go test ./pkg/pipeline -run '^$' -bench Benchmark -benchmem -benchtime=100ms -count 5 -timeout 300s
Notes: Default GOMAXPROCS. Build time excluded; run time only. All cases use realisticWork; work sweep uses factors 1, 2, 4, 8. Post StepFromChan and batch tracker refactor.

goos: darwin
goarch: amd64
pkg: github.com/askiada/go-pipeline/v2/pkg/pipeline
cpu: VirtualApple @ 2.50GHz
BenchmarkOneToOne/items=1k/conc=1/channels-8         	      99	   1082447 ns/op	     228 B/op	       4 allocs/op
BenchmarkOneToOne/items=1k/conc=1/channels-8         	     105	   1091109 ns/op	     205 B/op	       4 allocs/op
BenchmarkOneToOne/items=1k/conc=1/channels-8         	     108	   1089540 ns/op	     198 B/op	       4 allocs/op
BenchmarkOneToOne/items=1k/conc=1/channels-8         	      90	   1164557 ns/op	     203 B/op	       4 allocs/op
BenchmarkOneToOne/items=1k/conc=1/channels-8         	     109	   1098216 ns/op	     205 B/op	       4 allocs/op
BenchmarkOneToOne/items=1k/conc=1/pipeline-8         	     105	   1093039 ns/op	     844 B/op	      16 allocs/op
BenchmarkOneToOne/items=1k/conc=1/pipeline-8         	     104	   1131886 ns/op	     820 B/op	      16 allocs/op
BenchmarkOneToOne/items=1k/conc=1/pipeline-8         	     104	   1104731 ns/op	     868 B/op	      16 allocs/op
BenchmarkOneToOne/items=1k/conc=1/pipeline-8         	     105	   1086406 ns/op	     837 B/op	      16 allocs/op
BenchmarkOneToOne/items=1k/conc=1/pipeline-8         	      91	   1117369 ns/op	     788 B/op	      16 allocs/op
BenchmarkOneToOne/items=1k/conc=4/channels-8         	      73	   1795332 ns/op	     401 B/op	       7 allocs/op
BenchmarkOneToOne/items=1k/conc=4/channels-8         	      63	   1712674 ns/op	     560 B/op	       7 allocs/op
BenchmarkOneToOne/items=1k/conc=4/channels-8         	      64	   1729919 ns/op	     527 B/op	       7 allocs/op
BenchmarkOneToOne/items=1k/conc=4/channels-8         	      64	   1577512 ns/op	     382 B/op	       7 allocs/op
BenchmarkOneToOne/items=1k/conc=4/channels-8         	      64	   1581226 ns/op	     552 B/op	       8 allocs/op
BenchmarkOneToOne/items=1k/conc=4/pipeline-8         	      64	   1788928 ns/op	    2267 B/op	      32 allocs/op
BenchmarkOneToOne/items=1k/conc=4/pipeline-8         	      66	   1793321 ns/op	    2206 B/op	      31 allocs/op
BenchmarkOneToOne/items=1k/conc=4/pipeline-8         	      67	   1761642 ns/op	    2180 B/op	      31 allocs/op
BenchmarkOneToOne/items=1k/conc=4/pipeline-8         	      61	   1757437 ns/op	    2337 B/op	      33 allocs/op
BenchmarkOneToOne/items=1k/conc=4/pipeline-8         	      68	   1761524 ns/op	    2146 B/op	      31 allocs/op
BenchmarkOneToOneOrZero/items=1k/conc=1/channels-8   	     151	    790967 ns/op	     168 B/op	       4 allocs/op
BenchmarkOneToOneOrZero/items=1k/conc=1/channels-8   	     151	    801872 ns/op	     168 B/op	       4 allocs/op
BenchmarkOneToOneOrZero/items=1k/conc=1/channels-8   	     148	    778165 ns/op	     172 B/op	       4 allocs/op
BenchmarkOneToOneOrZero/items=1k/conc=1/channels-8   	     152	    775303 ns/op	     168 B/op	       4 allocs/op
BenchmarkOneToOneOrZero/items=1k/conc=1/channels-8   	     151	    772454 ns/op	     168 B/op	       4 allocs/op
BenchmarkOneToOneOrZero/items=1k/conc=1/pipeline-8   	     152	    774495 ns/op	     767 B/op	      16 allocs/op
BenchmarkOneToOneOrZero/items=1k/conc=1/pipeline-8   	     142	    779346 ns/op	     754 B/op	      16 allocs/op
BenchmarkOneToOneOrZero/items=1k/conc=1/pipeline-8   	     150	    905750 ns/op	     795 B/op	      16 allocs/op
BenchmarkOneToOneOrZero/items=1k/conc=1/pipeline-8   	     148	    775321 ns/op	     754 B/op	      16 allocs/op
BenchmarkOneToOneOrZero/items=1k/conc=1/pipeline-8   	     150	    781766 ns/op	     773 B/op	      16 allocs/op
BenchmarkOneToOneOrZero/items=1k/conc=4/channels-8   	      97	   1352179 ns/op	     375 B/op	       7 allocs/op
BenchmarkOneToOneOrZero/items=1k/conc=4/channels-8   	     108	   1355184 ns/op	     397 B/op	       7 allocs/op
BenchmarkOneToOneOrZero/items=1k/conc=4/channels-8   	      94	   1295378 ns/op	     365 B/op	       7 allocs/op
BenchmarkOneToOneOrZero/items=1k/conc=4/channels-8   	     108	   1242776 ns/op	     429 B/op	       7 allocs/op
BenchmarkOneToOneOrZero/items=1k/conc=4/channels-8   	      93	   1294366 ns/op	     377 B/op	       7 allocs/op
BenchmarkOneToOneOrZero/items=1k/conc=4/pipeline-8   	      92	   1249756 ns/op	    2139 B/op	      31 allocs/op
BenchmarkOneToOneOrZero/items=1k/conc=4/pipeline-8   	      87	   1239701 ns/op	    2185 B/op	      31 allocs/op
BenchmarkOneToOneOrZero/items=1k/conc=4/pipeline-8   	      93	   1251941 ns/op	    2214 B/op	      31 allocs/op
BenchmarkOneToOneOrZero/items=1k/conc=4/pipeline-8   	      91	   1240585 ns/op	    2300 B/op	      32 allocs/op
BenchmarkOneToOneOrZero/items=1k/conc=4/pipeline-8   	      96	   1253987 ns/op	    2223 B/op	      31 allocs/op
BenchmarkOneToMany/items=1k/conc=1/channels-8        	      76	   1536399 ns/op	     169 B/op	       4 allocs/op
BenchmarkOneToMany/items=1k/conc=1/channels-8        	      72	   1510755 ns/op	     173 B/op	       4 allocs/op
BenchmarkOneToMany/items=1k/conc=1/channels-8        	      79	   1497482 ns/op	     169 B/op	       4 allocs/op
BenchmarkOneToMany/items=1k/conc=1/channels-8        	      67	   1504261 ns/op	     169 B/op	       4 allocs/op
BenchmarkOneToMany/items=1k/conc=1/channels-8        	      69	   1515088 ns/op	     169 B/op	       4 allocs/op
BenchmarkOneToMany/items=1k/conc=1/pipeline-8        	      73	   1616558 ns/op	   17141 B/op	    1040 allocs/op
BenchmarkOneToMany/items=1k/conc=1/pipeline-8        	      66	   1654583 ns/op	   17170 B/op	    1040 allocs/op
BenchmarkOneToMany/items=1k/conc=1/pipeline-8        	      72	   1644364 ns/op	   17141 B/op	    1040 allocs/op
BenchmarkOneToMany/items=1k/conc=1/pipeline-8        	      64	   1686095 ns/op	   17147 B/op	    1040 allocs/op
BenchmarkOneToMany/items=1k/conc=1/pipeline-8        	      62	   1699849 ns/op	   17151 B/op	    1040 allocs/op
BenchmarkOneToMany/items=1k/conc=4/channels-8        	      48	   2691010 ns/op	     362 B/op	       7 allocs/op
BenchmarkOneToMany/items=1k/conc=4/channels-8        	      44	   2711309 ns/op	     362 B/op	       7 allocs/op
BenchmarkOneToMany/items=1k/conc=4/channels-8        	      46	   2688274 ns/op	     435 B/op	       7 allocs/op
BenchmarkOneToMany/items=1k/conc=4/channels-8        	      37	   2924424 ns/op	     394 B/op	       7 allocs/op
BenchmarkOneToMany/items=1k/conc=4/channels-8        	      49	   2393787 ns/op	     423 B/op	       7 allocs/op
BenchmarkOneToMany/items=1k/conc=4/pipeline-8        	      45	   2507423 ns/op	   18522 B/op	    1055 allocs/op
BenchmarkOneToMany/items=1k/conc=4/pipeline-8        	      40	   2544857 ns/op	   18500 B/op	    1055 allocs/op
BenchmarkOneToMany/items=1k/conc=4/pipeline-8        	      44	   2559164 ns/op	   18534 B/op	    1055 allocs/op
BenchmarkOneToMany/items=1k/conc=4/pipeline-8        	      46	   2497117 ns/op	   18510 B/op	    1055 allocs/op
BenchmarkOneToMany/items=1k/conc=4/pipeline-8        	      45	   2518670 ns/op	   18515 B/op	    1055 allocs/op
BenchmarkFromChan/items=1k/conc=1/channels-8         	     105	   1106332 ns/op	     169 B/op	       4 allocs/op
BenchmarkFromChan/items=1k/conc=1/channels-8         	     102	   1097197 ns/op	     169 B/op	       4 allocs/op
BenchmarkFromChan/items=1k/conc=1/channels-8         	     108	   1097315 ns/op	     169 B/op	       4 allocs/op
BenchmarkFromChan/items=1k/conc=1/channels-8         	      93	   1107811 ns/op	     169 B/op	       4 allocs/op
BenchmarkFromChan/items=1k/conc=1/channels-8         	     108	   1100091 ns/op	     185 B/op	       4 allocs/op
BenchmarkFromChan/items=1k/conc=1/pipeline-8         	      81	   1393380 ns/op	    1167 B/op	      20 allocs/op
BenchmarkFromChan/items=1k/conc=1/pipeline-8         	      70	   1459564 ns/op	    1077 B/op	      20 allocs/op
BenchmarkFromChan/items=1k/conc=1/pipeline-8         	      69	   1460318 ns/op	    1094 B/op	      20 allocs/op
BenchmarkFromChan/items=1k/conc=1/pipeline-8         	      79	   1425679 ns/op	    1150 B/op	      20 allocs/op
BenchmarkFromChan/items=1k/conc=1/pipeline-8         	      73	   1479474 ns/op	    1077 B/op	      20 allocs/op
BenchmarkFromChan/items=1k/conc=4/channels-8         	      68	   1871559 ns/op	     361 B/op	       7 allocs/op
BenchmarkFromChan/items=1k/conc=4/channels-8         	      68	   1917863 ns/op	     418 B/op	       7 allocs/op
BenchmarkFromChan/items=1k/conc=4/channels-8         	      67	   1876644 ns/op	     374 B/op	       7 allocs/op
BenchmarkFromChan/items=1k/conc=4/channels-8         	      75	   1727117 ns/op	     401 B/op	       7 allocs/op
BenchmarkFromChan/items=1k/conc=4/channels-8         	      68	   1545878 ns/op	     418 B/op	       7 allocs/op
BenchmarkFromChan/items=1k/conc=4/pipeline-8         	      75	   1539088 ns/op	    3290 B/op	      47 allocs/op
BenchmarkFromChan/items=1k/conc=4/pipeline-8         	      75	   1521689 ns/op	    3352 B/op	      47 allocs/op
BenchmarkFromChan/items=1k/conc=4/pipeline-8         	      76	   1537163 ns/op	    3356 B/op	      47 allocs/op
BenchmarkFromChan/items=1k/conc=4/pipeline-8         	      75	   1809522 ns/op	    3370 B/op	      47 allocs/op
BenchmarkFromChan/items=1k/conc=4/pipeline-8         	      75	   1539926 ns/op	    3356 B/op	      47 allocs/op
BenchmarkSink/items=1k/conc=1/channels-8             	     200	    574865 ns/op	     162 B/op	       4 allocs/op
BenchmarkSink/items=1k/conc=1/channels-8             	     201	    571738 ns/op	     160 B/op	       4 allocs/op
BenchmarkSink/items=1k/conc=1/channels-8             	     202	    573920 ns/op	     160 B/op	       4 allocs/op
BenchmarkSink/items=1k/conc=1/channels-8             	     206	    567716 ns/op	     160 B/op	       4 allocs/op
BenchmarkSink/items=1k/conc=1/channels-8             	     205	    576525 ns/op	     160 B/op	       4 allocs/op
BenchmarkSink/items=1k/conc=1/pipeline-8             	     222	    522403 ns/op	     647 B/op	      14 allocs/op
BenchmarkSink/items=1k/conc=1/pipeline-8             	     226	    515006 ns/op	     661 B/op	      14 allocs/op
BenchmarkSink/items=1k/conc=1/pipeline-8             	     224	    533607 ns/op	     641 B/op	      14 allocs/op
BenchmarkSink/items=1k/conc=1/pipeline-8             	     222	    519295 ns/op	     643 B/op	      14 allocs/op
BenchmarkSink/items=1k/conc=1/pipeline-8             	     225	    512476 ns/op	     648 B/op	      14 allocs/op
BenchmarkSink/items=1k/conc=4/channels-8             	     141	    837254 ns/op	     381 B/op	       7 allocs/op
BenchmarkSink/items=1k/conc=4/channels-8             	     140	    855475 ns/op	     352 B/op	       7 allocs/op
BenchmarkSink/items=1k/conc=4/channels-8             	     140	    826367 ns/op	     373 B/op	       7 allocs/op
BenchmarkSink/items=1k/conc=4/channels-8             	     140	    820479 ns/op	     356 B/op	       7 allocs/op
BenchmarkSink/items=1k/conc=4/channels-8             	     144	    836372 ns/op	     408 B/op	       7 allocs/op
BenchmarkSink/items=1k/conc=4/pipeline-8             	     153	    795322 ns/op	    2020 B/op	      29 allocs/op
BenchmarkSink/items=1k/conc=4/pipeline-8             	     148	    802256 ns/op	    2016 B/op	      29 allocs/op
BenchmarkSink/items=1k/conc=4/pipeline-8             	     138	    873678 ns/op	    1994 B/op	      29 allocs/op
BenchmarkSink/items=1k/conc=4/pipeline-8             	     134	    828394 ns/op	    2019 B/op	      29 allocs/op
BenchmarkSink/items=1k/conc=4/pipeline-8             	     146	    795521 ns/op	    1973 B/op	      29 allocs/op
BenchmarkSinkFromChan/items=1k/conc=1/channels-8     	     174	    575748 ns/op	     160 B/op	       4 allocs/op
BenchmarkSinkFromChan/items=1k/conc=1/channels-8     	     206	    577738 ns/op	     160 B/op	       4 allocs/op
BenchmarkSinkFromChan/items=1k/conc=1/channels-8     	     206	    572475 ns/op	     160 B/op	       4 allocs/op
BenchmarkSinkFromChan/items=1k/conc=1/channels-8     	     214	    551466 ns/op	     160 B/op	       4 allocs/op
BenchmarkSinkFromChan/items=1k/conc=1/channels-8     	     208	    587857 ns/op	     169 B/op	       4 allocs/op
BenchmarkSinkFromChan/items=1k/conc=1/pipeline-8     	     128	    900727 ns/op	    1034 B/op	      18 allocs/op
BenchmarkSinkFromChan/items=1k/conc=1/pipeline-8     	     135	    872281 ns/op	    1010 B/op	      18 allocs/op
BenchmarkSinkFromChan/items=1k/conc=1/pipeline-8     	     134	    865320 ns/op	    1043 B/op	      18 allocs/op
BenchmarkSinkFromChan/items=1k/conc=1/pipeline-8     	     135	    867393 ns/op	    1019 B/op	      18 allocs/op
BenchmarkSinkFromChan/items=1k/conc=1/pipeline-8     	     135	    885292 ns/op	    1010 B/op	      18 allocs/op
BenchmarkSinkFromChan/items=1k/conc=4/channels-8     	     133	    841146 ns/op	     360 B/op	       7 allocs/op
BenchmarkSinkFromChan/items=1k/conc=4/channels-8     	     142	    840388 ns/op	     352 B/op	       7 allocs/op
BenchmarkSinkFromChan/items=1k/conc=4/channels-8     	     141	    839978 ns/op	     424 B/op	       7 allocs/op
BenchmarkSinkFromChan/items=1k/conc=4/channels-8     	     136	    859838 ns/op	     425 B/op	       7 allocs/op
BenchmarkSinkFromChan/items=1k/conc=4/channels-8     	     140	    830937 ns/op	     375 B/op	       7 allocs/op
BenchmarkSinkFromChan/items=1k/conc=4/pipeline-8     	      87	   1249071 ns/op	    3387 B/op	      45 allocs/op
BenchmarkSinkFromChan/items=1k/conc=4/pipeline-8     	      94	   1227648 ns/op	    3366 B/op	      45 allocs/op
BenchmarkSinkFromChan/items=1k/conc=4/pipeline-8     	      86	   1234263 ns/op	    3364 B/op	      45 allocs/op
BenchmarkSinkFromChan/items=1k/conc=4/pipeline-8     	      96	   1236960 ns/op	    3367 B/op	      45 allocs/op
BenchmarkSinkFromChan/items=1k/conc=4/pipeline-8     	      94	   1243334 ns/op	    3423 B/op	      45 allocs/op
BenchmarkBatch/items=1k/conc=1/channels-8            	     205	    569336 ns/op	    8641 B/op	      37 allocs/op
BenchmarkBatch/items=1k/conc=1/channels-8            	     208	    556918 ns/op	    8616 B/op	      37 allocs/op
BenchmarkBatch/items=1k/conc=1/channels-8            	     212	    546847 ns/op	    8660 B/op	      37 allocs/op
BenchmarkBatch/items=1k/conc=1/channels-8            	     212	    542792 ns/op	    8625 B/op	      37 allocs/op
BenchmarkBatch/items=1k/conc=1/channels-8            	     222	    542168 ns/op	    8647 B/op	      37 allocs/op
BenchmarkBatch/items=1k/conc=1/pipeline-8            	     255	    465353 ns/op	   17112 B/op	     210 allocs/op
BenchmarkBatch/items=1k/conc=1/pipeline-8            	     258	    457174 ns/op	   17127 B/op	     210 allocs/op
BenchmarkBatch/items=1k/conc=1/pipeline-8            	     265	    448384 ns/op	   17131 B/op	     210 allocs/op
BenchmarkBatch/items=1k/conc=1/pipeline-8            	     256	    463169 ns/op	   17109 B/op	     210 allocs/op
BenchmarkBatch/items=1k/conc=1/pipeline-8            	     259	    453770 ns/op	   17110 B/op	     210 allocs/op
BenchmarkBatch/items=1k/conc=4/channels-8            	     150	    764933 ns/op	   10094 B/op	      45 allocs/op
BenchmarkBatch/items=1k/conc=4/channels-8            	     156	    786739 ns/op	   10097 B/op	      45 allocs/op
BenchmarkBatch/items=1k/conc=4/channels-8            	     154	    763742 ns/op	   10096 B/op	      45 allocs/op
BenchmarkBatch/items=1k/conc=4/channels-8            	     154	    763377 ns/op	   10094 B/op	      45 allocs/op
BenchmarkBatch/items=1k/conc=4/channels-8            	     157	    886831 ns/op	   10078 B/op	      45 allocs/op
BenchmarkBatch/items=1k/conc=4/pipeline-8            	     174	    663721 ns/op	   19128 B/op	     237 allocs/op
BenchmarkBatch/items=1k/conc=4/pipeline-8            	     181	    668889 ns/op	   19159 B/op	     237 allocs/op
BenchmarkBatch/items=1k/conc=4/pipeline-8            	     176	    694192 ns/op	   19177 B/op	     238 allocs/op
BenchmarkBatch/items=1k/conc=4/pipeline-8            	     168	    728942 ns/op	   19186 B/op	     238 allocs/op
BenchmarkBatch/items=1k/conc=4/pipeline-8            	     170	    676890 ns/op	   19193 B/op	     238 allocs/op
BenchmarkBatchChan/items=1k/conc=1/channels-8        	     103	   1138630 ns/op	    3753 B/op	      36 allocs/op
BenchmarkBatchChan/items=1k/conc=1/channels-8        	      91	   1170920 ns/op	    3760 B/op	      36 allocs/op
BenchmarkBatchChan/items=1k/conc=1/channels-8        	      86	   1177605 ns/op	    3761 B/op	      36 allocs/op
BenchmarkBatchChan/items=1k/conc=1/channels-8        	     104	   1139718 ns/op	    3759 B/op	      36 allocs/op
BenchmarkBatchChan/items=1k/conc=1/channels-8        	     104	   1129462 ns/op	    3753 B/op	      36 allocs/op
BenchmarkBatchChan/items=1k/conc=1/pipeline-8        	     120	    973629 ns/op	    4563 B/op	      50 allocs/op
BenchmarkBatchChan/items=1k/conc=1/pipeline-8        	     121	    958514 ns/op	    4547 B/op	      50 allocs/op
BenchmarkBatchChan/items=1k/conc=1/pipeline-8        	     122	    952806 ns/op	    4551 B/op	      50 allocs/op
BenchmarkBatchChan/items=1k/conc=1/pipeline-8        	     121	    948852 ns/op	    4548 B/op	      50 allocs/op
BenchmarkBatchChan/items=1k/conc=1/pipeline-8        	     121	    953314 ns/op	    4547 B/op	      50 allocs/op
BenchmarkBatchChan/items=1k/conc=4/channels-8        	     102	   1146359 ns/op	    4366 B/op	      42 allocs/op
BenchmarkBatchChan/items=1k/conc=4/channels-8        	     103	   1218484 ns/op	    4300 B/op	      42 allocs/op
BenchmarkBatchChan/items=1k/conc=4/channels-8        	     102	   1133628 ns/op	    4281 B/op	      42 allocs/op
BenchmarkBatchChan/items=1k/conc=4/channels-8        	     103	   1187075 ns/op	    4290 B/op	      42 allocs/op
BenchmarkBatchChan/items=1k/conc=4/channels-8        	      96	   1142200 ns/op	    4281 B/op	      42 allocs/op
BenchmarkBatchChan/items=1k/conc=4/pipeline-8        	     118	    968565 ns/op	    6773 B/op	      74 allocs/op
BenchmarkBatchChan/items=1k/conc=4/pipeline-8        	     120	    965569 ns/op	    6727 B/op	      74 allocs/op
BenchmarkBatchChan/items=1k/conc=4/pipeline-8        	     118	    969245 ns/op	    6770 B/op	      74 allocs/op
BenchmarkBatchChan/items=1k/conc=4/pipeline-8        	     121	    985810 ns/op	    6752 B/op	      74 allocs/op
BenchmarkBatchChan/items=1k/conc=4/pipeline-8        	     121	    962542 ns/op	    6714 B/op	      74 allocs/op
BenchmarkTwoStage/items=1k/conc=1/channels-8         	      62	   1876356 ns/op	     273 B/op	       7 allocs/op
BenchmarkTwoStage/items=1k/conc=1/channels-8         	      54	   1882163 ns/op	     274 B/op	       7 allocs/op
BenchmarkTwoStage/items=1k/conc=1/channels-8         	      57	   1886461 ns/op	     273 B/op	       7 allocs/op
BenchmarkTwoStage/items=1k/conc=1/channels-8         	      55	   1903553 ns/op	     274 B/op	       7 allocs/op
BenchmarkTwoStage/items=1k/conc=1/channels-8         	      63	   1859922 ns/op	     282 B/op	       7 allocs/op
BenchmarkTwoStage/items=1k/conc=1/pipeline-8         	      51	   2261991 ns/op	     876 B/op	      18 allocs/op
BenchmarkTwoStage/items=1k/conc=1/pipeline-8         	      54	   2016976 ns/op	     870 B/op	      18 allocs/op
BenchmarkTwoStage/items=1k/conc=1/pipeline-8         	      55	   2008245 ns/op	     950 B/op	      18 allocs/op
BenchmarkTwoStage/items=1k/conc=1/pipeline-8         	      58	   2004519 ns/op	     958 B/op	      18 allocs/op
BenchmarkTwoStage/items=1k/conc=1/pipeline-8         	      57	   1981219 ns/op	     907 B/op	      18 allocs/op
BenchmarkTwoStage/items=1k/conc=4/channels-8         	      33	   3449032 ns/op	     821 B/op	      13 allocs/op
BenchmarkTwoStage/items=1k/conc=4/channels-8         	      36	   3477358 ns/op	     675 B/op	      13 allocs/op
BenchmarkTwoStage/items=1k/conc=4/channels-8         	      31	   3464556 ns/op	     709 B/op	      13 allocs/op
BenchmarkTwoStage/items=1k/conc=4/channels-8         	      33	   4269019 ns/op	     714 B/op	      13 allocs/op
BenchmarkTwoStage/items=1k/conc=4/channels-8         	      27	   4172116 ns/op	     695 B/op	      13 allocs/op
BenchmarkTwoStage/items=1k/conc=4/pipeline-8         	      27	   4377691 ns/op	    3537 B/op	      47 allocs/op
BenchmarkTwoStage/items=1k/conc=4/pipeline-8         	      39	   2923398 ns/op	    3499 B/op	      47 allocs/op
BenchmarkTwoStage/items=1k/conc=4/pipeline-8         	      42	   2862392 ns/op	    3446 B/op	      46 allocs/op
BenchmarkTwoStage/items=1k/conc=4/pipeline-8         	      42	   2891704 ns/op	    3476 B/op	      47 allocs/op
BenchmarkTwoStage/items=1k/conc=4/pipeline-8         	      40	   2811626 ns/op	    3424 B/op	      46 allocs/op
BenchmarkSplitMerge/items=1k/conc=1/channels-8       	      20	   6029958 ns/op	     490 B/op	      12 allocs/op
BenchmarkSplitMerge/items=1k/conc=1/channels-8       	      24	   4621043 ns/op	     572 B/op	      12 allocs/op
BenchmarkSplitMerge/items=1k/conc=1/channels-8       	      21	   4892181 ns/op	     485 B/op	      12 allocs/op
BenchmarkSplitMerge/items=1k/conc=1/channels-8       	      22	   4814665 ns/op	     485 B/op	      12 allocs/op
BenchmarkSplitMerge/items=1k/conc=1/channels-8       	      26	   4421061 ns/op	     499 B/op	      12 allocs/op
BenchmarkSplitMerge/items=1k/conc=1/pipeline-8       	      18	   6339602 ns/op	   50559 B/op	    1051 allocs/op
BenchmarkSplitMerge/items=1k/conc=1/pipeline-8       	      21	   5203488 ns/op	   50467 B/op	    1050 allocs/op
BenchmarkSplitMerge/items=1k/conc=1/pipeline-8       	      21	   5279534 ns/op	   50557 B/op	    1051 allocs/op
BenchmarkSplitMerge/items=1k/conc=1/pipeline-8       	      20	   5231385 ns/op	   50418 B/op	    1050 allocs/op
BenchmarkSplitMerge/items=1k/conc=1/pipeline-8       	      20	   5201565 ns/op	   50938 B/op	    1053 allocs/op
BenchmarkSplitMerge/items=1k/conc=4/channels-8       	      16	   6445344 ns/op	    1177 B/op	      21 allocs/op
BenchmarkSplitMerge/items=1k/conc=4/channels-8       	      12	   8372399 ns/op	     873 B/op	      18 allocs/op
BenchmarkSplitMerge/items=1k/conc=4/channels-8       	      19	   6848031 ns/op	     869 B/op	      18 allocs/op
BenchmarkSplitMerge/items=1k/conc=4/channels-8       	      20	   6764777 ns/op	    1085 B/op	      20 allocs/op
BenchmarkSplitMerge/items=1k/conc=4/channels-8       	      19	   5793982 ns/op	    1077 B/op	      20 allocs/op
BenchmarkSplitMerge/items=1k/conc=4/pipeline-8       	      20	   5243017 ns/op	   52928 B/op	    1078 allocs/op
BenchmarkSplitMerge/items=1k/conc=4/pipeline-8       	      21	   5294524 ns/op	   53201 B/op	    1081 allocs/op
BenchmarkSplitMerge/items=1k/conc=4/pipeline-8       	      21	   5149232 ns/op	   53480 B/op	    1084 allocs/op
BenchmarkSplitMerge/items=1k/conc=4/pipeline-8       	      22	   5208311 ns/op	   53061 B/op	    1079 allocs/op
BenchmarkSplitMerge/items=1k/conc=4/pipeline-8       	      20	   5162877 ns/op	   53000 B/op	    1079 allocs/op
BenchmarkSplitBy/items=1k/conc=1/channels-8          	      46	   2492725 ns/op	     528 B/op	      12 allocs/op
BenchmarkSplitBy/items=1k/conc=1/channels-8          	      42	   3198098 ns/op	     482 B/op	      12 allocs/op
BenchmarkSplitBy/items=1k/conc=1/channels-8          	      44	   2761454 ns/op	     517 B/op	      12 allocs/op
BenchmarkSplitBy/items=1k/conc=1/channels-8          	      40	   2579275 ns/op	     552 B/op	      12 allocs/op
BenchmarkSplitBy/items=1k/conc=1/channels-8          	      40	   2777789 ns/op	     600 B/op	      13 allocs/op
BenchmarkSplitBy/items=1k/conc=1/pipeline-8          	      38	   2988098 ns/op	   50590 B/op	    1051 allocs/op
BenchmarkSplitBy/items=1k/conc=1/pipeline-8          	      36	   2992216 ns/op	   50503 B/op	    1050 allocs/op
BenchmarkSplitBy/items=1k/conc=1/pipeline-8          	      37	   2940017 ns/op	   50639 B/op	    1052 allocs/op
BenchmarkSplitBy/items=1k/conc=1/pipeline-8          	      36	   2916678 ns/op	   50442 B/op	    1050 allocs/op
BenchmarkSplitBy/items=1k/conc=1/pipeline-8          	      37	   2969739 ns/op	   50449 B/op	    1050 allocs/op
BenchmarkSplitBy/items=1k/conc=4/channels-8          	      36	   3398092 ns/op	     923 B/op	      18 allocs/op
BenchmarkSplitBy/items=1k/conc=4/channels-8          	      36	   3212005 ns/op	    1067 B/op	      20 allocs/op
BenchmarkSplitBy/items=1k/conc=4/channels-8          	      36	   3383023 ns/op	    1059 B/op	      20 allocs/op
BenchmarkSplitBy/items=1k/conc=4/channels-8          	      38	   3051278 ns/op	    1018 B/op	      19 allocs/op
BenchmarkSplitBy/items=1k/conc=4/channels-8          	      34	   3397894 ns/op	    1064 B/op	      20 allocs/op
BenchmarkSplitBy/items=1k/conc=4/pipeline-8          	      39	   2752388 ns/op	   52974 B/op	    1078 allocs/op
BenchmarkSplitBy/items=1k/conc=4/pipeline-8          	      38	   2819714 ns/op	   53003 B/op	    1078 allocs/op
BenchmarkSplitBy/items=1k/conc=4/pipeline-8          	      34	   3589137 ns/op	   52938 B/op	    1078 allocs/op
BenchmarkSplitBy/items=1k/conc=4/pipeline-8          	      40	   2800675 ns/op	   53076 B/op	    1079 allocs/op
BenchmarkSplitBy/items=1k/conc=4/pipeline-8          	      43	   2680931 ns/op	   52990 B/op	    1078 allocs/op
BenchmarkOverheadWorkSweep/factor=1/channels-8       	      24	   4468544 ns/op	     196 B/op	       4 allocs/op
BenchmarkOverheadWorkSweep/factor=1/channels-8       	      25	   4375313 ns/op	     214 B/op	       4 allocs/op
BenchmarkOverheadWorkSweep/factor=1/channels-8       	      25	   4386397 ns/op	     184 B/op	       4 allocs/op
BenchmarkOverheadWorkSweep/factor=1/channels-8       	      25	   4404895 ns/op	     172 B/op	       4 allocs/op
BenchmarkOverheadWorkSweep/factor=1/channels-8       	      26	   4363138 ns/op	     201 B/op	       4 allocs/op
BenchmarkOverheadWorkSweep/factor=1/pipeline-8       	      25	   4403115 ns/op	     812 B/op	      16 allocs/op
BenchmarkOverheadWorkSweep/factor=1/pipeline-8       	      24	   4336200 ns/op	     771 B/op	      16 allocs/op
BenchmarkOverheadWorkSweep/factor=1/pipeline-8       	      27	   4322192 ns/op	     765 B/op	      16 allocs/op
BenchmarkOverheadWorkSweep/factor=1/pipeline-8       	      25	   4407423 ns/op	     766 B/op	      16 allocs/op
BenchmarkOverheadWorkSweep/factor=1/pipeline-8       	      25	   4367493 ns/op	     782 B/op	      16 allocs/op
BenchmarkOverheadWorkSweep/factor=2/channels-8       	      21	   5163438 ns/op	     187 B/op	       4 allocs/op
BenchmarkOverheadWorkSweep/factor=2/channels-8       	      20	   5122829 ns/op	     183 B/op	       4 allocs/op
BenchmarkOverheadWorkSweep/factor=2/channels-8       	      21	   5074818 ns/op	     191 B/op	       4 allocs/op
BenchmarkOverheadWorkSweep/factor=2/channels-8       	      22	   5823875 ns/op	     186 B/op	       4 allocs/op
BenchmarkOverheadWorkSweep/factor=2/channels-8       	      21	   5104040 ns/op	     182 B/op	       4 allocs/op
BenchmarkOverheadWorkSweep/factor=2/pipeline-8       	      21	   5178528 ns/op	     787 B/op	      16 allocs/op
BenchmarkOverheadWorkSweep/factor=2/pipeline-8       	      20	   5140702 ns/op	     871 B/op	      17 allocs/op
BenchmarkOverheadWorkSweep/factor=2/pipeline-8       	      20	   5128000 ns/op	     842 B/op	      16 allocs/op
BenchmarkOverheadWorkSweep/factor=2/pipeline-8       	      21	   5183361 ns/op	     860 B/op	      17 allocs/op
BenchmarkOverheadWorkSweep/factor=2/pipeline-8       	      21	   5249833 ns/op	     874 B/op	      17 allocs/op
BenchmarkOverheadWorkSweep/factor=4/channels-8       	      19	   6840406 ns/op	     184 B/op	       4 allocs/op
BenchmarkOverheadWorkSweep/factor=4/channels-8       	      18	   6650764 ns/op	     174 B/op	       4 allocs/op
BenchmarkOverheadWorkSweep/factor=4/channels-8       	      15	   6719078 ns/op	     175 B/op	       4 allocs/op
BenchmarkOverheadWorkSweep/factor=4/channels-8       	      15	   6714683 ns/op	     175 B/op	       4 allocs/op
BenchmarkOverheadWorkSweep/factor=4/channels-8       	      16	   6645367 ns/op	     175 B/op	       4 allocs/op
BenchmarkOverheadWorkSweep/factor=4/pipeline-8       	      16	   6678727 ns/op	     775 B/op	      16 allocs/op
BenchmarkOverheadWorkSweep/factor=4/pipeline-8       	      15	   7117586 ns/op	     885 B/op	      17 allocs/op
BenchmarkOverheadWorkSweep/factor=4/pipeline-8       	      18	   6545326 ns/op	     815 B/op	      16 allocs/op
BenchmarkOverheadWorkSweep/factor=4/pipeline-8       	      16	   7442990 ns/op	     775 B/op	      16 allocs/op
BenchmarkOverheadWorkSweep/factor=4/pipeline-8       	      18	   6558088 ns/op	     772 B/op	      16 allocs/op
BenchmarkOverheadWorkSweep/factor=8/channels-8       	      10	  10286504 ns/op	     179 B/op	       4 allocs/op
BenchmarkOverheadWorkSweep/factor=8/channels-8       	      10	  10203675 ns/op	     179 B/op	       4 allocs/op
BenchmarkOverheadWorkSweep/factor=8/channels-8       	      10	  10288058 ns/op	     179 B/op	       4 allocs/op
BenchmarkOverheadWorkSweep/factor=8/channels-8       	      10	  10237117 ns/op	     179 B/op	       4 allocs/op
BenchmarkOverheadWorkSweep/factor=8/channels-8       	      10	  10266521 ns/op	     179 B/op	       4 allocs/op
BenchmarkOverheadWorkSweep/factor=8/pipeline-8       	      12	   9487795 ns/op	     838 B/op	      16 allocs/op
BenchmarkOverheadWorkSweep/factor=8/pipeline-8       	      13	   9644148 ns/op	     832 B/op	      16 allocs/op
BenchmarkOverheadWorkSweep/factor=8/pipeline-8       	      12	   9582208 ns/op	     886 B/op	      17 allocs/op
BenchmarkOverheadWorkSweep/factor=8/pipeline-8       	      12	   9561920 ns/op	     782 B/op	      16 allocs/op
BenchmarkOverheadWorkSweep/factor=8/pipeline-8       	      12	   9618868 ns/op	     822 B/op	      16 allocs/op
BenchmarkOneToOneOrZeroWorkSweep/factor=1/channels-8 	      30	   3439071 ns/op	     171 B/op	       4 allocs/op
BenchmarkOneToOneOrZeroWorkSweep/factor=1/channels-8 	      37	   3118978 ns/op	     191 B/op	       4 allocs/op
BenchmarkOneToOneOrZeroWorkSweep/factor=1/channels-8 	      37	   3110327 ns/op	     171 B/op	       4 allocs/op
BenchmarkOneToOneOrZeroWorkSweep/factor=1/channels-8 	      34	   3139031 ns/op	     171 B/op	       4 allocs/op
BenchmarkOneToOneOrZeroWorkSweep/factor=1/channels-8 	      32	   3133297 ns/op	     171 B/op	       4 allocs/op
BenchmarkOneToOneOrZeroWorkSweep/factor=1/pipeline-8 	      38	   3076216 ns/op	     840 B/op	      16 allocs/op
BenchmarkOneToOneOrZeroWorkSweep/factor=1/pipeline-8 	      34	   3071335 ns/op	     779 B/op	      16 allocs/op
BenchmarkOneToOneOrZeroWorkSweep/factor=1/pipeline-8 	      37	   3080584 ns/op	     774 B/op	      16 allocs/op
BenchmarkOneToOneOrZeroWorkSweep/factor=1/pipeline-8 	      38	   3053242 ns/op	     761 B/op	      16 allocs/op
BenchmarkOneToOneOrZeroWorkSweep/factor=1/pipeline-8 	      37	   3089011 ns/op	     761 B/op	      16 allocs/op
BenchmarkOneToOneOrZeroWorkSweep/factor=2/channels-8 	      28	   3598306 ns/op	     172 B/op	       4 allocs/op
BenchmarkOneToOneOrZeroWorkSweep/factor=2/channels-8 	      32	   3562398 ns/op	     174 B/op	       4 allocs/op
BenchmarkOneToOneOrZeroWorkSweep/factor=2/channels-8 	      32	   3566064 ns/op	     192 B/op	       4 allocs/op
BenchmarkOneToOneOrZeroWorkSweep/factor=2/channels-8 	      31	   3555837 ns/op	     171 B/op	       4 allocs/op
BenchmarkOneToOneOrZeroWorkSweep/factor=2/channels-8 	      31	   4063016 ns/op	     202 B/op	       4 allocs/op
BenchmarkOneToOneOrZeroWorkSweep/factor=2/pipeline-8 	      33	   3495117 ns/op	     789 B/op	      16 allocs/op
BenchmarkOneToOneOrZeroWorkSweep/factor=2/pipeline-8 	      30	   3520275 ns/op	     805 B/op	      16 allocs/op
BenchmarkOneToOneOrZeroWorkSweep/factor=2/pipeline-8 	      33	   3528965 ns/op	     763 B/op	      16 allocs/op
BenchmarkOneToOneOrZeroWorkSweep/factor=2/pipeline-8 	      32	   3521077 ns/op	     808 B/op	      16 allocs/op
BenchmarkOneToOneOrZeroWorkSweep/factor=2/pipeline-8 	      33	   3542797 ns/op	     809 B/op	      16 allocs/op
BenchmarkOneToOneOrZeroWorkSweep/factor=4/channels-8 	      25	   4512855 ns/op	     195 B/op	       4 allocs/op
BenchmarkOneToOneOrZeroWorkSweep/factor=4/channels-8 	      24	   4483733 ns/op	     192 B/op	       4 allocs/op
BenchmarkOneToOneOrZeroWorkSweep/factor=4/channels-8 	      25	   4482300 ns/op	     210 B/op	       4 allocs/op
BenchmarkOneToOneOrZeroWorkSweep/factor=4/channels-8 	      25	   4487102 ns/op	     180 B/op	       4 allocs/op
BenchmarkOneToOneOrZeroWorkSweep/factor=4/channels-8 	      26	   4621851 ns/op	     172 B/op	       4 allocs/op
BenchmarkOneToOneOrZeroWorkSweep/factor=4/pipeline-8 	      25	   5047660 ns/op	     766 B/op	      16 allocs/op
BenchmarkOneToOneOrZeroWorkSweep/factor=4/pipeline-8 	      27	   4327983 ns/op	     872 B/op	      17 allocs/op
BenchmarkOneToOneOrZeroWorkSweep/factor=4/pipeline-8 	      26	   4654425 ns/op	     847 B/op	      16 allocs/op
BenchmarkOneToOneOrZeroWorkSweep/factor=4/pipeline-8 	      26	   4340572 ns/op	     766 B/op	      16 allocs/op
BenchmarkOneToOneOrZeroWorkSweep/factor=4/pipeline-8 	      27	   4344764 ns/op	     801 B/op	      16 allocs/op
BenchmarkOneToOneOrZeroWorkSweep/factor=8/channels-8 	      19	   6088864 ns/op	     184 B/op	       4 allocs/op
BenchmarkOneToOneOrZeroWorkSweep/factor=8/channels-8 	      19	   6192980 ns/op	     173 B/op	       4 allocs/op
BenchmarkOneToOneOrZeroWorkSweep/factor=8/channels-8 	      18	   6258711 ns/op	     190 B/op	       4 allocs/op
BenchmarkOneToOneOrZeroWorkSweep/factor=8/channels-8 	      18	   6191308 ns/op	     174 B/op	       4 allocs/op
BenchmarkOneToOneOrZeroWorkSweep/factor=8/channels-8 	      18	   6212393 ns/op	     174 B/op	       4 allocs/op
BenchmarkOneToOneOrZeroWorkSweep/factor=8/pipeline-8 	      18	   5973944 ns/op	     772 B/op	      16 allocs/op
BenchmarkOneToOneOrZeroWorkSweep/factor=8/pipeline-8 	      18	   5944019 ns/op	     847 B/op	      16 allocs/op
BenchmarkOneToOneOrZeroWorkSweep/factor=8/pipeline-8 	      18	   5911058 ns/op	     884 B/op	      17 allocs/op
BenchmarkOneToOneOrZeroWorkSweep/factor=8/pipeline-8 	      18	   5935942 ns/op	     852 B/op	      17 allocs/op
BenchmarkOneToOneOrZeroWorkSweep/factor=8/pipeline-8 	      18	   5952090 ns/op	     809 B/op	      16 allocs/op
BenchmarkOneToManyWorkSweep/factor=1/channels-8      	      19	   6154392 ns/op	     173 B/op	       4 allocs/op
BenchmarkOneToManyWorkSweep/factor=1/channels-8      	      19	   6013103 ns/op	     209 B/op	       4 allocs/op
BenchmarkOneToManyWorkSweep/factor=1/channels-8      	      19	   6054158 ns/op	     209 B/op	       4 allocs/op
BenchmarkOneToManyWorkSweep/factor=1/channels-8      	      18	   6079729 ns/op	     211 B/op	       4 allocs/op
BenchmarkOneToManyWorkSweep/factor=1/channels-8      	      18	   6084613 ns/op	     222 B/op	       4 allocs/op
BenchmarkOneToManyWorkSweep/factor=1/pipeline-8      	      16	   6481573 ns/op	   66485 B/op	    4114 allocs/op
BenchmarkOneToManyWorkSweep/factor=1/pipeline-8      	      18	   6514653 ns/op	   66308 B/op	    4112 allocs/op
BenchmarkOneToManyWorkSweep/factor=1/pipeline-8      	      18	   6368746 ns/op	   66308 B/op	    4112 allocs/op
BenchmarkOneToManyWorkSweep/factor=1/pipeline-8      	      18	   6563431 ns/op	   66308 B/op	    4112 allocs/op
BenchmarkOneToManyWorkSweep/factor=1/pipeline-8      	      16	   6545034 ns/op	   66425 B/op	    4113 allocs/op
BenchmarkOneToManyWorkSweep/factor=2/channels-8      	      15	   6877422 ns/op	     175 B/op	       4 allocs/op
BenchmarkOneToManyWorkSweep/factor=2/channels-8      	      16	   6724719 ns/op	     175 B/op	       4 allocs/op
BenchmarkOneToManyWorkSweep/factor=2/channels-8      	      16	   6905305 ns/op	     175 B/op	       4 allocs/op
BenchmarkOneToManyWorkSweep/factor=2/channels-8      	      16	   6788750 ns/op	     175 B/op	       4 allocs/op
BenchmarkOneToManyWorkSweep/factor=2/channels-8      	      16	   6730891 ns/op	     175 B/op	       4 allocs/op
BenchmarkOneToManyWorkSweep/factor=2/pipeline-8      	      15	   7104772 ns/op	   66312 B/op	    4112 allocs/op
BenchmarkOneToManyWorkSweep/factor=2/pipeline-8      	      16	   9060406 ns/op	   66311 B/op	    4112 allocs/op
BenchmarkOneToManyWorkSweep/factor=2/pipeline-8      	      16	   7247648 ns/op	   66383 B/op	    4112 allocs/op
BenchmarkOneToManyWorkSweep/factor=2/pipeline-8      	      15	   8078067 ns/op	   66376 B/op	    4112 allocs/op
BenchmarkOneToManyWorkSweep/factor=2/pipeline-8      	      16	   7023547 ns/op	   66359 B/op	    4112 allocs/op
BenchmarkOneToManyWorkSweep/factor=4/channels-8      	      13	   8324458 ns/op	     176 B/op	       4 allocs/op
BenchmarkOneToManyWorkSweep/factor=4/channels-8      	      13	   8326385 ns/op	     176 B/op	       4 allocs/op
BenchmarkOneToManyWorkSweep/factor=4/channels-8      	      13	   8310692 ns/op	     176 B/op	       4 allocs/op
BenchmarkOneToManyWorkSweep/factor=4/channels-8      	      13	   8358234 ns/op	     176 B/op	       4 allocs/op
BenchmarkOneToManyWorkSweep/factor=4/channels-8      	      14	   8266628 ns/op	     176 B/op	       4 allocs/op
BenchmarkOneToManyWorkSweep/factor=4/pipeline-8      	      13	   9166961 ns/op	   66375 B/op	    4112 allocs/op
BenchmarkOneToManyWorkSweep/factor=4/pipeline-8      	      13	   8417888 ns/op	   66419 B/op	    4113 allocs/op
BenchmarkOneToManyWorkSweep/factor=4/pipeline-8      	      13	   8285205 ns/op	   66360 B/op	    4112 allocs/op
BenchmarkOneToManyWorkSweep/factor=4/pipeline-8      	      13	   8127212 ns/op	   66382 B/op	    4112 allocs/op
BenchmarkOneToManyWorkSweep/factor=4/pipeline-8      	      13	   8279529 ns/op	   66316 B/op	    4112 allocs/op
BenchmarkOneToManyWorkSweep/factor=8/channels-8      	      10	  10417804 ns/op	     217 B/op	       4 allocs/op
BenchmarkOneToManyWorkSweep/factor=8/channels-8      	      10	  10276025 ns/op	     179 B/op	       4 allocs/op
BenchmarkOneToManyWorkSweep/factor=8/channels-8      	      10	  10264758 ns/op	     179 B/op	       4 allocs/op
BenchmarkOneToManyWorkSweep/factor=8/channels-8      	      10	  10349263 ns/op	     188 B/op	       4 allocs/op
BenchmarkOneToManyWorkSweep/factor=8/channels-8      	      10	  10302746 ns/op	     246 B/op	       4 allocs/op
BenchmarkOneToManyWorkSweep/factor=8/pipeline-8      	      10	  12547650 ns/op	   66401 B/op	    4113 allocs/op
BenchmarkOneToManyWorkSweep/factor=8/pipeline-8      	      10	  10809113 ns/op	   66392 B/op	    4113 allocs/op
BenchmarkOneToManyWorkSweep/factor=8/pipeline-8      	      10	  10778004 ns/op	   66401 B/op	    4113 allocs/op
BenchmarkOneToManyWorkSweep/factor=8/pipeline-8      	      10	  10782467 ns/op	   66334 B/op	    4112 allocs/op
BenchmarkOneToManyWorkSweep/factor=8/pipeline-8      	      10	  10815850 ns/op	   66324 B/op	    4112 allocs/op
BenchmarkFromChanWorkSweep/factor=1/channels-8       	      24	   4419632 ns/op	     172 B/op	       4 allocs/op
BenchmarkFromChanWorkSweep/factor=1/channels-8       	      24	   4400030 ns/op	     172 B/op	       4 allocs/op
BenchmarkFromChanWorkSweep/factor=1/channels-8       	      26	   4325788 ns/op	     172 B/op	       4 allocs/op
BenchmarkFromChanWorkSweep/factor=1/channels-8       	      27	   4381358 ns/op	     172 B/op	       4 allocs/op
BenchmarkFromChanWorkSweep/factor=1/channels-8       	      25	   4368147 ns/op	     226 B/op	       4 allocs/op
BenchmarkFromChanWorkSweep/factor=1/pipeline-8       	      21	   5566956 ns/op	    1103 B/op	      20 allocs/op
BenchmarkFromChanWorkSweep/factor=1/pipeline-8       	      19	   6353625 ns/op	    1091 B/op	      20 allocs/op
BenchmarkFromChanWorkSweep/factor=1/pipeline-8       	      19	   5578673 ns/op	    1192 B/op	      21 allocs/op
BenchmarkFromChanWorkSweep/factor=1/pipeline-8       	      20	   5585425 ns/op	    1162 B/op	      20 allocs/op
BenchmarkFromChanWorkSweep/factor=1/pipeline-8       	      19	   5592202 ns/op	    1177 B/op	      21 allocs/op
BenchmarkFromChanWorkSweep/factor=2/channels-8       	      21	   5197063 ns/op	     173 B/op	       4 allocs/op
BenchmarkFromChanWorkSweep/factor=2/channels-8       	      21	   5104849 ns/op	     173 B/op	       4 allocs/op
BenchmarkFromChanWorkSweep/factor=2/channels-8       	      22	   5055646 ns/op	     194 B/op	       4 allocs/op
BenchmarkFromChanWorkSweep/factor=2/channels-8       	      21	   5469024 ns/op	     173 B/op	       4 allocs/op
BenchmarkFromChanWorkSweep/factor=2/channels-8       	      21	   5105103 ns/op	     187 B/op	       4 allocs/op
BenchmarkFromChanWorkSweep/factor=2/pipeline-8       	      18	   6314961 ns/op	    1209 B/op	      21 allocs/op
BenchmarkFromChanWorkSweep/factor=2/pipeline-8       	      16	   6342315 ns/op	    1095 B/op	      20 allocs/op
BenchmarkFromChanWorkSweep/factor=2/pipeline-8       	      19	   6276493 ns/op	    1091 B/op	      20 allocs/op
BenchmarkFromChanWorkSweep/factor=2/pipeline-8       	      18	   7264586 ns/op	    1225 B/op	      21 allocs/op
BenchmarkFromChanWorkSweep/factor=2/pipeline-8       	      16	   6252669 ns/op	    1383 B/op	      23 allocs/op
BenchmarkFromChanWorkSweep/factor=4/channels-8       	      16	   6758867 ns/op	     199 B/op	       4 allocs/op
BenchmarkFromChanWorkSweep/factor=4/channels-8       	      16	   6690836 ns/op	     229 B/op	       4 allocs/op
BenchmarkFromChanWorkSweep/factor=4/channels-8       	      18	   6724257 ns/op	     174 B/op	       4 allocs/op
BenchmarkFromChanWorkSweep/factor=4/channels-8       	      15	   6739300 ns/op	     175 B/op	       4 allocs/op
BenchmarkFromChanWorkSweep/factor=4/channels-8       	      16	   6702906 ns/op	     175 B/op	       4 allocs/op
BenchmarkFromChanWorkSweep/factor=4/pipeline-8       	      14	  10156223 ns/op	    1208 B/op	      21 allocs/op
BenchmarkFromChanWorkSweep/factor=4/pipeline-8       	      13	   7952885 ns/op	    1100 B/op	      20 allocs/op
BenchmarkFromChanWorkSweep/factor=4/pipeline-8       	      14	   7935854 ns/op	    1242 B/op	      21 allocs/op
BenchmarkFromChanWorkSweep/factor=4/pipeline-8       	      13	   7862545 ns/op	    1218 B/op	      21 allocs/op
BenchmarkFromChanWorkSweep/factor=4/pipeline-8       	      14	   7798530 ns/op	    1276 B/op	      22 allocs/op
BenchmarkFromChanWorkSweep/factor=8/channels-8       	      10	  10299996 ns/op	     275 B/op	       5 allocs/op
BenchmarkFromChanWorkSweep/factor=8/channels-8       	      10	  10252896 ns/op	     179 B/op	       4 allocs/op
BenchmarkFromChanWorkSweep/factor=8/channels-8       	      10	  10155942 ns/op	     188 B/op	       4 allocs/op
BenchmarkFromChanWorkSweep/factor=8/channels-8       	      10	  10245025 ns/op	     284 B/op	       5 allocs/op
BenchmarkFromChanWorkSweep/factor=8/channels-8       	      10	  10280263 ns/op	     236 B/op	       4 allocs/op
BenchmarkFromChanWorkSweep/factor=8/pipeline-8       	      10	  10999266 ns/op	    1214 B/op	      21 allocs/op
BenchmarkFromChanWorkSweep/factor=8/pipeline-8       	      10	  12046954 ns/op	    1262 B/op	      21 allocs/op
BenchmarkFromChanWorkSweep/factor=8/pipeline-8       	      10	  10910496 ns/op	    1320 B/op	      22 allocs/op
BenchmarkFromChanWorkSweep/factor=8/pipeline-8       	       9	  11190245 ns/op	    1208 B/op	      21 allocs/op
BenchmarkFromChanWorkSweep/factor=8/pipeline-8       	      10	  12225629 ns/op	    1108 B/op	      20 allocs/op
BenchmarkSinkWorkSweep/factor=1/channels-8           	      51	   2227198 ns/op	     162 B/op	       4 allocs/op
BenchmarkSinkWorkSweep/factor=1/channels-8           	      52	   2195067 ns/op	     162 B/op	       4 allocs/op
BenchmarkSinkWorkSweep/factor=1/channels-8           	      49	   2216425 ns/op	     162 B/op	       4 allocs/op
BenchmarkSinkWorkSweep/factor=1/channels-8           	      45	   2230093 ns/op	     168 B/op	       4 allocs/op
BenchmarkSinkWorkSweep/factor=1/channels-8           	      48	   2429602 ns/op	     162 B/op	       4 allocs/op
BenchmarkSinkWorkSweep/factor=1/pipeline-8           	      57	   2065670 ns/op	     661 B/op	      14 allocs/op
BenchmarkSinkWorkSweep/factor=1/pipeline-8           	      57	   2047953 ns/op	     646 B/op	      14 allocs/op
BenchmarkSinkWorkSweep/factor=1/pipeline-8           	      51	   2062871 ns/op	     650 B/op	      14 allocs/op
BenchmarkSinkWorkSweep/factor=1/pipeline-8           	      57	   2052349 ns/op	     653 B/op	      14 allocs/op
BenchmarkSinkWorkSweep/factor=1/pipeline-8           	      57	   2047829 ns/op	     646 B/op	      14 allocs/op
BenchmarkSinkWorkSweep/factor=2/channels-8           	      38	   3425419 ns/op	     162 B/op	       4 allocs/op
BenchmarkSinkWorkSweep/factor=2/channels-8           	      40	   2941627 ns/op	     162 B/op	       4 allocs/op
BenchmarkSinkWorkSweep/factor=2/channels-8           	      37	   2953301 ns/op	     163 B/op	       4 allocs/op
BenchmarkSinkWorkSweep/factor=2/channels-8           	      32	   3289518 ns/op	     163 B/op	       4 allocs/op
BenchmarkSinkWorkSweep/factor=2/channels-8           	      36	   2952271 ns/op	     163 B/op	       4 allocs/op
BenchmarkSinkWorkSweep/factor=2/pipeline-8           	      39	   2806401 ns/op	     649 B/op	      14 allocs/op
BenchmarkSinkWorkSweep/factor=2/pipeline-8           	      42	   2776405 ns/op	     648 B/op	      14 allocs/op
BenchmarkSinkWorkSweep/factor=2/pipeline-8           	      40	   2777006 ns/op	     649 B/op	      14 allocs/op
BenchmarkSinkWorkSweep/factor=2/pipeline-8           	      38	   2785718 ns/op	     649 B/op	      14 allocs/op
BenchmarkSinkWorkSweep/factor=2/pipeline-8           	      39	   2789268 ns/op	     649 B/op	      14 allocs/op
BenchmarkSinkWorkSweep/factor=4/channels-8           	      22	   5396099 ns/op	     165 B/op	       4 allocs/op
BenchmarkSinkWorkSweep/factor=4/channels-8           	      20	   5883831 ns/op	     165 B/op	       4 allocs/op
BenchmarkSinkWorkSweep/factor=4/channels-8           	      22	   5219441 ns/op	     165 B/op	       4 allocs/op
BenchmarkSinkWorkSweep/factor=4/channels-8           	      21	   5127083 ns/op	     174 B/op	       4 allocs/op
BenchmarkSinkWorkSweep/factor=4/channels-8           	      21	   5182510 ns/op	     197 B/op	       4 allocs/op
BenchmarkSinkWorkSweep/factor=4/pipeline-8           	      24	   4623745 ns/op	     683 B/op	      14 allocs/op
BenchmarkSinkWorkSweep/factor=4/pipeline-8           	      25	   4572547 ns/op	     654 B/op	      14 allocs/op
BenchmarkSinkWorkSweep/factor=4/pipeline-8           	      24	   4555549 ns/op	     655 B/op	      14 allocs/op
BenchmarkSinkWorkSweep/factor=4/pipeline-8           	      22	   4655396 ns/op	     656 B/op	      14 allocs/op
BenchmarkSinkWorkSweep/factor=4/pipeline-8           	      24	   4628009 ns/op	     751 B/op	      15 allocs/op
BenchmarkSinkWorkSweep/factor=8/channels-8           	      14	   7337390 ns/op	     209 B/op	       4 allocs/op
BenchmarkSinkWorkSweep/factor=8/channels-8           	      15	   7224347 ns/op	     212 B/op	       4 allocs/op
BenchmarkSinkWorkSweep/factor=8/channels-8           	      14	   7274834 ns/op	     181 B/op	       4 allocs/op
BenchmarkSinkWorkSweep/factor=8/channels-8           	      14	   7268702 ns/op	     168 B/op	       4 allocs/op
BenchmarkSinkWorkSweep/factor=8/channels-8           	      15	   7292053 ns/op	     167 B/op	       4 allocs/op
BenchmarkSinkWorkSweep/factor=8/pipeline-8           	      14	   7732420 ns/op	     714 B/op	      14 allocs/op
BenchmarkSinkWorkSweep/factor=8/pipeline-8           	      14	   7701476 ns/op	     741 B/op	      15 allocs/op
BenchmarkSinkWorkSweep/factor=8/pipeline-8           	      14	   7616821 ns/op	     666 B/op	      14 allocs/op
BenchmarkSinkWorkSweep/factor=8/pipeline-8           	      14	   7609497 ns/op	     666 B/op	      14 allocs/op
BenchmarkSinkWorkSweep/factor=8/pipeline-8           	      14	   7616030 ns/op	     666 B/op	      14 allocs/op
BenchmarkSinkFromChanWorkSweep/factor=1/channels-8   	      45	   2231878 ns/op	     162 B/op	       4 allocs/op
BenchmarkSinkFromChanWorkSweep/factor=1/channels-8   	      50	   2471518 ns/op	     162 B/op	       4 allocs/op
BenchmarkSinkFromChanWorkSweep/factor=1/channels-8   	      54	   2214559 ns/op	     162 B/op	       4 allocs/op
BenchmarkSinkFromChanWorkSweep/factor=1/channels-8   	      50	   2210419 ns/op	     162 B/op	       4 allocs/op
BenchmarkSinkFromChanWorkSweep/factor=1/channels-8   	      49	   2219490 ns/op	     162 B/op	       4 allocs/op
BenchmarkSinkFromChanWorkSweep/factor=1/pipeline-8   	      30	   3660957 ns/op	    1055 B/op	      18 allocs/op
BenchmarkSinkFromChanWorkSweep/factor=1/pipeline-8   	      33	   3419984 ns/op	    1051 B/op	      18 allocs/op
BenchmarkSinkFromChanWorkSweep/factor=1/pipeline-8   	      34	   3546802 ns/op	    1032 B/op	      18 allocs/op
BenchmarkSinkFromChanWorkSweep/factor=1/pipeline-8   	      31	   3567872 ns/op	    1019 B/op	      18 allocs/op
BenchmarkSinkFromChanWorkSweep/factor=1/pipeline-8   	      30	   3464739 ns/op	    1061 B/op	      18 allocs/op
BenchmarkSinkFromChanWorkSweep/factor=2/channels-8   	      37	   3948470 ns/op	     196 B/op	       4 allocs/op
BenchmarkSinkFromChanWorkSweep/factor=2/channels-8   	      40	   2937008 ns/op	     179 B/op	       4 allocs/op
BenchmarkSinkFromChanWorkSweep/factor=2/channels-8   	      40	   2948583 ns/op	     198 B/op	       4 allocs/op
BenchmarkSinkFromChanWorkSweep/factor=2/channels-8   	      40	   2928697 ns/op	     177 B/op	       4 allocs/op
BenchmarkSinkFromChanWorkSweep/factor=2/channels-8   	      40	   2930903 ns/op	     184 B/op	       4 allocs/op
BenchmarkSinkFromChanWorkSweep/factor=2/pipeline-8   	      26	   4218364 ns/op	    1066 B/op	      18 allocs/op
BenchmarkSinkFromChanWorkSweep/factor=2/pipeline-8   	      26	   4275457 ns/op	    1081 B/op	      18 allocs/op
BenchmarkSinkFromChanWorkSweep/factor=2/pipeline-8   	      25	   4289552 ns/op	    1022 B/op	      18 allocs/op
BenchmarkSinkFromChanWorkSweep/factor=2/pipeline-8   	      26	   4244197 ns/op	    1022 B/op	      18 allocs/op
BenchmarkSinkFromChanWorkSweep/factor=2/pipeline-8   	      26	   4255931 ns/op	    1022 B/op	      18 allocs/op
BenchmarkSinkFromChanWorkSweep/factor=4/channels-8   	      20	   6178606 ns/op	     165 B/op	       4 allocs/op
BenchmarkSinkFromChanWorkSweep/factor=4/channels-8   	      21	   5436375 ns/op	     165 B/op	       4 allocs/op
BenchmarkSinkFromChanWorkSweep/factor=4/channels-8   	      21	   5459115 ns/op	     165 B/op	       4 allocs/op
BenchmarkSinkFromChanWorkSweep/factor=4/channels-8   	      21	   5428044 ns/op	     183 B/op	       4 allocs/op
BenchmarkSinkFromChanWorkSweep/factor=4/channels-8   	      20	   6819812 ns/op	     199 B/op	       4 allocs/op
BenchmarkSinkFromChanWorkSweep/factor=4/pipeline-8   	      19	   5654230 ns/op	    1027 B/op	      18 allocs/op
BenchmarkSinkFromChanWorkSweep/factor=4/pipeline-8   	      18	   5744090 ns/op	    1028 B/op	      18 allocs/op
BenchmarkSinkFromChanWorkSweep/factor=4/pipeline-8   	      20	   5709098 ns/op	    1026 B/op	      18 allocs/op
BenchmarkSinkFromChanWorkSweep/factor=4/pipeline-8   	      18	   5702252 ns/op	    1028 B/op	      18 allocs/op
BenchmarkSinkFromChanWorkSweep/factor=4/pipeline-8   	      19	   5731546 ns/op	    1027 B/op	      18 allocs/op
BenchmarkSinkFromChanWorkSweep/factor=8/channels-8   	      15	   9012608 ns/op	     167 B/op	       4 allocs/op
BenchmarkSinkFromChanWorkSweep/factor=8/channels-8   	      15	   7462600 ns/op	     167 B/op	       4 allocs/op
BenchmarkSinkFromChanWorkSweep/factor=8/channels-8   	      15	   7359797 ns/op	     186 B/op	       4 allocs/op
BenchmarkSinkFromChanWorkSweep/factor=8/channels-8   	      14	   7305631 ns/op	     181 B/op	       4 allocs/op
BenchmarkSinkFromChanWorkSweep/factor=8/channels-8   	      15	   7285069 ns/op	     180 B/op	       4 allocs/op
BenchmarkSinkFromChanWorkSweep/factor=8/pipeline-8   	      13	   8497106 ns/op	    1139 B/op	      19 allocs/op
BenchmarkSinkFromChanWorkSweep/factor=8/pipeline-8   	      13	   8481990 ns/op	    1154 B/op	      19 allocs/op
BenchmarkSinkFromChanWorkSweep/factor=8/pipeline-8   	      13	   8543090 ns/op	    1036 B/op	      18 allocs/op
BenchmarkSinkFromChanWorkSweep/factor=8/pipeline-8   	      14	   8516497 ns/op	    1034 B/op	      18 allocs/op
BenchmarkSinkFromChanWorkSweep/factor=8/pipeline-8   	      13	   8536663 ns/op	    1036 B/op	      18 allocs/op
BenchmarkBatchWorkSweep/factor=1/channels-8          	      49	   2154596 ns/op	   33213 B/op	     133 allocs/op
BenchmarkBatchWorkSweep/factor=1/channels-8          	      54	   2103425 ns/op	   33197 B/op	     133 allocs/op
BenchmarkBatchWorkSweep/factor=1/channels-8          	      54	   2111574 ns/op	   33194 B/op	     133 allocs/op
BenchmarkBatchWorkSweep/factor=1/channels-8          	      46	   2265317 ns/op	   33194 B/op	     133 allocs/op
BenchmarkBatchWorkSweep/factor=1/channels-8          	      55	   2120400 ns/op	   33242 B/op	     133 allocs/op
BenchmarkBatchWorkSweep/factor=1/pipeline-8          	      63	   1720513 ns/op	   65520 B/op	     786 allocs/op
BenchmarkBatchWorkSweep/factor=1/pipeline-8          	      68	   1729481 ns/op	   65505 B/op	     786 allocs/op
BenchmarkBatchWorkSweep/factor=1/pipeline-8          	      68	   1739036 ns/op	   65598 B/op	     787 allocs/op
BenchmarkBatchWorkSweep/factor=1/pipeline-8          	      69	   1852196 ns/op	   65506 B/op	     786 allocs/op
BenchmarkBatchWorkSweep/factor=1/pipeline-8          	      52	   2092403 ns/op	   65549 B/op	     786 allocs/op
BenchmarkBatchWorkSweep/factor=2/channels-8          	      39	   2632279 ns/op	   33194 B/op	     133 allocs/op
BenchmarkBatchWorkSweep/factor=2/channels-8          	      45	   2533258 ns/op	   33194 B/op	     133 allocs/op
BenchmarkBatchWorkSweep/factor=2/channels-8          	      45	   2529224 ns/op	   33247 B/op	     133 allocs/op
BenchmarkBatchWorkSweep/factor=2/channels-8          	      46	   2557755 ns/op	   33194 B/op	     133 allocs/op
BenchmarkBatchWorkSweep/factor=2/channels-8          	      46	   2531248 ns/op	   33284 B/op	     133 allocs/op
BenchmarkBatchWorkSweep/factor=2/pipeline-8          	      52	   2187754 ns/op	   65503 B/op	     786 allocs/op
BenchmarkBatchWorkSweep/factor=2/pipeline-8          	      46	   2230072 ns/op	   65640 B/op	     787 allocs/op
BenchmarkBatchWorkSweep/factor=2/pipeline-8          	      44	   2409030 ns/op	   65486 B/op	     786 allocs/op
BenchmarkBatchWorkSweep/factor=2/pipeline-8          	      52	   2234462 ns/op	   65488 B/op	     786 allocs/op
BenchmarkBatchWorkSweep/factor=2/pipeline-8          	      52	   2219640 ns/op	   65488 B/op	     786 allocs/op
BenchmarkBatchWorkSweep/factor=4/channels-8          	      30	   3652286 ns/op	   33195 B/op	     133 allocs/op
BenchmarkBatchWorkSweep/factor=4/channels-8          	      28	   3617551 ns/op	   33196 B/op	     133 allocs/op
BenchmarkBatchWorkSweep/factor=4/channels-8          	      32	   3546312 ns/op	   33195 B/op	     133 allocs/op
BenchmarkBatchWorkSweep/factor=4/channels-8          	      31	   3639439 ns/op	   33195 B/op	     133 allocs/op
BenchmarkBatchWorkSweep/factor=4/channels-8          	      32	   3611077 ns/op	   33195 B/op	     133 allocs/op
BenchmarkBatchWorkSweep/factor=4/pipeline-8          	      30	   4080800 ns/op	   65616 B/op	     787 allocs/op
BenchmarkBatchWorkSweep/factor=4/pipeline-8          	      34	   3318681 ns/op	   65594 B/op	     787 allocs/op
BenchmarkBatchWorkSweep/factor=4/pipeline-8          	      31	   3580499 ns/op	   65491 B/op	     786 allocs/op
BenchmarkBatchWorkSweep/factor=4/pipeline-8          	      31	   3376371 ns/op	   65559 B/op	     786 allocs/op
BenchmarkBatchWorkSweep/factor=4/pipeline-8          	      30	   3475707 ns/op	   65577 B/op	     787 allocs/op
BenchmarkBatchWorkSweep/factor=8/channels-8          	      19	   5843996 ns/op	   33197 B/op	     133 allocs/op
BenchmarkBatchWorkSweep/factor=8/channels-8          	      19	   5801318 ns/op	   33197 B/op	     133 allocs/op
BenchmarkBatchWorkSweep/factor=8/channels-8          	      18	   5778926 ns/op	   33198 B/op	     133 allocs/op
BenchmarkBatchWorkSweep/factor=8/channels-8          	      19	   5819169 ns/op	   33197 B/op	     133 allocs/op
BenchmarkBatchWorkSweep/factor=8/channels-8          	      19	   6829643 ns/op	   33197 B/op	     133 allocs/op
BenchmarkBatchWorkSweep/factor=8/pipeline-8          	      20	   5696156 ns/op	   65533 B/op	     786 allocs/op
BenchmarkBatchWorkSweep/factor=8/pipeline-8          	      19	   5800618 ns/op	   65499 B/op	     786 allocs/op
BenchmarkBatchWorkSweep/factor=8/pipeline-8          	      19	   5773237 ns/op	   65639 B/op	     787 allocs/op
BenchmarkBatchWorkSweep/factor=8/pipeline-8          	      21	   5747772 ns/op	   65614 B/op	     787 allocs/op
BenchmarkBatchWorkSweep/factor=8/pipeline-8          	      18	   5764118 ns/op	   65501 B/op	     786 allocs/op
BenchmarkBatchChanWorkSweep/factor=1/channels-8      	      25	   4501862 ns/op	   14508 B/op	     132 allocs/op
BenchmarkBatchChanWorkSweep/factor=1/channels-8      	      25	   4461000 ns/op	   14543 B/op	     132 allocs/op
BenchmarkBatchChanWorkSweep/factor=1/channels-8      	      22	   5472123 ns/op	   14530 B/op	     132 allocs/op
BenchmarkBatchChanWorkSweep/factor=1/channels-8      	      26	   4481689 ns/op	   14508 B/op	     132 allocs/op
BenchmarkBatchChanWorkSweep/factor=1/channels-8      	      26	   4473833 ns/op	   14508 B/op	     132 allocs/op
BenchmarkBatchChanWorkSweep/factor=1/pipeline-8      	      26	   3910308 ns/op	   15369 B/op	     146 allocs/op
BenchmarkBatchChanWorkSweep/factor=1/pipeline-8      	      28	   3887500 ns/op	   15360 B/op	     146 allocs/op
BenchmarkBatchChanWorkSweep/factor=1/pipeline-8      	      28	   3775165 ns/op	   15346 B/op	     146 allocs/op
BenchmarkBatchChanWorkSweep/factor=1/pipeline-8      	      31	   3757481 ns/op	   15348 B/op	     146 allocs/op
BenchmarkBatchChanWorkSweep/factor=1/pipeline-8      	      28	   3791091 ns/op	   15473 B/op	     147 allocs/op
BenchmarkBatchChanWorkSweep/factor=2/channels-8      	      19	   6268969 ns/op	   14514 B/op	     132 allocs/op
BenchmarkBatchChanWorkSweep/factor=2/channels-8      	      20	   5431183 ns/op	   14538 B/op	     132 allocs/op
BenchmarkBatchChanWorkSweep/factor=2/channels-8      	      21	   5401022 ns/op	   14509 B/op	     132 allocs/op
BenchmarkBatchChanWorkSweep/factor=2/channels-8      	      21	   5361649 ns/op	   14509 B/op	     132 allocs/op
BenchmarkBatchChanWorkSweep/factor=2/channels-8      	      21	   5400498 ns/op	   14509 B/op	     132 allocs/op
BenchmarkBatchChanWorkSweep/factor=2/pipeline-8      	      22	   5083152 ns/op	   15452 B/op	     147 allocs/op
BenchmarkBatchChanWorkSweep/factor=2/pipeline-8      	      25	   4557987 ns/op	   15310 B/op	     146 allocs/op
BenchmarkBatchChanWorkSweep/factor=2/pipeline-8      	      24	   4679736 ns/op	   15311 B/op	     146 allocs/op
BenchmarkBatchChanWorkSweep/factor=2/pipeline-8      	      24	   4689471 ns/op	   15339 B/op	     146 allocs/op
BenchmarkBatchChanWorkSweep/factor=2/pipeline-8      	      24	   5012543 ns/op	   15443 B/op	     147 allocs/op
BenchmarkBatchChanWorkSweep/factor=4/channels-8      	      14	   7215071 ns/op	   14512 B/op	     132 allocs/op
BenchmarkBatchChanWorkSweep/factor=4/channels-8      	      15	   6988408 ns/op	   14511 B/op	     132 allocs/op
BenchmarkBatchChanWorkSweep/factor=4/channels-8      	      15	   6954428 ns/op	   14524 B/op	     132 allocs/op
BenchmarkBatchChanWorkSweep/factor=4/channels-8      	      15	   6976264 ns/op	   14530 B/op	     132 allocs/op
BenchmarkBatchChanWorkSweep/factor=4/channels-8      	      16	   6891682 ns/op	   14565 B/op	     132 allocs/op
BenchmarkBatchChanWorkSweep/factor=4/pipeline-8      	      18	   6100776 ns/op	   15455 B/op	     147 allocs/op
BenchmarkBatchChanWorkSweep/factor=4/pipeline-8      	      18	   6132650 ns/op	   15316 B/op	     146 allocs/op
BenchmarkBatchChanWorkSweep/factor=4/pipeline-8      	      18	   6715461 ns/op	   15417 B/op	     147 allocs/op
BenchmarkBatchChanWorkSweep/factor=4/pipeline-8      	      18	   6044933 ns/op	   15316 B/op	     146 allocs/op
BenchmarkBatchChanWorkSweep/factor=4/pipeline-8      	      18	   6083912 ns/op	   15316 B/op	     146 allocs/op
BenchmarkBatchChanWorkSweep/factor=8/channels-8      	      10	  10237733 ns/op	   14515 B/op	     132 allocs/op
BenchmarkBatchChanWorkSweep/factor=8/channels-8      	      12	  10021472 ns/op	   14553 B/op	     132 allocs/op
BenchmarkBatchChanWorkSweep/factor=8/channels-8      	      12	   9866285 ns/op	   14513 B/op	     132 allocs/op
BenchmarkBatchChanWorkSweep/factor=8/channels-8      	      12	   9980576 ns/op	   14545 B/op	     132 allocs/op
BenchmarkBatchChanWorkSweep/factor=8/channels-8      	      10	  10021137 ns/op	   14524 B/op	     132 allocs/op
BenchmarkBatchChanWorkSweep/factor=8/pipeline-8      	      12	  10102916 ns/op	   15526 B/op	     148 allocs/op
BenchmarkBatchChanWorkSweep/factor=8/pipeline-8      	      12	   8886948 ns/op	   15350 B/op	     146 allocs/op
BenchmarkBatchChanWorkSweep/factor=8/pipeline-8      	      12	   8933219 ns/op	   15550 B/op	     148 allocs/op
BenchmarkBatchChanWorkSweep/factor=8/pipeline-8      	      13	   8925898 ns/op	   15331 B/op	     146 allocs/op
BenchmarkBatchChanWorkSweep/factor=8/pipeline-8      	      12	   8853611 ns/op	   15326 B/op	     146 allocs/op
BenchmarkSplitByWorkSweep/factor=1/channels-8        	      12	  10800292 ns/op	     489 B/op	      12 allocs/op
BenchmarkSplitByWorkSweep/factor=1/channels-8        	      10	  10065188 ns/op	     491 B/op	      12 allocs/op
BenchmarkSplitByWorkSweep/factor=1/channels-8        	      10	  10760008 ns/op	     644 B/op	      13 allocs/op
BenchmarkSplitByWorkSweep/factor=1/channels-8        	      10	  11653517 ns/op	     539 B/op	      12 allocs/op
BenchmarkSplitByWorkSweep/factor=1/channels-8        	      10	  10974025 ns/op	     491 B/op	      12 allocs/op
BenchmarkSplitByWorkSweep/factor=1/pipeline-8        	       9	  11933343 ns/op	  197928 B/op	    4122 allocs/op
BenchmarkSplitByWorkSweep/factor=1/pipeline-8        	       9	  11878847 ns/op	  197928 B/op	    4122 allocs/op
BenchmarkSplitByWorkSweep/factor=1/pipeline-8        	       9	  11607217 ns/op	  198248 B/op	    4125 allocs/op
BenchmarkSplitByWorkSweep/factor=1/pipeline-8        	       9	  11323588 ns/op	  198270 B/op	    4125 allocs/op
BenchmarkSplitByWorkSweep/factor=1/pipeline-8        	       9	  11347560 ns/op	  197992 B/op	    4123 allocs/op
BenchmarkSplitByWorkSweep/factor=2/channels-8        	       9	  12553773 ns/op	     492 B/op	      12 allocs/op
BenchmarkSplitByWorkSweep/factor=2/channels-8        	      10	  11026488 ns/op	     510 B/op	      12 allocs/op
BenchmarkSplitByWorkSweep/factor=2/channels-8        	       9	  11806121 ns/op	     492 B/op	      12 allocs/op
BenchmarkSplitByWorkSweep/factor=2/channels-8        	      10	  11800437 ns/op	     520 B/op	      12 allocs/op
BenchmarkSplitByWorkSweep/factor=2/channels-8        	      10	  10872125 ns/op	     596 B/op	      13 allocs/op
BenchmarkSplitByWorkSweep/factor=2/pipeline-8        	       8	  12789833 ns/op	  198078 B/op	    4123 allocs/op
BenchmarkSplitByWorkSweep/factor=2/pipeline-8        	       8	  12683739 ns/op	  197934 B/op	    4122 allocs/op
BenchmarkSplitByWorkSweep/factor=2/pipeline-8        	       8	  12624911 ns/op	  197934 B/op	    4122 allocs/op
BenchmarkSplitByWorkSweep/factor=2/pipeline-8        	       9	  11981769 ns/op	  197928 B/op	    4122 allocs/op
BenchmarkSplitByWorkSweep/factor=2/pipeline-8        	       9	  12178273 ns/op	  198067 B/op	    4123 allocs/op
BenchmarkSplitByWorkSweep/factor=4/channels-8        	       8	  12823838 ns/op	     530 B/op	      12 allocs/op
BenchmarkSplitByWorkSweep/factor=4/channels-8        	       8	  13516479 ns/op	     638 B/op	      13 allocs/op
BenchmarkSplitByWorkSweep/factor=4/channels-8        	       8	  12731958 ns/op	     494 B/op	      12 allocs/op
BenchmarkSplitByWorkSweep/factor=4/channels-8        	       8	  13432964 ns/op	     494 B/op	      12 allocs/op
BenchmarkSplitByWorkSweep/factor=4/channels-8        	       8	  12670661 ns/op	     542 B/op	      12 allocs/op
BenchmarkSplitByWorkSweep/factor=4/pipeline-8        	       8	  15416625 ns/op	  198234 B/op	    4125 allocs/op
BenchmarkSplitByWorkSweep/factor=4/pipeline-8        	       8	  13370036 ns/op	  198030 B/op	    4123 allocs/op
BenchmarkSplitByWorkSweep/factor=4/pipeline-8        	       8	  13564192 ns/op	  197970 B/op	    4122 allocs/op
BenchmarkSplitByWorkSweep/factor=4/pipeline-8        	       8	  13413047 ns/op	  197994 B/op	    4123 allocs/op
BenchmarkSplitByWorkSweep/factor=4/pipeline-8        	       8	  13352005 ns/op	  197970 B/op	    4122 allocs/op
BenchmarkSplitByWorkSweep/factor=8/channels-8        	       7	  15914393 ns/op	     496 B/op	      12 allocs/op
BenchmarkSplitByWorkSweep/factor=8/channels-8        	       7	  15661768 ns/op	     496 B/op	      12 allocs/op
BenchmarkSplitByWorkSweep/factor=8/channels-8        	       7	  16056792 ns/op	     496 B/op	      12 allocs/op
BenchmarkSplitByWorkSweep/factor=8/channels-8        	       7	  15525250 ns/op	     496 B/op	      12 allocs/op
BenchmarkSplitByWorkSweep/factor=8/channels-8        	       7	  15745405 ns/op	     674 B/op	      14 allocs/op
BenchmarkSplitByWorkSweep/factor=8/pipeline-8        	       7	  18819250 ns/op	  198448 B/op	    4127 allocs/op
BenchmarkSplitByWorkSweep/factor=8/pipeline-8        	       7	  15781476 ns/op	  197940 B/op	    4122 allocs/op
BenchmarkSplitByWorkSweep/factor=8/pipeline-8        	       7	  15867482 ns/op	  198022 B/op	    4123 allocs/op
BenchmarkSplitByWorkSweep/factor=8/pipeline-8        	       7	  15149905 ns/op	  197981 B/op	    4122 allocs/op
BenchmarkSplitByWorkSweep/factor=8/pipeline-8        	       7	  15392393 ns/op	  198310 B/op	    4126 allocs/op
BenchmarkOverheadStepSweep/steps=1/channels-8        	      21	   5303210 ns/op	     219 B/op	       6 allocs/op
BenchmarkOverheadStepSweep/steps=1/channels-8        	      26	   4341253 ns/op	     237 B/op	       6 allocs/op
BenchmarkOverheadStepSweep/steps=1/channels-8        	      25	   4350550 ns/op	     204 B/op	       6 allocs/op
BenchmarkOverheadStepSweep/steps=1/channels-8        	      26	   4355383 ns/op	     211 B/op	       6 allocs/op
BenchmarkOverheadStepSweep/steps=1/channels-8        	      27	   4332472 ns/op	     204 B/op	       6 allocs/op
BenchmarkOverheadStepSweep/steps=1/pipeline-8        	      26	   4317651 ns/op	     880 B/op	      17 allocs/op
BenchmarkOverheadStepSweep/steps=1/pipeline-8        	      24	   4387325 ns/op	     795 B/op	      16 allocs/op
BenchmarkOverheadStepSweep/steps=1/pipeline-8        	      26	   4397157 ns/op	     766 B/op	      16 allocs/op
BenchmarkOverheadStepSweep/steps=1/pipeline-8        	      24	   4983781 ns/op	     767 B/op	      16 allocs/op
BenchmarkOverheadStepSweep/steps=1/pipeline-8        	      27	   4315043 ns/op	     794 B/op	      16 allocs/op
BenchmarkOverheadStepSweep/steps=2/channels-8        	      14	   7564381 ns/op	     344 B/op	      11 allocs/op
BenchmarkOverheadStepSweep/steps=2/channels-8        	      14	   7413634 ns/op	     371 B/op	      11 allocs/op
BenchmarkOverheadStepSweep/steps=2/channels-8        	      15	   7499481 ns/op	     356 B/op	      11 allocs/op
BenchmarkOverheadStepSweep/steps=2/channels-8        	      14	   7446396 ns/op	     378 B/op	      11 allocs/op
BenchmarkOverheadStepSweep/steps=2/channels-8        	      14	   7374506 ns/op	     433 B/op	      12 allocs/op
BenchmarkOverheadStepSweep/steps=2/pipeline-8        	      14	   8418815 ns/op	     924 B/op	      18 allocs/op
BenchmarkOverheadStepSweep/steps=2/pipeline-8        	      13	   7840891 ns/op	    1003 B/op	      19 allocs/op
BenchmarkOverheadStepSweep/steps=2/pipeline-8        	      14	   7856592 ns/op	     945 B/op	      18 allocs/op
BenchmarkOverheadStepSweep/steps=2/pipeline-8        	      14	   7765030 ns/op	     890 B/op	      18 allocs/op
BenchmarkOverheadStepSweep/steps=2/pipeline-8        	      13	   7789664 ns/op	     892 B/op	      18 allocs/op
BenchmarkOverheadStepSweep/steps=4/channels-8        	       8	  14349427 ns/op	     622 B/op	      21 allocs/op
BenchmarkOverheadStepSweep/steps=4/channels-8        	       8	  13425385 ns/op	     622 B/op	      21 allocs/op
BenchmarkOverheadStepSweep/steps=4/channels-8        	       8	  13660802 ns/op	     646 B/op	      21 allocs/op
BenchmarkOverheadStepSweep/steps=4/channels-8        	       8	  13687432 ns/op	     862 B/op	      23 allocs/op
BenchmarkOverheadStepSweep/steps=4/channels-8        	       8	  14090047 ns/op	     694 B/op	      21 allocs/op
BenchmarkOverheadStepSweep/steps=4/pipeline-8        	       7	  14797756 ns/op	    1689 B/op	      28 allocs/op
BenchmarkOverheadStepSweep/steps=4/pipeline-8        	       7	  15157667 ns/op	    1483 B/op	      26 allocs/op
BenchmarkOverheadStepSweep/steps=4/pipeline-8        	       7	  15060476 ns/op	    1168 B/op	      22 allocs/op
BenchmarkOverheadStepSweep/steps=4/pipeline-8        	       7	  15180911 ns/op	    1140 B/op	      22 allocs/op
BenchmarkOverheadStepSweep/steps=4/pipeline-8        	       7	  15168357 ns/op	    1209 B/op	      23 allocs/op
BenchmarkOverheadStepSweep/steps=8/channels-8        	       3	  34981500 ns/op	    1189 B/op	      41 allocs/op
BenchmarkOverheadStepSweep/steps=8/channels-8        	       4	  27873469 ns/op	    1276 B/op	      42 allocs/op
BenchmarkOverheadStepSweep/steps=8/channels-8        	       4	  29440552 ns/op	    1468 B/op	      44 allocs/op
BenchmarkOverheadStepSweep/steps=8/channels-8        	       4	  28864177 ns/op	    1180 B/op	      41 allocs/op
BenchmarkOverheadStepSweep/steps=8/channels-8        	       4	  28142834 ns/op	    1396 B/op	      43 allocs/op
BenchmarkOverheadStepSweep/steps=8/pipeline-8        	       4	  30428948 ns/op	    2540 B/op	      40 allocs/op
BenchmarkOverheadStepSweep/steps=8/pipeline-8        	       4	  29902834 ns/op	    2468 B/op	      39 allocs/op
BenchmarkOverheadStepSweep/steps=8/pipeline-8        	       4	  30304844 ns/op	    1628 B/op	      30 allocs/op
BenchmarkOverheadStepSweep/steps=8/pipeline-8        	       4	  30751709 ns/op	    2396 B/op	      38 allocs/op
BenchmarkOverheadStepSweep/steps=8/pipeline-8        	       4	  31000792 ns/op	    1652 B/op	      31 allocs/op
BenchmarkOverheadStepSweep/steps=16/channels-8       	       2	  63862208 ns/op	    4504 B/op	     104 allocs/op
BenchmarkOverheadStepSweep/steps=16/channels-8       	       2	  59737062 ns/op	    2776 B/op	      86 allocs/op
BenchmarkOverheadStepSweep/steps=16/channels-8       	       2	  67964582 ns/op	    3592 B/op	      95 allocs/op
BenchmarkOverheadStepSweep/steps=16/channels-8       	       2	  64782250 ns/op	    4504 B/op	     104 allocs/op
BenchmarkOverheadStepSweep/steps=16/channels-8       	       1	 115544250 ns/op	    2352 B/op	      82 allocs/op
BenchmarkOverheadStepSweep/steps=16/pipeline-8       	       2	  68178146 ns/op	    3096 B/op	      52 allocs/op
BenchmarkOverheadStepSweep/steps=16/pipeline-8       	       2	  66226083 ns/op	    3144 B/op	      53 allocs/op
BenchmarkOverheadStepSweep/steps=16/pipeline-8       	       2	  66245333 ns/op	    4488 B/op	      67 allocs/op
BenchmarkOverheadStepSweep/steps=16/pipeline-8       	       2	  66824958 ns/op	    2616 B/op	      47 allocs/op
BenchmarkOverheadStepSweep/steps=16/pipeline-8       	       2	  64308584 ns/op	    2856 B/op	      50 allocs/op
PASS
ok  	github.com/askiada/go-pipeline/v2/pkg/pipeline	103.617s
```

## Channel vs pipeline diff

Diff is percent vs channels (medians from count=5).

| Benchmark | Channels ns/op | Pipeline ns/op | Diff |
| --- | ---: | ---: | ---: |
| Batch/items=1k/conc=1 | 546,847.0 | 457,174.0 | -16.4% |
| Batch/items=1k/conc=4 | 764,933.0 | 676,890.0 | -11.5% |
| BatchChan/items=1k/conc=1 | 1,139,718.0 | 953,314.0 | -16.4% |
| BatchChan/items=1k/conc=4 | 1,146,359.0 | 968,565.0 | -15.5% |
| BatchChanWorkSweep/factor=1 | 4,481,689.0 | 3,791,091.0 | -15.4% |
| BatchChanWorkSweep/factor=2 | 5,401,022.0 | 4,689,471.0 | -13.2% |
| BatchChanWorkSweep/factor=4 | 6,976,264.0 | 6,100,776.0 | -12.5% |
| BatchChanWorkSweep/factor=8 | 10,021,137.0 | 8,925,898.0 | -10.9% |
| BatchWorkSweep/factor=1 | 2,120,400.0 | 1,739,036.0 | -18.0% |
| BatchWorkSweep/factor=2 | 2,533,258.0 | 2,230,072.0 | -12.0% |
| BatchWorkSweep/factor=4 | 3,617,551.0 | 3,475,707.0 | -3.9% |
| BatchWorkSweep/factor=8 | 5,819,169.0 | 5,764,118.0 | -0.9% |
| FromChan/items=1k/conc=1 | 1,100,091.0 | 1,459,564.0 | +32.7% |
| FromChan/items=1k/conc=4 | 1,871,559.0 | 1,539,088.0 | -17.8% |
| FromChanWorkSweep/factor=1 | 4,381,358.0 | 5,585,425.0 | +27.5% |
| FromChanWorkSweep/factor=2 | 5,105,103.0 | 6,314,961.0 | +23.7% |
| FromChanWorkSweep/factor=4 | 6,724,257.0 | 7,935,854.0 | +18.0% |
| FromChanWorkSweep/factor=8 | 10,252,896.0 | 11,190,245.0 | +9.1% |
| OneToMany/items=1k/conc=1 | 1,510,755.0 | 1,654,583.0 | +9.5% |
| OneToMany/items=1k/conc=4 | 2,691,010.0 | 2,518,670.0 | -6.4% |
| OneToManyWorkSweep/factor=1 | 6,079,729.0 | 6,514,653.0 | +7.2% |
| OneToManyWorkSweep/factor=2 | 6,788,750.0 | 7,247,648.0 | +6.8% |
| OneToManyWorkSweep/factor=4 | 8,324,458.0 | 8,285,205.0 | -0.5% |
| OneToManyWorkSweep/factor=8 | 10,302,746.0 | 10,809,113.0 | +4.9% |
| OneToOne/items=1k/conc=1 | 1,091,109.0 | 1,104,731.0 | +1.2% |
| OneToOne/items=1k/conc=4 | 1,712,674.0 | 1,761,642.0 | +2.9% |
| OneToOneOrZero/items=1k/conc=1 | 778,165.0 | 779,346.0 | +0.2% |
| OneToOneOrZero/items=1k/conc=4 | 1,295,378.0 | 1,249,756.0 | -3.5% |
| OneToOneOrZeroWorkSweep/factor=1 | 3,133,297.0 | 3,076,216.0 | -1.8% |
| OneToOneOrZeroWorkSweep/factor=2 | 3,566,064.0 | 3,521,077.0 | -1.3% |
| OneToOneOrZeroWorkSweep/factor=4 | 4,487,102.0 | 4,344,764.0 | -3.2% |
| OneToOneOrZeroWorkSweep/factor=8 | 6,192,980.0 | 5,944,019.0 | -4.0% |
| OverheadStepSweep/steps=1 | 4,350,550.0 | 4,387,325.0 | +0.8% |
| OverheadStepSweep/steps=16 | 64,782,250.0 | 66,245,333.0 | +2.3% |
| OverheadStepSweep/steps=2 | 7,446,396.0 | 7,840,891.0 | +5.3% |
| OverheadStepSweep/steps=4 | 13,687,432.0 | 15,157,667.0 | +10.7% |
| OverheadStepSweep/steps=8 | 28,864,177.0 | 30,428,948.0 | +5.4% |
| OverheadWorkSweep/factor=1 | 4,386,397.0 | 4,367,493.0 | -0.4% |
| OverheadWorkSweep/factor=2 | 5,122,829.0 | 5,178,528.0 | +1.1% |
| OverheadWorkSweep/factor=4 | 6,714,683.0 | 6,678,727.0 | -0.5% |
| OverheadWorkSweep/factor=8 | 10,266,521.0 | 9,582,208.0 | -6.7% |
| Sink/items=1k/conc=1 | 573,920.0 | 519,295.0 | -9.5% |
| Sink/items=1k/conc=4 | 836,372.0 | 802,256.0 | -4.1% |
| SinkFromChan/items=1k/conc=1 | 575,748.0 | 872,281.0 | +51.5% |
| SinkFromChan/items=1k/conc=4 | 840,388.0 | 1,236,960.0 | +47.2% |
| SinkFromChanWorkSweep/factor=1 | 2,219,490.0 | 3,546,802.0 | +59.8% |
| SinkFromChanWorkSweep/factor=2 | 2,937,008.0 | 4,255,931.0 | +44.9% |
| SinkFromChanWorkSweep/factor=4 | 5,459,115.0 | 5,709,098.0 | +4.6% |
| SinkFromChanWorkSweep/factor=8 | 7,359,797.0 | 8,516,497.0 | +15.7% |
| SinkWorkSweep/factor=1 | 2,227,198.0 | 2,052,349.0 | -7.9% |
| SinkWorkSweep/factor=2 | 2,953,301.0 | 2,785,718.0 | -5.7% |
| SinkWorkSweep/factor=4 | 5,219,441.0 | 4,623,745.0 | -11.4% |
| SinkWorkSweep/factor=8 | 7,274,834.0 | 7,616,821.0 | +4.7% |
| SplitBy/items=1k/conc=1 | 2,761,454.0 | 2,969,739.0 | +7.5% |
| SplitBy/items=1k/conc=4 | 3,383,023.0 | 2,800,675.0 | -17.2% |
| SplitByWorkSweep/factor=1 | 10,800,292.0 | 11,607,217.0 | +7.5% |
| SplitByWorkSweep/factor=2 | 11,800,437.0 | 12,624,911.0 | +7.0% |
| SplitByWorkSweep/factor=4 | 12,823,838.0 | 13,413,047.0 | +4.6% |
| SplitByWorkSweep/factor=8 | 15,745,405.0 | 15,781,476.0 | +0.2% |
| SplitMerge/items=1k/conc=1 | 4,814,665.0 | 5,231,385.0 | +8.7% |
| SplitMerge/items=1k/conc=4 | 6,764,777.0 | 5,208,311.0 | -23.0% |
| TwoStage/items=1k/conc=1 | 1,882,163.0 | 2,008,245.0 | +6.7% |
| TwoStage/items=1k/conc=4 | 3,477,358.0 | 2,891,704.0 | -16.8% |
