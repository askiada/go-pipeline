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
Notes: Default GOMAXPROCS. Build time excluded; run time only. All cases use realisticWork; work sweep uses factors 1, 2, 4, 8.

goos: darwin
goarch: amd64
pkg: github.com/askiada/go-pipeline/v2/pkg/pipeline
cpu: VirtualApple @ 2.50GHz
BenchmarkOneToOne/items=1k/conc=1/channels-8         	     100	   1153936 ns/op	     299 B/op	       4 allocs/op
BenchmarkOneToOne/items=1k/conc=1/channels-8         	      91	   1124611 ns/op	     192 B/op	       4 allocs/op
BenchmarkOneToOne/items=1k/conc=1/channels-8         	     109	   1095278 ns/op	     206 B/op	       4 allocs/op
BenchmarkOneToOne/items=1k/conc=1/channels-8         	     104	   1108432 ns/op	     216 B/op	       4 allocs/op
BenchmarkOneToOne/items=1k/conc=1/channels-8         	     108	   1085645 ns/op	     201 B/op	       4 allocs/op
BenchmarkOneToOne/items=1k/conc=1/pipeline-8         	      99	   1142077 ns/op	     896 B/op	      16 allocs/op
BenchmarkOneToOne/items=1k/conc=1/pipeline-8         	      96	   1195057 ns/op	     843 B/op	      16 allocs/op
BenchmarkOneToOne/items=1k/conc=1/pipeline-8         	     100	   1143562 ns/op	     827 B/op	      16 allocs/op
BenchmarkOneToOne/items=1k/conc=1/pipeline-8         	      92	   1145041 ns/op	     807 B/op	      16 allocs/op
BenchmarkOneToOne/items=1k/conc=1/pipeline-8         	      97	   1140466 ns/op	     772 B/op	      16 allocs/op
BenchmarkOneToOne/items=1k/conc=4/channels-8         	      69	   1915278 ns/op	     470 B/op	       7 allocs/op
BenchmarkOneToOne/items=1k/conc=4/channels-8         	      73	   1894616 ns/op	     610 B/op	       8 allocs/op
BenchmarkOneToOne/items=1k/conc=4/channels-8         	      68	   1721423 ns/op	     584 B/op	       8 allocs/op
BenchmarkOneToOne/items=1k/conc=4/channels-8         	      68	   1891517 ns/op	     541 B/op	       8 allocs/op
BenchmarkOneToOne/items=1k/conc=4/channels-8         	      75	   2157058 ns/op	     403 B/op	       7 allocs/op
BenchmarkOneToOne/items=1k/conc=4/pipeline-8         	      63	   1731811 ns/op	    2323 B/op	      33 allocs/op
BenchmarkOneToOne/items=1k/conc=4/pipeline-8         	      68	   1773895 ns/op	    2252 B/op	      32 allocs/op
BenchmarkOneToOne/items=1k/conc=4/pipeline-8         	      61	   1779444 ns/op	    2134 B/op	      31 allocs/op
BenchmarkOneToOne/items=1k/conc=4/pipeline-8         	      66	   2087609 ns/op	    2200 B/op	      31 allocs/op
BenchmarkOneToOne/items=1k/conc=4/pipeline-8         	      64	   1899192 ns/op	    2294 B/op	      32 allocs/op
BenchmarkOneToOneOrZero/items=1k/conc=1/channels-8   	     130	    852821 ns/op	     185 B/op	       4 allocs/op
BenchmarkOneToOneOrZero/items=1k/conc=1/channels-8   	     140	    779198 ns/op	     168 B/op	       4 allocs/op
BenchmarkOneToOneOrZero/items=1k/conc=1/channels-8   	     152	    803221 ns/op	     187 B/op	       4 allocs/op
BenchmarkOneToOneOrZero/items=1k/conc=1/channels-8   	     148	    777868 ns/op	     168 B/op	       4 allocs/op
BenchmarkOneToOneOrZero/items=1k/conc=1/channels-8   	     150	    788681 ns/op	     173 B/op	       4 allocs/op
BenchmarkOneToOneOrZero/items=1k/conc=1/pipeline-8   	     132	    785520 ns/op	     807 B/op	      16 allocs/op
BenchmarkOneToOneOrZero/items=1k/conc=1/pipeline-8   	     133	    853330 ns/op	     769 B/op	      16 allocs/op
BenchmarkOneToOneOrZero/items=1k/conc=1/pipeline-8   	     130	    873399 ns/op	     779 B/op	      16 allocs/op
BenchmarkOneToOneOrZero/items=1k/conc=1/pipeline-8   	     140	    845460 ns/op	     754 B/op	      16 allocs/op
BenchmarkOneToOneOrZero/items=1k/conc=1/pipeline-8   	     145	    813372 ns/op	     754 B/op	      16 allocs/op
BenchmarkOneToOneOrZero/items=1k/conc=4/channels-8   	      87	   1448826 ns/op	     383 B/op	       7 allocs/op
BenchmarkOneToOneOrZero/items=1k/conc=4/channels-8   	      92	   1476515 ns/op	     431 B/op	       7 allocs/op
BenchmarkOneToOneOrZero/items=1k/conc=4/channels-8   	      88	   1395634 ns/op	     366 B/op	       7 allocs/op
BenchmarkOneToOneOrZero/items=1k/conc=4/channels-8   	      98	   1506164 ns/op	     412 B/op	       7 allocs/op
BenchmarkOneToOneOrZero/items=1k/conc=4/channels-8   	      94	   1123174 ns/op	     393 B/op	       7 allocs/op
BenchmarkOneToOneOrZero/items=1k/conc=4/pipeline-8   	      81	   1284078 ns/op	    2228 B/op	      32 allocs/op
BenchmarkOneToOneOrZero/items=1k/conc=4/pipeline-8   	      85	   1258928 ns/op	    2132 B/op	      31 allocs/op
BenchmarkOneToOneOrZero/items=1k/conc=4/pipeline-8   	      94	   1252208 ns/op	    2234 B/op	      32 allocs/op
BenchmarkOneToOneOrZero/items=1k/conc=4/pipeline-8   	      85	   1293193 ns/op	    2177 B/op	      31 allocs/op
BenchmarkOneToOneOrZero/items=1k/conc=4/pipeline-8   	      85	   1248070 ns/op	    2228 B/op	      31 allocs/op
BenchmarkOneToMany/items=1k/conc=1/channels-8        	      76	   1506259 ns/op	     169 B/op	       4 allocs/op
BenchmarkOneToMany/items=1k/conc=1/channels-8        	      75	   1541758 ns/op	     169 B/op	       4 allocs/op
BenchmarkOneToMany/items=1k/conc=1/channels-8        	      74	   1502061 ns/op	     169 B/op	       4 allocs/op
BenchmarkOneToMany/items=1k/conc=1/channels-8        	      68	   1501856 ns/op	     176 B/op	       4 allocs/op
BenchmarkOneToMany/items=1k/conc=1/channels-8        	      72	   1535155 ns/op	     190 B/op	       4 allocs/op
BenchmarkOneToMany/items=1k/conc=1/pipeline-8        	      69	   1767562 ns/op	   17141 B/op	    1040 allocs/op
BenchmarkOneToMany/items=1k/conc=1/pipeline-8        	      66	   1760857 ns/op	   17209 B/op	    1040 allocs/op
BenchmarkOneToMany/items=1k/conc=1/pipeline-8        	      68	   1679099 ns/op	   17141 B/op	    1040 allocs/op
BenchmarkOneToMany/items=1k/conc=1/pipeline-8        	      67	   1694162 ns/op	   17141 B/op	    1040 allocs/op
BenchmarkOneToMany/items=1k/conc=1/pipeline-8        	      67	   1657147 ns/op	   17141 B/op	    1040 allocs/op
BenchmarkOneToMany/items=1k/conc=4/channels-8        	      48	   2439755 ns/op	     396 B/op	       7 allocs/op
BenchmarkOneToMany/items=1k/conc=4/channels-8        	      48	   2638760 ns/op	     398 B/op	       7 allocs/op
BenchmarkOneToMany/items=1k/conc=4/channels-8        	      45	   2657850 ns/op	     362 B/op	       7 allocs/op
BenchmarkOneToMany/items=1k/conc=4/channels-8        	      49	   2644838 ns/op	     362 B/op	       7 allocs/op
BenchmarkOneToMany/items=1k/conc=4/channels-8        	      52	   2185264 ns/op	     426 B/op	       7 allocs/op
BenchmarkOneToMany/items=1k/conc=4/pipeline-8        	      40	   2508674 ns/op	   18589 B/op	    1056 allocs/op
BenchmarkOneToMany/items=1k/conc=4/pipeline-8        	      46	   2521844 ns/op	   18512 B/op	    1055 allocs/op
BenchmarkOneToMany/items=1k/conc=4/pipeline-8        	      46	   2535340 ns/op	   18491 B/op	    1055 allocs/op
BenchmarkOneToMany/items=1k/conc=4/pipeline-8        	      46	   2518813 ns/op	   18456 B/op	    1055 allocs/op
BenchmarkOneToMany/items=1k/conc=4/pipeline-8        	      45	   2533038 ns/op	   18554 B/op	    1056 allocs/op
BenchmarkFromChan/items=1k/conc=1/channels-8         	     108	   1133196 ns/op	     178 B/op	       4 allocs/op
BenchmarkFromChan/items=1k/conc=1/channels-8         	      91	   1107005 ns/op	     179 B/op	       4 allocs/op
BenchmarkFromChan/items=1k/conc=1/channels-8         	     108	   1095500 ns/op	     183 B/op	       4 allocs/op
BenchmarkFromChan/items=1k/conc=1/channels-8         	     103	   1128822 ns/op	     179 B/op	       4 allocs/op
BenchmarkFromChan/items=1k/conc=1/channels-8         	     106	   1103615 ns/op	     169 B/op	       4 allocs/op
BenchmarkFromChan/items=1k/conc=1/pipeline-8         	      72	   1434630 ns/op	    1161 B/op	      20 allocs/op
BenchmarkFromChan/items=1k/conc=1/pipeline-8         	      84	   1422284 ns/op	    1124 B/op	      20 allocs/op
BenchmarkFromChan/items=1k/conc=1/pipeline-8         	      81	   1412470 ns/op	    1128 B/op	      20 allocs/op
BenchmarkFromChan/items=1k/conc=1/pipeline-8         	      78	   1419848 ns/op	    1191 B/op	      20 allocs/op
BenchmarkFromChan/items=1k/conc=1/pipeline-8         	      73	   1428928 ns/op	    1125 B/op	      20 allocs/op
BenchmarkFromChan/items=1k/conc=4/channels-8         	      70	   1878535 ns/op	     361 B/op	       7 allocs/op
BenchmarkFromChan/items=1k/conc=4/channels-8         	      75	   1733738 ns/op	     394 B/op	       7 allocs/op
BenchmarkFromChan/items=1k/conc=4/channels-8         	      67	   1728483 ns/op	     416 B/op	       7 allocs/op
BenchmarkFromChan/items=1k/conc=4/channels-8         	      75	   1733746 ns/op	     361 B/op	       7 allocs/op
BenchmarkFromChan/items=1k/conc=4/channels-8         	      68	   1566211 ns/op	     361 B/op	       7 allocs/op
BenchmarkFromChan/items=1k/conc=4/pipeline-8         	      76	   1540773 ns/op	    3657 B/op	      48 allocs/op
BenchmarkFromChan/items=1k/conc=4/pipeline-8         	      70	   1542896 ns/op	    3581 B/op	      48 allocs/op
BenchmarkFromChan/items=1k/conc=4/pipeline-8         	      76	   1547574 ns/op	    3512 B/op	      47 allocs/op
BenchmarkFromChan/items=1k/conc=4/pipeline-8         	      64	   1563244 ns/op	    3507 B/op	      47 allocs/op
BenchmarkFromChan/items=1k/conc=4/pipeline-8         	      66	   1588479 ns/op	    3702 B/op	      48 allocs/op
BenchmarkSink/items=1k/conc=1/channels-8             	     217	    555026 ns/op	     160 B/op	       4 allocs/op
BenchmarkSink/items=1k/conc=1/channels-8             	     208	    567815 ns/op	     160 B/op	       4 allocs/op
BenchmarkSink/items=1k/conc=1/channels-8             	     207	    591823 ns/op	     160 B/op	       4 allocs/op
BenchmarkSink/items=1k/conc=1/channels-8             	     206	    573705 ns/op	     160 B/op	       4 allocs/op
BenchmarkSink/items=1k/conc=1/channels-8             	     208	    587556 ns/op	     168 B/op	       4 allocs/op
BenchmarkSink/items=1k/conc=1/pipeline-8             	     214	    519809 ns/op	     643 B/op	      14 allocs/op
BenchmarkSink/items=1k/conc=1/pipeline-8             	     206	    529689 ns/op	     665 B/op	      14 allocs/op
BenchmarkSink/items=1k/conc=1/pipeline-8             	     198	    528140 ns/op	     670 B/op	      14 allocs/op
BenchmarkSink/items=1k/conc=1/pipeline-8             	     222	    528373 ns/op	     648 B/op	      14 allocs/op
BenchmarkSink/items=1k/conc=1/pipeline-8             	     222	    531523 ns/op	     665 B/op	      14 allocs/op
BenchmarkSink/items=1k/conc=4/channels-8             	     126	    840212 ns/op	     393 B/op	       7 allocs/op
BenchmarkSink/items=1k/conc=4/channels-8             	     142	    850735 ns/op	     415 B/op	       7 allocs/op
BenchmarkSink/items=1k/conc=4/channels-8             	     144	    823849 ns/op	     365 B/op	       7 allocs/op
BenchmarkSink/items=1k/conc=4/channels-8             	     139	    824756 ns/op	     394 B/op	       7 allocs/op
BenchmarkSink/items=1k/conc=4/channels-8             	     132	    856817 ns/op	     357 B/op	       7 allocs/op
BenchmarkSink/items=1k/conc=4/pipeline-8             	     142	    781138 ns/op	    2001 B/op	      29 allocs/op
BenchmarkSink/items=1k/conc=4/pipeline-8             	     148	    772444 ns/op	    1963 B/op	      29 allocs/op
BenchmarkSink/items=1k/conc=4/pipeline-8             	     151	    813105 ns/op	    1965 B/op	      29 allocs/op
BenchmarkSink/items=1k/conc=4/pipeline-8             	     146	    794836 ns/op	    1993 B/op	      29 allocs/op
BenchmarkSink/items=1k/conc=4/pipeline-8             	     142	    856823 ns/op	    1957 B/op	      29 allocs/op
BenchmarkSinkFromChan/items=1k/conc=1/channels-8     	     211	    619370 ns/op	     161 B/op	       4 allocs/op
BenchmarkSinkFromChan/items=1k/conc=1/channels-8     	     207	    556950 ns/op	     166 B/op	       4 allocs/op
BenchmarkSinkFromChan/items=1k/conc=1/channels-8     	     211	    546405 ns/op	     165 B/op	       4 allocs/op
BenchmarkSinkFromChan/items=1k/conc=1/channels-8     	     208	    544032 ns/op	     163 B/op	       4 allocs/op
BenchmarkSinkFromChan/items=1k/conc=1/channels-8     	     214	    571456 ns/op	     160 B/op	       4 allocs/op
BenchmarkSinkFromChan/items=1k/conc=1/pipeline-8     	     135	    908201 ns/op	    1016 B/op	      18 allocs/op
BenchmarkSinkFromChan/items=1k/conc=1/pipeline-8     	     136	    874616 ns/op	    1047 B/op	      18 allocs/op
BenchmarkSinkFromChan/items=1k/conc=1/pipeline-8     	     138	    860024 ns/op	    1010 B/op	      18 allocs/op
BenchmarkSinkFromChan/items=1k/conc=1/pipeline-8     	     139	    872519 ns/op	    1024 B/op	      18 allocs/op
BenchmarkSinkFromChan/items=1k/conc=1/pipeline-8     	     133	    908101 ns/op	    1030 B/op	      18 allocs/op
BenchmarkSinkFromChan/items=1k/conc=4/channels-8     	     139	    832471 ns/op	     391 B/op	       7 allocs/op
BenchmarkSinkFromChan/items=1k/conc=4/channels-8     	     142	    865138 ns/op	     373 B/op	       7 allocs/op
BenchmarkSinkFromChan/items=1k/conc=4/channels-8     	     142	    849609 ns/op	     414 B/op	       7 allocs/op
BenchmarkSinkFromChan/items=1k/conc=4/channels-8     	     138	    828358 ns/op	     371 B/op	       7 allocs/op
BenchmarkSinkFromChan/items=1k/conc=4/channels-8     	     141	    842111 ns/op	     373 B/op	       7 allocs/op
BenchmarkSinkFromChan/items=1k/conc=4/pipeline-8     	      78	   1365243 ns/op	    3367 B/op	      45 allocs/op
BenchmarkSinkFromChan/items=1k/conc=4/pipeline-8     	      97	   1226001 ns/op	    3427 B/op	      45 allocs/op
BenchmarkSinkFromChan/items=1k/conc=4/pipeline-8     	      98	   1225416 ns/op	    3433 B/op	      45 allocs/op
BenchmarkSinkFromChan/items=1k/conc=4/pipeline-8     	      97	   1248172 ns/op	    3453 B/op	      45 allocs/op
BenchmarkSinkFromChan/items=1k/conc=4/pipeline-8     	      96	   1232408 ns/op	    3494 B/op	      46 allocs/op
BenchmarkBatch/items=1k/conc=1/channels-8            	     224	    729551 ns/op	    8624 B/op	      37 allocs/op
BenchmarkBatch/items=1k/conc=1/channels-8            	     205	    547294 ns/op	    8616 B/op	      37 allocs/op
BenchmarkBatch/items=1k/conc=1/channels-8            	     211	    553441 ns/op	    8621 B/op	      37 allocs/op
BenchmarkBatch/items=1k/conc=1/channels-8            	     222	    554482 ns/op	    8625 B/op	      37 allocs/op
BenchmarkBatch/items=1k/conc=1/channels-8            	     211	    530809 ns/op	    8642 B/op	      37 allocs/op
BenchmarkBatch/items=1k/conc=1/pipeline-8            	     246	    454855 ns/op	   17084 B/op	     209 allocs/op
BenchmarkBatch/items=1k/conc=1/pipeline-8            	     261	    454338 ns/op	   17098 B/op	     209 allocs/op
BenchmarkBatch/items=1k/conc=1/pipeline-8            	     208	    509981 ns/op	   17072 B/op	     209 allocs/op
BenchmarkBatch/items=1k/conc=1/pipeline-8            	     249	    476864 ns/op	   17084 B/op	     209 allocs/op
BenchmarkBatch/items=1k/conc=1/pipeline-8            	     259	    455732 ns/op	   17072 B/op	     209 allocs/op
BenchmarkBatch/items=1k/conc=4/channels-8            	     151	    767975 ns/op	   10113 B/op	      45 allocs/op
BenchmarkBatch/items=1k/conc=4/channels-8            	     154	    793645 ns/op	   10119 B/op	      45 allocs/op
BenchmarkBatch/items=1k/conc=4/channels-8            	     152	    755792 ns/op	   10129 B/op	      45 allocs/op
BenchmarkBatch/items=1k/conc=4/channels-8            	     154	    759287 ns/op	   10071 B/op	      44 allocs/op
BenchmarkBatch/items=1k/conc=4/channels-8            	     146	    769937 ns/op	   10098 B/op	      45 allocs/op
BenchmarkBatch/items=1k/conc=4/pipeline-8            	     172	    692183 ns/op	   19038 B/op	     234 allocs/op
BenchmarkBatch/items=1k/conc=4/pipeline-8            	     175	    684523 ns/op	   19070 B/op	     234 allocs/op
BenchmarkBatch/items=1k/conc=4/pipeline-8            	     182	    671635 ns/op	   19077 B/op	     234 allocs/op
BenchmarkBatch/items=1k/conc=4/pipeline-8            	     163	    655503 ns/op	   19039 B/op	     234 allocs/op
BenchmarkBatch/items=1k/conc=4/pipeline-8            	     162	    737726 ns/op	   19150 B/op	     235 allocs/op
BenchmarkBatchChan/items=1k/conc=1/channels-8        	     103	   1122606 ns/op	    3753 B/op	      36 allocs/op
BenchmarkBatchChan/items=1k/conc=1/channels-8        	     103	   1131665 ns/op	    3775 B/op	      36 allocs/op
BenchmarkBatchChan/items=1k/conc=1/channels-8        	     104	   1123556 ns/op	    3762 B/op	      36 allocs/op
BenchmarkBatchChan/items=1k/conc=1/channels-8        	      93	   1146062 ns/op	    3765 B/op	      36 allocs/op
BenchmarkBatchChan/items=1k/conc=1/channels-8        	      90	   1137573 ns/op	    3759 B/op	      36 allocs/op
BenchmarkBatchChan/items=1k/conc=1/pipeline-8        	     118	    952146 ns/op	    4536 B/op	      49 allocs/op
BenchmarkBatchChan/items=1k/conc=1/pipeline-8        	     123	    953244 ns/op	    4530 B/op	      49 allocs/op
BenchmarkBatchChan/items=1k/conc=1/pipeline-8        	     122	    937963 ns/op	    4538 B/op	      49 allocs/op
BenchmarkBatchChan/items=1k/conc=1/pipeline-8        	     121	    941678 ns/op	    4531 B/op	      49 allocs/op
BenchmarkBatchChan/items=1k/conc=1/pipeline-8        	     117	    938918 ns/op	    4531 B/op	      49 allocs/op
BenchmarkBatchChan/items=1k/conc=4/channels-8        	     100	   1158813 ns/op	    4289 B/op	      42 allocs/op
BenchmarkBatchChan/items=1k/conc=4/channels-8        	     103	   1133299 ns/op	    4329 B/op	      42 allocs/op
BenchmarkBatchChan/items=1k/conc=4/channels-8        	      91	   1178959 ns/op	    4297 B/op	      42 allocs/op
BenchmarkBatchChan/items=1k/conc=4/channels-8        	     103	   1144055 ns/op	    4312 B/op	      42 allocs/op
BenchmarkBatchChan/items=1k/conc=4/channels-8        	      92	   1144870 ns/op	    4281 B/op	      42 allocs/op
BenchmarkBatchChan/items=1k/conc=4/pipeline-8        	     121	    952498 ns/op	    6642 B/op	      70 allocs/op
BenchmarkBatchChan/items=1k/conc=4/pipeline-8        	     118	    964659 ns/op	    6639 B/op	      70 allocs/op
BenchmarkBatchChan/items=1k/conc=4/pipeline-8        	     120	    953758 ns/op	    6635 B/op	      70 allocs/op
BenchmarkBatchChan/items=1k/conc=4/pipeline-8        	     121	    957286 ns/op	    6665 B/op	      70 allocs/op
BenchmarkBatchChan/items=1k/conc=4/pipeline-8        	     106	    973761 ns/op	    6664 B/op	      70 allocs/op
BenchmarkTwoStage/items=1k/conc=1/channels-8         	      61	   1893203 ns/op	     273 B/op	       7 allocs/op
BenchmarkTwoStage/items=1k/conc=1/channels-8         	      58	   1890616 ns/op	     273 B/op	       7 allocs/op
BenchmarkTwoStage/items=1k/conc=1/channels-8         	      56	   1873948 ns/op	     277 B/op	       7 allocs/op
BenchmarkTwoStage/items=1k/conc=1/channels-8         	      56	   1878685 ns/op	     296 B/op	       7 allocs/op
BenchmarkTwoStage/items=1k/conc=1/channels-8         	      54	   1897630 ns/op	     282 B/op	       7 allocs/op
BenchmarkTwoStage/items=1k/conc=1/pipeline-8         	      60	   1972790 ns/op	     935 B/op	      18 allocs/op
BenchmarkTwoStage/items=1k/conc=1/pipeline-8         	      57	   2023077 ns/op	     870 B/op	      18 allocs/op
BenchmarkTwoStage/items=1k/conc=1/pipeline-8         	      55	   1966346 ns/op	     870 B/op	      18 allocs/op
BenchmarkTwoStage/items=1k/conc=1/pipeline-8         	      51	   1993122 ns/op	     871 B/op	      18 allocs/op
BenchmarkTwoStage/items=1k/conc=1/pipeline-8         	      58	   1964109 ns/op	     893 B/op	      18 allocs/op
BenchmarkTwoStage/items=1k/conc=4/channels-8         	      31	   4078753 ns/op	     693 B/op	      13 allocs/op
BenchmarkTwoStage/items=1k/conc=4/channels-8         	      34	   4534239 ns/op	     670 B/op	      13 allocs/op
BenchmarkTwoStage/items=1k/conc=4/channels-8         	      62	   3135322 ns/op	     747 B/op	      13 allocs/op
BenchmarkTwoStage/items=1k/conc=4/channels-8         	      34	   3615776 ns/op	     727 B/op	      13 allocs/op
BenchmarkTwoStage/items=1k/conc=4/channels-8         	      39	   3284129 ns/op	     789 B/op	      14 allocs/op
BenchmarkTwoStage/items=1k/conc=4/pipeline-8         	      43	   2767906 ns/op	    3632 B/op	      48 allocs/op
BenchmarkTwoStage/items=1k/conc=4/pipeline-8         	      43	   2733819 ns/op	    3417 B/op	      46 allocs/op
BenchmarkTwoStage/items=1k/conc=4/pipeline-8         	      39	   2758674 ns/op	    3524 B/op	      47 allocs/op
BenchmarkTwoStage/items=1k/conc=4/pipeline-8         	      38	   2756655 ns/op	    3443 B/op	      46 allocs/op
BenchmarkTwoStage/items=1k/conc=4/pipeline-8         	      37	   2746028 ns/op	    3608 B/op	      48 allocs/op
BenchmarkSplitMerge/items=1k/conc=1/channels-8       	      24	   4451743 ns/op	     484 B/op	      12 allocs/op
BenchmarkSplitMerge/items=1k/conc=1/channels-8       	      26	   5355578 ns/op	     484 B/op	      12 allocs/op
BenchmarkSplitMerge/items=1k/conc=1/channels-8       	      24	   4416707 ns/op	     500 B/op	      12 allocs/op
BenchmarkSplitMerge/items=1k/conc=1/channels-8       	      26	   4948418 ns/op	     584 B/op	      13 allocs/op
BenchmarkSplitMerge/items=1k/conc=1/channels-8       	      26	   4681522 ns/op	     484 B/op	      12 allocs/op
BenchmarkSplitMerge/items=1k/conc=1/pipeline-8       	      18	   6285833 ns/op	    1636 B/op	      30 allocs/op
BenchmarkSplitMerge/items=1k/conc=1/pipeline-8       	      18	   6278498 ns/op	    1476 B/op	      29 allocs/op
BenchmarkSplitMerge/items=1k/conc=1/pipeline-8       	      19	   6211987 ns/op	    1495 B/op	      29 allocs/op
BenchmarkSplitMerge/items=1k/conc=1/pipeline-8       	      16	   6270901 ns/op	    1521 B/op	      29 allocs/op
BenchmarkSplitMerge/items=1k/conc=1/pipeline-8       	      19	   6301783 ns/op	    1475 B/op	      29 allocs/op
BenchmarkSplitMerge/items=1k/conc=4/channels-8       	      19	   6316471 ns/op	     900 B/op	      18 allocs/op
BenchmarkSplitMerge/items=1k/conc=4/channels-8       	      20	   6811790 ns/op	     994 B/op	      19 allocs/op
BenchmarkSplitMerge/items=1k/conc=4/channels-8       	      19	   6379265 ns/op	    1370 B/op	      23 allocs/op
BenchmarkSplitMerge/items=1k/conc=4/channels-8       	      20	   6118815 ns/op	     888 B/op	      18 allocs/op
BenchmarkSplitMerge/items=1k/conc=4/channels-8       	      18	   5824776 ns/op	     891 B/op	      18 allocs/op
BenchmarkSplitMerge/items=1k/conc=4/pipeline-8       	      18	   5975387 ns/op	    4041 B/op	      57 allocs/op
BenchmarkSplitMerge/items=1k/conc=4/pipeline-8       	      18	   5886076 ns/op	    4185 B/op	      59 allocs/op
BenchmarkSplitMerge/items=1k/conc=4/pipeline-8       	      16	   6280591 ns/op	    4683 B/op	      64 allocs/op
BenchmarkSplitMerge/items=1k/conc=4/pipeline-8       	      19	   6148033 ns/op	    4016 B/op	      57 allocs/op
BenchmarkSplitMerge/items=1k/conc=4/pipeline-8       	      19	   5856798 ns/op	    4400 B/op	      61 allocs/op
BenchmarkSplitBy/items=1k/conc=1/channels-8          	      45	   2643142 ns/op	     505 B/op	      12 allocs/op
BenchmarkSplitBy/items=1k/conc=1/channels-8          	      38	   3092303 ns/op	     482 B/op	      12 allocs/op
BenchmarkSplitBy/items=1k/conc=1/channels-8          	      40	   2801520 ns/op	     581 B/op	      13 allocs/op
BenchmarkSplitBy/items=1k/conc=1/channels-8          	      46	   2649251 ns/op	     482 B/op	      12 allocs/op
BenchmarkSplitBy/items=1k/conc=1/channels-8          	      43	   2739730 ns/op	     482 B/op	      12 allocs/op
BenchmarkSplitBy/items=1k/conc=1/pipeline-8          	      28	   4123830 ns/op	    1638 B/op	      30 allocs/op
BenchmarkSplitBy/items=1k/conc=1/pipeline-8          	      28	   4039094 ns/op	    1809 B/op	      32 allocs/op
BenchmarkSplitBy/items=1k/conc=1/pipeline-8          	      28	   3983225 ns/op	    1501 B/op	      29 allocs/op
BenchmarkSplitBy/items=1k/conc=1/pipeline-8          	      27	   4042026 ns/op	    1764 B/op	      31 allocs/op
BenchmarkSplitBy/items=1k/conc=1/pipeline-8          	      25	   4014053 ns/op	    1502 B/op	      29 allocs/op
BenchmarkSplitBy/items=1k/conc=4/channels-8          	      37	   3015570 ns/op	     916 B/op	      18 allocs/op
BenchmarkSplitBy/items=1k/conc=4/channels-8          	      32	   3390519 ns/op	     933 B/op	      18 allocs/op
BenchmarkSplitBy/items=1k/conc=4/channels-8          	      31	   3476847 ns/op	     880 B/op	      18 allocs/op
BenchmarkSplitBy/items=1k/conc=4/channels-8          	      32	   3394053 ns/op	     915 B/op	      18 allocs/op
BenchmarkSplitBy/items=1k/conc=4/channels-8          	      38	   3504581 ns/op	    1091 B/op	      20 allocs/op
BenchmarkSplitBy/items=1k/conc=4/pipeline-8          	      28	   4824725 ns/op	    4343 B/op	      60 allocs/op
BenchmarkSplitBy/items=1k/conc=4/pipeline-8          	      27	   4026438 ns/op	    4004 B/op	      57 allocs/op
BenchmarkSplitBy/items=1k/conc=4/pipeline-8          	      28	   3914731 ns/op	    4312 B/op	      60 allocs/op
BenchmarkSplitBy/items=1k/conc=4/pipeline-8          	      28	   3837167 ns/op	    4370 B/op	      61 allocs/op
BenchmarkSplitBy/items=1k/conc=4/pipeline-8          	      31	   3768294 ns/op	    3995 B/op	      57 allocs/op
BenchmarkOverheadWorkSweep/factor=1/channels-8       	      24	   4621516 ns/op	     172 B/op	       4 allocs/op
BenchmarkOverheadWorkSweep/factor=1/channels-8       	      24	   4486733 ns/op	     172 B/op	       4 allocs/op
BenchmarkOverheadWorkSweep/factor=1/channels-8       	      22	   5247212 ns/op	     194 B/op	       4 allocs/op
BenchmarkOverheadWorkSweep/factor=1/channels-8       	      20	   5036004 ns/op	     202 B/op	       4 allocs/op
BenchmarkOverheadWorkSweep/factor=1/channels-8       	      24	   4670951 ns/op	     204 B/op	       4 allocs/op
BenchmarkOverheadWorkSweep/factor=1/pipeline-8       	      24	   4863561 ns/op	     791 B/op	      16 allocs/op
BenchmarkOverheadWorkSweep/factor=1/pipeline-8       	      25	   4352737 ns/op	     766 B/op	      16 allocs/op
BenchmarkOverheadWorkSweep/factor=1/pipeline-8       	      25	   4392058 ns/op	     766 B/op	      16 allocs/op
BenchmarkOverheadWorkSweep/factor=1/pipeline-8       	      30	   4325894 ns/op	     764 B/op	      16 allocs/op
BenchmarkOverheadWorkSweep/factor=1/pipeline-8       	      26	   4313492 ns/op	     766 B/op	      16 allocs/op
BenchmarkOverheadWorkSweep/factor=2/channels-8       	      22	   5371663 ns/op	     199 B/op	       4 allocs/op
BenchmarkOverheadWorkSweep/factor=2/channels-8       	      19	   5781123 ns/op	     189 B/op	       4 allocs/op
BenchmarkOverheadWorkSweep/factor=2/channels-8       	      20	   5190604 ns/op	     173 B/op	       4 allocs/op
BenchmarkOverheadWorkSweep/factor=2/channels-8       	      21	   5172714 ns/op	     173 B/op	       4 allocs/op
BenchmarkOverheadWorkSweep/factor=2/channels-8       	      21	   5166341 ns/op	     173 B/op	       4 allocs/op
BenchmarkOverheadWorkSweep/factor=2/pipeline-8       	      21	   5038808 ns/op	     769 B/op	      16 allocs/op
BenchmarkOverheadWorkSweep/factor=2/pipeline-8       	      20	   5144002 ns/op	     823 B/op	      16 allocs/op
BenchmarkOverheadWorkSweep/factor=2/pipeline-8       	      21	   5094938 ns/op	     769 B/op	      16 allocs/op
BenchmarkOverheadWorkSweep/factor=2/pipeline-8       	      22	   5052992 ns/op	     799 B/op	      16 allocs/op
BenchmarkOverheadWorkSweep/factor=2/pipeline-8       	      22	   5111409 ns/op	     899 B/op	      17 allocs/op
BenchmarkOverheadWorkSweep/factor=4/channels-8       	      15	   7741836 ns/op	     201 B/op	       4 allocs/op
BenchmarkOverheadWorkSweep/factor=4/channels-8       	      16	   6729193 ns/op	     217 B/op	       4 allocs/op
BenchmarkOverheadWorkSweep/factor=4/channels-8       	      16	   6700083 ns/op	     229 B/op	       4 allocs/op
BenchmarkOverheadWorkSweep/factor=4/channels-8       	      16	   6738573 ns/op	     235 B/op	       4 allocs/op
BenchmarkOverheadWorkSweep/factor=4/channels-8       	      16	   6905247 ns/op	     211 B/op	       4 allocs/op
BenchmarkOverheadWorkSweep/factor=4/pipeline-8       	      15	   6737356 ns/op	     776 B/op	      16 allocs/op
BenchmarkOverheadWorkSweep/factor=4/pipeline-8       	      16	   6646474 ns/op	     811 B/op	      16 allocs/op
BenchmarkOverheadWorkSweep/factor=4/pipeline-8       	      18	   6510259 ns/op	     793 B/op	      16 allocs/op
BenchmarkOverheadWorkSweep/factor=4/pipeline-8       	      16	   6502651 ns/op	     853 B/op	      17 allocs/op
BenchmarkOverheadWorkSweep/factor=4/pipeline-8       	      18	   6575648 ns/op	     847 B/op	      16 allocs/op
BenchmarkOverheadWorkSweep/factor=8/channels-8       	      10	  10198825 ns/op	     179 B/op	       4 allocs/op
BenchmarkOverheadWorkSweep/factor=8/channels-8       	      10	  10259875 ns/op	     179 B/op	       4 allocs/op
BenchmarkOverheadWorkSweep/factor=8/channels-8       	      10	  10318466 ns/op	     179 B/op	       4 allocs/op
BenchmarkOverheadWorkSweep/factor=8/channels-8       	      10	  10232575 ns/op	     179 B/op	       4 allocs/op
BenchmarkOverheadWorkSweep/factor=8/channels-8       	      10	  10204517 ns/op	     179 B/op	       4 allocs/op
BenchmarkOverheadWorkSweep/factor=8/pipeline-8       	      12	   9562913 ns/op	     782 B/op	      16 allocs/op
BenchmarkOverheadWorkSweep/factor=8/pipeline-8       	      12	   9632538 ns/op	     782 B/op	      16 allocs/op
BenchmarkOverheadWorkSweep/factor=8/pipeline-8       	      12	   9564069 ns/op	     782 B/op	      16 allocs/op
BenchmarkOverheadWorkSweep/factor=8/pipeline-8       	      12	   9636062 ns/op	     782 B/op	      16 allocs/op
BenchmarkOverheadWorkSweep/factor=8/pipeline-8       	      12	   9565260 ns/op	     934 B/op	      17 allocs/op
BenchmarkOneToOneOrZeroWorkSweep/factor=1/channels-8 	      37	   3205436 ns/op	     189 B/op	       4 allocs/op
BenchmarkOneToOneOrZeroWorkSweep/factor=1/channels-8 	      36	   3100310 ns/op	     171 B/op	       4 allocs/op
BenchmarkOneToOneOrZeroWorkSweep/factor=1/channels-8 	      37	   3094029 ns/op	     171 B/op	       4 allocs/op
BenchmarkOneToOneOrZeroWorkSweep/factor=1/channels-8 	      37	   3078680 ns/op	     171 B/op	       4 allocs/op
BenchmarkOneToOneOrZeroWorkSweep/factor=1/channels-8 	      38	   3084980 ns/op	     170 B/op	       4 allocs/op
BenchmarkOneToOneOrZeroWorkSweep/factor=1/pipeline-8 	      37	   3129791 ns/op	     761 B/op	      16 allocs/op
BenchmarkOneToOneOrZeroWorkSweep/factor=1/pipeline-8 	      36	   3357122 ns/op	     762 B/op	      16 allocs/op
BenchmarkOneToOneOrZeroWorkSweep/factor=1/pipeline-8 	      33	   3165672 ns/op	     768 B/op	      16 allocs/op
BenchmarkOneToOneOrZeroWorkSweep/factor=1/pipeline-8 	      34	   3127556 ns/op	     844 B/op	      16 allocs/op
BenchmarkOneToOneOrZeroWorkSweep/factor=1/pipeline-8 	      33	   3168352 ns/op	     771 B/op	      16 allocs/op
BenchmarkOneToOneOrZeroWorkSweep/factor=2/channels-8 	      31	   3583046 ns/op	     171 B/op	       4 allocs/op
BenchmarkOneToOneOrZeroWorkSweep/factor=2/channels-8 	      33	   3533822 ns/op	     171 B/op	       4 allocs/op
BenchmarkOneToOneOrZeroWorkSweep/factor=2/channels-8 	      32	   3615603 ns/op	     183 B/op	       4 allocs/op
BenchmarkOneToOneOrZeroWorkSweep/factor=2/channels-8 	      31	   3572872 ns/op	     171 B/op	       4 allocs/op
BenchmarkOneToOneOrZeroWorkSweep/factor=2/channels-8 	      33	   3524086 ns/op	     171 B/op	       4 allocs/op
BenchmarkOneToOneOrZeroWorkSweep/factor=2/pipeline-8 	      33	   3519696 ns/op	     763 B/op	      16 allocs/op
BenchmarkOneToOneOrZeroWorkSweep/factor=2/pipeline-8 	      31	   3549106 ns/op	     807 B/op	      16 allocs/op
BenchmarkOneToOneOrZeroWorkSweep/factor=2/pipeline-8 	      32	   3539325 ns/op	     841 B/op	      16 allocs/op
BenchmarkOneToOneOrZeroWorkSweep/factor=2/pipeline-8 	      31	   3535370 ns/op	     878 B/op	      17 allocs/op
BenchmarkOneToOneOrZeroWorkSweep/factor=2/pipeline-8 	      32	   3569551 ns/op	     826 B/op	      16 allocs/op
BenchmarkOneToOneOrZeroWorkSweep/factor=4/channels-8 	      25	   4427010 ns/op	     172 B/op	       4 allocs/op
BenchmarkOneToOneOrZeroWorkSweep/factor=4/channels-8 	      25	   4398433 ns/op	     172 B/op	       4 allocs/op
BenchmarkOneToOneOrZeroWorkSweep/factor=4/channels-8 	      24	   4492559 ns/op	     172 B/op	       4 allocs/op
BenchmarkOneToOneOrZeroWorkSweep/factor=4/channels-8 	      25	   4436437 ns/op	     172 B/op	       4 allocs/op
BenchmarkOneToOneOrZeroWorkSweep/factor=4/channels-8 	      25	   4771378 ns/op	     172 B/op	       4 allocs/op
BenchmarkOneToOneOrZeroWorkSweep/factor=4/pipeline-8 	      24	   4374585 ns/op	     799 B/op	      16 allocs/op
BenchmarkOneToOneOrZeroWorkSweep/factor=4/pipeline-8 	      25	   4342582 ns/op	     812 B/op	      16 allocs/op
BenchmarkOneToOneOrZeroWorkSweep/factor=4/pipeline-8 	      25	   4350148 ns/op	     766 B/op	      16 allocs/op
BenchmarkOneToOneOrZeroWorkSweep/factor=4/pipeline-8 	      24	   4347580 ns/op	     783 B/op	      16 allocs/op
BenchmarkOneToOneOrZeroWorkSweep/factor=4/pipeline-8 	      25	   4354037 ns/op	     805 B/op	      16 allocs/op
BenchmarkOneToOneOrZeroWorkSweep/factor=8/channels-8 	      18	   6301960 ns/op	     174 B/op	       4 allocs/op
BenchmarkOneToOneOrZeroWorkSweep/factor=8/channels-8 	      18	   6749482 ns/op	     174 B/op	       4 allocs/op
BenchmarkOneToOneOrZeroWorkSweep/factor=8/channels-8 	      19	   6237535 ns/op	     189 B/op	       4 allocs/op
BenchmarkOneToOneOrZeroWorkSweep/factor=8/channels-8 	      16	   6860729 ns/op	     295 B/op	       5 allocs/op
BenchmarkOneToOneOrZeroWorkSweep/factor=8/channels-8 	      16	   6841057 ns/op	     199 B/op	       4 allocs/op
BenchmarkOneToOneOrZeroWorkSweep/factor=8/pipeline-8 	      16	   6306448 ns/op	     835 B/op	      16 allocs/op
BenchmarkOneToOneOrZeroWorkSweep/factor=8/pipeline-8 	      19	   6004044 ns/op	     771 B/op	      16 allocs/op
BenchmarkOneToOneOrZeroWorkSweep/factor=8/pipeline-8 	      18	   5983602 ns/op	     772 B/op	      16 allocs/op
BenchmarkOneToOneOrZeroWorkSweep/factor=8/pipeline-8 	      18	   6095120 ns/op	     905 B/op	      17 allocs/op
BenchmarkOneToOneOrZeroWorkSweep/factor=8/pipeline-8 	      18	   5982866 ns/op	     831 B/op	      16 allocs/op
BenchmarkOneToManyWorkSweep/factor=1/channels-8      	      19	   6149759 ns/op	     173 B/op	       4 allocs/op
BenchmarkOneToManyWorkSweep/factor=1/channels-8      	      19	   6040588 ns/op	     173 B/op	       4 allocs/op
BenchmarkOneToManyWorkSweep/factor=1/channels-8      	      18	   5997479 ns/op	     174 B/op	       4 allocs/op
BenchmarkOneToManyWorkSweep/factor=1/channels-8      	      19	   5997419 ns/op	     173 B/op	       4 allocs/op
BenchmarkOneToManyWorkSweep/factor=1/channels-8      	      19	   6050605 ns/op	     173 B/op	       4 allocs/op
BenchmarkOneToManyWorkSweep/factor=1/pipeline-8      	      16	   6681682 ns/op	   66311 B/op	    4112 allocs/op
BenchmarkOneToManyWorkSweep/factor=1/pipeline-8      	      18	   7659838 ns/op	   66329 B/op	    4112 allocs/op
BenchmarkOneToManyWorkSweep/factor=1/pipeline-8      	      16	   6537050 ns/op	   66311 B/op	    4112 allocs/op
BenchmarkOneToManyWorkSweep/factor=1/pipeline-8      	      18	   6591778 ns/op	   66308 B/op	    4112 allocs/op
BenchmarkOneToManyWorkSweep/factor=1/pipeline-8      	      16	   6806471 ns/op	   66311 B/op	    4112 allocs/op
BenchmarkOneToManyWorkSweep/factor=2/channels-8      	      16	   6757451 ns/op	     175 B/op	       4 allocs/op
BenchmarkOneToManyWorkSweep/factor=2/channels-8      	      16	   6805211 ns/op	     175 B/op	       4 allocs/op
BenchmarkOneToManyWorkSweep/factor=2/channels-8      	      16	   6755271 ns/op	     175 B/op	       4 allocs/op
BenchmarkOneToManyWorkSweep/factor=2/channels-8      	      16	   6723862 ns/op	     175 B/op	       4 allocs/op
BenchmarkOneToManyWorkSweep/factor=2/channels-8      	      16	   6816214 ns/op	     229 B/op	       4 allocs/op
BenchmarkOneToManyWorkSweep/factor=2/pipeline-8      	      15	   7131956 ns/op	   66517 B/op	    4114 allocs/op
BenchmarkOneToManyWorkSweep/factor=2/pipeline-8      	      15	   7197642 ns/op	   66466 B/op	    4113 allocs/op
BenchmarkOneToManyWorkSweep/factor=2/pipeline-8      	      15	   7144636 ns/op	   66408 B/op	    4113 allocs/op
BenchmarkOneToManyWorkSweep/factor=2/pipeline-8      	      15	   7180408 ns/op	   66459 B/op	    4113 allocs/op
BenchmarkOneToManyWorkSweep/factor=2/pipeline-8      	      16	   7236779 ns/op	   66407 B/op	    4113 allocs/op
BenchmarkOneToManyWorkSweep/factor=4/channels-8      	      13	   8282532 ns/op	     191 B/op	       4 allocs/op
BenchmarkOneToManyWorkSweep/factor=4/channels-8      	      13	   8202135 ns/op	     176 B/op	       4 allocs/op
BenchmarkOneToManyWorkSweep/factor=4/channels-8      	      13	   8253010 ns/op	     176 B/op	       4 allocs/op
BenchmarkOneToManyWorkSweep/factor=4/channels-8      	      14	   8205143 ns/op	     196 B/op	       4 allocs/op
BenchmarkOneToManyWorkSweep/factor=4/channels-8      	      13	   8242955 ns/op	     228 B/op	       4 allocs/op
BenchmarkOneToManyWorkSweep/factor=4/pipeline-8      	      13	   8810712 ns/op	   66441 B/op	    4113 allocs/op
BenchmarkOneToManyWorkSweep/factor=4/pipeline-8      	      13	   8291821 ns/op	   66419 B/op	    4113 allocs/op
BenchmarkOneToManyWorkSweep/factor=4/pipeline-8      	      13	   8454045 ns/op	   66500 B/op	    4114 allocs/op
BenchmarkOneToManyWorkSweep/factor=4/pipeline-8      	      13	   8256333 ns/op	   66390 B/op	    4113 allocs/op
BenchmarkOneToManyWorkSweep/factor=4/pipeline-8      	      13	   8531401 ns/op	   66316 B/op	    4112 allocs/op
BenchmarkOneToManyWorkSweep/factor=8/channels-8      	      10	  10751463 ns/op	     179 B/op	       4 allocs/op
BenchmarkOneToManyWorkSweep/factor=8/channels-8      	      10	  10265371 ns/op	     179 B/op	       4 allocs/op
BenchmarkOneToManyWorkSweep/factor=8/channels-8      	      10	  10326333 ns/op	     179 B/op	       4 allocs/op
BenchmarkOneToManyWorkSweep/factor=8/channels-8      	      10	  10282717 ns/op	     179 B/op	       4 allocs/op
BenchmarkOneToManyWorkSweep/factor=8/channels-8      	      10	  10300358 ns/op	     265 B/op	       5 allocs/op
BenchmarkOneToManyWorkSweep/factor=8/pipeline-8      	       9	  12247871 ns/op	   66328 B/op	    4112 allocs/op
BenchmarkOneToManyWorkSweep/factor=8/pipeline-8      	      10	  10940142 ns/op	   66344 B/op	    4112 allocs/op
BenchmarkOneToManyWorkSweep/factor=8/pipeline-8      	       9	  12374306 ns/op	   66328 B/op	    4112 allocs/op
BenchmarkOneToManyWorkSweep/factor=8/pipeline-8      	       9	  12280551 ns/op	   66424 B/op	    4113 allocs/op
BenchmarkOneToManyWorkSweep/factor=8/pipeline-8      	       9	  11444597 ns/op	   66328 B/op	    4112 allocs/op
BenchmarkFromChanWorkSweep/factor=1/channels-8       	      21	   6021845 ns/op	     173 B/op	       4 allocs/op
BenchmarkFromChanWorkSweep/factor=1/channels-8       	      25	   4470437 ns/op	     172 B/op	       4 allocs/op
BenchmarkFromChanWorkSweep/factor=1/channels-8       	      26	   4797623 ns/op	     179 B/op	       4 allocs/op
BenchmarkFromChanWorkSweep/factor=1/channels-8       	      25	   4390628 ns/op	     172 B/op	       4 allocs/op
BenchmarkFromChanWorkSweep/factor=1/channels-8       	      26	   4392107 ns/op	     172 B/op	       4 allocs/op
BenchmarkFromChanWorkSweep/factor=1/pipeline-8       	      21	   5572067 ns/op	    1137 B/op	      20 allocs/op
BenchmarkFromChanWorkSweep/factor=1/pipeline-8       	      19	   5581772 ns/op	    1139 B/op	      20 allocs/op
BenchmarkFromChanWorkSweep/factor=1/pipeline-8       	      19	   5612675 ns/op	    1139 B/op	      20 allocs/op
BenchmarkFromChanWorkSweep/factor=1/pipeline-8       	      21	   5640996 ns/op	    1238 B/op	      21 allocs/op
BenchmarkFromChanWorkSweep/factor=1/pipeline-8       	      21	   6056476 ns/op	    1283 B/op	      21 allocs/op
BenchmarkFromChanWorkSweep/factor=2/channels-8       	      21	   5096605 ns/op	     182 B/op	       4 allocs/op
BenchmarkFromChanWorkSweep/factor=2/channels-8       	      21	   5172695 ns/op	     187 B/op	       4 allocs/op
BenchmarkFromChanWorkSweep/factor=2/channels-8       	      21	   5140129 ns/op	     205 B/op	       4 allocs/op
BenchmarkFromChanWorkSweep/factor=2/channels-8       	      21	   5479605 ns/op	     214 B/op	       4 allocs/op
BenchmarkFromChanWorkSweep/factor=2/channels-8       	      21	   5151633 ns/op	     173 B/op	       4 allocs/op
BenchmarkFromChanWorkSweep/factor=2/pipeline-8       	      16	   6889393 ns/op	    1245 B/op	      21 allocs/op
BenchmarkFromChanWorkSweep/factor=2/pipeline-8       	      16	   6415250 ns/op	    1245 B/op	      21 allocs/op
BenchmarkFromChanWorkSweep/factor=2/pipeline-8       	      18	   6282347 ns/op	    1225 B/op	      21 allocs/op
BenchmarkFromChanWorkSweep/factor=2/pipeline-8       	      18	   6547845 ns/op	    1183 B/op	      20 allocs/op
BenchmarkFromChanWorkSweep/factor=2/pipeline-8       	      18	   6415486 ns/op	    1305 B/op	      21 allocs/op
BenchmarkFromChanWorkSweep/factor=4/channels-8       	      16	   6697919 ns/op	     205 B/op	       4 allocs/op
BenchmarkFromChanWorkSweep/factor=4/channels-8       	      16	   6756831 ns/op	     175 B/op	       4 allocs/op
BenchmarkFromChanWorkSweep/factor=4/channels-8       	      16	   6724786 ns/op	     175 B/op	       4 allocs/op
BenchmarkFromChanWorkSweep/factor=4/channels-8       	      15	   7290356 ns/op	     175 B/op	       4 allocs/op
BenchmarkFromChanWorkSweep/factor=4/channels-8       	      14	   7239292 ns/op	     176 B/op	       4 allocs/op
BenchmarkFromChanWorkSweep/factor=4/pipeline-8       	      14	   8313304 ns/op	    1146 B/op	      20 allocs/op
BenchmarkFromChanWorkSweep/factor=4/pipeline-8       	      14	   7992473 ns/op	    1304 B/op	      21 allocs/op
BenchmarkFromChanWorkSweep/factor=4/pipeline-8       	      14	   7957937 ns/op	    1256 B/op	      21 allocs/op
BenchmarkFromChanWorkSweep/factor=4/pipeline-8       	      14	   7994378 ns/op	    1146 B/op	      20 allocs/op
BenchmarkFromChanWorkSweep/factor=4/pipeline-8       	      14	   7966735 ns/op	    1146 B/op	      20 allocs/op
BenchmarkFromChanWorkSweep/factor=8/channels-8       	      10	  10268596 ns/op	     179 B/op	       4 allocs/op
BenchmarkFromChanWorkSweep/factor=8/channels-8       	      10	  10245142 ns/op	     179 B/op	       4 allocs/op
BenchmarkFromChanWorkSweep/factor=8/channels-8       	      10	  10228588 ns/op	     179 B/op	       4 allocs/op
BenchmarkFromChanWorkSweep/factor=8/channels-8       	      10	  10274642 ns/op	     179 B/op	       4 allocs/op
BenchmarkFromChanWorkSweep/factor=8/channels-8       	      10	  10171329 ns/op	     179 B/op	       4 allocs/op
BenchmarkFromChanWorkSweep/factor=8/pipeline-8       	      10	  11035379 ns/op	    1156 B/op	      20 allocs/op
BenchmarkFromChanWorkSweep/factor=8/pipeline-8       	       9	  11221292 ns/op	    1160 B/op	      20 allocs/op
BenchmarkFromChanWorkSweep/factor=8/pipeline-8       	       9	  14533861 ns/op	    1320 B/op	      22 allocs/op
BenchmarkFromChanWorkSweep/factor=8/pipeline-8       	       9	  15663625 ns/op	    1342 B/op	      22 allocs/op
BenchmarkFromChanWorkSweep/factor=8/pipeline-8       	       9	  11211690 ns/op	    1160 B/op	      20 allocs/op
BenchmarkSinkWorkSweep/factor=1/channels-8           	      34	   3672971 ns/op	     163 B/op	       4 allocs/op
BenchmarkSinkWorkSweep/factor=1/channels-8           	      54	   2257432 ns/op	     162 B/op	       4 allocs/op
BenchmarkSinkWorkSweep/factor=1/channels-8           	      54	   2211989 ns/op	     162 B/op	       4 allocs/op
BenchmarkSinkWorkSweep/factor=1/channels-8           	      46	   2295971 ns/op	     170 B/op	       4 allocs/op
BenchmarkSinkWorkSweep/factor=1/channels-8           	      52	   2236695 ns/op	     162 B/op	       4 allocs/op
BenchmarkSinkWorkSweep/factor=1/pipeline-8           	      54	   2115298 ns/op	     655 B/op	      14 allocs/op
BenchmarkSinkWorkSweep/factor=1/pipeline-8           	      45	   2410462 ns/op	     707 B/op	      14 allocs/op
BenchmarkSinkWorkSweep/factor=1/pipeline-8           	      55	   2132127 ns/op	     692 B/op	      14 allocs/op
BenchmarkSinkWorkSweep/factor=1/pipeline-8           	      56	   2096035 ns/op	     716 B/op	      14 allocs/op
BenchmarkSinkWorkSweep/factor=1/pipeline-8           	      56	   2067900 ns/op	     646 B/op	      14 allocs/op
BenchmarkSinkWorkSweep/factor=2/channels-8           	      37	   2986677 ns/op	     163 B/op	       4 allocs/op
BenchmarkSinkWorkSweep/factor=2/channels-8           	      37	   2979196 ns/op	     163 B/op	       4 allocs/op
BenchmarkSinkWorkSweep/factor=2/channels-8           	      39	   3230119 ns/op	     162 B/op	       4 allocs/op
BenchmarkSinkWorkSweep/factor=2/channels-8           	      37	   2971989 ns/op	     163 B/op	       4 allocs/op
BenchmarkSinkWorkSweep/factor=2/channels-8           	      38	   2955993 ns/op	     162 B/op	       4 allocs/op
BenchmarkSinkWorkSweep/factor=2/pipeline-8           	      40	   2789846 ns/op	     649 B/op	      14 allocs/op
BenchmarkSinkWorkSweep/factor=2/pipeline-8           	      40	   2785766 ns/op	     716 B/op	      14 allocs/op
BenchmarkSinkWorkSweep/factor=2/pipeline-8           	      39	   2775653 ns/op	     693 B/op	      14 allocs/op
BenchmarkSinkWorkSweep/factor=2/pipeline-8           	      42	   3002414 ns/op	     648 B/op	      14 allocs/op
BenchmarkSinkWorkSweep/factor=2/pipeline-8           	      38	   2818103 ns/op	     649 B/op	      14 allocs/op
BenchmarkSinkWorkSweep/factor=4/channels-8           	      21	   5475230 ns/op	     165 B/op	       4 allocs/op
BenchmarkSinkWorkSweep/factor=4/channels-8           	      21	   5364542 ns/op	     165 B/op	       4 allocs/op
BenchmarkSinkWorkSweep/factor=4/channels-8           	      21	   5310209 ns/op	     165 B/op	       4 allocs/op
BenchmarkSinkWorkSweep/factor=4/channels-8           	      19	   5371261 ns/op	     165 B/op	       4 allocs/op
BenchmarkSinkWorkSweep/factor=4/channels-8           	      21	   5503206 ns/op	     165 B/op	       4 allocs/op
BenchmarkSinkWorkSweep/factor=4/pipeline-8           	      22	   4688646 ns/op	     713 B/op	      14 allocs/op
BenchmarkSinkWorkSweep/factor=4/pipeline-8           	      24	   4891682 ns/op	     671 B/op	      14 allocs/op
BenchmarkSinkWorkSweep/factor=4/pipeline-8           	      25	   4824760 ns/op	     654 B/op	      14 allocs/op
BenchmarkSinkWorkSweep/factor=4/pipeline-8           	      24	   4748450 ns/op	     671 B/op	      14 allocs/op
BenchmarkSinkWorkSweep/factor=4/pipeline-8           	      24	   4799908 ns/op	     655 B/op	      14 allocs/op
BenchmarkSinkWorkSweep/factor=8/channels-8           	      14	   7309190 ns/op	     168 B/op	       4 allocs/op
BenchmarkSinkWorkSweep/factor=8/channels-8           	      15	   7237286 ns/op	     167 B/op	       4 allocs/op
BenchmarkSinkWorkSweep/factor=8/channels-8           	      15	   7438022 ns/op	     167 B/op	       4 allocs/op
BenchmarkSinkWorkSweep/factor=8/channels-8           	      14	   7388042 ns/op	     168 B/op	       4 allocs/op
BenchmarkSinkWorkSweep/factor=8/channels-8           	      15	   7304728 ns/op	     205 B/op	       4 allocs/op
BenchmarkSinkWorkSweep/factor=8/pipeline-8           	      14	   8301479 ns/op	     714 B/op	      14 allocs/op
BenchmarkSinkWorkSweep/factor=8/pipeline-8           	      14	   7521452 ns/op	     666 B/op	      14 allocs/op
BenchmarkSinkWorkSweep/factor=8/pipeline-8           	      14	   7865071 ns/op	     666 B/op	      14 allocs/op
BenchmarkSinkWorkSweep/factor=8/pipeline-8           	      14	   7965560 ns/op	     817 B/op	      15 allocs/op
BenchmarkSinkWorkSweep/factor=8/pipeline-8           	      13	   7877359 ns/op	     690 B/op	      14 allocs/op
BenchmarkSinkFromChanWorkSweep/factor=1/channels-8   	      46	   2258772 ns/op	     172 B/op	       4 allocs/op
BenchmarkSinkFromChanWorkSweep/factor=1/channels-8   	      51	   2235933 ns/op	     162 B/op	       4 allocs/op
BenchmarkSinkFromChanWorkSweep/factor=1/channels-8   	      54	   2224400 ns/op	     162 B/op	       4 allocs/op
BenchmarkSinkFromChanWorkSweep/factor=1/channels-8   	      46	   2224887 ns/op	     162 B/op	       4 allocs/op
BenchmarkSinkFromChanWorkSweep/factor=1/channels-8   	      50	   2248293 ns/op	     162 B/op	       4 allocs/op
BenchmarkSinkFromChanWorkSweep/factor=1/pipeline-8   	      31	   3442841 ns/op	    1019 B/op	      18 allocs/op
BenchmarkSinkFromChanWorkSweep/factor=1/pipeline-8   	      33	   3422073 ns/op	    1019 B/op	      18 allocs/op
BenchmarkSinkFromChanWorkSweep/factor=1/pipeline-8   	      33	   3569817 ns/op	    1068 B/op	      18 allocs/op
BenchmarkSinkFromChanWorkSweep/factor=1/pipeline-8   	      30	   3432982 ns/op	    1039 B/op	      18 allocs/op
BenchmarkSinkFromChanWorkSweep/factor=1/pipeline-8   	      32	   3420224 ns/op	    1022 B/op	      18 allocs/op
BenchmarkSinkFromChanWorkSweep/factor=2/channels-8   	      39	   2960118 ns/op	     162 B/op	       4 allocs/op
BenchmarkSinkFromChanWorkSweep/factor=2/channels-8   	      39	   2988587 ns/op	     162 B/op	       4 allocs/op
BenchmarkSinkFromChanWorkSweep/factor=2/channels-8   	      36	   2951396 ns/op	     163 B/op	       4 allocs/op
BenchmarkSinkFromChanWorkSweep/factor=2/channels-8   	      34	   3250298 ns/op	     163 B/op	       4 allocs/op
BenchmarkSinkFromChanWorkSweep/factor=2/channels-8   	      34	   2966809 ns/op	     163 B/op	       4 allocs/op
BenchmarkSinkFromChanWorkSweep/factor=2/pipeline-8   	      26	   4256628 ns/op	    1022 B/op	      18 allocs/op
BenchmarkSinkFromChanWorkSweep/factor=2/pipeline-8   	      27	   4212443 ns/op	    1064 B/op	      18 allocs/op
BenchmarkSinkFromChanWorkSweep/factor=2/pipeline-8   	      27	   4251727 ns/op	    1078 B/op	      18 allocs/op
BenchmarkSinkFromChanWorkSweep/factor=2/pipeline-8   	      24	   4635844 ns/op	    1099 B/op	      18 allocs/op
BenchmarkSinkFromChanWorkSweep/factor=2/pipeline-8   	      25	   4238497 ns/op	    1126 B/op	      19 allocs/op
BenchmarkSinkFromChanWorkSweep/factor=4/channels-8   	      21	   5273137 ns/op	     165 B/op	       4 allocs/op
BenchmarkSinkFromChanWorkSweep/factor=4/channels-8   	      20	   5407123 ns/op	     165 B/op	       4 allocs/op
BenchmarkSinkFromChanWorkSweep/factor=4/channels-8   	      19	   5416450 ns/op	     165 B/op	       4 allocs/op
BenchmarkSinkFromChanWorkSweep/factor=4/channels-8   	      21	   5462216 ns/op	     197 B/op	       4 allocs/op
BenchmarkSinkFromChanWorkSweep/factor=4/channels-8   	      21	   5918062 ns/op	     174 B/op	       4 allocs/op
BenchmarkSinkFromChanWorkSweep/factor=4/pipeline-8   	      18	   5721405 ns/op	    1028 B/op	      18 allocs/op
BenchmarkSinkFromChanWorkSweep/factor=4/pipeline-8   	      20	   5703227 ns/op	    1026 B/op	      18 allocs/op
BenchmarkSinkFromChanWorkSweep/factor=4/pipeline-8   	      19	   5682765 ns/op	    1027 B/op	      18 allocs/op
BenchmarkSinkFromChanWorkSweep/factor=4/pipeline-8   	      19	   5666449 ns/op	    1027 B/op	      18 allocs/op
BenchmarkSinkFromChanWorkSweep/factor=4/pipeline-8   	      19	   5681307 ns/op	    1082 B/op	      18 allocs/op
BenchmarkSinkFromChanWorkSweep/factor=8/channels-8   	      14	   8055655 ns/op	     168 B/op	       4 allocs/op
BenchmarkSinkFromChanWorkSweep/factor=8/channels-8   	      15	   7240286 ns/op	     167 B/op	       4 allocs/op
BenchmarkSinkFromChanWorkSweep/factor=8/channels-8   	      15	   7321386 ns/op	     167 B/op	       4 allocs/op
BenchmarkSinkFromChanWorkSweep/factor=8/channels-8   	      14	   7322812 ns/op	     181 B/op	       4 allocs/op
BenchmarkSinkFromChanWorkSweep/factor=8/channels-8   	      14	   7278018 ns/op	     209 B/op	       4 allocs/op
BenchmarkSinkFromChanWorkSweep/factor=8/pipeline-8   	      13	   9189644 ns/op	    1124 B/op	      19 allocs/op
BenchmarkSinkFromChanWorkSweep/factor=8/pipeline-8   	      13	   8457936 ns/op	    1176 B/op	      19 allocs/op
BenchmarkSinkFromChanWorkSweep/factor=8/pipeline-8   	      13	   8438926 ns/op	    1191 B/op	      19 allocs/op
BenchmarkSinkFromChanWorkSweep/factor=8/pipeline-8   	      13	   8428670 ns/op	    1117 B/op	      19 allocs/op
BenchmarkSinkFromChanWorkSweep/factor=8/pipeline-8   	      13	   8401426 ns/op	    1206 B/op	      20 allocs/op
BenchmarkBatchWorkSweep/factor=1/channels-8          	      55	   2091278 ns/op	   33194 B/op	     133 allocs/op
BenchmarkBatchWorkSweep/factor=1/channels-8          	      55	   2093430 ns/op	   33230 B/op	     133 allocs/op
BenchmarkBatchWorkSweep/factor=1/channels-8          	      50	   2104112 ns/op	   33200 B/op	     133 allocs/op
BenchmarkBatchWorkSweep/factor=1/channels-8          	      54	   2154999 ns/op	   33194 B/op	     133 allocs/op
BenchmarkBatchWorkSweep/factor=1/channels-8          	      50	   2132006 ns/op	   33194 B/op	     133 allocs/op
BenchmarkBatchWorkSweep/factor=1/pipeline-8          	      62	   1944739 ns/op	   65515 B/op	     785 allocs/op
BenchmarkBatchWorkSweep/factor=1/pipeline-8          	      66	   1743162 ns/op	   65473 B/op	     785 allocs/op
BenchmarkBatchWorkSweep/factor=1/pipeline-8          	      70	   1696333 ns/op	   65508 B/op	     785 allocs/op
BenchmarkBatchWorkSweep/factor=1/pipeline-8          	      69	   1693531 ns/op	   65523 B/op	     785 allocs/op
BenchmarkBatchWorkSweep/factor=1/pipeline-8          	      69	   1699359 ns/op	   65533 B/op	     785 allocs/op
BenchmarkBatchWorkSweep/factor=2/channels-8          	      40	   2513491 ns/op	   33194 B/op	     133 allocs/op
BenchmarkBatchWorkSweep/factor=2/channels-8          	      46	   2572197 ns/op	   33204 B/op	     133 allocs/op
BenchmarkBatchWorkSweep/factor=2/channels-8          	      40	   2527692 ns/op	   33211 B/op	     133 allocs/op
BenchmarkBatchWorkSweep/factor=2/channels-8          	      45	   2671554 ns/op	   33205 B/op	     133 allocs/op
BenchmarkBatchWorkSweep/factor=2/channels-8          	      46	   2536565 ns/op	   33252 B/op	     133 allocs/op
BenchmarkBatchWorkSweep/factor=2/pipeline-8          	      46	   2279286 ns/op	   65504 B/op	     785 allocs/op
BenchmarkBatchWorkSweep/factor=2/pipeline-8          	      54	   2200944 ns/op	   65494 B/op	     785 allocs/op
BenchmarkBatchWorkSweep/factor=2/pipeline-8          	      49	   2217608 ns/op	   65465 B/op	     785 allocs/op
BenchmarkBatchWorkSweep/factor=2/pipeline-8          	      48	   2213019 ns/op	   65505 B/op	     785 allocs/op
BenchmarkBatchWorkSweep/factor=2/pipeline-8          	      54	   2197110 ns/op	   65503 B/op	     785 allocs/op
BenchmarkBatchWorkSweep/factor=4/channels-8          	      30	   3864800 ns/op	   33218 B/op	     133 allocs/op
BenchmarkBatchWorkSweep/factor=4/channels-8          	      28	   3585624 ns/op	   33196 B/op	     133 allocs/op
BenchmarkBatchWorkSweep/factor=4/channels-8          	      31	   3560780 ns/op	   33195 B/op	     133 allocs/op
BenchmarkBatchWorkSweep/factor=4/channels-8          	      28	   3591738 ns/op	   33196 B/op	     133 allocs/op
BenchmarkBatchWorkSweep/factor=4/channels-8          	      31	   3609168 ns/op	   33263 B/op	     133 allocs/op
BenchmarkBatchWorkSweep/factor=4/pipeline-8          	      33	   3393480 ns/op	   65456 B/op	     785 allocs/op
BenchmarkBatchWorkSweep/factor=4/pipeline-8          	      33	   3530153 ns/op	   65452 B/op	     785 allocs/op
BenchmarkBatchWorkSweep/factor=4/pipeline-8          	      32	   3487837 ns/op	   65456 B/op	     785 allocs/op
BenchmarkBatchWorkSweep/factor=4/pipeline-8          	      32	   3336486 ns/op	   65530 B/op	     785 allocs/op
BenchmarkBatchWorkSweep/factor=4/pipeline-8          	      31	   3340836 ns/op	   65547 B/op	     786 allocs/op
BenchmarkBatchWorkSweep/factor=8/channels-8          	      19	   6112728 ns/op	   33197 B/op	     133 allocs/op
BenchmarkBatchWorkSweep/factor=8/channels-8          	      22	   5673241 ns/op	   33197 B/op	     133 allocs/op
BenchmarkBatchWorkSweep/factor=8/channels-8          	      19	   5814498 ns/op	   33223 B/op	     133 allocs/op
BenchmarkBatchWorkSweep/factor=8/channels-8          	      19	   5760180 ns/op	   33208 B/op	     133 allocs/op
BenchmarkBatchWorkSweep/factor=8/channels-8          	      18	   5761873 ns/op	   33267 B/op	     133 allocs/op
BenchmarkBatchWorkSweep/factor=8/pipeline-8          	      20	   6028273 ns/op	   65466 B/op	     785 allocs/op
BenchmarkBatchWorkSweep/factor=8/pipeline-8          	      19	   5629897 ns/op	   65463 B/op	     785 allocs/op
BenchmarkBatchWorkSweep/factor=8/pipeline-8          	      20	   5688977 ns/op	   65465 B/op	     785 allocs/op
BenchmarkBatchWorkSweep/factor=8/pipeline-8          	      20	   5644073 ns/op	   65465 B/op	     785 allocs/op
BenchmarkBatchWorkSweep/factor=8/pipeline-8          	      20	   5703725 ns/op	   65463 B/op	     785 allocs/op
BenchmarkBatchChanWorkSweep/factor=1/channels-8      	      20	   5073435 ns/op	   14509 B/op	     132 allocs/op
BenchmarkBatchChanWorkSweep/factor=1/channels-8      	      25	   4489058 ns/op	   14508 B/op	     132 allocs/op
BenchmarkBatchChanWorkSweep/factor=1/channels-8      	      25	   4613567 ns/op	   14554 B/op	     132 allocs/op
BenchmarkBatchChanWorkSweep/factor=1/channels-8      	      25	   4523858 ns/op	   14508 B/op	     132 allocs/op
BenchmarkBatchChanWorkSweep/factor=1/channels-8      	      22	   4567422 ns/op	   14513 B/op	     132 allocs/op
BenchmarkBatchChanWorkSweep/factor=1/pipeline-8      	      30	   3875037 ns/op	   15378 B/op	     146 allocs/op
BenchmarkBatchChanWorkSweep/factor=1/pipeline-8      	      26	   3893821 ns/op	   15294 B/op	     145 allocs/op
BenchmarkBatchChanWorkSweep/factor=1/pipeline-8      	      31	   3770820 ns/op	   15291 B/op	     145 allocs/op
BenchmarkBatchChanWorkSweep/factor=1/pipeline-8      	      30	   3863585 ns/op	   15292 B/op	     145 allocs/op
BenchmarkBatchChanWorkSweep/factor=1/pipeline-8      	      28	   3872665 ns/op	   15293 B/op	     145 allocs/op
BenchmarkBatchChanWorkSweep/factor=2/channels-8      	      19	   5677368 ns/op	   14509 B/op	     132 allocs/op
BenchmarkBatchChanWorkSweep/factor=2/channels-8      	      20	   5416625 ns/op	   14509 B/op	     132 allocs/op
BenchmarkBatchChanWorkSweep/factor=2/channels-8      	      21	   5481561 ns/op	   14509 B/op	     132 allocs/op
BenchmarkBatchChanWorkSweep/factor=2/channels-8      	      20	   5452854 ns/op	   14509 B/op	     132 allocs/op
BenchmarkBatchChanWorkSweep/factor=2/channels-8      	      21	   5460145 ns/op	   14509 B/op	     132 allocs/op
BenchmarkBatchChanWorkSweep/factor=2/pipeline-8      	      25	   4926012 ns/op	   15425 B/op	     146 allocs/op
BenchmarkBatchChanWorkSweep/factor=2/pipeline-8      	      24	   4681969 ns/op	   15347 B/op	     145 allocs/op
BenchmarkBatchChanWorkSweep/factor=2/pipeline-8      	      24	   4632026 ns/op	   15295 B/op	     145 allocs/op
BenchmarkBatchChanWorkSweep/factor=2/pipeline-8      	      22	   4622138 ns/op	   15296 B/op	     145 allocs/op
BenchmarkBatchChanWorkSweep/factor=2/pipeline-8      	      25	   4666372 ns/op	   15294 B/op	     145 allocs/op
BenchmarkBatchChanWorkSweep/factor=4/channels-8      	      14	   7270967 ns/op	   14518 B/op	     132 allocs/op
BenchmarkBatchChanWorkSweep/factor=4/channels-8      	      15	   6869011 ns/op	   14511 B/op	     132 allocs/op
BenchmarkBatchChanWorkSweep/factor=4/channels-8      	      16	   6948841 ns/op	   14511 B/op	     132 allocs/op
BenchmarkBatchChanWorkSweep/factor=4/channels-8      	      15	   6920772 ns/op	   14511 B/op	     132 allocs/op
BenchmarkBatchChanWorkSweep/factor=4/channels-8      	      16	   6952753 ns/op	   14511 B/op	     132 allocs/op
BenchmarkBatchChanWorkSweep/factor=4/pipeline-8      	      16	   6721471 ns/op	   15477 B/op	     147 allocs/op
BenchmarkBatchChanWorkSweep/factor=4/pipeline-8      	      18	   6021602 ns/op	   15300 B/op	     145 allocs/op
BenchmarkBatchChanWorkSweep/factor=4/pipeline-8      	      19	   5981169 ns/op	   15400 B/op	     146 allocs/op
BenchmarkBatchChanWorkSweep/factor=4/pipeline-8      	      18	   6141579 ns/op	   15353 B/op	     145 allocs/op
BenchmarkBatchChanWorkSweep/factor=4/pipeline-8      	      18	   6014176 ns/op	   15417 B/op	     146 allocs/op
BenchmarkBatchChanWorkSweep/factor=8/channels-8      	      10	  10716817 ns/op	   14563 B/op	     132 allocs/op
BenchmarkBatchChanWorkSweep/factor=8/channels-8      	      12	   9980413 ns/op	   14521 B/op	     132 allocs/op
BenchmarkBatchChanWorkSweep/factor=8/channels-8      	      12	   9900524 ns/op	   14577 B/op	     132 allocs/op
BenchmarkBatchChanWorkSweep/factor=8/channels-8      	      10	  10024133 ns/op	   14553 B/op	     132 allocs/op
BenchmarkBatchChanWorkSweep/factor=8/channels-8      	      12	   9893448 ns/op	   14513 B/op	     132 allocs/op
BenchmarkBatchChanWorkSweep/factor=8/pipeline-8      	      13	   9590910 ns/op	   15611 B/op	     148 allocs/op
BenchmarkBatchChanWorkSweep/factor=8/pipeline-8      	      12	   8835129 ns/op	   15358 B/op	     145 allocs/op
BenchmarkBatchChanWorkSweep/factor=8/pipeline-8      	      12	   8886816 ns/op	   15590 B/op	     148 allocs/op
BenchmarkBatchChanWorkSweep/factor=8/pipeline-8      	      12	   8867430 ns/op	   15406 B/op	     146 allocs/op
BenchmarkBatchChanWorkSweep/factor=8/pipeline-8      	      12	   8872601 ns/op	   15310 B/op	     145 allocs/op
BenchmarkSplitByWorkSweep/factor=1/channels-8        	      10	  10976637 ns/op	     539 B/op	      12 allocs/op
BenchmarkSplitByWorkSweep/factor=1/channels-8        	      10	  10561996 ns/op	     520 B/op	      12 allocs/op
BenchmarkSplitByWorkSweep/factor=1/channels-8        	      10	  11021954 ns/op	     692 B/op	      14 allocs/op
BenchmarkSplitByWorkSweep/factor=1/channels-8        	      12	  11227056 ns/op	     489 B/op	      12 allocs/op
BenchmarkSplitByWorkSweep/factor=1/channels-8        	      10	  11054013 ns/op	     491 B/op	      12 allocs/op
BenchmarkSplitByWorkSweep/factor=1/pipeline-8        	       8	  16537188 ns/op	    1942 B/op	      33 allocs/op
BenchmarkSplitByWorkSweep/factor=1/pipeline-8        	       7	  16169863 ns/op	    1842 B/op	      32 allocs/op
BenchmarkSplitByWorkSweep/factor=1/pipeline-8        	       7	  16252619 ns/op	    1540 B/op	      29 allocs/op
BenchmarkSplitByWorkSweep/factor=1/pipeline-8        	       7	  16093917 ns/op	    1540 B/op	      29 allocs/op
BenchmarkSplitByWorkSweep/factor=1/pipeline-8        	       7	  16152220 ns/op	    2034 B/op	      34 allocs/op
BenchmarkSplitByWorkSweep/factor=2/channels-8        	       9	  11559889 ns/op	     620 B/op	      13 allocs/op
BenchmarkSplitByWorkSweep/factor=2/channels-8        	       9	  11519787 ns/op	     609 B/op	      13 allocs/op
BenchmarkSplitByWorkSweep/factor=2/channels-8        	       9	  11398111 ns/op	     492 B/op	      12 allocs/op
BenchmarkSplitByWorkSweep/factor=2/channels-8        	       9	  11866546 ns/op	     588 B/op	      13 allocs/op
BenchmarkSplitByWorkSweep/factor=2/channels-8        	       8	  12747266 ns/op	     674 B/op	      14 allocs/op
BenchmarkSplitByWorkSweep/factor=2/pipeline-8        	       6	  17172583 ns/op	    2029 B/op	      34 allocs/op
BenchmarkSplitByWorkSweep/factor=2/pipeline-8        	       6	  16948882 ns/op	    1645 B/op	      30 allocs/op
BenchmarkSplitByWorkSweep/factor=2/pipeline-8        	       7	  17541125 ns/op	    1540 B/op	      29 allocs/op
BenchmarkSplitByWorkSweep/factor=2/pipeline-8        	       6	  17134403 ns/op	    2205 B/op	      36 allocs/op
BenchmarkSplitByWorkSweep/factor=2/pipeline-8        	       6	  16786389 ns/op	    1549 B/op	      29 allocs/op
BenchmarkSplitByWorkSweep/factor=4/channels-8        	       8	  13133927 ns/op	     494 B/op	      12 allocs/op
BenchmarkSplitByWorkSweep/factor=4/channels-8        	       8	  13061912 ns/op	     566 B/op	      12 allocs/op
BenchmarkSplitByWorkSweep/factor=4/channels-8        	       9	  12550523 ns/op	     673 B/op	      14 allocs/op
BenchmarkSplitByWorkSweep/factor=4/channels-8        	       8	  13606594 ns/op	     650 B/op	      13 allocs/op
BenchmarkSplitByWorkSweep/factor=4/channels-8        	       8	  12529802 ns/op	     530 B/op	      12 allocs/op
BenchmarkSplitByWorkSweep/factor=4/pipeline-8        	       6	  18397090 ns/op	    1837 B/op	      32 allocs/op
BenchmarkSplitByWorkSweep/factor=4/pipeline-8        	       6	  18005132 ns/op	    1869 B/op	      32 allocs/op
BenchmarkSplitByWorkSweep/factor=4/pipeline-8        	       6	  18120618 ns/op	    2013 B/op	      34 allocs/op
BenchmarkSplitByWorkSweep/factor=4/pipeline-8        	       6	  17993652 ns/op	    1549 B/op	      29 allocs/op
BenchmarkSplitByWorkSweep/factor=4/pipeline-8        	       6	  18025729 ns/op	    1917 B/op	      33 allocs/op
BenchmarkSplitByWorkSweep/factor=8/channels-8        	       7	  15836976 ns/op	     496 B/op	      12 allocs/op
BenchmarkSplitByWorkSweep/factor=8/channels-8        	       7	  16165744 ns/op	     496 B/op	      12 allocs/op
BenchmarkSplitByWorkSweep/factor=8/channels-8        	       7	  15508946 ns/op	     496 B/op	      12 allocs/op
BenchmarkSplitByWorkSweep/factor=8/channels-8        	       7	  16118214 ns/op	     496 B/op	      12 allocs/op
BenchmarkSplitByWorkSweep/factor=8/channels-8        	       7	  16179155 ns/op	     715 B/op	      14 allocs/op
BenchmarkSplitByWorkSweep/factor=8/pipeline-8        	       6	  19722493 ns/op	    1549 B/op	      29 allocs/op
BenchmarkSplitByWorkSweep/factor=8/pipeline-8        	       6	  19687271 ns/op	    1677 B/op	      30 allocs/op
BenchmarkSplitByWorkSweep/factor=8/pipeline-8        	       6	  19591306 ns/op	    3069 B/op	      45 allocs/op
BenchmarkSplitByWorkSweep/factor=8/pipeline-8        	       6	  19898416 ns/op	    2125 B/op	      35 allocs/op
BenchmarkSplitByWorkSweep/factor=8/pipeline-8        	       6	  19610500 ns/op	    2413 B/op	      38 allocs/op
BenchmarkOverheadStepSweep/steps=1/channels-8        	      26	   4835795 ns/op	     204 B/op	       6 allocs/op
BenchmarkOverheadStepSweep/steps=1/channels-8        	      22	   4762970 ns/op	     205 B/op	       6 allocs/op
BenchmarkOverheadStepSweep/steps=1/channels-8        	      21	   4947484 ns/op	     205 B/op	       6 allocs/op
BenchmarkOverheadStepSweep/steps=1/channels-8        	      27	   4313803 ns/op	     204 B/op	       6 allocs/op
BenchmarkOverheadStepSweep/steps=1/channels-8        	      26	   4349732 ns/op	     211 B/op	       6 allocs/op
BenchmarkOverheadStepSweep/steps=1/pipeline-8        	      27	   4268358 ns/op	     765 B/op	      16 allocs/op
BenchmarkOverheadStepSweep/steps=1/pipeline-8        	      24	   4635259 ns/op	     767 B/op	      16 allocs/op
BenchmarkOverheadStepSweep/steps=1/pipeline-8        	      27	   4275042 ns/op	     765 B/op	      16 allocs/op
BenchmarkOverheadStepSweep/steps=1/pipeline-8        	      25	   4277107 ns/op	     820 B/op	      16 allocs/op
BenchmarkOverheadStepSweep/steps=1/pipeline-8        	      26	   4293452 ns/op	     766 B/op	      16 allocs/op
BenchmarkOverheadStepSweep/steps=2/channels-8        	      15	   7896647 ns/op	     343 B/op	      11 allocs/op
BenchmarkOverheadStepSweep/steps=2/channels-8        	      14	   7364628 ns/op	     344 B/op	      11 allocs/op
BenchmarkOverheadStepSweep/steps=2/channels-8        	      15	   7427283 ns/op	     343 B/op	      11 allocs/op
BenchmarkOverheadStepSweep/steps=2/channels-8        	      15	   7375970 ns/op	     343 B/op	      11 allocs/op
BenchmarkOverheadStepSweep/steps=2/channels-8        	      14	   7370140 ns/op	     446 B/op	      12 allocs/op
BenchmarkOverheadStepSweep/steps=2/pipeline-8        	      14	   8031140 ns/op	     890 B/op	      18 allocs/op
BenchmarkOverheadStepSweep/steps=2/pipeline-8        	      15	   7670681 ns/op	    1042 B/op	      19 allocs/op
BenchmarkOverheadStepSweep/steps=2/pipeline-8        	      14	   7706459 ns/op	    1020 B/op	      19 allocs/op
BenchmarkOverheadStepSweep/steps=2/pipeline-8        	      14	   7708536 ns/op	     897 B/op	      18 allocs/op
BenchmarkOverheadStepSweep/steps=2/pipeline-8        	      15	   7631911 ns/op	     888 B/op	      18 allocs/op
BenchmarkOverheadStepSweep/steps=4/channels-8        	       8	  14258256 ns/op	     694 B/op	      21 allocs/op
BenchmarkOverheadStepSweep/steps=4/channels-8        	       8	  14608901 ns/op	     790 B/op	      22 allocs/op
BenchmarkOverheadStepSweep/steps=4/channels-8        	       8	  13279479 ns/op	     850 B/op	      23 allocs/op
BenchmarkOverheadStepSweep/steps=4/channels-8        	       8	  13496714 ns/op	     826 B/op	      23 allocs/op
BenchmarkOverheadStepSweep/steps=4/channels-8        	       8	  13964323 ns/op	     622 B/op	      21 allocs/op
BenchmarkOverheadStepSweep/steps=4/pipeline-8        	       7	  14521857 ns/op	    1346 B/op	      24 allocs/op
BenchmarkOverheadStepSweep/steps=4/pipeline-8        	       7	  14430768 ns/op	    1236 B/op	      23 allocs/op
BenchmarkOverheadStepSweep/steps=4/pipeline-8        	       7	  14521899 ns/op	    1140 B/op	      22 allocs/op
BenchmarkOverheadStepSweep/steps=4/pipeline-8        	       7	  14668833 ns/op	    1140 B/op	      22 allocs/op
BenchmarkOverheadStepSweep/steps=4/pipeline-8        	       7	  14497435 ns/op	    1593 B/op	      27 allocs/op
BenchmarkOverheadStepSweep/steps=8/channels-8        	       4	  28750156 ns/op	    1636 B/op	      46 allocs/op
BenchmarkOverheadStepSweep/steps=8/channels-8        	       4	  28679375 ns/op	    1180 B/op	      41 allocs/op
BenchmarkOverheadStepSweep/steps=8/channels-8        	       4	  27847844 ns/op	    1852 B/op	      48 allocs/op
BenchmarkOverheadStepSweep/steps=8/channels-8        	       4	  33156635 ns/op	    1780 B/op	      47 allocs/op
BenchmarkOverheadStepSweep/steps=8/channels-8        	       4	  32828198 ns/op	    1588 B/op	      45 allocs/op
BenchmarkOverheadStepSweep/steps=8/pipeline-8        	       4	  29645104 ns/op	    2684 B/op	      41 allocs/op
BenchmarkOverheadStepSweep/steps=8/pipeline-8        	       4	  29216760 ns/op	    2540 B/op	      40 allocs/op
BenchmarkOverheadStepSweep/steps=8/pipeline-8        	       4	  29411240 ns/op	    2156 B/op	      36 allocs/op
BenchmarkOverheadStepSweep/steps=8/pipeline-8        	       4	  29524188 ns/op	    2588 B/op	      40 allocs/op
BenchmarkOverheadStepSweep/steps=8/pipeline-8        	       4	  29294573 ns/op	    1892 B/op	      33 allocs/op
BenchmarkOverheadStepSweep/steps=16/channels-8       	       2	  65031271 ns/op	    5800 B/op	     118 allocs/op
BenchmarkOverheadStepSweep/steps=16/channels-8       	       2	  61770104 ns/op	    3160 B/op	      90 allocs/op
BenchmarkOverheadStepSweep/steps=16/channels-8       	       2	  60514000 ns/op	    3880 B/op	      98 allocs/op
BenchmarkOverheadStepSweep/steps=16/channels-8       	       2	  58645271 ns/op	    4984 B/op	     109 allocs/op
BenchmarkOverheadStepSweep/steps=16/channels-8       	       2	  73023500 ns/op	    3112 B/op	      90 allocs/op
BenchmarkOverheadStepSweep/steps=16/pipeline-8       	       2	  71954271 ns/op	    2904 B/op	      50 allocs/op
BenchmarkOverheadStepSweep/steps=16/pipeline-8       	       2	  75635312 ns/op	    2616 B/op	      47 allocs/op
BenchmarkOverheadStepSweep/steps=16/pipeline-8       	       2	  62983604 ns/op	    4008 B/op	      62 allocs/op
BenchmarkOverheadStepSweep/steps=16/pipeline-8       	       2	  63530646 ns/op	    2664 B/op	      48 allocs/op
BenchmarkOverheadStepSweep/steps=16/pipeline-8       	       2	  64024521 ns/op	    3144 B/op	      53 allocs/op
PASS
ok  	github.com/askiada/go-pipeline/v2/pkg/pipeline	102.610s
```

## Channel vs pipeline diff

Diff is percent vs channels (medians from count=5).

| Benchmark | Channels ns/op | Pipeline ns/op | Diff |
| --- | ---: | ---: | ---: |
| Batch/items=1k/conc=1 | 553,441.0 | 455,732.0 | -17.7% |
| Batch/items=1k/conc=4 | 767,975.0 | 684,523.0 | -10.9% |
| BatchChan/items=1k/conc=1 | 1,131,665.0 | 941,678.0 | -16.8% |
| BatchChan/items=1k/conc=4 | 1,144,870.0 | 957,286.0 | -16.4% |
| BatchChanWorkSweep/factor=1 | 4,567,422.0 | 3,872,665.0 | -15.2% |
| BatchChanWorkSweep/factor=2 | 5,460,145.0 | 4,666,372.0 | -14.5% |
| BatchChanWorkSweep/factor=4 | 6,948,841.0 | 6,021,602.0 | -13.3% |
| BatchChanWorkSweep/factor=8 | 9,980,413.0 | 8,872,601.0 | -11.1% |
| BatchWorkSweep/factor=1 | 2,104,112.0 | 1,699,359.0 | -19.2% |
| BatchWorkSweep/factor=2 | 2,536,565.0 | 2,213,019.0 | -12.8% |
| BatchWorkSweep/factor=4 | 3,591,738.0 | 3,393,480.0 | -5.5% |
| BatchWorkSweep/factor=8 | 5,761,873.0 | 5,688,977.0 | -1.3% |
| FromChan/items=1k/conc=1 | 1,107,005.0 | 1,422,284.0 | +28.5% |
| FromChan/items=1k/conc=4 | 1,733,738.0 | 1,547,574.0 | -10.7% |
| FromChanWorkSweep/factor=1 | 4,470,437.0 | 5,612,675.0 | +25.6% |
| FromChanWorkSweep/factor=2 | 5,151,633.0 | 6,415,486.0 | +24.5% |
| FromChanWorkSweep/factor=4 | 6,756,831.0 | 7,992,473.0 | +18.3% |
| FromChanWorkSweep/factor=8 | 10,245,142.0 | 11,221,292.0 | +9.5% |
| OneToMany/items=1k/conc=1 | 1,506,259.0 | 1,694,162.0 | +12.5% |
| OneToMany/items=1k/conc=4 | 2,638,760.0 | 2,521,844.0 | -4.4% |
| OneToManyWorkSweep/factor=1 | 6,040,588.0 | 6,681,682.0 | +10.6% |
| OneToManyWorkSweep/factor=2 | 6,757,451.0 | 7,180,408.0 | +6.3% |
| OneToManyWorkSweep/factor=4 | 8,242,955.0 | 8,454,045.0 | +2.6% |
| OneToManyWorkSweep/factor=8 | 10,300,358.0 | 12,247,871.0 | +18.9% |
| OneToOne/items=1k/conc=1 | 1,108,432.0 | 1,143,562.0 | +3.2% |
| OneToOne/items=1k/conc=4 | 1,894,616.0 | 1,779,444.0 | -6.1% |
| OneToOneOrZero/items=1k/conc=1 | 788,681.0 | 845,460.0 | +7.2% |
| OneToOneOrZero/items=1k/conc=4 | 1,448,826.0 | 1,258,928.0 | -13.1% |
| OneToOneOrZeroWorkSweep/factor=1 | 3,094,029.0 | 3,165,672.0 | +2.3% |
| OneToOneOrZeroWorkSweep/factor=2 | 3,572,872.0 | 3,539,325.0 | -0.9% |
| OneToOneOrZeroWorkSweep/factor=4 | 4,436,437.0 | 4,350,148.0 | -1.9% |
| OneToOneOrZeroWorkSweep/factor=8 | 6,749,482.0 | 6,004,044.0 | -11.0% |
| OverheadStepSweep/steps=1 | 4,762,970.0 | 4,277,107.0 | -10.2% |
| OverheadStepSweep/steps=16 | 61,770,104.0 | 64,024,521.0 | +3.6% |
| OverheadStepSweep/steps=2 | 7,375,970.0 | 7,706,459.0 | +4.5% |
| OverheadStepSweep/steps=4 | 13,964,323.0 | 14,521,857.0 | +4.0% |
| OverheadStepSweep/steps=8 | 28,750,156.0 | 29,411,240.0 | +2.3% |
| OverheadWorkSweep/factor=1 | 4,670,951.0 | 4,352,737.0 | -6.8% |
| OverheadWorkSweep/factor=2 | 5,190,604.0 | 5,094,938.0 | -1.8% |
| OverheadWorkSweep/factor=4 | 6,738,573.0 | 6,575,648.0 | -2.4% |
| OverheadWorkSweep/factor=8 | 10,232,575.0 | 9,565,260.0 | -6.5% |
| Sink/items=1k/conc=1 | 573,705.0 | 528,373.0 | -7.9% |
| Sink/items=1k/conc=4 | 840,212.0 | 794,836.0 | -5.4% |
| SinkFromChan/items=1k/conc=1 | 556,950.0 | 874,616.0 | +57.0% |
| SinkFromChan/items=1k/conc=4 | 842,111.0 | 1,232,408.0 | +46.3% |
| SinkFromChanWorkSweep/factor=1 | 2,235,933.0 | 3,432,982.0 | +53.5% |
| SinkFromChanWorkSweep/factor=2 | 2,966,809.0 | 4,251,727.0 | +43.3% |
| SinkFromChanWorkSweep/factor=4 | 5,416,450.0 | 5,682,765.0 | +4.9% |
| SinkFromChanWorkSweep/factor=8 | 7,321,386.0 | 8,438,926.0 | +15.3% |
| SinkWorkSweep/factor=1 | 2,257,432.0 | 2,115,298.0 | -6.3% |
| SinkWorkSweep/factor=2 | 2,979,196.0 | 2,789,846.0 | -6.4% |
| SinkWorkSweep/factor=4 | 5,371,261.0 | 4,799,908.0 | -10.6% |
| SinkWorkSweep/factor=8 | 7,309,190.0 | 7,877,359.0 | +7.8% |
| SplitBy/items=1k/conc=1 | 2,739,730.0 | 4,039,094.0 | +47.4% |
| SplitBy/items=1k/conc=4 | 3,394,053.0 | 3,914,731.0 | +15.3% |
| SplitByWorkSweep/factor=1 | 11,021,954.0 | 16,169,863.0 | +46.7% |
| SplitByWorkSweep/factor=2 | 11,559,889.0 | 17,134,403.0 | +48.2% |
| SplitByWorkSweep/factor=4 | 13,061,912.0 | 18,025,729.0 | +38.0% |
| SplitByWorkSweep/factor=8 | 16,118,214.0 | 19,687,271.0 | +22.1% |
| SplitMerge/items=1k/conc=1 | 4,681,522.0 | 6,278,498.0 | +34.1% |
| SplitMerge/items=1k/conc=4 | 6,316,471.0 | 5,975,387.0 | -5.4% |
| TwoStage/items=1k/conc=1 | 1,890,616.0 | 1,972,790.0 | +4.3% |
| TwoStage/items=1k/conc=4 | 3,615,776.0 | 2,756,655.0 | -23.8% |
