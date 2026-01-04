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
BenchmarkOneToOne/items=1k/conc=1/channels-8         	     106	   1115112 ns/op	     213 B/op	       4 allocs/op
BenchmarkOneToOne/items=1k/conc=1/channels-8         	     108	   1073341 ns/op	     197 B/op	       4 allocs/op
BenchmarkOneToOne/items=1k/conc=1/channels-8         	      96	   1080097 ns/op	     207 B/op	       4 allocs/op
BenchmarkOneToOne/items=1k/conc=1/channels-8         	     110	   1059343 ns/op	     174 B/op	       4 allocs/op
BenchmarkOneToOne/items=1k/conc=1/channels-8         	     111	   1070723 ns/op	     293 B/op	       4 allocs/op
BenchmarkOneToOne/items=1k/conc=1/pipeline-8         	     106	   1074085 ns/op	     836 B/op	      16 allocs/op
BenchmarkOneToOne/items=1k/conc=1/pipeline-8         	     106	   1075526 ns/op	     802 B/op	      16 allocs/op
BenchmarkOneToOne/items=1k/conc=1/pipeline-8         	     108	   1084427 ns/op	     780 B/op	      16 allocs/op
BenchmarkOneToOne/items=1k/conc=1/pipeline-8         	     106	   1081177 ns/op	     755 B/op	      16 allocs/op
BenchmarkOneToOne/items=1k/conc=1/pipeline-8         	     108	   1082018 ns/op	     819 B/op	      16 allocs/op
BenchmarkOneToOne/items=1k/conc=4/channels-8         	      67	   1797347 ns/op	     428 B/op	       7 allocs/op
BenchmarkOneToOne/items=1k/conc=4/channels-8         	      66	   1784528 ns/op	     414 B/op	       7 allocs/op
BenchmarkOneToOne/items=1k/conc=4/channels-8         	      70	   1566421 ns/op	     434 B/op	       7 allocs/op
BenchmarkOneToOne/items=1k/conc=4/channels-8         	      72	   1836990 ns/op	     361 B/op	       7 allocs/op
BenchmarkOneToOne/items=1k/conc=4/channels-8         	      67	   1876531 ns/op	     361 B/op	       7 allocs/op
BenchmarkOneToOne/items=1k/conc=4/pipeline-8         	      68	   1750369 ns/op	    2548 B/op	      32 allocs/op
BenchmarkOneToOne/items=1k/conc=4/pipeline-8         	      69	   1701044 ns/op	    2156 B/op	      31 allocs/op
BenchmarkOneToOne/items=1k/conc=4/pipeline-8         	      62	   1710091 ns/op	    2177 B/op	      31 allocs/op
BenchmarkOneToOne/items=1k/conc=4/pipeline-8         	      66	   1746141 ns/op	    2143 B/op	      31 allocs/op
BenchmarkOneToOne/items=1k/conc=4/pipeline-8         	      63	   1704480 ns/op	    2250 B/op	      32 allocs/op
BenchmarkOneToOneOrZero/items=1k/conc=1/channels-8   	     152	    783143 ns/op	     184 B/op	       4 allocs/op
BenchmarkOneToOneOrZero/items=1k/conc=1/channels-8   	     153	    761866 ns/op	     169 B/op	       4 allocs/op
BenchmarkOneToOneOrZero/items=1k/conc=1/channels-8   	     156	    775904 ns/op	     168 B/op	       4 allocs/op
BenchmarkOneToOneOrZero/items=1k/conc=1/channels-8   	     156	    793452 ns/op	     178 B/op	       4 allocs/op
BenchmarkOneToOneOrZero/items=1k/conc=1/channels-8   	     138	    787245 ns/op	     193 B/op	       4 allocs/op
BenchmarkOneToOneOrZero/items=1k/conc=1/pipeline-8   	     151	    791236 ns/op	     778 B/op	      16 allocs/op
BenchmarkOneToOneOrZero/items=1k/conc=1/pipeline-8   	     148	    784623 ns/op	     754 B/op	      16 allocs/op
BenchmarkOneToOneOrZero/items=1k/conc=1/pipeline-8   	     148	    786045 ns/op	     773 B/op	      16 allocs/op
BenchmarkOneToOneOrZero/items=1k/conc=1/pipeline-8   	     148	    773287 ns/op	     781 B/op	      16 allocs/op
BenchmarkOneToOneOrZero/items=1k/conc=1/pipeline-8   	     148	    778736 ns/op	     818 B/op	      16 allocs/op
BenchmarkOneToOneOrZero/items=1k/conc=4/channels-8   	      91	   1351095 ns/op	     361 B/op	       7 allocs/op
BenchmarkOneToOneOrZero/items=1k/conc=4/channels-8   	      82	   1230967 ns/op	     412 B/op	       7 allocs/op
BenchmarkOneToOneOrZero/items=1k/conc=4/channels-8   	     110	   1190039 ns/op	     389 B/op	       7 allocs/op
BenchmarkOneToOneOrZero/items=1k/conc=4/channels-8   	     100	   1170118 ns/op	     364 B/op	       7 allocs/op
BenchmarkOneToOneOrZero/items=1k/conc=4/channels-8   	     100	   1200278 ns/op	     364 B/op	       7 allocs/op
BenchmarkOneToOneOrZero/items=1k/conc=4/pipeline-8   	      94	   1234949 ns/op	    2187 B/op	      31 allocs/op
BenchmarkOneToOneOrZero/items=1k/conc=4/pipeline-8   	      87	   1236945 ns/op	    2234 B/op	      32 allocs/op
BenchmarkOneToOneOrZero/items=1k/conc=4/pipeline-8   	      88	   1275098 ns/op	    2180 B/op	      31 allocs/op
BenchmarkOneToOneOrZero/items=1k/conc=4/pipeline-8   	      93	   1284571 ns/op	    2237 B/op	      32 allocs/op
BenchmarkOneToOneOrZero/items=1k/conc=4/pipeline-8   	      93	   1235558 ns/op	    2193 B/op	      31 allocs/op
BenchmarkOneToMany/items=1k/conc=1/channels-8        	      76	   1515444 ns/op	     182 B/op	       4 allocs/op
BenchmarkOneToMany/items=1k/conc=1/channels-8        	      80	   1487349 ns/op	     180 B/op	       4 allocs/op
BenchmarkOneToMany/items=1k/conc=1/channels-8        	      78	   1500470 ns/op	     190 B/op	       4 allocs/op
BenchmarkOneToMany/items=1k/conc=1/channels-8        	      64	   1651691 ns/op	     183 B/op	       4 allocs/op
BenchmarkOneToMany/items=1k/conc=1/channels-8        	      79	   1497814 ns/op	     169 B/op	       4 allocs/op
BenchmarkOneToMany/items=1k/conc=1/pipeline-8        	      72	   1642973 ns/op	   17205 B/op	    1040 allocs/op
BenchmarkOneToMany/items=1k/conc=1/pipeline-8        	      72	   1635061 ns/op	   17141 B/op	    1040 allocs/op
BenchmarkOneToMany/items=1k/conc=1/pipeline-8        	      61	   1655676 ns/op	   17142 B/op	    1040 allocs/op
BenchmarkOneToMany/items=1k/conc=1/pipeline-8        	      62	   1688517 ns/op	   17141 B/op	    1040 allocs/op
BenchmarkOneToMany/items=1k/conc=1/pipeline-8        	      62	   1708025 ns/op	   17141 B/op	    1040 allocs/op
BenchmarkOneToMany/items=1k/conc=4/channels-8        	      45	   2408275 ns/op	     362 B/op	       7 allocs/op
BenchmarkOneToMany/items=1k/conc=4/channels-8        	      54	   2406418 ns/op	     362 B/op	       7 allocs/op
BenchmarkOneToMany/items=1k/conc=4/channels-8        	      49	   2592980 ns/op	     362 B/op	       7 allocs/op
BenchmarkOneToMany/items=1k/conc=4/channels-8        	      55	   2395600 ns/op	     369 B/op	       7 allocs/op
BenchmarkOneToMany/items=1k/conc=4/channels-8        	      55	   2578389 ns/op	     430 B/op	       7 allocs/op
BenchmarkOneToMany/items=1k/conc=4/pipeline-8        	      46	   2514134 ns/op	   18526 B/op	    1055 allocs/op
BenchmarkOneToMany/items=1k/conc=4/pipeline-8        	      46	   2531021 ns/op	   18456 B/op	    1055 allocs/op
BenchmarkOneToMany/items=1k/conc=4/pipeline-8        	      46	   2527987 ns/op	   18474 B/op	    1055 allocs/op
BenchmarkOneToMany/items=1k/conc=4/pipeline-8        	      46	   2511636 ns/op	   18564 B/op	    1056 allocs/op
BenchmarkOneToMany/items=1k/conc=4/pipeline-8        	      43	   2507921 ns/op	   18577 B/op	    1056 allocs/op
BenchmarkFromChan/items=1k/conc=1/channels-8         	     106	   1084083 ns/op	     169 B/op	       4 allocs/op
BenchmarkFromChan/items=1k/conc=1/channels-8         	      94	   1079527 ns/op	     169 B/op	       4 allocs/op
BenchmarkFromChan/items=1k/conc=1/channels-8         	     109	   1074864 ns/op	     169 B/op	       4 allocs/op
BenchmarkFromChan/items=1k/conc=1/channels-8         	     106	   1081759 ns/op	     176 B/op	       4 allocs/op
BenchmarkFromChan/items=1k/conc=1/channels-8         	      93	   1079555 ns/op	     169 B/op	       4 allocs/op
BenchmarkFromChan/items=1k/conc=1/pipeline-8         	      85	   1400446 ns/op	    1135 B/op	      20 allocs/op
BenchmarkFromChan/items=1k/conc=1/pipeline-8         	      72	   1457597 ns/op	    1142 B/op	      20 allocs/op
BenchmarkFromChan/items=1k/conc=1/pipeline-8         	      74	   1414895 ns/op	    1170 B/op	      20 allocs/op
BenchmarkFromChan/items=1k/conc=1/pipeline-8         	      72	   1560612 ns/op	    1161 B/op	      20 allocs/op
BenchmarkFromChan/items=1k/conc=1/pipeline-8         	      81	   1407276 ns/op	    1124 B/op	      20 allocs/op
BenchmarkFromChan/items=1k/conc=4/channels-8         	      69	   1547030 ns/op	     361 B/op	       7 allocs/op
BenchmarkFromChan/items=1k/conc=4/channels-8         	      76	   1690236 ns/op	     410 B/op	       7 allocs/op
BenchmarkFromChan/items=1k/conc=4/channels-8         	      67	   1844254 ns/op	     394 B/op	       7 allocs/op
BenchmarkFromChan/items=1k/conc=4/channels-8         	      78	   1708484 ns/op	     365 B/op	       7 allocs/op
BenchmarkFromChan/items=1k/conc=4/channels-8         	      62	   1722043 ns/op	     405 B/op	       7 allocs/op
BenchmarkFromChan/items=1k/conc=4/pipeline-8         	      78	   1513473 ns/op	    3494 B/op	      47 allocs/op
BenchmarkFromChan/items=1k/conc=4/pipeline-8         	      67	   1572516 ns/op	    3596 B/op	      48 allocs/op
BenchmarkFromChan/items=1k/conc=4/pipeline-8         	      78	   1548021 ns/op	    3540 B/op	      47 allocs/op
BenchmarkFromChan/items=1k/conc=4/pipeline-8         	      76	   1519654 ns/op	    3652 B/op	      48 allocs/op
BenchmarkFromChan/items=1k/conc=4/pipeline-8         	      75	   1685393 ns/op	    3514 B/op	      47 allocs/op
BenchmarkSink/items=1k/conc=1/channels-8             	     212	    551971 ns/op	     160 B/op	       4 allocs/op
BenchmarkSink/items=1k/conc=1/channels-8             	     217	    553663 ns/op	     160 B/op	       4 allocs/op
BenchmarkSink/items=1k/conc=1/channels-8             	     210	    569882 ns/op	     160 B/op	       4 allocs/op
BenchmarkSink/items=1k/conc=1/channels-8             	     184	    560307 ns/op	     162 B/op	       4 allocs/op
BenchmarkSink/items=1k/conc=1/channels-8             	     208	    569415 ns/op	     160 B/op	       4 allocs/op
BenchmarkSink/items=1k/conc=1/pipeline-8             	     225	    527589 ns/op	     667 B/op	      14 allocs/op
BenchmarkSink/items=1k/conc=1/pipeline-8             	     223	    528913 ns/op	     646 B/op	      14 allocs/op
BenchmarkSink/items=1k/conc=1/pipeline-8             	     223	    526436 ns/op	     647 B/op	      14 allocs/op
BenchmarkSink/items=1k/conc=1/pipeline-8             	     222	    520900 ns/op	     641 B/op	      14 allocs/op
BenchmarkSink/items=1k/conc=1/pipeline-8             	     228	    509326 ns/op	     641 B/op	      14 allocs/op
BenchmarkSink/items=1k/conc=4/channels-8             	     134	    822195 ns/op	     370 B/op	       7 allocs/op
BenchmarkSink/items=1k/conc=4/channels-8             	     145	    853074 ns/op	     352 B/op	       7 allocs/op
BenchmarkSink/items=1k/conc=4/channels-8             	     144	    827504 ns/op	     372 B/op	       7 allocs/op
BenchmarkSink/items=1k/conc=4/channels-8             	     145	    820689 ns/op	     377 B/op	       7 allocs/op
BenchmarkSink/items=1k/conc=4/channels-8             	     144	    833535 ns/op	     396 B/op	       7 allocs/op
BenchmarkSink/items=1k/conc=4/pipeline-8             	     144	    766162 ns/op	    1971 B/op	      29 allocs/op
BenchmarkSink/items=1k/conc=4/pipeline-8             	     139	    806279 ns/op	    1957 B/op	      29 allocs/op
BenchmarkSink/items=1k/conc=4/pipeline-8             	     144	    792286 ns/op	    1985 B/op	      29 allocs/op
BenchmarkSink/items=1k/conc=4/pipeline-8             	     146	    830816 ns/op	    2013 B/op	      29 allocs/op
BenchmarkSink/items=1k/conc=4/pipeline-8             	     146	    817931 ns/op	    1986 B/op	      29 allocs/op
BenchmarkSinkFromChan/items=1k/conc=1/channels-8     	     216	    550266 ns/op	     160 B/op	       4 allocs/op
BenchmarkSinkFromChan/items=1k/conc=1/channels-8     	     210	    566015 ns/op	     160 B/op	       4 allocs/op
BenchmarkSinkFromChan/items=1k/conc=1/channels-8     	     207	    568383 ns/op	     161 B/op	       4 allocs/op
BenchmarkSinkFromChan/items=1k/conc=1/channels-8     	     208	    566697 ns/op	     170 B/op	       4 allocs/op
BenchmarkSinkFromChan/items=1k/conc=1/channels-8     	     208	    564916 ns/op	     163 B/op	       4 allocs/op
BenchmarkSinkFromChan/items=1k/conc=1/pipeline-8     	     135	    853818 ns/op	    1010 B/op	      18 allocs/op
BenchmarkSinkFromChan/items=1k/conc=1/pipeline-8     	     139	    846389 ns/op	    1018 B/op	      18 allocs/op
BenchmarkSinkFromChan/items=1k/conc=1/pipeline-8     	     138	    847620 ns/op	    1010 B/op	      18 allocs/op
BenchmarkSinkFromChan/items=1k/conc=1/pipeline-8     	     140	    852165 ns/op	    1023 B/op	      18 allocs/op
BenchmarkSinkFromChan/items=1k/conc=1/pipeline-8     	     127	    848812 ns/op	    1034 B/op	      18 allocs/op
BenchmarkSinkFromChan/items=1k/conc=4/channels-8     	     134	    837927 ns/op	     405 B/op	       7 allocs/op
BenchmarkSinkFromChan/items=1k/conc=4/channels-8     	     140	    823648 ns/op	     352 B/op	       7 allocs/op
BenchmarkSinkFromChan/items=1k/conc=4/channels-8     	     145	    814096 ns/op	     355 B/op	       7 allocs/op
BenchmarkSinkFromChan/items=1k/conc=4/channels-8     	     145	    822775 ns/op	     420 B/op	       7 allocs/op
BenchmarkSinkFromChan/items=1k/conc=4/channels-8     	     142	    845094 ns/op	     398 B/op	       7 allocs/op
BenchmarkSinkFromChan/items=1k/conc=4/pipeline-8     	      93	   1220265 ns/op	    3363 B/op	      45 allocs/op
BenchmarkSinkFromChan/items=1k/conc=4/pipeline-8     	      90	   1222723 ns/op	    3421 B/op	      45 allocs/op
BenchmarkSinkFromChan/items=1k/conc=4/pipeline-8     	      97	   1210957 ns/op	    3411 B/op	      45 allocs/op
BenchmarkSinkFromChan/items=1k/conc=4/pipeline-8     	      97	   1225964 ns/op	    3381 B/op	      45 allocs/op
BenchmarkSinkFromChan/items=1k/conc=4/pipeline-8     	      85	   1228266 ns/op	    3404 B/op	      45 allocs/op
BenchmarkBatch/items=1k/conc=1/channels-8            	     205	    533914 ns/op	    8616 B/op	      37 allocs/op
BenchmarkBatch/items=1k/conc=1/channels-8            	     224	    564663 ns/op	    8647 B/op	      37 allocs/op
BenchmarkBatch/items=1k/conc=1/channels-8            	     223	    558109 ns/op	    8617 B/op	      37 allocs/op
BenchmarkBatch/items=1k/conc=1/channels-8            	     222	    545312 ns/op	    8616 B/op	      37 allocs/op
BenchmarkBatch/items=1k/conc=1/channels-8            	     218	    543386 ns/op	    8645 B/op	      37 allocs/op
BenchmarkBatch/items=1k/conc=1/pipeline-8            	     253	    446879 ns/op	   17090 B/op	     209 allocs/op
BenchmarkBatch/items=1k/conc=1/pipeline-8            	     270	    438723 ns/op	   17075 B/op	     209 allocs/op
BenchmarkBatch/items=1k/conc=1/pipeline-8            	     266	    438638 ns/op	   17087 B/op	     209 allocs/op
BenchmarkBatch/items=1k/conc=1/pipeline-8            	     265	    437027 ns/op	   17083 B/op	     209 allocs/op
BenchmarkBatch/items=1k/conc=1/pipeline-8            	     265	    441462 ns/op	   17088 B/op	     209 allocs/op
BenchmarkBatch/items=1k/conc=4/channels-8            	     142	    824861 ns/op	   10131 B/op	      45 allocs/op
BenchmarkBatch/items=1k/conc=4/channels-8            	     140	    755315 ns/op	   10131 B/op	      45 allocs/op
BenchmarkBatch/items=1k/conc=4/channels-8            	     159	    756294 ns/op	   10056 B/op	      44 allocs/op
BenchmarkBatch/items=1k/conc=4/channels-8            	     157	    754924 ns/op	   10118 B/op	      45 allocs/op
BenchmarkBatch/items=1k/conc=4/channels-8            	     158	    751128 ns/op	   10059 B/op	      44 allocs/op
BenchmarkBatch/items=1k/conc=4/pipeline-8            	     170	    733526 ns/op	   19037 B/op	     234 allocs/op
BenchmarkBatch/items=1k/conc=4/pipeline-8            	     168	    676980 ns/op	   19036 B/op	     234 allocs/op
BenchmarkBatch/items=1k/conc=4/pipeline-8            	     180	    658965 ns/op	   19042 B/op	     234 allocs/op
BenchmarkBatch/items=1k/conc=4/pipeline-8            	     182	    647063 ns/op	   19018 B/op	     233 allocs/op
BenchmarkBatch/items=1k/conc=4/pipeline-8            	     184	    650418 ns/op	   19043 B/op	     233 allocs/op
BenchmarkBatchChan/items=1k/conc=1/channels-8        	     103	   1111096 ns/op	    3753 B/op	      36 allocs/op
BenchmarkBatchChan/items=1k/conc=1/channels-8        	      94	   1117769 ns/op	    3753 B/op	      36 allocs/op
BenchmarkBatchChan/items=1k/conc=1/channels-8        	      97	   1122282 ns/op	    3753 B/op	      36 allocs/op
BenchmarkBatchChan/items=1k/conc=1/channels-8        	     106	   1137228 ns/op	    3753 B/op	      36 allocs/op
BenchmarkBatchChan/items=1k/conc=1/channels-8        	      90	   1123950 ns/op	    3753 B/op	      36 allocs/op
BenchmarkBatchChan/items=1k/conc=1/pipeline-8        	     128	    918793 ns/op	    4541 B/op	      49 allocs/op
BenchmarkBatchChan/items=1k/conc=1/pipeline-8        	     129	    912501 ns/op	    4540 B/op	      49 allocs/op
BenchmarkBatchChan/items=1k/conc=1/pipeline-8        	     127	    913484 ns/op	    4530 B/op	      49 allocs/op
BenchmarkBatchChan/items=1k/conc=1/pipeline-8        	     126	    942763 ns/op	    4530 B/op	      49 allocs/op
BenchmarkBatchChan/items=1k/conc=1/pipeline-8        	     123	    911042 ns/op	    4530 B/op	      49 allocs/op
BenchmarkBatchChan/items=1k/conc=4/channels-8        	     102	   1128904 ns/op	    4283 B/op	      42 allocs/op
BenchmarkBatchChan/items=1k/conc=4/channels-8        	      91	   1151152 ns/op	    4322 B/op	      42 allocs/op
BenchmarkBatchChan/items=1k/conc=4/channels-8        	      96	   1134506 ns/op	    4283 B/op	      42 allocs/op
BenchmarkBatchChan/items=1k/conc=4/channels-8        	      91	   1139898 ns/op	    4281 B/op	      42 allocs/op
BenchmarkBatchChan/items=1k/conc=4/channels-8        	      97	   1125441 ns/op	    4293 B/op	      42 allocs/op
BenchmarkBatchChan/items=1k/conc=4/pipeline-8        	     123	    940629 ns/op	    6679 B/op	      70 allocs/op
BenchmarkBatchChan/items=1k/conc=4/pipeline-8        	     124	    930680 ns/op	    6648 B/op	      70 allocs/op
BenchmarkBatchChan/items=1k/conc=4/pipeline-8        	     123	    938177 ns/op	    6637 B/op	      70 allocs/op
BenchmarkBatchChan/items=1k/conc=4/pipeline-8        	     123	    944831 ns/op	    6669 B/op	      70 allocs/op
BenchmarkBatchChan/items=1k/conc=4/pipeline-8        	     123	    929954 ns/op	    6675 B/op	      70 allocs/op
BenchmarkTwoStage/items=1k/conc=1/channels-8         	      63	   1844421 ns/op	     273 B/op	       7 allocs/op
BenchmarkTwoStage/items=1k/conc=1/channels-8         	      62	   1872360 ns/op	     273 B/op	       7 allocs/op
BenchmarkTwoStage/items=1k/conc=1/channels-8         	      55	   1868791 ns/op	     274 B/op	       7 allocs/op
BenchmarkTwoStage/items=1k/conc=1/channels-8         	      64	   1841911 ns/op	     287 B/op	       7 allocs/op
BenchmarkTwoStage/items=1k/conc=1/channels-8         	      64	   1860872 ns/op	     282 B/op	       7 allocs/op
BenchmarkTwoStage/items=1k/conc=1/pipeline-8         	      60	   1944426 ns/op	     870 B/op	      18 allocs/op
BenchmarkTwoStage/items=1k/conc=1/pipeline-8         	      56	   1971786 ns/op	     870 B/op	      18 allocs/op
BenchmarkTwoStage/items=1k/conc=1/pipeline-8         	      50	   2110487 ns/op	     871 B/op	      18 allocs/op
BenchmarkTwoStage/items=1k/conc=1/pipeline-8         	      60	   1954488 ns/op	     945 B/op	      18 allocs/op
BenchmarkTwoStage/items=1k/conc=1/pipeline-8         	      60	   1997365 ns/op	     916 B/op	      18 allocs/op
BenchmarkTwoStage/items=1k/conc=4/channels-8         	      34	   3461821 ns/op	     881 B/op	      13 allocs/op
BenchmarkTwoStage/items=1k/conc=4/channels-8         	      40	   3942321 ns/op	     658 B/op	      13 allocs/op
BenchmarkTwoStage/items=1k/conc=4/channels-8         	      33	   3461491 ns/op	     945 B/op	      15 allocs/op
BenchmarkTwoStage/items=1k/conc=4/channels-8         	      34	   3415013 ns/op	     684 B/op	      13 allocs/op
BenchmarkTwoStage/items=1k/conc=4/channels-8         	      40	   3433152 ns/op	     798 B/op	      14 allocs/op
BenchmarkTwoStage/items=1k/conc=4/pipeline-8         	      39	   2898371 ns/op	    3605 B/op	      48 allocs/op
BenchmarkTwoStage/items=1k/conc=4/pipeline-8         	      40	   2796307 ns/op	    3438 B/op	      46 allocs/op
BenchmarkTwoStage/items=1k/conc=4/pipeline-8         	      42	   2888091 ns/op	    3494 B/op	      47 allocs/op
BenchmarkTwoStage/items=1k/conc=4/pipeline-8         	      37	   2882779 ns/op	    3509 B/op	      47 allocs/op
BenchmarkTwoStage/items=1k/conc=4/pipeline-8         	      40	   2812262 ns/op	    3376 B/op	      46 allocs/op
BenchmarkSplitMerge/items=1k/conc=1/channels-8       	      26	   4818061 ns/op	     484 B/op	      12 allocs/op
BenchmarkSplitMerge/items=1k/conc=1/channels-8       	      26	   4677474 ns/op	     484 B/op	      12 allocs/op
BenchmarkSplitMerge/items=1k/conc=1/channels-8       	      26	   4414510 ns/op	     491 B/op	      12 allocs/op
BenchmarkSplitMerge/items=1k/conc=1/channels-8       	      22	   4699134 ns/op	     616 B/op	      13 allocs/op
BenchmarkSplitMerge/items=1k/conc=1/channels-8       	      26	   4740734 ns/op	     565 B/op	      12 allocs/op
BenchmarkSplitMerge/items=1k/conc=1/pipeline-8       	      20	   5148652 ns/op	    1333 B/op	      26 allocs/op
BenchmarkSplitMerge/items=1k/conc=1/pipeline-8       	      22	   5500521 ns/op	    1365 B/op	      27 allocs/op
BenchmarkSplitMerge/items=1k/conc=1/pipeline-8       	      22	   5078462 ns/op	    1264 B/op	      26 allocs/op
BenchmarkSplitMerge/items=1k/conc=1/pipeline-8       	      22	   5116080 ns/op	    1640 B/op	      30 allocs/op
BenchmarkSplitMerge/items=1k/conc=1/pipeline-8       	      24	   4960483 ns/op	    1671 B/op	      30 allocs/op
BenchmarkSplitMerge/items=1k/conc=4/channels-8       	      20	   5795271 ns/op	     884 B/op	      18 allocs/op
BenchmarkSplitMerge/items=1k/conc=4/channels-8       	      15	   6669103 ns/op	    1121 B/op	      20 allocs/op
BenchmarkSplitMerge/items=1k/conc=4/channels-8       	      20	   6746342 ns/op	     869 B/op	      18 allocs/op
BenchmarkSplitMerge/items=1k/conc=4/channels-8       	      20	   6669035 ns/op	    1093 B/op	      18 allocs/op
BenchmarkSplitMerge/items=1k/conc=4/channels-8       	      19	   6252327 ns/op	     950 B/op	      18 allocs/op
BenchmarkSplitMerge/items=1k/conc=4/pipeline-8       	      21	   5749758 ns/op	    4419 B/op	      61 allocs/op
BenchmarkSplitMerge/items=1k/conc=4/pipeline-8       	      18	   6068479 ns/op	    4121 B/op	      57 allocs/op
BenchmarkSplitMerge/items=1k/conc=4/pipeline-8       	      20	   5181198 ns/op	    3983 B/op	      54 allocs/op
BenchmarkSplitMerge/items=1k/conc=4/pipeline-8       	      20	   5080390 ns/op	    4108 B/op	      57 allocs/op
BenchmarkSplitMerge/items=1k/conc=4/pipeline-8       	      20	   5337994 ns/op	    3877 B/op	      54 allocs/op
BenchmarkSplitBy/items=1k/conc=1/channels-8          	      43	   2656965 ns/op	     482 B/op	      12 allocs/op
BenchmarkSplitBy/items=1k/conc=1/channels-8          	      38	   2787695 ns/op	     538 B/op	      12 allocs/op
BenchmarkSplitBy/items=1k/conc=1/channels-8          	      44	   3068852 ns/op	     489 B/op	      12 allocs/op
BenchmarkSplitBy/items=1k/conc=1/channels-8          	      40	   2853804 ns/op	     521 B/op	      12 allocs/op
BenchmarkSplitBy/items=1k/conc=1/channels-8          	      42	   2629248 ns/op	     619 B/op	      13 allocs/op
BenchmarkSplitBy/items=1k/conc=1/pipeline-8          	      34	   3131314 ns/op	    1341 B/op	      26 allocs/op
BenchmarkSplitBy/items=1k/conc=1/pipeline-8          	      38	   3076613 ns/op	    1385 B/op	      27 allocs/op
BenchmarkSplitBy/items=1k/conc=1/pipeline-8          	      34	   3042934 ns/op	    1290 B/op	      26 allocs/op
BenchmarkSplitBy/items=1k/conc=1/pipeline-8          	      37	   3085676 ns/op	    1432 B/op	      27 allocs/op
BenchmarkSplitBy/items=1k/conc=1/pipeline-8          	      34	   3132746 ns/op	    1378 B/op	      27 allocs/op
BenchmarkSplitBy/items=1k/conc=4/channels-8          	      32	   3273621 ns/op	    1008 B/op	      19 allocs/op
BenchmarkSplitBy/items=1k/conc=4/channels-8          	      31	   3435574 ns/op	    1025 B/op	      19 allocs/op
BenchmarkSplitBy/items=1k/conc=4/channels-8          	      34	   3436947 ns/op	     988 B/op	      19 allocs/op
BenchmarkSplitBy/items=1k/conc=4/channels-8          	      33	   3217658 ns/op	     867 B/op	      18 allocs/op
BenchmarkSplitBy/items=1k/conc=4/channels-8          	      34	   3225973 ns/op	     943 B/op	      18 allocs/op
BenchmarkSplitBy/items=1k/conc=4/pipeline-8          	      38	   2698919 ns/op	    4116 B/op	      57 allocs/op
BenchmarkSplitBy/items=1k/conc=4/pipeline-8          	      40	   2696613 ns/op	    3785 B/op	      54 allocs/op
BenchmarkSplitBy/items=1k/conc=4/pipeline-8          	      38	   3142090 ns/op	    3798 B/op	      54 allocs/op
BenchmarkSplitBy/items=1k/conc=4/pipeline-8          	      72	   2884713 ns/op	    3907 B/op	      55 allocs/op
BenchmarkSplitBy/items=1k/conc=4/pipeline-8          	      42	   2827882 ns/op	    4013 B/op	      56 allocs/op
BenchmarkOverheadWorkSweep/factor=1/channels-8       	      24	   4434389 ns/op	     172 B/op	       4 allocs/op
BenchmarkOverheadWorkSweep/factor=1/channels-8       	      24	   4354858 ns/op	     172 B/op	       4 allocs/op
BenchmarkOverheadWorkSweep/factor=1/channels-8       	      26	   4382207 ns/op	     172 B/op	       4 allocs/op
BenchmarkOverheadWorkSweep/factor=1/channels-8       	      26	   4367784 ns/op	     172 B/op	       4 allocs/op
BenchmarkOverheadWorkSweep/factor=1/channels-8       	      24	   4364809 ns/op	     176 B/op	       4 allocs/op
BenchmarkOverheadWorkSweep/factor=1/pipeline-8       	      24	   4357033 ns/op	     855 B/op	      17 allocs/op
BenchmarkOverheadWorkSweep/factor=1/pipeline-8       	      26	   4315793 ns/op	     766 B/op	      16 allocs/op
BenchmarkOverheadWorkSweep/factor=1/pipeline-8       	      26	   4354236 ns/op	     766 B/op	      16 allocs/op
BenchmarkOverheadWorkSweep/factor=1/pipeline-8       	      25	   4374930 ns/op	     766 B/op	      16 allocs/op
BenchmarkOverheadWorkSweep/factor=1/pipeline-8       	      25	   4615347 ns/op	     785 B/op	      16 allocs/op
BenchmarkOverheadWorkSweep/factor=2/channels-8       	      21	   5140327 ns/op	     173 B/op	       4 allocs/op
BenchmarkOverheadWorkSweep/factor=2/channels-8       	      21	   5279472 ns/op	     191 B/op	       4 allocs/op
BenchmarkOverheadWorkSweep/factor=2/channels-8       	      20	   5230990 ns/op	     173 B/op	       4 allocs/op
BenchmarkOverheadWorkSweep/factor=2/channels-8       	      22	   5182532 ns/op	     173 B/op	       4 allocs/op
BenchmarkOverheadWorkSweep/factor=2/channels-8       	      21	   6486522 ns/op	     173 B/op	       4 allocs/op
BenchmarkOverheadWorkSweep/factor=2/pipeline-8       	      21	   5114941 ns/op	     769 B/op	      16 allocs/op
BenchmarkOverheadWorkSweep/factor=2/pipeline-8       	      21	   5284958 ns/op	     769 B/op	      16 allocs/op
BenchmarkOverheadWorkSweep/factor=2/pipeline-8       	      22	   5423697 ns/op	     768 B/op	      16 allocs/op
BenchmarkOverheadWorkSweep/factor=2/pipeline-8       	      21	   5372760 ns/op	     815 B/op	      16 allocs/op
BenchmarkOverheadWorkSweep/factor=2/pipeline-8       	      21	   5360863 ns/op	     769 B/op	      16 allocs/op
BenchmarkOverheadWorkSweep/factor=4/channels-8       	      16	   6993445 ns/op	     175 B/op	       4 allocs/op
BenchmarkOverheadWorkSweep/factor=4/channels-8       	      15	   6749128 ns/op	     175 B/op	       4 allocs/op
BenchmarkOverheadWorkSweep/factor=4/channels-8       	      16	   6660560 ns/op	     217 B/op	       4 allocs/op
BenchmarkOverheadWorkSweep/factor=4/channels-8       	      16	   6736492 ns/op	     211 B/op	       4 allocs/op
BenchmarkOverheadWorkSweep/factor=4/channels-8       	      15	   6948886 ns/op	     220 B/op	       4 allocs/op
BenchmarkOverheadWorkSweep/factor=4/pipeline-8       	      15	   7222181 ns/op	     827 B/op	      16 allocs/op
BenchmarkOverheadWorkSweep/factor=4/pipeline-8       	      16	   6933591 ns/op	     775 B/op	      16 allocs/op
BenchmarkOverheadWorkSweep/factor=4/pipeline-8       	      16	   6607000 ns/op	     775 B/op	      16 allocs/op
BenchmarkOverheadWorkSweep/factor=4/pipeline-8       	      18	   6832340 ns/op	     772 B/op	      16 allocs/op
BenchmarkOverheadWorkSweep/factor=4/pipeline-8       	      16	   6756700 ns/op	     775 B/op	      16 allocs/op
BenchmarkOverheadWorkSweep/factor=8/channels-8       	      10	  12691133 ns/op	     179 B/op	       4 allocs/op
BenchmarkOverheadWorkSweep/factor=8/channels-8       	      10	  10446971 ns/op	     179 B/op	       4 allocs/op
BenchmarkOverheadWorkSweep/factor=8/channels-8       	      10	  10516138 ns/op	     179 B/op	       4 allocs/op
BenchmarkOverheadWorkSweep/factor=8/channels-8       	      10	  10336258 ns/op	     179 B/op	       4 allocs/op
BenchmarkOverheadWorkSweep/factor=8/channels-8       	      10	  11190204 ns/op	     179 B/op	       4 allocs/op
BenchmarkOverheadWorkSweep/factor=8/pipeline-8       	      12	   9605924 ns/op	     790 B/op	      16 allocs/op
BenchmarkOverheadWorkSweep/factor=8/pipeline-8       	      12	   9856219 ns/op	     782 B/op	      16 allocs/op
BenchmarkOverheadWorkSweep/factor=8/pipeline-8       	      12	  11057427 ns/op	     846 B/op	      16 allocs/op
BenchmarkOverheadWorkSweep/factor=8/pipeline-8       	      12	   9683108 ns/op	     782 B/op	      16 allocs/op
BenchmarkOverheadWorkSweep/factor=8/pipeline-8       	      12	   9762250 ns/op	     782 B/op	      16 allocs/op
BenchmarkOneToOneOrZeroWorkSweep/factor=1/channels-8 	      36	   3186920 ns/op	     171 B/op	       4 allocs/op
BenchmarkOneToOneOrZeroWorkSweep/factor=1/channels-8 	      33	   3261521 ns/op	     206 B/op	       4 allocs/op
BenchmarkOneToOneOrZeroWorkSweep/factor=1/channels-8 	      36	   3197082 ns/op	     195 B/op	       4 allocs/op
BenchmarkOneToOneOrZeroWorkSweep/factor=1/channels-8 	      31	   3236685 ns/op	     180 B/op	       4 allocs/op
BenchmarkOneToOneOrZeroWorkSweep/factor=1/channels-8 	      33	   3235042 ns/op	     177 B/op	       4 allocs/op
BenchmarkOneToOneOrZeroWorkSweep/factor=1/pipeline-8 	      32	   3341221 ns/op	     796 B/op	      16 allocs/op
BenchmarkOneToOneOrZeroWorkSweep/factor=1/pipeline-8 	      36	   3247763 ns/op	     762 B/op	      16 allocs/op
BenchmarkOneToOneOrZeroWorkSweep/factor=1/pipeline-8 	      36	   3265436 ns/op	     863 B/op	      17 allocs/op
BenchmarkOneToOneOrZeroWorkSweep/factor=1/pipeline-8 	      33	   3595595 ns/op	     774 B/op	      16 allocs/op
BenchmarkOneToOneOrZeroWorkSweep/factor=1/pipeline-8 	      31	   3252911 ns/op	     776 B/op	      16 allocs/op
BenchmarkOneToOneOrZeroWorkSweep/factor=2/channels-8 	      33	   3660524 ns/op	     220 B/op	       4 allocs/op
BenchmarkOneToOneOrZeroWorkSweep/factor=2/channels-8 	      26	   4567579 ns/op	     201 B/op	       4 allocs/op
BenchmarkOneToOneOrZeroWorkSweep/factor=2/channels-8 	      32	   3655299 ns/op	     171 B/op	       4 allocs/op
BenchmarkOneToOneOrZeroWorkSweep/factor=2/channels-8 	      28	   3635827 ns/op	     172 B/op	       4 allocs/op
BenchmarkOneToOneOrZeroWorkSweep/factor=2/channels-8 	      32	   3634470 ns/op	     171 B/op	       4 allocs/op
BenchmarkOneToOneOrZeroWorkSweep/factor=2/pipeline-8 	      28	   4046438 ns/op	     765 B/op	      16 allocs/op
BenchmarkOneToOneOrZeroWorkSweep/factor=2/pipeline-8 	      32	   3652887 ns/op	     763 B/op	      16 allocs/op
BenchmarkOneToOneOrZeroWorkSweep/factor=2/pipeline-8 	      28	   3955251 ns/op	     765 B/op	      16 allocs/op
BenchmarkOneToOneOrZeroWorkSweep/factor=2/pipeline-8 	      28	   3961955 ns/op	     765 B/op	      16 allocs/op
BenchmarkOneToOneOrZeroWorkSweep/factor=2/pipeline-8 	      31	   3852337 ns/op	     816 B/op	      16 allocs/op
BenchmarkOneToOneOrZeroWorkSweep/factor=4/channels-8 	      25	   4451853 ns/op	     172 B/op	       4 allocs/op
BenchmarkOneToOneOrZeroWorkSweep/factor=4/channels-8 	      26	   4457433 ns/op	     172 B/op	       4 allocs/op
BenchmarkOneToOneOrZeroWorkSweep/factor=4/channels-8 	      22	   4776858 ns/op	     173 B/op	       4 allocs/op
BenchmarkOneToOneOrZeroWorkSweep/factor=4/channels-8 	      26	   4489508 ns/op	     172 B/op	       4 allocs/op
BenchmarkOneToOneOrZeroWorkSweep/factor=4/channels-8 	      26	   5145486 ns/op	     172 B/op	       4 allocs/op
BenchmarkOneToOneOrZeroWorkSweep/factor=4/pipeline-8 	      26	   4490550 ns/op	     840 B/op	      16 allocs/op
BenchmarkOneToOneOrZeroWorkSweep/factor=4/pipeline-8 	      25	   4502182 ns/op	     839 B/op	      16 allocs/op
BenchmarkOneToOneOrZeroWorkSweep/factor=4/pipeline-8 	      26	   4505931 ns/op	     810 B/op	      16 allocs/op
BenchmarkOneToOneOrZeroWorkSweep/factor=4/pipeline-8 	      25	   4516375 ns/op	     766 B/op	      16 allocs/op
BenchmarkOneToOneOrZeroWorkSweep/factor=4/pipeline-8 	      24	   4517545 ns/op	     767 B/op	      16 allocs/op
BenchmarkOneToOneOrZeroWorkSweep/factor=8/channels-8 	      19	   6348171 ns/op	     173 B/op	       4 allocs/op
BenchmarkOneToOneOrZeroWorkSweep/factor=8/channels-8 	      18	   6295280 ns/op	     174 B/op	       4 allocs/op
BenchmarkOneToOneOrZeroWorkSweep/factor=8/channels-8 	      18	   6308660 ns/op	     232 B/op	       4 allocs/op
BenchmarkOneToOneOrZeroWorkSweep/factor=8/channels-8 	      16	   6328253 ns/op	     247 B/op	       4 allocs/op
BenchmarkOneToOneOrZeroWorkSweep/factor=8/channels-8 	      16	   6330958 ns/op	     253 B/op	       4 allocs/op
BenchmarkOneToOneOrZeroWorkSweep/factor=8/pipeline-8 	      18	   6604269 ns/op	     831 B/op	      16 allocs/op
BenchmarkOneToOneOrZeroWorkSweep/factor=8/pipeline-8 	      18	   6089025 ns/op	     879 B/op	      17 allocs/op
BenchmarkOneToOneOrZeroWorkSweep/factor=8/pipeline-8 	      19	   7427605 ns/op	     922 B/op	      17 allocs/op
BenchmarkOneToOneOrZeroWorkSweep/factor=8/pipeline-8 	      16	   6268485 ns/op	     799 B/op	      16 allocs/op
BenchmarkOneToOneOrZeroWorkSweep/factor=8/pipeline-8 	      19	   6120601 ns/op	     771 B/op	      16 allocs/op
BenchmarkOneToManyWorkSweep/factor=1/channels-8      	      18	   6292095 ns/op	     174 B/op	       4 allocs/op
BenchmarkOneToManyWorkSweep/factor=1/channels-8      	      16	   6259896 ns/op	     223 B/op	       4 allocs/op
BenchmarkOneToManyWorkSweep/factor=1/channels-8      	      18	   6125576 ns/op	     174 B/op	       4 allocs/op
BenchmarkOneToManyWorkSweep/factor=1/channels-8      	      19	   6253507 ns/op	     173 B/op	       4 allocs/op
BenchmarkOneToManyWorkSweep/factor=1/channels-8      	      19	   6040197 ns/op	     173 B/op	       4 allocs/op
BenchmarkOneToManyWorkSweep/factor=1/pipeline-8      	      15	   6770777 ns/op	   66312 B/op	    4112 allocs/op
BenchmarkOneToManyWorkSweep/factor=1/pipeline-8      	      15	   6753375 ns/op	   66312 B/op	    4112 allocs/op
BenchmarkOneToManyWorkSweep/factor=1/pipeline-8      	      16	   6577112 ns/op	   66311 B/op	    4112 allocs/op
BenchmarkOneToManyWorkSweep/factor=1/pipeline-8      	      15	   6736550 ns/op	   66312 B/op	    4112 allocs/op
BenchmarkOneToManyWorkSweep/factor=1/pipeline-8      	      16	   6568867 ns/op	   66311 B/op	    4112 allocs/op
BenchmarkOneToManyWorkSweep/factor=2/channels-8      	      15	   6799811 ns/op	     220 B/op	       4 allocs/op
BenchmarkOneToManyWorkSweep/factor=2/channels-8      	      16	   6765573 ns/op	     247 B/op	       4 allocs/op
BenchmarkOneToManyWorkSweep/factor=2/channels-8      	      16	   7017563 ns/op	     187 B/op	       4 allocs/op
BenchmarkOneToManyWorkSweep/factor=2/channels-8      	      15	   6973600 ns/op	     175 B/op	       4 allocs/op
BenchmarkOneToManyWorkSweep/factor=2/channels-8      	      16	   7040547 ns/op	     193 B/op	       4 allocs/op
BenchmarkOneToManyWorkSweep/factor=2/pipeline-8      	      14	   9454527 ns/op	   66376 B/op	    4112 allocs/op
BenchmarkOneToManyWorkSweep/factor=2/pipeline-8      	      14	   7397375 ns/op	   66389 B/op	    4113 allocs/op
BenchmarkOneToManyWorkSweep/factor=2/pipeline-8      	      15	   7484342 ns/op	   66491 B/op	    4114 allocs/op
BenchmarkOneToManyWorkSweep/factor=2/pipeline-8      	      14	   7265875 ns/op	   66437 B/op	    4113 allocs/op
BenchmarkOneToManyWorkSweep/factor=2/pipeline-8      	      16	   7697052 ns/op	   66359 B/op	    4112 allocs/op
BenchmarkOneToManyWorkSweep/factor=4/channels-8      	      12	   8461153 ns/op	     185 B/op	       4 allocs/op
BenchmarkOneToManyWorkSweep/factor=4/channels-8      	      13	   8352167 ns/op	     250 B/op	       4 allocs/op
BenchmarkOneToManyWorkSweep/factor=4/channels-8      	      13	   8360253 ns/op	     198 B/op	       4 allocs/op
BenchmarkOneToManyWorkSweep/factor=4/channels-8      	      13	   8315997 ns/op	     176 B/op	       4 allocs/op
BenchmarkOneToManyWorkSweep/factor=4/channels-8      	      13	   8523122 ns/op	     213 B/op	       4 allocs/op
BenchmarkOneToManyWorkSweep/factor=4/pipeline-8      	      12	   8687886 ns/op	   66318 B/op	    4112 allocs/op
BenchmarkOneToManyWorkSweep/factor=4/pipeline-8      	      12	   8867642 ns/op	   66318 B/op	    4112 allocs/op
BenchmarkOneToManyWorkSweep/factor=4/pipeline-8      	      12	   9058448 ns/op	   66318 B/op	    4112 allocs/op
BenchmarkOneToManyWorkSweep/factor=4/pipeline-8      	      14	   8593509 ns/op	   66376 B/op	    4112 allocs/op
BenchmarkOneToManyWorkSweep/factor=4/pipeline-8      	      13	  10027343 ns/op	   66338 B/op	    4112 allocs/op
BenchmarkOneToManyWorkSweep/factor=8/channels-8      	      10	  10684304 ns/op	     179 B/op	       4 allocs/op
BenchmarkOneToManyWorkSweep/factor=8/channels-8      	      10	  10606829 ns/op	     179 B/op	       4 allocs/op
BenchmarkOneToManyWorkSweep/factor=8/channels-8      	      10	  11040133 ns/op	     236 B/op	       4 allocs/op
BenchmarkOneToManyWorkSweep/factor=8/channels-8      	      10	  10255034 ns/op	     246 B/op	       4 allocs/op
BenchmarkOneToManyWorkSweep/factor=8/channels-8      	      10	  10234467 ns/op	     236 B/op	       4 allocs/op
BenchmarkOneToManyWorkSweep/factor=8/pipeline-8      	       8	  13993667 ns/op	   66370 B/op	    4112 allocs/op
BenchmarkOneToManyWorkSweep/factor=8/pipeline-8      	       9	  11512287 ns/op	   66499 B/op	    4114 allocs/op
BenchmarkOneToManyWorkSweep/factor=8/pipeline-8      	       9	  11535523 ns/op	   66360 B/op	    4112 allocs/op
BenchmarkOneToManyWorkSweep/factor=8/pipeline-8      	       9	  11477699 ns/op	   66446 B/op	    4113 allocs/op
BenchmarkOneToManyWorkSweep/factor=8/pipeline-8      	       9	  11439940 ns/op	   66446 B/op	    4113 allocs/op
BenchmarkFromChanWorkSweep/factor=1/channels-8       	      26	   4386950 ns/op	     172 B/op	       4 allocs/op
BenchmarkFromChanWorkSweep/factor=1/channels-8       	      26	   4324354 ns/op	     172 B/op	       4 allocs/op
BenchmarkFromChanWorkSweep/factor=1/channels-8       	      25	   4354227 ns/op	     172 B/op	       4 allocs/op
BenchmarkFromChanWorkSweep/factor=1/channels-8       	      26	   4352223 ns/op	     183 B/op	       4 allocs/op
BenchmarkFromChanWorkSweep/factor=1/channels-8       	      26	   4383045 ns/op	     172 B/op	       4 allocs/op
BenchmarkFromChanWorkSweep/factor=1/pipeline-8       	      18	   5861664 ns/op	    1295 B/op	      21 allocs/op
BenchmarkFromChanWorkSweep/factor=1/pipeline-8       	      20	   5687531 ns/op	    1138 B/op	      20 allocs/op
BenchmarkFromChanWorkSweep/factor=1/pipeline-8       	      19	   5649974 ns/op	    1139 B/op	      20 allocs/op
BenchmarkFromChanWorkSweep/factor=1/pipeline-8       	      26	   5719673 ns/op	    1134 B/op	      20 allocs/op
BenchmarkFromChanWorkSweep/factor=1/pipeline-8       	      20	   5608554 ns/op	    1229 B/op	      21 allocs/op
BenchmarkFromChanWorkSweep/factor=2/channels-8       	      22	   5122822 ns/op	     173 B/op	       4 allocs/op
BenchmarkFromChanWorkSweep/factor=2/channels-8       	      22	   5224703 ns/op	     181 B/op	       4 allocs/op
BenchmarkFromChanWorkSweep/factor=2/channels-8       	      20	   5097327 ns/op	     188 B/op	       4 allocs/op
BenchmarkFromChanWorkSweep/factor=2/channels-8       	      22	   5094767 ns/op	     181 B/op	       4 allocs/op
BenchmarkFromChanWorkSweep/factor=2/channels-8       	      20	   5131236 ns/op	     173 B/op	       4 allocs/op
BenchmarkFromChanWorkSweep/factor=2/pipeline-8       	      18	   6621572 ns/op	    1316 B/op	      22 allocs/op
BenchmarkFromChanWorkSweep/factor=2/pipeline-8       	      16	   6704612 ns/op	    1143 B/op	      20 allocs/op
BenchmarkFromChanWorkSweep/factor=2/pipeline-8       	      14	   7338902 ns/op	    1249 B/op	      21 allocs/op
BenchmarkFromChanWorkSweep/factor=2/pipeline-8       	      15	   7178139 ns/op	    1246 B/op	      21 allocs/op
BenchmarkFromChanWorkSweep/factor=2/pipeline-8       	      15	   6929753 ns/op	    1240 B/op	      21 allocs/op
BenchmarkFromChanWorkSweep/factor=4/channels-8       	      16	   7109344 ns/op	     211 B/op	       4 allocs/op
BenchmarkFromChanWorkSweep/factor=4/channels-8       	      16	   6822482 ns/op	     187 B/op	       4 allocs/op
BenchmarkFromChanWorkSweep/factor=4/channels-8       	      16	   6918766 ns/op	     229 B/op	       4 allocs/op
BenchmarkFromChanWorkSweep/factor=4/channels-8       	      15	   6951072 ns/op	     188 B/op	       4 allocs/op
BenchmarkFromChanWorkSweep/factor=4/channels-8       	      18	   6923113 ns/op	     174 B/op	       4 allocs/op
BenchmarkFromChanWorkSweep/factor=4/pipeline-8       	      14	   8307613 ns/op	    1146 B/op	      20 allocs/op
BenchmarkFromChanWorkSweep/factor=4/pipeline-8       	      13	   8405330 ns/op	    1148 B/op	      20 allocs/op
BenchmarkFromChanWorkSweep/factor=4/pipeline-8       	      13	   8330737 ns/op	    1214 B/op	      20 allocs/op
BenchmarkFromChanWorkSweep/factor=4/pipeline-8       	      14	   8347771 ns/op	    1146 B/op	      20 allocs/op
BenchmarkFromChanWorkSweep/factor=4/pipeline-8       	      13	   8471734 ns/op	    1148 B/op	      20 allocs/op
BenchmarkFromChanWorkSweep/factor=8/channels-8       	      10	  10433183 ns/op	     179 B/op	       4 allocs/op
BenchmarkFromChanWorkSweep/factor=8/channels-8       	      10	  10380942 ns/op	     179 B/op	       4 allocs/op
BenchmarkFromChanWorkSweep/factor=8/channels-8       	      10	  10490296 ns/op	     198 B/op	       4 allocs/op
BenchmarkFromChanWorkSweep/factor=8/channels-8       	      10	  10463533 ns/op	     227 B/op	       4 allocs/op
BenchmarkFromChanWorkSweep/factor=8/channels-8       	      10	  10420216 ns/op	     284 B/op	       5 allocs/op
BenchmarkFromChanWorkSweep/factor=8/pipeline-8       	       9	  11345592 ns/op	    1320 B/op	      22 allocs/op
BenchmarkFromChanWorkSweep/factor=8/pipeline-8       	       9	  11185958 ns/op	    1224 B/op	      21 allocs/op
BenchmarkFromChanWorkSweep/factor=8/pipeline-8       	       9	  11246185 ns/op	    1160 B/op	      20 allocs/op
BenchmarkFromChanWorkSweep/factor=8/pipeline-8       	      10	  11077158 ns/op	    1156 B/op	      20 allocs/op
BenchmarkFromChanWorkSweep/factor=8/pipeline-8       	       9	  11309190 ns/op	    1352 B/op	      22 allocs/op
BenchmarkSinkWorkSweep/factor=1/channels-8           	      51	   2251079 ns/op	     162 B/op	       4 allocs/op
BenchmarkSinkWorkSweep/factor=1/channels-8           	      52	   2270488 ns/op	     171 B/op	       4 allocs/op
BenchmarkSinkWorkSweep/factor=1/channels-8           	      54	   2297154 ns/op	     167 B/op	       4 allocs/op
BenchmarkSinkWorkSweep/factor=1/channels-8           	      51	   2271101 ns/op	     165 B/op	       4 allocs/op
BenchmarkSinkWorkSweep/factor=1/channels-8           	      42	   2548268 ns/op	     167 B/op	       4 allocs/op
BenchmarkSinkWorkSweep/factor=1/pipeline-8           	      54	   2140131 ns/op	     666 B/op	      14 allocs/op
BenchmarkSinkWorkSweep/factor=1/pipeline-8           	      54	   2154949 ns/op	     648 B/op	      14 allocs/op
BenchmarkSinkWorkSweep/factor=1/pipeline-8           	      55	   2142192 ns/op	     671 B/op	      14 allocs/op
BenchmarkSinkWorkSweep/factor=1/pipeline-8           	      49	   2134038 ns/op	     647 B/op	      14 allocs/op
BenchmarkSinkWorkSweep/factor=1/pipeline-8           	      54	   2151594 ns/op	     689 B/op	      14 allocs/op
BenchmarkSinkWorkSweep/factor=2/channels-8           	      39	   3035277 ns/op	     162 B/op	       4 allocs/op
BenchmarkSinkWorkSweep/factor=2/channels-8           	      39	   2963435 ns/op	     162 B/op	       4 allocs/op
BenchmarkSinkWorkSweep/factor=2/channels-8           	      39	   3587688 ns/op	     162 B/op	       4 allocs/op
BenchmarkSinkWorkSweep/factor=2/channels-8           	      34	   3033718 ns/op	     163 B/op	       4 allocs/op
BenchmarkSinkWorkSweep/factor=2/channels-8           	      37	   3017054 ns/op	     163 B/op	       4 allocs/op
BenchmarkSinkWorkSweep/factor=2/pipeline-8           	      38	   2917450 ns/op	     649 B/op	      14 allocs/op
BenchmarkSinkWorkSweep/factor=2/pipeline-8           	      38	   2895189 ns/op	     669 B/op	      14 allocs/op
BenchmarkSinkWorkSweep/factor=2/pipeline-8           	      40	   2844902 ns/op	     666 B/op	      14 allocs/op
BenchmarkSinkWorkSweep/factor=2/pipeline-8           	      38	   2896868 ns/op	     664 B/op	      14 allocs/op
BenchmarkSinkWorkSweep/factor=2/pipeline-8           	      37	   2962395 ns/op	     649 B/op	      14 allocs/op
BenchmarkSinkWorkSweep/factor=4/channels-8           	      19	   5319439 ns/op	     176 B/op	       4 allocs/op
BenchmarkSinkWorkSweep/factor=4/channels-8           	      21	   5292778 ns/op	     174 B/op	       4 allocs/op
BenchmarkSinkWorkSweep/factor=4/channels-8           	      24	   5371240 ns/op	     164 B/op	       4 allocs/op
BenchmarkSinkWorkSweep/factor=4/channels-8           	      21	   5496641 ns/op	     188 B/op	       4 allocs/op
BenchmarkSinkWorkSweep/factor=4/channels-8           	      21	   5572853 ns/op	     165 B/op	       4 allocs/op
BenchmarkSinkWorkSweep/factor=4/pipeline-8           	      24	   5572469 ns/op	     735 B/op	      14 allocs/op
BenchmarkSinkWorkSweep/factor=4/pipeline-8           	      24	   4890045 ns/op	     767 B/op	      15 allocs/op
BenchmarkSinkWorkSweep/factor=4/pipeline-8           	      22	   4976494 ns/op	     757 B/op	      15 allocs/op
BenchmarkSinkWorkSweep/factor=4/pipeline-8           	      24	   4965998 ns/op	     679 B/op	      14 allocs/op
BenchmarkSinkWorkSweep/factor=4/pipeline-8           	      24	   5470677 ns/op	     655 B/op	      14 allocs/op
BenchmarkSinkWorkSweep/factor=8/channels-8           	      14	   7343604 ns/op	     168 B/op	       4 allocs/op
BenchmarkSinkWorkSweep/factor=8/channels-8           	      14	   7527211 ns/op	     168 B/op	       4 allocs/op
BenchmarkSinkWorkSweep/factor=8/channels-8           	      15	   7456914 ns/op	     167 B/op	       4 allocs/op
BenchmarkSinkWorkSweep/factor=8/channels-8           	      15	   7480239 ns/op	     167 B/op	       4 allocs/op
BenchmarkSinkWorkSweep/factor=8/channels-8           	      14	   8266089 ns/op	     168 B/op	       4 allocs/op
BenchmarkSinkWorkSweep/factor=8/pipeline-8           	      13	   8127728 ns/op	     668 B/op	      14 allocs/op
BenchmarkSinkWorkSweep/factor=8/pipeline-8           	      13	   7962718 ns/op	     668 B/op	      14 allocs/op
BenchmarkSinkWorkSweep/factor=8/pipeline-8           	      15	   7899136 ns/op	     664 B/op	      14 allocs/op
BenchmarkSinkWorkSweep/factor=8/pipeline-8           	      14	   7862063 ns/op	     666 B/op	      14 allocs/op
BenchmarkSinkWorkSweep/factor=8/pipeline-8           	      14	   7640357 ns/op	     666 B/op	      14 allocs/op
BenchmarkSinkFromChanWorkSweep/factor=1/channels-8   	      46	   2262954 ns/op	     162 B/op	       4 allocs/op
BenchmarkSinkFromChanWorkSweep/factor=1/channels-8   	      51	   2249936 ns/op	     162 B/op	       4 allocs/op
BenchmarkSinkFromChanWorkSweep/factor=1/channels-8   	      52	   2327591 ns/op	     182 B/op	       4 allocs/op
BenchmarkSinkFromChanWorkSweep/factor=1/channels-8   	      49	   2322166 ns/op	     162 B/op	       4 allocs/op
BenchmarkSinkFromChanWorkSweep/factor=1/channels-8   	      45	   2298635 ns/op	     166 B/op	       4 allocs/op
BenchmarkSinkFromChanWorkSweep/factor=1/pipeline-8   	      33	   3498948 ns/op	    1106 B/op	      19 allocs/op
BenchmarkSinkFromChanWorkSweep/factor=1/pipeline-8   	      34	   3488767 ns/op	    1030 B/op	      18 allocs/op
BenchmarkSinkFromChanWorkSweep/factor=1/pipeline-8   	      33	   3415885 ns/op	    1019 B/op	      18 allocs/op
BenchmarkSinkFromChanWorkSweep/factor=1/pipeline-8   	      34	   3397042 ns/op	    1018 B/op	      18 allocs/op
BenchmarkSinkFromChanWorkSweep/factor=1/pipeline-8   	      34	   3645301 ns/op	    1018 B/op	      18 allocs/op
BenchmarkSinkFromChanWorkSweep/factor=2/channels-8   	      38	   2956514 ns/op	     162 B/op	       4 allocs/op
BenchmarkSinkFromChanWorkSweep/factor=2/channels-8   	      38	   2952494 ns/op	     162 B/op	       4 allocs/op
BenchmarkSinkFromChanWorkSweep/factor=2/channels-8   	      39	   2944675 ns/op	     162 B/op	       4 allocs/op
BenchmarkSinkFromChanWorkSweep/factor=2/channels-8   	      40	   2953856 ns/op	     162 B/op	       4 allocs/op
BenchmarkSinkFromChanWorkSweep/factor=2/channels-8   	      39	   3156496 ns/op	     162 B/op	       4 allocs/op
BenchmarkSinkFromChanWorkSweep/factor=2/pipeline-8   	      26	   4277833 ns/op	    1118 B/op	      19 allocs/op
BenchmarkSinkFromChanWorkSweep/factor=2/pipeline-8   	      26	   4245176 ns/op	    1110 B/op	      19 allocs/op
BenchmarkSinkFromChanWorkSweep/factor=2/pipeline-8   	      27	   4223753 ns/op	    1042 B/op	      18 allocs/op
BenchmarkSinkFromChanWorkSweep/factor=2/pipeline-8   	      26	   4292380 ns/op	    1022 B/op	      18 allocs/op
BenchmarkSinkFromChanWorkSweep/factor=2/pipeline-8   	      27	   4244161 ns/op	    1042 B/op	      18 allocs/op
BenchmarkSinkFromChanWorkSweep/factor=4/channels-8   	      22	   5438123 ns/op	     165 B/op	       4 allocs/op
BenchmarkSinkFromChanWorkSweep/factor=4/channels-8   	      21	   5426472 ns/op	     179 B/op	       4 allocs/op
BenchmarkSinkFromChanWorkSweep/factor=4/channels-8   	      21	   5387726 ns/op	     165 B/op	       4 allocs/op
BenchmarkSinkFromChanWorkSweep/factor=4/channels-8   	      22	   5456822 ns/op	     165 B/op	       4 allocs/op
BenchmarkSinkFromChanWorkSweep/factor=4/channels-8   	      20	   5558463 ns/op	     165 B/op	       4 allocs/op
BenchmarkSinkFromChanWorkSweep/factor=4/pipeline-8   	      20	   5670863 ns/op	    1031 B/op	      18 allocs/op
BenchmarkSinkFromChanWorkSweep/factor=4/pipeline-8   	      20	   5722377 ns/op	    1045 B/op	      18 allocs/op
BenchmarkSinkFromChanWorkSweep/factor=4/pipeline-8   	      19	   5612509 ns/op	    1027 B/op	      18 allocs/op
BenchmarkSinkFromChanWorkSweep/factor=4/pipeline-8   	      19	   5640219 ns/op	    1027 B/op	      18 allocs/op
BenchmarkSinkFromChanWorkSweep/factor=4/pipeline-8   	      19	   6087814 ns/op	    1027 B/op	      18 allocs/op
BenchmarkSinkFromChanWorkSweep/factor=8/channels-8   	      15	   7292939 ns/op	     167 B/op	       4 allocs/op
BenchmarkSinkFromChanWorkSweep/factor=8/channels-8   	      15	   7269167 ns/op	     167 B/op	       4 allocs/op
BenchmarkSinkFromChanWorkSweep/factor=8/channels-8   	      15	   7279233 ns/op	     167 B/op	       4 allocs/op
BenchmarkSinkFromChanWorkSweep/factor=8/channels-8   	      15	   7392700 ns/op	     167 B/op	       4 allocs/op
BenchmarkSinkFromChanWorkSweep/factor=8/channels-8   	      14	   7827518 ns/op	     168 B/op	       4 allocs/op
BenchmarkSinkFromChanWorkSweep/factor=8/pipeline-8   	      13	   8475872 ns/op	    1036 B/op	      18 allocs/op
BenchmarkSinkFromChanWorkSweep/factor=8/pipeline-8   	      13	   8650667 ns/op	    1174 B/op	      18 allocs/op
BenchmarkSinkFromChanWorkSweep/factor=8/pipeline-8   	      13	   8402305 ns/op	    1169 B/op	      19 allocs/op
BenchmarkSinkFromChanWorkSweep/factor=8/pipeline-8   	      13	   9105384 ns/op	    1036 B/op	      18 allocs/op
BenchmarkSinkFromChanWorkSweep/factor=8/pipeline-8   	      13	   8435411 ns/op	    1036 B/op	      18 allocs/op
BenchmarkBatchWorkSweep/factor=1/channels-8          	      46	   2196609 ns/op	   33194 B/op	     133 allocs/op
BenchmarkBatchWorkSweep/factor=1/channels-8          	      51	   2197086 ns/op	   33194 B/op	     133 allocs/op
BenchmarkBatchWorkSweep/factor=1/channels-8          	      56	   2177999 ns/op	   33271 B/op	     133 allocs/op
BenchmarkBatchWorkSweep/factor=1/channels-8          	      46	   2390606 ns/op	   33238 B/op	     133 allocs/op
BenchmarkBatchWorkSweep/factor=1/channels-8          	      52	   2188814 ns/op	   33194 B/op	     133 allocs/op
BenchmarkBatchWorkSweep/factor=1/pipeline-8          	      61	   1721933 ns/op	   65464 B/op	     785 allocs/op
BenchmarkBatchWorkSweep/factor=1/pipeline-8          	      56	   1786491 ns/op	   65558 B/op	     786 allocs/op
BenchmarkBatchWorkSweep/factor=1/pipeline-8          	      70	   1752686 ns/op	   65495 B/op	     785 allocs/op
BenchmarkBatchWorkSweep/factor=1/pipeline-8          	      61	   1898145 ns/op	   65458 B/op	     785 allocs/op
BenchmarkBatchWorkSweep/factor=1/pipeline-8          	      61	   1764118 ns/op	   65492 B/op	     785 allocs/op
BenchmarkBatchWorkSweep/factor=2/channels-8          	      42	   2681479 ns/op	   33194 B/op	     133 allocs/op
BenchmarkBatchWorkSweep/factor=2/channels-8          	      45	   2653534 ns/op	   33194 B/op	     133 allocs/op
BenchmarkBatchWorkSweep/factor=2/channels-8          	      45	   2551180 ns/op	   33194 B/op	     133 allocs/op
BenchmarkBatchWorkSweep/factor=2/channels-8          	      44	   2535552 ns/op	   33194 B/op	     133 allocs/op
BenchmarkBatchWorkSweep/factor=2/channels-8          	      44	   2630167 ns/op	   33194 B/op	     133 allocs/op
BenchmarkBatchWorkSweep/factor=2/pipeline-8          	      50	   2258142 ns/op	   65455 B/op	     785 allocs/op
BenchmarkBatchWorkSweep/factor=2/pipeline-8          	      51	   2304229 ns/op	   65454 B/op	     785 allocs/op
BenchmarkBatchWorkSweep/factor=2/pipeline-8          	      51	   2238187 ns/op	   65482 B/op	     785 allocs/op
BenchmarkBatchWorkSweep/factor=2/pipeline-8          	      52	   2339571 ns/op	   65481 B/op	     785 allocs/op
BenchmarkBatchWorkSweep/factor=2/pipeline-8          	      55	   2459662 ns/op	   65473 B/op	     785 allocs/op
BenchmarkBatchWorkSweep/factor=4/channels-8          	      30	   3793306 ns/op	   33208 B/op	     133 allocs/op
BenchmarkBatchWorkSweep/factor=4/channels-8          	      31	   3859472 ns/op	   33229 B/op	     133 allocs/op
BenchmarkBatchWorkSweep/factor=4/channels-8          	      27	   4476838 ns/op	   33242 B/op	     133 allocs/op
BenchmarkBatchWorkSweep/factor=4/channels-8          	      30	   3836620 ns/op	   33253 B/op	     133 allocs/op
BenchmarkBatchWorkSweep/factor=4/channels-8          	      31	   4005971 ns/op	   33195 B/op	     133 allocs/op
BenchmarkBatchWorkSweep/factor=4/pipeline-8          	      33	   3428083 ns/op	   65457 B/op	     785 allocs/op
BenchmarkBatchWorkSweep/factor=4/pipeline-8          	      33	   4126047 ns/op	   65477 B/op	     785 allocs/op
BenchmarkBatchWorkSweep/factor=4/pipeline-8          	      32	   3381488 ns/op	   65458 B/op	     785 allocs/op
BenchmarkBatchWorkSweep/factor=4/pipeline-8          	      33	   3692833 ns/op	   65604 B/op	     786 allocs/op
BenchmarkBatchWorkSweep/factor=4/pipeline-8          	      33	   3460850 ns/op	   65469 B/op	     785 allocs/op
BenchmarkBatchWorkSweep/factor=8/channels-8          	      19	   6205349 ns/op	   33197 B/op	     133 allocs/op
BenchmarkBatchWorkSweep/factor=8/channels-8          	      19	   5883489 ns/op	   33213 B/op	     133 allocs/op
BenchmarkBatchWorkSweep/factor=8/channels-8          	      19	   5790127 ns/op	   33278 B/op	     133 allocs/op
BenchmarkBatchWorkSweep/factor=8/channels-8          	      18	   5911097 ns/op	   33288 B/op	     134 allocs/op
BenchmarkBatchWorkSweep/factor=8/channels-8          	      19	   5970105 ns/op	   33263 B/op	     133 allocs/op
BenchmarkBatchWorkSweep/factor=8/pipeline-8          	      19	   6143248 ns/op	   65569 B/op	     786 allocs/op
BenchmarkBatchWorkSweep/factor=8/pipeline-8          	      20	   5862558 ns/op	   65478 B/op	     785 allocs/op
BenchmarkBatchWorkSweep/factor=8/pipeline-8          	      19	   5915072 ns/op	   65522 B/op	     785 allocs/op
BenchmarkBatchWorkSweep/factor=8/pipeline-8          	      20	   5802333 ns/op	   65574 B/op	     786 allocs/op
BenchmarkBatchWorkSweep/factor=8/pipeline-8          	      20	   7240023 ns/op	   65617 B/op	     786 allocs/op
BenchmarkBatchChanWorkSweep/factor=1/channels-8      	      16	   7810609 ns/op	   14553 B/op	     132 allocs/op
BenchmarkBatchChanWorkSweep/factor=1/channels-8      	      25	   4584673 ns/op	   14554 B/op	     132 allocs/op
BenchmarkBatchChanWorkSweep/factor=1/channels-8      	      25	   4585500 ns/op	   14527 B/op	     132 allocs/op
BenchmarkBatchChanWorkSweep/factor=1/channels-8      	      25	   4561437 ns/op	   14508 B/op	     132 allocs/op
BenchmarkBatchChanWorkSweep/factor=1/channels-8      	      24	   4634198 ns/op	   14508 B/op	     132 allocs/op
BenchmarkBatchChanWorkSweep/factor=1/pipeline-8      	      25	   4530455 ns/op	   15294 B/op	     145 allocs/op
BenchmarkBatchChanWorkSweep/factor=1/pipeline-8      	      30	   3824719 ns/op	   15292 B/op	     145 allocs/op
BenchmarkBatchChanWorkSweep/factor=1/pipeline-8      	      31	   3837751 ns/op	   15291 B/op	     145 allocs/op
BenchmarkBatchChanWorkSweep/factor=1/pipeline-8      	      31	   3650539 ns/op	   15291 B/op	     145 allocs/op
BenchmarkBatchChanWorkSweep/factor=1/pipeline-8      	      32	   3828422 ns/op	   15381 B/op	     146 allocs/op
BenchmarkBatchChanWorkSweep/factor=2/channels-8      	      19	   5646665 ns/op	   14509 B/op	     132 allocs/op
BenchmarkBatchChanWorkSweep/factor=2/channels-8      	      20	   5458719 ns/op	   14509 B/op	     132 allocs/op
BenchmarkBatchChanWorkSweep/factor=2/channels-8      	      19	   5576601 ns/op	   14509 B/op	     132 allocs/op
BenchmarkBatchChanWorkSweep/factor=2/channels-8      	      19	   5412221 ns/op	   14509 B/op	     132 allocs/op
BenchmarkBatchChanWorkSweep/factor=2/channels-8      	      20	   5416229 ns/op	   14509 B/op	     132 allocs/op
BenchmarkBatchChanWorkSweep/factor=2/pipeline-8      	      25	   4869308 ns/op	   15394 B/op	     146 allocs/op
BenchmarkBatchChanWorkSweep/factor=2/pipeline-8      	      25	   4624383 ns/op	   15294 B/op	     145 allocs/op
BenchmarkBatchChanWorkSweep/factor=2/pipeline-8      	      25	   4518897 ns/op	   15294 B/op	     145 allocs/op
BenchmarkBatchChanWorkSweep/factor=2/pipeline-8      	      24	   4598540 ns/op	   15311 B/op	     145 allocs/op
BenchmarkBatchChanWorkSweep/factor=2/pipeline-8      	      22	   4566080 ns/op	   15327 B/op	     145 allocs/op
BenchmarkBatchChanWorkSweep/factor=4/channels-8      	      15	   7611525 ns/op	   14511 B/op	     132 allocs/op
BenchmarkBatchChanWorkSweep/factor=4/channels-8      	      16	   6889576 ns/op	   14523 B/op	     132 allocs/op
BenchmarkBatchChanWorkSweep/factor=4/channels-8      	      15	   6925375 ns/op	   14581 B/op	     132 allocs/op
BenchmarkBatchChanWorkSweep/factor=4/channels-8      	      16	   6918646 ns/op	   14535 B/op	     132 allocs/op
BenchmarkBatchChanWorkSweep/factor=4/channels-8      	      15	   6937789 ns/op	   14549 B/op	     132 allocs/op
BenchmarkBatchChanWorkSweep/factor=4/pipeline-8      	      18	   6054005 ns/op	   15353 B/op	     145 allocs/op
BenchmarkBatchChanWorkSweep/factor=4/pipeline-8      	      18	   6031338 ns/op	   15300 B/op	     145 allocs/op
BenchmarkBatchChanWorkSweep/factor=4/pipeline-8      	      18	   5841157 ns/op	   15380 B/op	     146 allocs/op
BenchmarkBatchChanWorkSweep/factor=4/pipeline-8      	      19	   6024824 ns/op	   15435 B/op	     146 allocs/op
BenchmarkBatchChanWorkSweep/factor=4/pipeline-8      	      18	   6148646 ns/op	   15332 B/op	     145 allocs/op
BenchmarkBatchChanWorkSweep/factor=8/channels-8      	      10	  10329058 ns/op	   14592 B/op	     132 allocs/op
BenchmarkBatchChanWorkSweep/factor=8/channels-8      	      10	  10098383 ns/op	   14563 B/op	     132 allocs/op
BenchmarkBatchChanWorkSweep/factor=8/channels-8      	      12	   9920611 ns/op	   14585 B/op	     132 allocs/op
BenchmarkBatchChanWorkSweep/factor=8/channels-8      	      10	  10718658 ns/op	   14582 B/op	     132 allocs/op
BenchmarkBatchChanWorkSweep/factor=8/channels-8      	      12	   9841990 ns/op	   14513 B/op	     132 allocs/op
BenchmarkBatchChanWorkSweep/factor=8/pipeline-8      	      13	   8865228 ns/op	   15463 B/op	     146 allocs/op
BenchmarkBatchChanWorkSweep/factor=8/pipeline-8      	      13	   8735154 ns/op	   15308 B/op	     145 allocs/op
BenchmarkBatchChanWorkSweep/factor=8/pipeline-8      	      13	   9456189 ns/op	   15308 B/op	     145 allocs/op
BenchmarkBatchChanWorkSweep/factor=8/pipeline-8      	      13	   8771686 ns/op	   15308 B/op	     145 allocs/op
BenchmarkBatchChanWorkSweep/factor=8/pipeline-8      	      13	   8742208 ns/op	   15308 B/op	     145 allocs/op
BenchmarkSplitByWorkSweep/factor=1/channels-8        	      12	  10015444 ns/op	     521 B/op	      12 allocs/op
BenchmarkSplitByWorkSweep/factor=1/channels-8        	      12	  10753146 ns/op	     489 B/op	      12 allocs/op
BenchmarkSplitByWorkSweep/factor=1/channels-8        	      12	   9867917 ns/op	     577 B/op	      13 allocs/op
BenchmarkSplitByWorkSweep/factor=1/channels-8        	      10	  10764233 ns/op	     644 B/op	      13 allocs/op
BenchmarkSplitByWorkSweep/factor=1/channels-8        	      12	  10612340 ns/op	     689 B/op	      14 allocs/op
BenchmarkSplitByWorkSweep/factor=1/pipeline-8        	       9	  12564875 ns/op	    1619 B/op	      29 allocs/op
BenchmarkSplitByWorkSweep/factor=1/pipeline-8        	       9	  11753176 ns/op	    1320 B/op	      26 allocs/op
BenchmarkSplitByWorkSweep/factor=1/pipeline-8        	       9	  11333338 ns/op	    1416 B/op	      27 allocs/op
BenchmarkSplitByWorkSweep/factor=1/pipeline-8        	       9	  11286380 ns/op	    1320 B/op	      26 allocs/op
BenchmarkSplitByWorkSweep/factor=1/pipeline-8        	       9	  11646625 ns/op	    1320 B/op	      26 allocs/op
BenchmarkSplitByWorkSweep/factor=2/channels-8        	      10	  10590917 ns/op	     539 B/op	      12 allocs/op
BenchmarkSplitByWorkSweep/factor=2/channels-8        	      10	  10393892 ns/op	     491 B/op	      12 allocs/op
BenchmarkSplitByWorkSweep/factor=2/channels-8        	      10	  12629775 ns/op	     606 B/op	      13 allocs/op
BenchmarkSplitByWorkSweep/factor=2/channels-8        	       9	  11662796 ns/op	     545 B/op	      12 allocs/op
BenchmarkSplitByWorkSweep/factor=2/channels-8        	       9	  11649759 ns/op	     652 B/op	      13 allocs/op
BenchmarkSplitByWorkSweep/factor=2/pipeline-8        	       9	  12343630 ns/op	    1630 B/op	      29 allocs/op
BenchmarkSplitByWorkSweep/factor=2/pipeline-8        	       8	  16369677 ns/op	    1362 B/op	      26 allocs/op
BenchmarkSplitByWorkSweep/factor=2/pipeline-8        	       9	  11787314 ns/op	    1640 B/op	      29 allocs/op
BenchmarkSplitByWorkSweep/factor=2/pipeline-8        	       9	  12418871 ns/op	    1320 B/op	      26 allocs/op
BenchmarkSplitByWorkSweep/factor=2/pipeline-8        	       9	  12050778 ns/op	    1598 B/op	      29 allocs/op
BenchmarkSplitByWorkSweep/factor=4/channels-8        	       8	  12650313 ns/op	     578 B/op	      13 allocs/op
BenchmarkSplitByWorkSweep/factor=4/channels-8        	       8	  13789412 ns/op	     722 B/op	      14 allocs/op
BenchmarkSplitByWorkSweep/factor=4/channels-8        	       8	  13000578 ns/op	     578 B/op	      13 allocs/op
BenchmarkSplitByWorkSweep/factor=4/channels-8        	       8	  13662912 ns/op	     698 B/op	      14 allocs/op
BenchmarkSplitByWorkSweep/factor=4/channels-8        	       8	  12811666 ns/op	     590 B/op	      13 allocs/op
BenchmarkSplitByWorkSweep/factor=4/pipeline-8        	       7	  15811786 ns/op	    1634 B/op	      29 allocs/op
BenchmarkSplitByWorkSweep/factor=4/pipeline-8        	       8	  13653271 ns/op	    1326 B/op	      26 allocs/op
BenchmarkSplitByWorkSweep/factor=4/pipeline-8        	       8	  13586469 ns/op	    1338 B/op	      26 allocs/op
BenchmarkSplitByWorkSweep/factor=4/pipeline-8        	       8	  13841964 ns/op	    1506 B/op	      28 allocs/op
BenchmarkSplitByWorkSweep/factor=4/pipeline-8        	       8	  13434714 ns/op	    1350 B/op	      26 allocs/op
BenchmarkSplitByWorkSweep/factor=8/channels-8        	       7	  15628000 ns/op	     496 B/op	      12 allocs/op
BenchmarkSplitByWorkSweep/factor=8/channels-8        	       7	  18759268 ns/op	     496 B/op	      12 allocs/op
BenchmarkSplitByWorkSweep/factor=8/channels-8        	       7	  16248911 ns/op	     838 B/op	      15 allocs/op
BenchmarkSplitByWorkSweep/factor=8/channels-8        	       7	  16436453 ns/op	     729 B/op	      14 allocs/op
BenchmarkSplitByWorkSweep/factor=8/channels-8        	       7	  16467208 ns/op	     880 B/op	      16 allocs/op
BenchmarkSplitByWorkSweep/factor=8/pipeline-8        	       7	  15511732 ns/op	    1538 B/op	      28 allocs/op
BenchmarkSplitByWorkSweep/factor=8/pipeline-8        	       7	  15245821 ns/op	    1497 B/op	      28 allocs/op
BenchmarkSplitByWorkSweep/factor=8/pipeline-8        	       7	  14812375 ns/op	    1401 B/op	      27 allocs/op
BenchmarkSplitByWorkSweep/factor=8/pipeline-8        	       7	  14946732 ns/op	    1401 B/op	      27 allocs/op
BenchmarkSplitByWorkSweep/factor=8/pipeline-8        	       7	  14971738 ns/op	    1332 B/op	      26 allocs/op
BenchmarkOverheadStepSweep/steps=1/channels-8        	      26	   4421742 ns/op	     215 B/op	       6 allocs/op
BenchmarkOverheadStepSweep/steps=1/channels-8        	      24	   4527972 ns/op	     212 B/op	       6 allocs/op
BenchmarkOverheadStepSweep/steps=1/channels-8        	      25	   4389155 ns/op	     204 B/op	       6 allocs/op
BenchmarkOverheadStepSweep/steps=1/channels-8        	      26	   4376901 ns/op	     204 B/op	       6 allocs/op
BenchmarkOverheadStepSweep/steps=1/channels-8        	      24	   4372059 ns/op	     204 B/op	       6 allocs/op
BenchmarkOverheadStepSweep/steps=1/pipeline-8        	      25	   4581858 ns/op	     824 B/op	      16 allocs/op
BenchmarkOverheadStepSweep/steps=1/pipeline-8        	      24	   4592831 ns/op	     911 B/op	      17 allocs/op
BenchmarkOverheadStepSweep/steps=1/pipeline-8        	      24	   4304724 ns/op	     787 B/op	      16 allocs/op
BenchmarkOverheadStepSweep/steps=1/pipeline-8        	      27	   4311870 ns/op	     790 B/op	      16 allocs/op
BenchmarkOverheadStepSweep/steps=1/pipeline-8        	      26	   4346961 ns/op	     766 B/op	      16 allocs/op
BenchmarkOverheadStepSweep/steps=2/channels-8        	      15	   8012125 ns/op	     343 B/op	      11 allocs/op
BenchmarkOverheadStepSweep/steps=2/channels-8        	      15	   7412642 ns/op	     343 B/op	      11 allocs/op
BenchmarkOverheadStepSweep/steps=2/channels-8        	      15	   7392906 ns/op	     445 B/op	      12 allocs/op
BenchmarkOverheadStepSweep/steps=2/channels-8        	      15	   7434289 ns/op	     375 B/op	      11 allocs/op
BenchmarkOverheadStepSweep/steps=2/channels-8        	      15	   7424928 ns/op	     369 B/op	      11 allocs/op
BenchmarkOverheadStepSweep/steps=2/pipeline-8        	      13	   8402468 ns/op	     899 B/op	      18 allocs/op
BenchmarkOverheadStepSweep/steps=2/pipeline-8        	      14	   7822217 ns/op	     890 B/op	      18 allocs/op
BenchmarkOverheadStepSweep/steps=2/pipeline-8        	      14	   7783292 ns/op	    1096 B/op	      20 allocs/op
BenchmarkOverheadStepSweep/steps=2/pipeline-8        	      14	   7796485 ns/op	     890 B/op	      18 allocs/op
BenchmarkOverheadStepSweep/steps=2/pipeline-8        	      14	   8088056 ns/op	    1082 B/op	      20 allocs/op
BenchmarkOverheadStepSweep/steps=4/channels-8        	       8	  13719312 ns/op	     814 B/op	      23 allocs/op
BenchmarkOverheadStepSweep/steps=4/channels-8        	       8	  14153021 ns/op	     622 B/op	      21 allocs/op
BenchmarkOverheadStepSweep/steps=4/channels-8        	       8	  13689734 ns/op	     622 B/op	      21 allocs/op
BenchmarkOverheadStepSweep/steps=4/channels-8        	       8	  13617323 ns/op	     838 B/op	      23 allocs/op
BenchmarkOverheadStepSweep/steps=4/channels-8        	       8	  14152896 ns/op	     898 B/op	      24 allocs/op
BenchmarkOverheadStepSweep/steps=4/pipeline-8        	       7	  14401351 ns/op	    1360 B/op	      24 allocs/op
BenchmarkOverheadStepSweep/steps=4/pipeline-8        	       7	  14738994 ns/op	    1209 B/op	      23 allocs/op
BenchmarkOverheadStepSweep/steps=4/pipeline-8        	       7	  14469726 ns/op	    1140 B/op	      22 allocs/op
BenchmarkOverheadStepSweep/steps=4/pipeline-8        	       7	  14470934 ns/op	    1140 B/op	      22 allocs/op
BenchmarkOverheadStepSweep/steps=4/pipeline-8        	       7	  14710417 ns/op	    1140 B/op	      22 allocs/op
BenchmarkOverheadStepSweep/steps=8/channels-8        	       4	  28467386 ns/op	    1300 B/op	      42 allocs/op
BenchmarkOverheadStepSweep/steps=8/channels-8        	       4	  27493896 ns/op	    1300 B/op	      42 allocs/op
BenchmarkOverheadStepSweep/steps=8/channels-8        	       4	  28560708 ns/op	    1180 B/op	      41 allocs/op
BenchmarkOverheadStepSweep/steps=8/channels-8        	       4	  28388979 ns/op	    1420 B/op	      43 allocs/op
BenchmarkOverheadStepSweep/steps=8/channels-8        	       4	  28358500 ns/op	    1708 B/op	      46 allocs/op
BenchmarkOverheadStepSweep/steps=8/pipeline-8        	       4	  30145292 ns/op	    1628 B/op	      30 allocs/op
BenchmarkOverheadStepSweep/steps=8/pipeline-8        	       4	  30732032 ns/op	    1628 B/op	      30 allocs/op
BenchmarkOverheadStepSweep/steps=8/pipeline-8        	       4	  29919448 ns/op	    2420 B/op	      39 allocs/op
BenchmarkOverheadStepSweep/steps=8/pipeline-8        	       4	  30152240 ns/op	    2180 B/op	      36 allocs/op
BenchmarkOverheadStepSweep/steps=8/pipeline-8        	       4	  29513167 ns/op	    1628 B/op	      30 allocs/op
BenchmarkOverheadStepSweep/steps=16/channels-8       	       2	  60231834 ns/op	    3208 B/op	      91 allocs/op
BenchmarkOverheadStepSweep/steps=16/channels-8       	       2	  62236521 ns/op	    2680 B/op	      85 allocs/op
BenchmarkOverheadStepSweep/steps=16/channels-8       	       2	  61636917 ns/op	    4360 B/op	     103 allocs/op
BenchmarkOverheadStepSweep/steps=16/channels-8       	       2	  62739458 ns/op	    2536 B/op	      84 allocs/op
BenchmarkOverheadStepSweep/steps=16/channels-8       	       2	  60051626 ns/op	    3496 B/op	      94 allocs/op
BenchmarkOverheadStepSweep/steps=16/pipeline-8       	       2	  65776958 ns/op	    3048 B/op	      52 allocs/op
BenchmarkOverheadStepSweep/steps=16/pipeline-8       	       2	  65673000 ns/op	    5304 B/op	      53 allocs/op
BenchmarkOverheadStepSweep/steps=16/pipeline-8       	       2	  66551062 ns/op	    3480 B/op	      56 allocs/op
BenchmarkOverheadStepSweep/steps=16/pipeline-8       	       2	  64717542 ns/op	   11240 B/op	      75 allocs/op
BenchmarkOverheadStepSweep/steps=16/pipeline-8       	       2	  76805500 ns/op	    5224 B/op	      65 allocs/op
PASS
ok  	github.com/askiada/go-pipeline/v2/pkg/pipeline	103.457s
```

## Channel vs pipeline diff

Diff is percent vs channels (medians from count=5).

| Benchmark | Channels ns/op | Pipeline ns/op | Diff |
| --- | ---: | ---: | ---: |
| Batch/items=1k/conc=1 | 545,312.0 | 438,723.0 | -19.5% |
| Batch/items=1k/conc=4 | 755,315.0 | 658,965.0 | -12.8% |
| BatchChan/items=1k/conc=1 | 1,122,282.0 | 913,484.0 | -18.6% |
| BatchChan/items=1k/conc=4 | 1,134,506.0 | 938,177.0 | -17.3% |
| BatchChanWorkSweep/factor=1 | 4,585,500.0 | 3,828,422.0 | -16.5% |
| BatchChanWorkSweep/factor=2 | 5,458,719.0 | 4,598,540.0 | -15.8% |
| BatchChanWorkSweep/factor=4 | 6,925,375.0 | 6,031,338.0 | -12.9% |
| BatchChanWorkSweep/factor=8 | 10,098,383.0 | 8,771,686.0 | -13.1% |
| BatchWorkSweep/factor=1 | 2,196,609.0 | 1,764,118.0 | -19.7% |
| BatchWorkSweep/factor=2 | 2,630,167.0 | 2,304,229.0 | -12.4% |
| BatchWorkSweep/factor=4 | 3,859,472.0 | 3,460,850.0 | -10.3% |
| BatchWorkSweep/factor=8 | 5,911,097.0 | 5,915,072.0 | +0.1% |
| FromChan/items=1k/conc=1 | 1,079,555.0 | 1,414,895.0 | +31.1% |
| FromChan/items=1k/conc=4 | 1,708,484.0 | 1,548,021.0 | -9.4% |
| FromChanWorkSweep/factor=1 | 4,354,227.0 | 5,687,531.0 | +30.6% |
| FromChanWorkSweep/factor=2 | 5,122,822.0 | 6,929,753.0 | +35.3% |
| FromChanWorkSweep/factor=4 | 6,923,113.0 | 8,347,771.0 | +20.6% |
| FromChanWorkSweep/factor=8 | 10,433,183.0 | 11,246,185.0 | +7.8% |
| OneToMany/items=1k/conc=1 | 1,500,470.0 | 1,655,676.0 | +10.3% |
| OneToMany/items=1k/conc=4 | 2,408,275.0 | 2,514,134.0 | +4.4% |
| OneToManyWorkSweep/factor=1 | 6,253,507.0 | 6,736,550.0 | +7.7% |
| OneToManyWorkSweep/factor=2 | 6,973,600.0 | 7,484,342.0 | +7.3% |
| OneToManyWorkSweep/factor=4 | 8,360,253.0 | 8,867,642.0 | +6.1% |
| OneToManyWorkSweep/factor=8 | 10,606,829.0 | 11,512,287.0 | +8.5% |
| OneToOne/items=1k/conc=1 | 1,073,341.0 | 1,081,177.0 | +0.7% |
| OneToOne/items=1k/conc=4 | 1,797,347.0 | 1,710,091.0 | -4.9% |
| OneToOneOrZero/items=1k/conc=1 | 783,143.0 | 784,623.0 | +0.2% |
| OneToOneOrZero/items=1k/conc=4 | 1,200,278.0 | 1,236,945.0 | +3.1% |
| OneToOneOrZeroWorkSweep/factor=1 | 3,235,042.0 | 3,265,436.0 | +0.9% |
| OneToOneOrZeroWorkSweep/factor=2 | 3,655,299.0 | 3,955,251.0 | +8.2% |
| OneToOneOrZeroWorkSweep/factor=4 | 4,489,508.0 | 4,505,931.0 | +0.4% |
| OneToOneOrZeroWorkSweep/factor=8 | 6,328,253.0 | 6,268,485.0 | -0.9% |
| OverheadStepSweep/steps=1 | 4,389,155.0 | 4,346,961.0 | -1.0% |
| OverheadStepSweep/steps=16 | 61,636,917.0 | 65,776,958.0 | +6.7% |
| OverheadStepSweep/steps=2 | 7,424,928.0 | 7,822,217.0 | +5.4% |
| OverheadStepSweep/steps=4 | 13,719,312.0 | 14,470,934.0 | +5.5% |
| OverheadStepSweep/steps=8 | 28,388,979.0 | 30,145,292.0 | +6.2% |
| OverheadWorkSweep/factor=1 | 4,367,784.0 | 4,357,033.0 | -0.2% |
| OverheadWorkSweep/factor=2 | 5,230,990.0 | 5,360,863.0 | +2.5% |
| OverheadWorkSweep/factor=4 | 6,749,128.0 | 6,832,340.0 | +1.2% |
| OverheadWorkSweep/factor=8 | 10,516,138.0 | 9,762,250.0 | -7.2% |
| Sink/items=1k/conc=1 | 560,307.0 | 526,436.0 | -6.0% |
| Sink/items=1k/conc=4 | 827,504.0 | 806,279.0 | -2.6% |
| SinkFromChan/items=1k/conc=1 | 566,015.0 | 848,812.0 | +50.0% |
| SinkFromChan/items=1k/conc=4 | 823,648.0 | 1,222,723.0 | +48.5% |
| SinkFromChanWorkSweep/factor=1 | 2,298,635.0 | 3,488,767.0 | +51.8% |
| SinkFromChanWorkSweep/factor=2 | 2,953,856.0 | 4,245,176.0 | +43.7% |
| SinkFromChanWorkSweep/factor=4 | 5,438,123.0 | 5,670,863.0 | +4.3% |
| SinkFromChanWorkSweep/factor=8 | 7,292,939.0 | 8,475,872.0 | +16.2% |
| SinkWorkSweep/factor=1 | 2,271,101.0 | 2,142,192.0 | -5.7% |
| SinkWorkSweep/factor=2 | 3,033,718.0 | 2,896,868.0 | -4.5% |
| SinkWorkSweep/factor=4 | 5,371,240.0 | 4,976,494.0 | -7.3% |
| SinkWorkSweep/factor=8 | 7,480,239.0 | 7,899,136.0 | +5.6% |
| SplitBy/items=1k/conc=1 | 2,787,695.0 | 3,085,676.0 | +10.7% |
| SplitBy/items=1k/conc=4 | 3,273,621.0 | 2,827,882.0 | -13.6% |
| SplitByWorkSweep/factor=1 | 10,612,340.0 | 11,646,625.0 | +9.7% |
| SplitByWorkSweep/factor=2 | 11,649,759.0 | 12,343,630.0 | +6.0% |
| SplitByWorkSweep/factor=4 | 13,000,578.0 | 13,653,271.0 | +5.0% |
| SplitByWorkSweep/factor=8 | 16,436,453.0 | 14,971,738.0 | -8.9% |
| SplitMerge/items=1k/conc=1 | 4,699,134.0 | 5,116,080.0 | +8.9% |
| SplitMerge/items=1k/conc=4 | 6,669,035.0 | 5,337,994.0 | -20.0% |
| TwoStage/items=1k/conc=1 | 1,860,872.0 | 1,971,786.0 | +6.0% |
| TwoStage/items=1k/conc=4 | 3,461,491.0 | 2,882,779.0 | -16.7% |
