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
BenchmarkOneToOne/items=1k/conc=1/channels-8         	      99	   1173261 ns/op	     218 B/op	       4 allocs/op
BenchmarkOneToOne/items=1k/conc=1/channels-8         	     100	   1120653 ns/op	     174 B/op	       4 allocs/op
BenchmarkOneToOne/items=1k/conc=1/channels-8         	      94	   1153101 ns/op	     190 B/op	       4 allocs/op
BenchmarkOneToOne/items=1k/conc=1/channels-8         	      97	   1110090 ns/op	     210 B/op	       4 allocs/op
BenchmarkOneToOne/items=1k/conc=1/channels-8         	      91	   1101704 ns/op	     184 B/op	       4 allocs/op
BenchmarkOneToOne/items=1k/conc=1/pipeline-8         	     104	   1156478 ns/op	     853 B/op	      16 allocs/op
BenchmarkOneToOne/items=1k/conc=1/pipeline-8         	      91	   1203424 ns/op	     759 B/op	      16 allocs/op
BenchmarkOneToOne/items=1k/conc=1/pipeline-8         	      96	   1118651 ns/op	     770 B/op	      16 allocs/op
BenchmarkOneToOne/items=1k/conc=1/pipeline-8         	     104	   1162851 ns/op	     820 B/op	      16 allocs/op
BenchmarkOneToOne/items=1k/conc=1/pipeline-8         	      91	   1118360 ns/op	     791 B/op	      16 allocs/op
BenchmarkOneToOne/items=1k/conc=4/channels-8         	      75	   1854374 ns/op	     696 B/op	       8 allocs/op
BenchmarkOneToOne/items=1k/conc=4/channels-8         	      67	   1776412 ns/op	     424 B/op	       7 allocs/op
BenchmarkOneToOne/items=1k/conc=4/channels-8         	      57	   1867654 ns/op	     468 B/op	       7 allocs/op
BenchmarkOneToOne/items=1k/conc=4/channels-8         	      64	   1854546 ns/op	     381 B/op	       7 allocs/op
BenchmarkOneToOne/items=1k/conc=4/channels-8         	      66	   1625835 ns/op	     394 B/op	       7 allocs/op
BenchmarkOneToOne/items=1k/conc=4/pipeline-8         	      67	   1735578 ns/op	    2279 B/op	      32 allocs/op
BenchmarkOneToOne/items=1k/conc=4/pipeline-8         	      60	   1738798 ns/op	    2335 B/op	      31 allocs/op
BenchmarkOneToOne/items=1k/conc=4/pipeline-8         	      62	   1781622 ns/op	    2189 B/op	      31 allocs/op
BenchmarkOneToOne/items=1k/conc=4/pipeline-8         	      62	   1765410 ns/op	    2152 B/op	      31 allocs/op
BenchmarkOneToOne/items=1k/conc=4/pipeline-8         	      63	   1753778 ns/op	    2185 B/op	      31 allocs/op
BenchmarkOneToOneOrZero/items=1k/conc=1/channels-8   	     148	    816273 ns/op	     168 B/op	       4 allocs/op
BenchmarkOneToOneOrZero/items=1k/conc=1/channels-8   	     140	    817322 ns/op	     173 B/op	       4 allocs/op
BenchmarkOneToOneOrZero/items=1k/conc=1/channels-8   	     148	    886094 ns/op	     180 B/op	       4 allocs/op
BenchmarkOneToOneOrZero/items=1k/conc=1/channels-8   	     148	    774477 ns/op	     168 B/op	       4 allocs/op
BenchmarkOneToOneOrZero/items=1k/conc=1/channels-8   	     153	    773835 ns/op	     168 B/op	       4 allocs/op
BenchmarkOneToOneOrZero/items=1k/conc=1/pipeline-8   	     147	    799262 ns/op	     766 B/op	      16 allocs/op
BenchmarkOneToOneOrZero/items=1k/conc=1/pipeline-8   	     148	    785487 ns/op	     754 B/op	      16 allocs/op
BenchmarkOneToOneOrZero/items=1k/conc=1/pipeline-8   	     148	    778571 ns/op	     768 B/op	      16 allocs/op
BenchmarkOneToOneOrZero/items=1k/conc=1/pipeline-8   	     145	    804000 ns/op	     771 B/op	      16 allocs/op
BenchmarkOneToOneOrZero/items=1k/conc=1/pipeline-8   	     150	    804514 ns/op	     760 B/op	      16 allocs/op
BenchmarkOneToOneOrZero/items=1k/conc=4/channels-8   	      94	   1262900 ns/op	     396 B/op	       7 allocs/op
BenchmarkOneToOneOrZero/items=1k/conc=4/channels-8   	     108	   1351353 ns/op	     381 B/op	       7 allocs/op
BenchmarkOneToOneOrZero/items=1k/conc=4/channels-8   	      87	   1358919 ns/op	     393 B/op	       7 allocs/op
BenchmarkOneToOneOrZero/items=1k/conc=4/channels-8   	      93	   1324433 ns/op	     374 B/op	       7 allocs/op
BenchmarkOneToOneOrZero/items=1k/conc=4/channels-8   	      81	   1542092 ns/op	     391 B/op	       7 allocs/op
BenchmarkOneToOneOrZero/items=1k/conc=4/pipeline-8   	      87	   1298129 ns/op	    2187 B/op	      31 allocs/op
BenchmarkOneToOneOrZero/items=1k/conc=4/pipeline-8   	      90	   1279343 ns/op	    2191 B/op	      31 allocs/op
BenchmarkOneToOneOrZero/items=1k/conc=4/pipeline-8   	      88	   1303263 ns/op	    2264 B/op	      32 allocs/op
BenchmarkOneToOneOrZero/items=1k/conc=4/pipeline-8   	      92	   1290364 ns/op	    2167 B/op	      31 allocs/op
BenchmarkOneToOneOrZero/items=1k/conc=4/pipeline-8   	      90	   1248302 ns/op	    2136 B/op	      31 allocs/op
BenchmarkOneToMany/items=1k/conc=1/channels-8        	      78	   1522362 ns/op	     169 B/op	       4 allocs/op
BenchmarkOneToMany/items=1k/conc=1/channels-8        	      70	   1581893 ns/op	     172 B/op	       4 allocs/op
BenchmarkOneToMany/items=1k/conc=1/channels-8        	      72	   1596491 ns/op	     176 B/op	       4 allocs/op
BenchmarkOneToMany/items=1k/conc=1/channels-8        	      68	   1594878 ns/op	     186 B/op	       4 allocs/op
BenchmarkOneToMany/items=1k/conc=1/channels-8        	      72	   1572629 ns/op	     169 B/op	       4 allocs/op
BenchmarkOneToMany/items=1k/conc=1/pipeline-8        	      70	   1843252 ns/op	   17178 B/op	    1040 allocs/op
BenchmarkOneToMany/items=1k/conc=1/pipeline-8        	      67	   1849616 ns/op	   17142 B/op	    1040 allocs/op
BenchmarkOneToMany/items=1k/conc=1/pipeline-8        	      57	   1796493 ns/op	   17142 B/op	    1040 allocs/op
BenchmarkOneToMany/items=1k/conc=1/pipeline-8        	      56	   1834863 ns/op	   17164 B/op	    1040 allocs/op
BenchmarkOneToMany/items=1k/conc=1/pipeline-8        	      60	   1818742 ns/op	   17142 B/op	    1040 allocs/op
BenchmarkOneToMany/items=1k/conc=4/channels-8        	      49	   2244566 ns/op	     415 B/op	       7 allocs/op
BenchmarkOneToMany/items=1k/conc=4/channels-8        	      52	   2211465 ns/op	     434 B/op	       7 allocs/op
BenchmarkOneToMany/items=1k/conc=4/channels-8        	      42	   2559347 ns/op	     431 B/op	       7 allocs/op
BenchmarkOneToMany/items=1k/conc=4/channels-8        	      48	   2189280 ns/op	     396 B/op	       7 allocs/op
BenchmarkOneToMany/items=1k/conc=4/channels-8        	      46	   2520929 ns/op	     379 B/op	       7 allocs/op
BenchmarkOneToMany/items=1k/conc=4/pipeline-8        	      45	   2663191 ns/op	   18554 B/op	    1055 allocs/op
BenchmarkOneToMany/items=1k/conc=4/pipeline-8        	      43	   2733054 ns/op	   18782 B/op	    1056 allocs/op
BenchmarkOneToMany/items=1k/conc=4/pipeline-8        	      43	   2676108 ns/op	   18779 B/op	    1056 allocs/op
BenchmarkOneToMany/items=1k/conc=4/pipeline-8        	      43	   2897014 ns/op	   18694 B/op	    1056 allocs/op
BenchmarkOneToMany/items=1k/conc=4/pipeline-8        	      42	   2662178 ns/op	   18493 B/op	    1055 allocs/op
BenchmarkFromChan/items=1k/conc=1/channels-8         	     104	   1123160 ns/op	     171 B/op	       4 allocs/op
BenchmarkFromChan/items=1k/conc=1/channels-8         	     100	   1118573 ns/op	     169 B/op	       4 allocs/op
BenchmarkFromChan/items=1k/conc=1/channels-8         	      96	   1127093 ns/op	     170 B/op	       4 allocs/op
BenchmarkFromChan/items=1k/conc=1/channels-8         	      88	   1260958 ns/op	     169 B/op	       4 allocs/op
BenchmarkFromChan/items=1k/conc=1/channels-8         	      92	   1146183 ns/op	     169 B/op	       4 allocs/op
BenchmarkFromChan/items=1k/conc=1/pipeline-8         	      81	   1491809 ns/op	    1135 B/op	      20 allocs/op
BenchmarkFromChan/items=1k/conc=1/pipeline-8         	      73	   1493070 ns/op	    1139 B/op	      20 allocs/op
BenchmarkFromChan/items=1k/conc=1/pipeline-8         	      74	   1460423 ns/op	    1172 B/op	      20 allocs/op
BenchmarkFromChan/items=1k/conc=1/pipeline-8         	      72	   1535228 ns/op	    1210 B/op	      20 allocs/op
BenchmarkFromChan/items=1k/conc=1/pipeline-8         	      75	   1458507 ns/op	    1124 B/op	      20 allocs/op
BenchmarkFromChan/items=1k/conc=4/channels-8         	      73	   1634072 ns/op	     366 B/op	       7 allocs/op
BenchmarkFromChan/items=1k/conc=4/channels-8         	      66	   1913849 ns/op	     366 B/op	       7 allocs/op
BenchmarkFromChan/items=1k/conc=4/channels-8         	      68	   1927406 ns/op	     364 B/op	       7 allocs/op
BenchmarkFromChan/items=1k/conc=4/channels-8         	      67	   1893139 ns/op	     361 B/op	       7 allocs/op
BenchmarkFromChan/items=1k/conc=4/channels-8         	      73	   1895687 ns/op	     381 B/op	       7 allocs/op
BenchmarkFromChan/items=1k/conc=4/pipeline-8         	      72	   1599455 ns/op	    3555 B/op	      47 allocs/op
BenchmarkFromChan/items=1k/conc=4/pipeline-8         	      73	   1597833 ns/op	    3536 B/op	      47 allocs/op
BenchmarkFromChan/items=1k/conc=4/pipeline-8         	      66	   1535429 ns/op	    3553 B/op	      47 allocs/op
BenchmarkFromChan/items=1k/conc=4/pipeline-8         	      74	   1605340 ns/op	    3549 B/op	      47 allocs/op
BenchmarkFromChan/items=1k/conc=4/pipeline-8         	      66	   1641010 ns/op	    3481 B/op	      47 allocs/op
BenchmarkSink/items=1k/conc=1/channels-8             	     217	    575149 ns/op	     165 B/op	       4 allocs/op
BenchmarkSink/items=1k/conc=1/channels-8             	     190	    579421 ns/op	     163 B/op	       4 allocs/op
BenchmarkSink/items=1k/conc=1/channels-8             	     205	    586875 ns/op	     168 B/op	       4 allocs/op
BenchmarkSink/items=1k/conc=1/channels-8             	     201	    582482 ns/op	     167 B/op	       4 allocs/op
BenchmarkSink/items=1k/conc=1/channels-8             	     208	    578697 ns/op	     165 B/op	       4 allocs/op
BenchmarkSink/items=1k/conc=1/pipeline-8             	     219	    544305 ns/op	     651 B/op	      14 allocs/op
BenchmarkSink/items=1k/conc=1/pipeline-8             	     211	    537706 ns/op	     641 B/op	      14 allocs/op
BenchmarkSink/items=1k/conc=1/pipeline-8             	     216	    540172 ns/op	     641 B/op	      14 allocs/op
BenchmarkSink/items=1k/conc=1/pipeline-8             	     222	    532651 ns/op	     657 B/op	      14 allocs/op
BenchmarkSink/items=1k/conc=1/pipeline-8             	     211	    557607 ns/op	     641 B/op	      14 allocs/op
BenchmarkSink/items=1k/conc=4/channels-8             	     128	    857067 ns/op	     432 B/op	       7 allocs/op
BenchmarkSink/items=1k/conc=4/channels-8             	     140	    864402 ns/op	     415 B/op	       7 allocs/op
BenchmarkSink/items=1k/conc=4/channels-8             	     142	    857835 ns/op	     380 B/op	       7 allocs/op
BenchmarkSink/items=1k/conc=4/channels-8             	     138	    953882 ns/op	     386 B/op	       7 allocs/op
BenchmarkSink/items=1k/conc=4/channels-8             	     128	    851880 ns/op	     394 B/op	       7 allocs/op
BenchmarkSink/items=1k/conc=4/pipeline-8             	     144	    777265 ns/op	    2005 B/op	      29 allocs/op
BenchmarkSink/items=1k/conc=4/pipeline-8             	     144	    821149 ns/op	    1966 B/op	      29 allocs/op
BenchmarkSink/items=1k/conc=4/pipeline-8             	     144	    848571 ns/op	    2024 B/op	      29 allocs/op
BenchmarkSink/items=1k/conc=4/pipeline-8             	     140	    802011 ns/op	    1990 B/op	      29 allocs/op
BenchmarkSink/items=1k/conc=4/pipeline-8             	     147	    833797 ns/op	    2036 B/op	      29 allocs/op
BenchmarkSinkFromChan/items=1k/conc=1/channels-8     	     208	    598736 ns/op	     160 B/op	       4 allocs/op
BenchmarkSinkFromChan/items=1k/conc=1/channels-8     	     200	    600094 ns/op	     160 B/op	       4 allocs/op
BenchmarkSinkFromChan/items=1k/conc=1/channels-8     	     175	    583976 ns/op	     172 B/op	       4 allocs/op
BenchmarkSinkFromChan/items=1k/conc=1/channels-8     	     208	    583607 ns/op	     169 B/op	       4 allocs/op
BenchmarkSinkFromChan/items=1k/conc=1/channels-8     	     193	    604406 ns/op	     166 B/op	       4 allocs/op
BenchmarkSinkFromChan/items=1k/conc=1/pipeline-8     	     133	    877510 ns/op	    1012 B/op	      18 allocs/op
BenchmarkSinkFromChan/items=1k/conc=1/pipeline-8     	     132	   1043519 ns/op	    1010 B/op	      18 allocs/op
BenchmarkSinkFromChan/items=1k/conc=1/pipeline-8     	     129	    873884 ns/op	    1010 B/op	      18 allocs/op
BenchmarkSinkFromChan/items=1k/conc=1/pipeline-8     	     132	    889828 ns/op	    1023 B/op	      18 allocs/op
BenchmarkSinkFromChan/items=1k/conc=1/pipeline-8     	     135	    878671 ns/op	    1014 B/op	      18 allocs/op
BenchmarkSinkFromChan/items=1k/conc=4/channels-8     	     141	    821701 ns/op	     356 B/op	       7 allocs/op
BenchmarkSinkFromChan/items=1k/conc=4/channels-8     	     142	    850781 ns/op	     371 B/op	       7 allocs/op
BenchmarkSinkFromChan/items=1k/conc=4/channels-8     	     129	    864437 ns/op	     359 B/op	       7 allocs/op
BenchmarkSinkFromChan/items=1k/conc=4/channels-8     	     139	    916430 ns/op	     365 B/op	       7 allocs/op
BenchmarkSinkFromChan/items=1k/conc=4/channels-8     	     138	    830090 ns/op	     352 B/op	       7 allocs/op
BenchmarkSinkFromChan/items=1k/conc=4/pipeline-8     	      88	   1296477 ns/op	    3364 B/op	      45 allocs/op
BenchmarkSinkFromChan/items=1k/conc=4/pipeline-8     	      67	   1497364 ns/op	    3382 B/op	      45 allocs/op
BenchmarkSinkFromChan/items=1k/conc=4/pipeline-8     	      92	   1292454 ns/op	    3473 B/op	      46 allocs/op
BenchmarkSinkFromChan/items=1k/conc=4/pipeline-8     	      87	   1379334 ns/op	    3395 B/op	      45 allocs/op
BenchmarkSinkFromChan/items=1k/conc=4/pipeline-8     	      85	   1263820 ns/op	    3456 B/op	      45 allocs/op
BenchmarkBatch/items=1k/conc=1/channels-8            	     210	    552266 ns/op	    8629 B/op	      37 allocs/op
BenchmarkBatch/items=1k/conc=1/channels-8            	     218	    588871 ns/op	    8623 B/op	      37 allocs/op
BenchmarkBatch/items=1k/conc=1/channels-8            	     212	    578497 ns/op	    8621 B/op	      37 allocs/op
BenchmarkBatch/items=1k/conc=1/channels-8            	     218	    577665 ns/op	    8616 B/op	      37 allocs/op
BenchmarkBatch/items=1k/conc=1/channels-8            	     206	    580299 ns/op	    8618 B/op	      37 allocs/op
BenchmarkBatch/items=1k/conc=1/pipeline-8            	     243	    454961 ns/op	   17119 B/op	     209 allocs/op
BenchmarkBatch/items=1k/conc=1/pipeline-8            	     267	    462539 ns/op	   17063 B/op	     209 allocs/op
BenchmarkBatch/items=1k/conc=1/pipeline-8            	     243	    467663 ns/op	   17076 B/op	     209 allocs/op
BenchmarkBatch/items=1k/conc=1/pipeline-8            	     246	    474737 ns/op	   17069 B/op	     209 allocs/op
BenchmarkBatch/items=1k/conc=1/pipeline-8            	     247	    446803 ns/op	   17073 B/op	     209 allocs/op
BenchmarkBatch/items=1k/conc=4/channels-8            	     132	    797209 ns/op	   10088 B/op	      45 allocs/op
BenchmarkBatch/items=1k/conc=4/channels-8            	     151	    789460 ns/op	   10092 B/op	      45 allocs/op
BenchmarkBatch/items=1k/conc=4/channels-8            	     139	    804542 ns/op	   10112 B/op	      45 allocs/op
BenchmarkBatch/items=1k/conc=4/channels-8            	     147	    762514 ns/op	   10076 B/op	      45 allocs/op
BenchmarkBatch/items=1k/conc=4/channels-8            	     152	    916226 ns/op	   10064 B/op	      45 allocs/op
BenchmarkBatch/items=1k/conc=4/pipeline-8            	     159	    694091 ns/op	   19140 B/op	     234 allocs/op
BenchmarkBatch/items=1k/conc=4/pipeline-8            	     170	    709045 ns/op	   19084 B/op	     234 allocs/op
BenchmarkBatch/items=1k/conc=4/pipeline-8            	     168	    683593 ns/op	   19071 B/op	     234 allocs/op
BenchmarkBatch/items=1k/conc=4/pipeline-8            	     176	    703570 ns/op	   19092 B/op	     234 allocs/op
BenchmarkBatch/items=1k/conc=4/pipeline-8            	     169	    773192 ns/op	   19043 B/op	     234 allocs/op
BenchmarkBatchChan/items=1k/conc=1/channels-8        	      99	   1243208 ns/op	    3759 B/op	      36 allocs/op
BenchmarkBatchChan/items=1k/conc=1/channels-8        	     100	   1152664 ns/op	    3775 B/op	      36 allocs/op
BenchmarkBatchChan/items=1k/conc=1/channels-8        	      93	   1148311 ns/op	    3755 B/op	      36 allocs/op
BenchmarkBatchChan/items=1k/conc=1/channels-8        	      93	   1213216 ns/op	    3772 B/op	      36 allocs/op
BenchmarkBatchChan/items=1k/conc=1/channels-8        	      88	   1255512 ns/op	    3763 B/op	      36 allocs/op
BenchmarkBatchChan/items=1k/conc=1/pipeline-8        	     117	    928217 ns/op	    4531 B/op	      49 allocs/op
BenchmarkBatchChan/items=1k/conc=1/pipeline-8        	     116	   1011004 ns/op	    4570 B/op	      49 allocs/op
BenchmarkBatchChan/items=1k/conc=1/pipeline-8        	     121	    939407 ns/op	    4531 B/op	      49 allocs/op
BenchmarkBatchChan/items=1k/conc=1/pipeline-8        	     126	    934891 ns/op	    4550 B/op	      49 allocs/op
BenchmarkBatchChan/items=1k/conc=1/pipeline-8        	     123	    940017 ns/op	    4530 B/op	      49 allocs/op
BenchmarkBatchChan/items=1k/conc=4/channels-8        	     100	   1173990 ns/op	    4281 B/op	      42 allocs/op
BenchmarkBatchChan/items=1k/conc=4/channels-8        	      88	   1206023 ns/op	    4281 B/op	      42 allocs/op
BenchmarkBatchChan/items=1k/conc=4/channels-8        	      87	   1238178 ns/op	    4318 B/op	      42 allocs/op
BenchmarkBatchChan/items=1k/conc=4/channels-8        	      90	   1168175 ns/op	    4281 B/op	      42 allocs/op
BenchmarkBatchChan/items=1k/conc=4/channels-8        	      85	   1179499 ns/op	    4282 B/op	      42 allocs/op
BenchmarkBatchChan/items=1k/conc=4/pipeline-8        	     116	    991732 ns/op	    6656 B/op	      70 allocs/op
BenchmarkBatchChan/items=1k/conc=4/pipeline-8        	     112	   1019721 ns/op	    6632 B/op	      70 allocs/op
BenchmarkBatchChan/items=1k/conc=4/pipeline-8        	     120	    985486 ns/op	    6631 B/op	      70 allocs/op
BenchmarkBatchChan/items=1k/conc=4/pipeline-8        	      93	   1140607 ns/op	    6685 B/op	      70 allocs/op
BenchmarkBatchChan/items=1k/conc=4/pipeline-8        	     100	   1054179 ns/op	    6699 B/op	      70 allocs/op
BenchmarkTwoStage/items=1k/conc=1/channels-8         	      61	   1923739 ns/op	     273 B/op	       7 allocs/op
BenchmarkTwoStage/items=1k/conc=1/channels-8         	      60	   1905051 ns/op	     273 B/op	       7 allocs/op
BenchmarkTwoStage/items=1k/conc=1/channels-8         	      61	   1909280 ns/op	     273 B/op	       7 allocs/op
BenchmarkTwoStage/items=1k/conc=1/channels-8         	      61	   1929344 ns/op	     273 B/op	       7 allocs/op
BenchmarkTwoStage/items=1k/conc=1/channels-8         	      58	   2092202 ns/op	     273 B/op	       7 allocs/op
BenchmarkTwoStage/items=1k/conc=1/pipeline-8         	      51	   2042606 ns/op	     929 B/op	      18 allocs/op
BenchmarkTwoStage/items=1k/conc=1/pipeline-8         	      57	   2029890 ns/op	     894 B/op	      18 allocs/op
BenchmarkTwoStage/items=1k/conc=1/pipeline-8         	      54	   2053027 ns/op	     908 B/op	      18 allocs/op
BenchmarkTwoStage/items=1k/conc=1/pipeline-8         	      49	   2095229 ns/op	     871 B/op	      18 allocs/op
BenchmarkTwoStage/items=1k/conc=1/pipeline-8         	      51	   2086000 ns/op	     920 B/op	      18 allocs/op
BenchmarkTwoStage/items=1k/conc=4/channels-8         	      38	   3959105 ns/op	     694 B/op	      13 allocs/op
BenchmarkTwoStage/items=1k/conc=4/channels-8         	      36	   3040201 ns/op	     877 B/op	      13 allocs/op
BenchmarkTwoStage/items=1k/conc=4/channels-8         	      38	   3133707 ns/op	     739 B/op	      13 allocs/op
BenchmarkTwoStage/items=1k/conc=4/channels-8         	      39	   3782303 ns/op	     725 B/op	      13 allocs/op
BenchmarkTwoStage/items=1k/conc=4/channels-8         	      34	   3867294 ns/op	     735 B/op	      13 allocs/op
BenchmarkTwoStage/items=1k/conc=4/pipeline-8         	      42	   3018280 ns/op	    3648 B/op	      48 allocs/op
BenchmarkTwoStage/items=1k/conc=4/pipeline-8         	      38	   2869734 ns/op	    3488 B/op	      47 allocs/op
BenchmarkTwoStage/items=1k/conc=4/pipeline-8         	      37	   3032791 ns/op	    3382 B/op	      46 allocs/op
BenchmarkTwoStage/items=1k/conc=4/pipeline-8         	      40	   2909745 ns/op	    3506 B/op	      47 allocs/op
BenchmarkTwoStage/items=1k/conc=4/pipeline-8         	      39	   2878558 ns/op	    3583 B/op	      48 allocs/op
BenchmarkSplitMerge/items=1k/conc=1/channels-8       	      22	   4609953 ns/op	     594 B/op	      13 allocs/op
BenchmarkSplitMerge/items=1k/conc=1/channels-8       	      26	   4585340 ns/op	     484 B/op	      12 allocs/op
BenchmarkSplitMerge/items=1k/conc=1/channels-8       	      22	   4907206 ns/op	     485 B/op	      12 allocs/op
BenchmarkSplitMerge/items=1k/conc=1/channels-8       	      27	   4699600 ns/op	     651 B/op	      13 allocs/op
BenchmarkSplitMerge/items=1k/conc=1/channels-8       	      22	   4877030 ns/op	     524 B/op	      12 allocs/op
BenchmarkSplitMerge/items=1k/conc=1/pipeline-8       	      18	   6438111 ns/op	    1503 B/op	      28 allocs/op
BenchmarkSplitMerge/items=1k/conc=1/pipeline-8       	      18	   6663650 ns/op	    1641 B/op	      30 allocs/op
BenchmarkSplitMerge/items=1k/conc=1/pipeline-8       	      16	   6585203 ns/op	    1481 B/op	      28 allocs/op
BenchmarkSplitMerge/items=1k/conc=1/pipeline-8       	      18	   6518215 ns/op	    1663 B/op	      30 allocs/op
BenchmarkSplitMerge/items=1k/conc=1/pipeline-8       	      18	   7233988 ns/op	    1481 B/op	      28 allocs/op
BenchmarkSplitMerge/items=1k/conc=4/channels-8       	      16	   7260052 ns/op	     997 B/op	      19 allocs/op
BenchmarkSplitMerge/items=1k/conc=4/channels-8       	      19	   6020594 ns/op	     869 B/op	      18 allocs/op
BenchmarkSplitMerge/items=1k/conc=4/channels-8       	      20	   6026438 ns/op	    1152 B/op	      21 allocs/op
BenchmarkSplitMerge/items=1k/conc=4/channels-8       	      20	   6916969 ns/op	     970 B/op	      19 allocs/op
BenchmarkSplitMerge/items=1k/conc=4/channels-8       	      14	   7278354 ns/op	     913 B/op	      18 allocs/op
BenchmarkSplitMerge/items=1k/conc=4/pipeline-8       	      16	   6389370 ns/op	    4661 B/op	      63 allocs/op
BenchmarkSplitMerge/items=1k/conc=4/pipeline-8       	      19	   6197952 ns/op	    4309 B/op	      59 allocs/op
BenchmarkSplitMerge/items=1k/conc=4/pipeline-8       	      18	   6122125 ns/op	    4217 B/op	      58 allocs/op
BenchmarkSplitMerge/items=1k/conc=4/pipeline-8       	      18	   5892391 ns/op	    4020 B/op	      56 allocs/op
BenchmarkSplitMerge/items=1k/conc=4/pipeline-8       	      19	   6066945 ns/op	    4076 B/op	      57 allocs/op
BenchmarkSplitBy/items=1k/conc=1/channels-8          	      46	   2504417 ns/op	     486 B/op	      12 allocs/op
BenchmarkSplitBy/items=1k/conc=1/channels-8          	      39	   2770505 ns/op	     490 B/op	      12 allocs/op
BenchmarkSplitBy/items=1k/conc=1/channels-8          	      43	   2646364 ns/op	     562 B/op	      12 allocs/op
BenchmarkSplitBy/items=1k/conc=1/channels-8          	      38	   2717935 ns/op	     533 B/op	      12 allocs/op
BenchmarkSplitBy/items=1k/conc=1/channels-8          	      44	   2685569 ns/op	     484 B/op	      12 allocs/op
BenchmarkSplitBy/items=1k/conc=1/pipeline-8          	      32	   3649378 ns/op	    1624 B/op	      29 allocs/op
BenchmarkSplitBy/items=1k/conc=1/pipeline-8          	      31	   3708220 ns/op	    1511 B/op	      28 allocs/op
BenchmarkSplitBy/items=1k/conc=1/pipeline-8          	      32	   3649342 ns/op	    1489 B/op	      28 allocs/op
BenchmarkSplitBy/items=1k/conc=1/pipeline-8          	      30	   3748696 ns/op	    1516 B/op	      28 allocs/op
BenchmarkSplitBy/items=1k/conc=1/pipeline-8          	      27	   3992148 ns/op	    1546 B/op	      28 allocs/op
BenchmarkSplitBy/items=1k/conc=4/channels-8          	      37	   3185991 ns/op	     880 B/op	      18 allocs/op
BenchmarkSplitBy/items=1k/conc=4/channels-8          	      34	   3189650 ns/op	     887 B/op	      18 allocs/op
BenchmarkSplitBy/items=1k/conc=4/channels-8          	      33	   3151184 ns/op	    1161 B/op	      21 allocs/op
BenchmarkSplitBy/items=1k/conc=4/channels-8          	      36	   3446412 ns/op	     903 B/op	      18 allocs/op
BenchmarkSplitBy/items=1k/conc=4/channels-8          	      36	   3435184 ns/op	     997 B/op	      19 allocs/op
BenchmarkSplitBy/items=1k/conc=4/pipeline-8          	      28	   3718110 ns/op	    4125 B/op	      57 allocs/op
BenchmarkSplitBy/items=1k/conc=4/pipeline-8          	      24	   4364092 ns/op	    3983 B/op	      56 allocs/op
BenchmarkSplitBy/items=1k/conc=4/pipeline-8          	      32	   3567639 ns/op	    4021 B/op	      56 allocs/op
BenchmarkSplitBy/items=1k/conc=4/pipeline-8          	      33	   3297779 ns/op	    4153 B/op	      57 allocs/op
BenchmarkSplitBy/items=1k/conc=4/pipeline-8          	      31	   3505593 ns/op	    3979 B/op	      56 allocs/op
BenchmarkOverheadWorkSweep/factor=1/channels-8       	      22	   4562189 ns/op	     173 B/op	       4 allocs/op
BenchmarkOverheadWorkSweep/factor=1/channels-8       	      26	   4473539 ns/op	     172 B/op	       4 allocs/op
BenchmarkOverheadWorkSweep/factor=1/channels-8       	      26	   4486047 ns/op	     172 B/op	       4 allocs/op
BenchmarkOverheadWorkSweep/factor=1/channels-8       	      26	   4373646 ns/op	     172 B/op	       4 allocs/op
BenchmarkOverheadWorkSweep/factor=1/channels-8       	      26	   4378599 ns/op	     172 B/op	       4 allocs/op
BenchmarkOverheadWorkSweep/factor=1/pipeline-8       	      24	   5558976 ns/op	     839 B/op	      16 allocs/op
BenchmarkOverheadWorkSweep/factor=1/pipeline-8       	      25	   4584947 ns/op	     908 B/op	      17 allocs/op
BenchmarkOverheadWorkSweep/factor=1/pipeline-8       	      24	   4579849 ns/op	     807 B/op	      16 allocs/op
BenchmarkOverheadWorkSweep/factor=1/pipeline-8       	      26	   4461982 ns/op	     835 B/op	      16 allocs/op
BenchmarkOverheadWorkSweep/factor=1/pipeline-8       	      26	   4502753 ns/op	     876 B/op	      17 allocs/op
BenchmarkOverheadWorkSweep/factor=2/channels-8       	      22	   5943670 ns/op	     181 B/op	       4 allocs/op
BenchmarkOverheadWorkSweep/factor=2/channels-8       	      21	   5236214 ns/op	     177 B/op	       4 allocs/op
BenchmarkOverheadWorkSweep/factor=2/channels-8       	      21	   5318976 ns/op	     182 B/op	       4 allocs/op
BenchmarkOverheadWorkSweep/factor=2/channels-8       	      21	   5316357 ns/op	     187 B/op	       4 allocs/op
BenchmarkOverheadWorkSweep/factor=2/channels-8       	      22	   5672299 ns/op	     216 B/op	       4 allocs/op
BenchmarkOverheadWorkSweep/factor=2/pipeline-8       	      20	   5149617 ns/op	     770 B/op	      16 allocs/op
BenchmarkOverheadWorkSweep/factor=2/pipeline-8       	      20	   5093842 ns/op	     770 B/op	      16 allocs/op
BenchmarkOverheadWorkSweep/factor=2/pipeline-8       	      22	   5103612 ns/op	     851 B/op	      17 allocs/op
BenchmarkOverheadWorkSweep/factor=2/pipeline-8       	      22	   5047138 ns/op	     877 B/op	      17 allocs/op
BenchmarkOverheadWorkSweep/factor=2/pipeline-8       	      21	   5237312 ns/op	     792 B/op	      16 allocs/op
BenchmarkOverheadWorkSweep/factor=4/channels-8       	      15	   7683250 ns/op	     175 B/op	       4 allocs/op
BenchmarkOverheadWorkSweep/factor=4/channels-8       	      16	   6803221 ns/op	     175 B/op	       4 allocs/op
BenchmarkOverheadWorkSweep/factor=4/channels-8       	      16	   6789622 ns/op	     175 B/op	       4 allocs/op
BenchmarkOverheadWorkSweep/factor=4/channels-8       	      15	   6866311 ns/op	     175 B/op	       4 allocs/op
BenchmarkOverheadWorkSweep/factor=4/channels-8       	      15	   6696422 ns/op	     175 B/op	       4 allocs/op
BenchmarkOverheadWorkSweep/factor=4/pipeline-8       	      18	   6522338 ns/op	    1101 B/op	      16 allocs/op
BenchmarkOverheadWorkSweep/factor=4/pipeline-8       	      16	   6545969 ns/op	     865 B/op	      17 allocs/op
BenchmarkOverheadWorkSweep/factor=4/pipeline-8       	      16	   6535336 ns/op	     781 B/op	      16 allocs/op
BenchmarkOverheadWorkSweep/factor=4/pipeline-8       	      18	   6540752 ns/op	     879 B/op	      17 allocs/op
BenchmarkOverheadWorkSweep/factor=4/pipeline-8       	      18	   6499530 ns/op	     868 B/op	      17 allocs/op
BenchmarkOverheadWorkSweep/factor=8/channels-8       	      10	  11221942 ns/op	     208 B/op	       4 allocs/op
BenchmarkOverheadWorkSweep/factor=8/channels-8       	      10	  10173392 ns/op	     208 B/op	       4 allocs/op
BenchmarkOverheadWorkSweep/factor=8/channels-8       	      10	  10235113 ns/op	     188 B/op	       4 allocs/op
BenchmarkOverheadWorkSweep/factor=8/channels-8       	      10	  10172058 ns/op	     217 B/op	       4 allocs/op
BenchmarkOverheadWorkSweep/factor=8/channels-8       	      10	  10242929 ns/op	     227 B/op	       4 allocs/op
BenchmarkOverheadWorkSweep/factor=8/pipeline-8       	      12	   9622490 ns/op	     830 B/op	      16 allocs/op
BenchmarkOverheadWorkSweep/factor=8/pipeline-8       	      12	   9473150 ns/op	     918 B/op	      17 allocs/op
BenchmarkOverheadWorkSweep/factor=8/pipeline-8       	      12	   9506031 ns/op	    1006 B/op	      18 allocs/op
BenchmarkOverheadWorkSweep/factor=8/pipeline-8       	      12	   9555326 ns/op	     854 B/op	      17 allocs/op
BenchmarkOverheadWorkSweep/factor=8/pipeline-8       	      12	   9509302 ns/op	     782 B/op	      16 allocs/op
BenchmarkOneToOneOrZeroWorkSweep/factor=1/channels-8 	      30	   3738404 ns/op	     171 B/op	       4 allocs/op
BenchmarkOneToOneOrZeroWorkSweep/factor=1/channels-8 	      33	   3180765 ns/op	     171 B/op	       4 allocs/op
BenchmarkOneToOneOrZeroWorkSweep/factor=1/channels-8 	      37	   3167273 ns/op	     171 B/op	       4 allocs/op
BenchmarkOneToOneOrZeroWorkSweep/factor=1/channels-8 	      37	   3220096 ns/op	     171 B/op	       4 allocs/op
BenchmarkOneToOneOrZeroWorkSweep/factor=1/channels-8 	      36	   3236505 ns/op	     211 B/op	       4 allocs/op
BenchmarkOneToOneOrZeroWorkSweep/factor=1/pipeline-8 	      37	   3159154 ns/op	     795 B/op	      16 allocs/op
BenchmarkOneToOneOrZeroWorkSweep/factor=1/pipeline-8 	      36	   3138052 ns/op	     818 B/op	      16 allocs/op
BenchmarkOneToOneOrZeroWorkSweep/factor=1/pipeline-8 	      37	   3128033 ns/op	     767 B/op	      16 allocs/op
BenchmarkOneToOneOrZeroWorkSweep/factor=1/pipeline-8 	      36	   3145031 ns/op	     818 B/op	      16 allocs/op
BenchmarkOneToOneOrZeroWorkSweep/factor=1/pipeline-8 	      36	   3418464 ns/op	     762 B/op	      16 allocs/op
BenchmarkOneToOneOrZeroWorkSweep/factor=2/channels-8 	      31	   3570897 ns/op	     171 B/op	       4 allocs/op
BenchmarkOneToOneOrZeroWorkSweep/factor=2/channels-8 	      31	   3579317 ns/op	     171 B/op	       4 allocs/op
BenchmarkOneToOneOrZeroWorkSweep/factor=2/channels-8 	      32	   3588254 ns/op	     171 B/op	       4 allocs/op
BenchmarkOneToOneOrZeroWorkSweep/factor=2/channels-8 	      30	   3588079 ns/op	     171 B/op	       4 allocs/op
BenchmarkOneToOneOrZeroWorkSweep/factor=2/channels-8 	      31	   3601657 ns/op	     177 B/op	       4 allocs/op
BenchmarkOneToOneOrZeroWorkSweep/factor=2/pipeline-8 	      28	   3576448 ns/op	     765 B/op	      16 allocs/op
BenchmarkOneToOneOrZeroWorkSweep/factor=2/pipeline-8 	      31	   3560888 ns/op	     766 B/op	      16 allocs/op
BenchmarkOneToOneOrZeroWorkSweep/factor=2/pipeline-8 	      32	   3584831 ns/op	     766 B/op	      16 allocs/op
BenchmarkOneToOneOrZeroWorkSweep/factor=2/pipeline-8 	      30	   3780869 ns/op	     764 B/op	      16 allocs/op
BenchmarkOneToOneOrZeroWorkSweep/factor=2/pipeline-8 	      33	   3561384 ns/op	     832 B/op	      16 allocs/op
BenchmarkOneToOneOrZeroWorkSweep/factor=4/channels-8 	      24	   4456500 ns/op	     172 B/op	       4 allocs/op
BenchmarkOneToOneOrZeroWorkSweep/factor=4/channels-8 	      25	   4455147 ns/op	     184 B/op	       4 allocs/op
BenchmarkOneToOneOrZeroWorkSweep/factor=4/channels-8 	      26	   4496040 ns/op	     194 B/op	       4 allocs/op
BenchmarkOneToOneOrZeroWorkSweep/factor=4/channels-8 	      25	   4516073 ns/op	     191 B/op	       4 allocs/op
BenchmarkOneToOneOrZeroWorkSweep/factor=4/channels-8 	      24	   4492696 ns/op	     200 B/op	       4 allocs/op
BenchmarkOneToOneOrZeroWorkSweep/factor=4/pipeline-8 	      24	   4415431 ns/op	     807 B/op	      16 allocs/op
BenchmarkOneToOneOrZeroWorkSweep/factor=4/pipeline-8 	      26	   4807976 ns/op	     766 B/op	      16 allocs/op
BenchmarkOneToOneOrZeroWorkSweep/factor=4/pipeline-8 	      27	   4368914 ns/op	     790 B/op	      16 allocs/op
BenchmarkOneToOneOrZeroWorkSweep/factor=4/pipeline-8 	      26	   4322221 ns/op	     766 B/op	      16 allocs/op
BenchmarkOneToOneOrZeroWorkSweep/factor=4/pipeline-8 	      26	   4371854 ns/op	     806 B/op	      16 allocs/op
BenchmarkOneToOneOrZeroWorkSweep/factor=8/channels-8 	      19	   6212221 ns/op	     209 B/op	       4 allocs/op
BenchmarkOneToOneOrZeroWorkSweep/factor=8/channels-8 	      19	   6259392 ns/op	     229 B/op	       4 allocs/op
BenchmarkOneToOneOrZeroWorkSweep/factor=8/channels-8 	      19	   6225919 ns/op	     224 B/op	       4 allocs/op
BenchmarkOneToOneOrZeroWorkSweep/factor=8/channels-8 	      18	   6225505 ns/op	     174 B/op	       4 allocs/op
BenchmarkOneToOneOrZeroWorkSweep/factor=8/channels-8 	      18	   6294630 ns/op	     184 B/op	       4 allocs/op
BenchmarkOneToOneOrZeroWorkSweep/factor=8/pipeline-8 	      19	   6052711 ns/op	     771 B/op	      16 allocs/op
BenchmarkOneToOneOrZeroWorkSweep/factor=8/pipeline-8 	      19	   6059825 ns/op	     781 B/op	      16 allocs/op
BenchmarkOneToOneOrZeroWorkSweep/factor=8/pipeline-8 	      19	   6094333 ns/op	     912 B/op	      17 allocs/op
BenchmarkOneToOneOrZeroWorkSweep/factor=8/pipeline-8 	      19	   6074450 ns/op	     771 B/op	      16 allocs/op
BenchmarkOneToOneOrZeroWorkSweep/factor=8/pipeline-8 	      18	   6136319 ns/op	     772 B/op	      16 allocs/op
BenchmarkOneToManyWorkSweep/factor=1/channels-8      	      18	   6237035 ns/op	     174 B/op	       4 allocs/op
BenchmarkOneToManyWorkSweep/factor=1/channels-8      	      18	   6031435 ns/op	     174 B/op	       4 allocs/op
BenchmarkOneToManyWorkSweep/factor=1/channels-8      	      18	   6073456 ns/op	     174 B/op	       4 allocs/op
BenchmarkOneToManyWorkSweep/factor=1/channels-8      	      19	   6129384 ns/op	     178 B/op	       4 allocs/op
BenchmarkOneToManyWorkSweep/factor=1/channels-8      	      19	   6126184 ns/op	     173 B/op	       4 allocs/op
BenchmarkOneToManyWorkSweep/factor=1/pipeline-8      	      16	   6805594 ns/op	   66467 B/op	    4113 allocs/op
BenchmarkOneToManyWorkSweep/factor=1/pipeline-8      	      16	   7322961 ns/op	   66395 B/op	    4113 allocs/op
BenchmarkOneToManyWorkSweep/factor=1/pipeline-8      	      15	   6860420 ns/op	   66414 B/op	    4113 allocs/op
BenchmarkOneToManyWorkSweep/factor=1/pipeline-8      	      16	   6894380 ns/op	   66389 B/op	    4113 allocs/op
BenchmarkOneToManyWorkSweep/factor=1/pipeline-8      	      16	   6862700 ns/op	   66473 B/op	    4113 allocs/op
BenchmarkOneToManyWorkSweep/factor=2/channels-8      	      16	   6774747 ns/op	     175 B/op	       4 allocs/op
BenchmarkOneToManyWorkSweep/factor=2/channels-8      	      16	   6793672 ns/op	     175 B/op	       4 allocs/op
BenchmarkOneToManyWorkSweep/factor=2/channels-8      	      16	   6796987 ns/op	     175 B/op	       4 allocs/op
BenchmarkOneToManyWorkSweep/factor=2/channels-8      	      16	   6796227 ns/op	     175 B/op	       4 allocs/op
BenchmarkOneToManyWorkSweep/factor=2/channels-8      	      16	   6767226 ns/op	     175 B/op	       4 allocs/op
BenchmarkOneToManyWorkSweep/factor=2/pipeline-8      	      15	   7318458 ns/op	   66312 B/op	    4112 allocs/op
BenchmarkOneToManyWorkSweep/factor=2/pipeline-8      	      15	   7442120 ns/op	   66312 B/op	    4112 allocs/op
BenchmarkOneToManyWorkSweep/factor=2/pipeline-8      	      15	   7147275 ns/op	   66325 B/op	    4112 allocs/op
BenchmarkOneToManyWorkSweep/factor=2/pipeline-8      	      14	   7172238 ns/op	   66314 B/op	    4112 allocs/op
BenchmarkOneToManyWorkSweep/factor=2/pipeline-8      	      15	   7120344 ns/op	   66312 B/op	    4112 allocs/op
BenchmarkOneToManyWorkSweep/factor=4/channels-8      	      14	   8357554 ns/op	     176 B/op	       4 allocs/op
BenchmarkOneToManyWorkSweep/factor=4/channels-8      	      13	   8379272 ns/op	     206 B/op	       4 allocs/op
BenchmarkOneToManyWorkSweep/factor=4/channels-8      	      14	   8369595 ns/op	     210 B/op	       4 allocs/op
BenchmarkOneToManyWorkSweep/factor=4/channels-8      	      13	   8359990 ns/op	     191 B/op	       4 allocs/op
BenchmarkOneToManyWorkSweep/factor=4/channels-8      	      13	   8300164 ns/op	     228 B/op	       4 allocs/op
BenchmarkOneToManyWorkSweep/factor=4/pipeline-8      	      13	   8565096 ns/op	   66353 B/op	    4112 allocs/op
BenchmarkOneToManyWorkSweep/factor=4/pipeline-8      	      12	   8379518 ns/op	   66318 B/op	    4112 allocs/op
BenchmarkOneToManyWorkSweep/factor=4/pipeline-8      	      13	   8473747 ns/op	   66316 B/op	    4112 allocs/op
BenchmarkOneToManyWorkSweep/factor=4/pipeline-8      	      13	   8343391 ns/op	   66427 B/op	    4113 allocs/op
BenchmarkOneToManyWorkSweep/factor=4/pipeline-8      	      13	   8308090 ns/op	   66323 B/op	    4112 allocs/op
BenchmarkOneToManyWorkSweep/factor=8/channels-8      	      10	  10453688 ns/op	     198 B/op	       4 allocs/op
BenchmarkOneToManyWorkSweep/factor=8/channels-8      	      10	  10264475 ns/op	     208 B/op	       4 allocs/op
BenchmarkOneToManyWorkSweep/factor=8/channels-8      	      10	  10221546 ns/op	     179 B/op	       4 allocs/op
BenchmarkOneToManyWorkSweep/factor=8/channels-8      	      10	  11307929 ns/op	     227 B/op	       4 allocs/op
BenchmarkOneToManyWorkSweep/factor=8/channels-8      	      10	  10155246 ns/op	     188 B/op	       4 allocs/op
BenchmarkOneToManyWorkSweep/factor=8/pipeline-8      	       9	  11140213 ns/op	   66360 B/op	    4112 allocs/op
BenchmarkOneToManyWorkSweep/factor=8/pipeline-8      	       9	  11218278 ns/op	   66424 B/op	    4113 allocs/op
BenchmarkOneToManyWorkSweep/factor=8/pipeline-8      	       9	  11160032 ns/op	   66328 B/op	    4112 allocs/op
BenchmarkOneToManyWorkSweep/factor=8/pipeline-8      	       9	  11144588 ns/op	   66328 B/op	    4112 allocs/op
BenchmarkOneToManyWorkSweep/factor=8/pipeline-8      	       9	  11151894 ns/op	   66339 B/op	    4112 allocs/op
BenchmarkFromChanWorkSweep/factor=1/channels-8       	      22	   4954384 ns/op	     177 B/op	       4 allocs/op
BenchmarkFromChanWorkSweep/factor=1/channels-8       	      25	   4392352 ns/op	     176 B/op	       4 allocs/op
BenchmarkFromChanWorkSweep/factor=1/channels-8       	      25	   4418272 ns/op	     176 B/op	       4 allocs/op
BenchmarkFromChanWorkSweep/factor=1/channels-8       	      25	   4420887 ns/op	     210 B/op	       4 allocs/op
BenchmarkFromChanWorkSweep/factor=1/channels-8       	      27	   4433605 ns/op	     175 B/op	       4 allocs/op
BenchmarkFromChanWorkSweep/factor=1/pipeline-8       	      19	   5738482 ns/op	    1235 B/op	      21 allocs/op
BenchmarkFromChanWorkSweep/factor=1/pipeline-8       	      19	   5923504 ns/op	    1210 B/op	      20 allocs/op
BenchmarkFromChanWorkSweep/factor=1/pipeline-8       	      19	   5717121 ns/op	    1205 B/op	      20 allocs/op
BenchmarkFromChanWorkSweep/factor=1/pipeline-8       	      20	   5949219 ns/op	    1258 B/op	      21 allocs/op
BenchmarkFromChanWorkSweep/factor=1/pipeline-8       	      19	   5614908 ns/op	    1139 B/op	      20 allocs/op
BenchmarkFromChanWorkSweep/factor=2/channels-8       	      21	   5160788 ns/op	     173 B/op	       4 allocs/op
BenchmarkFromChanWorkSweep/factor=2/channels-8       	      22	   5151691 ns/op	     173 B/op	       4 allocs/op
BenchmarkFromChanWorkSweep/factor=2/channels-8       	      21	   5141909 ns/op	     173 B/op	       4 allocs/op
BenchmarkFromChanWorkSweep/factor=2/channels-8       	      20	   5144963 ns/op	     178 B/op	       4 allocs/op
BenchmarkFromChanWorkSweep/factor=2/channels-8       	      21	   5163687 ns/op	     196 B/op	       4 allocs/op
BenchmarkFromChanWorkSweep/factor=2/pipeline-8       	      16	   7003318 ns/op	    1239 B/op	      21 allocs/op
BenchmarkFromChanWorkSweep/factor=2/pipeline-8       	      16	   6400521 ns/op	    1299 B/op	      21 allocs/op
BenchmarkFromChanWorkSweep/factor=2/pipeline-8       	      16	   6390393 ns/op	    1287 B/op	      21 allocs/op
BenchmarkFromChanWorkSweep/factor=2/pipeline-8       	      16	   6344875 ns/op	    1155 B/op	      20 allocs/op
BenchmarkFromChanWorkSweep/factor=2/pipeline-8       	      18	   6395116 ns/op	    1140 B/op	      20 allocs/op
BenchmarkFromChanWorkSweep/factor=4/channels-8       	      16	   6748008 ns/op	     175 B/op	       4 allocs/op
BenchmarkFromChanWorkSweep/factor=4/channels-8       	      18	   6617942 ns/op	     238 B/op	       4 allocs/op
BenchmarkFromChanWorkSweep/factor=4/channels-8       	      16	   6658120 ns/op	     175 B/op	       4 allocs/op
BenchmarkFromChanWorkSweep/factor=4/channels-8       	      16	   7358175 ns/op	     175 B/op	       4 allocs/op
BenchmarkFromChanWorkSweep/factor=4/channels-8       	      15	   6763936 ns/op	     175 B/op	       4 allocs/op
BenchmarkFromChanWorkSweep/factor=4/pipeline-8       	      14	   8024220 ns/op	    1146 B/op	      20 allocs/op
BenchmarkFromChanWorkSweep/factor=4/pipeline-8       	      14	   7970330 ns/op	    1146 B/op	      20 allocs/op
BenchmarkFromChanWorkSweep/factor=4/pipeline-8       	      14	   8247137 ns/op	    1269 B/op	      21 allocs/op
BenchmarkFromChanWorkSweep/factor=4/pipeline-8       	      14	   8052804 ns/op	    1283 B/op	      21 allocs/op
BenchmarkFromChanWorkSweep/factor=4/pipeline-8       	      13	   8065602 ns/op	    1273 B/op	      21 allocs/op
BenchmarkFromChanWorkSweep/factor=8/channels-8       	      10	  10281517 ns/op	     179 B/op	       4 allocs/op
BenchmarkFromChanWorkSweep/factor=8/channels-8       	      10	  10247188 ns/op	     179 B/op	       4 allocs/op
BenchmarkFromChanWorkSweep/factor=8/channels-8       	      10	  10295688 ns/op	     179 B/op	       4 allocs/op
BenchmarkFromChanWorkSweep/factor=8/channels-8       	      10	  10275367 ns/op	     179 B/op	       4 allocs/op
BenchmarkFromChanWorkSweep/factor=8/channels-8       	      10	  10305008 ns/op	     179 B/op	       4 allocs/op
BenchmarkFromChanWorkSweep/factor=8/pipeline-8       	      10	  11129671 ns/op	    1348 B/op	      22 allocs/op
BenchmarkFromChanWorkSweep/factor=8/pipeline-8       	      10	  10895350 ns/op	    1281 B/op	      21 allocs/op
BenchmarkFromChanWorkSweep/factor=8/pipeline-8       	      10	  10873096 ns/op	    1176 B/op	      20 allocs/op
BenchmarkFromChanWorkSweep/factor=8/pipeline-8       	      10	  10851904 ns/op	    1156 B/op	      20 allocs/op
BenchmarkFromChanWorkSweep/factor=8/pipeline-8       	      10	  11048934 ns/op	    1310 B/op	      21 allocs/op
BenchmarkSinkWorkSweep/factor=1/channels-8           	      46	   2227371 ns/op	     162 B/op	       4 allocs/op
BenchmarkSinkWorkSweep/factor=1/channels-8           	      50	   2238275 ns/op	     162 B/op	       4 allocs/op
BenchmarkSinkWorkSweep/factor=1/channels-8           	      49	   2238563 ns/op	     162 B/op	       4 allocs/op
BenchmarkSinkWorkSweep/factor=1/channels-8           	      50	   2445528 ns/op	     162 B/op	       4 allocs/op
BenchmarkSinkWorkSweep/factor=1/channels-8           	      45	   2228082 ns/op	     162 B/op	       4 allocs/op
BenchmarkSinkWorkSweep/factor=1/pipeline-8           	      54	   2079620 ns/op	     689 B/op	      14 allocs/op
BenchmarkSinkWorkSweep/factor=1/pipeline-8           	      52	   2096890 ns/op	     647 B/op	      14 allocs/op
BenchmarkSinkWorkSweep/factor=1/pipeline-8           	      54	   2113681 ns/op	     662 B/op	      14 allocs/op
BenchmarkSinkWorkSweep/factor=1/pipeline-8           	      54	   2100244 ns/op	     646 B/op	      14 allocs/op
BenchmarkSinkWorkSweep/factor=1/pipeline-8           	      57	   2074675 ns/op	     646 B/op	      14 allocs/op
BenchmarkSinkWorkSweep/factor=2/channels-8           	      37	   3709348 ns/op	     188 B/op	       4 allocs/op
BenchmarkSinkWorkSweep/factor=2/channels-8           	      37	   2942654 ns/op	     165 B/op	       4 allocs/op
BenchmarkSinkWorkSweep/factor=2/channels-8           	      38	   2936778 ns/op	     162 B/op	       4 allocs/op
BenchmarkSinkWorkSweep/factor=2/channels-8           	      38	   2955498 ns/op	     175 B/op	       4 allocs/op
BenchmarkSinkWorkSweep/factor=2/channels-8           	      38	   2949466 ns/op	     168 B/op	       4 allocs/op
BenchmarkSinkWorkSweep/factor=2/pipeline-8           	      39	   2813242 ns/op	     669 B/op	      14 allocs/op
BenchmarkSinkWorkSweep/factor=2/pipeline-8           	      39	   2809628 ns/op	     649 B/op	      14 allocs/op
BenchmarkSinkWorkSweep/factor=2/pipeline-8           	      42	   2934776 ns/op	     671 B/op	      14 allocs/op
BenchmarkSinkWorkSweep/factor=2/pipeline-8           	      38	   2826234 ns/op	     738 B/op	      15 allocs/op
BenchmarkSinkWorkSweep/factor=2/pipeline-8           	      39	   2795813 ns/op	     649 B/op	      14 allocs/op
BenchmarkSinkWorkSweep/factor=4/channels-8           	      21	   5491714 ns/op	     165 B/op	       4 allocs/op
BenchmarkSinkWorkSweep/factor=4/channels-8           	      21	   5492560 ns/op	     165 B/op	       4 allocs/op
BenchmarkSinkWorkSweep/factor=4/channels-8           	      20	   5487256 ns/op	     165 B/op	       4 allocs/op
BenchmarkSinkWorkSweep/factor=4/channels-8           	      20	   5441738 ns/op	     165 B/op	       4 allocs/op
BenchmarkSinkWorkSweep/factor=4/channels-8           	      19	   5958943 ns/op	     165 B/op	       4 allocs/op
BenchmarkSinkWorkSweep/factor=4/pipeline-8           	      21	   5202028 ns/op	     662 B/op	      14 allocs/op
BenchmarkSinkWorkSweep/factor=4/pipeline-8           	      22	   5231807 ns/op	     656 B/op	      14 allocs/op
BenchmarkSinkWorkSweep/factor=4/pipeline-8           	      22	   5143962 ns/op	     678 B/op	      14 allocs/op
BenchmarkSinkWorkSweep/factor=4/pipeline-8           	      21	   5244778 ns/op	     657 B/op	      14 allocs/op
BenchmarkSinkWorkSweep/factor=4/pipeline-8           	      22	   5105051 ns/op	     656 B/op	      14 allocs/op
BenchmarkSinkWorkSweep/factor=8/channels-8           	      15	   7920439 ns/op	     212 B/op	       4 allocs/op
BenchmarkSinkWorkSweep/factor=8/channels-8           	      14	   7252610 ns/op	     202 B/op	       4 allocs/op
BenchmarkSinkWorkSweep/factor=8/channels-8           	      15	   7239306 ns/op	     193 B/op	       4 allocs/op
BenchmarkSinkWorkSweep/factor=8/channels-8           	      15	   7242320 ns/op	     180 B/op	       4 allocs/op
BenchmarkSinkWorkSweep/factor=8/channels-8           	      15	   7246186 ns/op	     199 B/op	       4 allocs/op
BenchmarkSinkWorkSweep/factor=8/pipeline-8           	      14	   7420711 ns/op	     734 B/op	      14 allocs/op
BenchmarkSinkWorkSweep/factor=8/pipeline-8           	      15	   7282558 ns/op	     690 B/op	      14 allocs/op
BenchmarkSinkWorkSweep/factor=8/pipeline-8           	      15	   7309739 ns/op	     722 B/op	      14 allocs/op
BenchmarkSinkWorkSweep/factor=8/pipeline-8           	      14	   7343024 ns/op	     782 B/op	      15 allocs/op
BenchmarkSinkWorkSweep/factor=8/pipeline-8           	      15	   7282478 ns/op	     760 B/op	      15 allocs/op
BenchmarkSinkFromChanWorkSweep/factor=1/channels-8   	      52	   2240549 ns/op	     169 B/op	       4 allocs/op
BenchmarkSinkFromChanWorkSweep/factor=1/channels-8   	      52	   2217100 ns/op	     171 B/op	       4 allocs/op
BenchmarkSinkFromChanWorkSweep/factor=1/channels-8   	      50	   2222167 ns/op	     164 B/op	       4 allocs/op
BenchmarkSinkFromChanWorkSweep/factor=1/channels-8   	      51	   2274203 ns/op	     162 B/op	       4 allocs/op
BenchmarkSinkFromChanWorkSweep/factor=1/channels-8   	      45	   2255361 ns/op	     162 B/op	       4 allocs/op
BenchmarkSinkFromChanWorkSweep/factor=1/pipeline-8   	      32	   3452372 ns/op	    1040 B/op	      18 allocs/op
BenchmarkSinkFromChanWorkSweep/factor=1/pipeline-8   	      31	   3463241 ns/op	    1094 B/op	      18 allocs/op
BenchmarkSinkFromChanWorkSweep/factor=1/pipeline-8   	      32	   3429272 ns/op	    1031 B/op	      18 allocs/op
BenchmarkSinkFromChanWorkSweep/factor=1/pipeline-8   	      30	   3448924 ns/op	    1020 B/op	      18 allocs/op
BenchmarkSinkFromChanWorkSweep/factor=1/pipeline-8   	      32	   3821310 ns/op	    1019 B/op	      18 allocs/op
BenchmarkSinkFromChanWorkSweep/factor=2/channels-8   	      39	   2958147 ns/op	     162 B/op	       4 allocs/op
BenchmarkSinkFromChanWorkSweep/factor=2/channels-8   	      37	   2955536 ns/op	     163 B/op	       4 allocs/op
BenchmarkSinkFromChanWorkSweep/factor=2/channels-8   	      39	   2965166 ns/op	     162 B/op	       4 allocs/op
BenchmarkSinkFromChanWorkSweep/factor=2/channels-8   	      37	   2931423 ns/op	     165 B/op	       4 allocs/op
BenchmarkSinkFromChanWorkSweep/factor=2/channels-8   	      38	   2948205 ns/op	     173 B/op	       4 allocs/op
BenchmarkSinkFromChanWorkSweep/factor=2/pipeline-8   	      26	   4555155 ns/op	    1070 B/op	      18 allocs/op
BenchmarkSinkFromChanWorkSweep/factor=2/pipeline-8   	      27	   4210367 ns/op	    1021 B/op	      18 allocs/op
BenchmarkSinkFromChanWorkSweep/factor=2/pipeline-8   	      24	   4306064 ns/op	    1047 B/op	      18 allocs/op
BenchmarkSinkFromChanWorkSweep/factor=2/pipeline-8   	      27	   4243716 ns/op	    1064 B/op	      18 allocs/op
BenchmarkSinkFromChanWorkSweep/factor=2/pipeline-8   	      27	   4276623 ns/op	    1074 B/op	      18 allocs/op
BenchmarkSinkFromChanWorkSweep/factor=4/channels-8   	      21	   5515577 ns/op	     169 B/op	       4 allocs/op
BenchmarkSinkFromChanWorkSweep/factor=4/channels-8   	      21	   5785115 ns/op	     192 B/op	       4 allocs/op
BenchmarkSinkFromChanWorkSweep/factor=4/channels-8   	      22	   5476771 ns/op	     178 B/op	       4 allocs/op
BenchmarkSinkFromChanWorkSweep/factor=4/channels-8   	      22	   5519487 ns/op	     165 B/op	       4 allocs/op
BenchmarkSinkFromChanWorkSweep/factor=4/channels-8   	      21	   5513678 ns/op	     165 B/op	       4 allocs/op
BenchmarkSinkFromChanWorkSweep/factor=4/pipeline-8   	      20	   5719648 ns/op	    1122 B/op	      19 allocs/op
BenchmarkSinkFromChanWorkSweep/factor=4/pipeline-8   	      19	   5695298 ns/op	    1057 B/op	      18 allocs/op
BenchmarkSinkFromChanWorkSweep/factor=4/pipeline-8   	      19	   6198186 ns/op	    1027 B/op	      18 allocs/op
BenchmarkSinkFromChanWorkSweep/factor=4/pipeline-8   	      18	   5707933 ns/op	    1028 B/op	      18 allocs/op
BenchmarkSinkFromChanWorkSweep/factor=4/pipeline-8   	      19	   5676697 ns/op	    1027 B/op	      18 allocs/op
BenchmarkSinkFromChanWorkSweep/factor=8/channels-8   	      15	   7285953 ns/op	     186 B/op	       4 allocs/op
BenchmarkSinkFromChanWorkSweep/factor=8/channels-8   	      15	   7287417 ns/op	     167 B/op	       4 allocs/op
BenchmarkSinkFromChanWorkSweep/factor=8/channels-8   	      15	   7268661 ns/op	     167 B/op	       4 allocs/op
BenchmarkSinkFromChanWorkSweep/factor=8/channels-8   	      14	   8017601 ns/op	     168 B/op	       4 allocs/op
BenchmarkSinkFromChanWorkSweep/factor=8/channels-8   	      14	   7244931 ns/op	     168 B/op	       4 allocs/op
BenchmarkSinkFromChanWorkSweep/factor=8/pipeline-8   	      13	   8534436 ns/op	    1080 B/op	      18 allocs/op
BenchmarkSinkFromChanWorkSweep/factor=8/pipeline-8   	      13	   8439648 ns/op	    1161 B/op	      19 allocs/op
BenchmarkSinkFromChanWorkSweep/factor=8/pipeline-8   	      13	   9493141 ns/op	    1088 B/op	      18 allocs/op
BenchmarkSinkFromChanWorkSweep/factor=8/pipeline-8   	      12	   8473583 ns/op	    1038 B/op	      18 allocs/op
BenchmarkSinkFromChanWorkSweep/factor=8/pipeline-8   	      12	   9718135 ns/op	    1038 B/op	      18 allocs/op
BenchmarkBatchWorkSweep/factor=1/channels-8          	      52	   2168483 ns/op	   33208 B/op	     133 allocs/op
BenchmarkBatchWorkSweep/factor=1/channels-8          	      49	   2151149 ns/op	   33204 B/op	     133 allocs/op
BenchmarkBatchWorkSweep/factor=1/channels-8          	      51	   2149388 ns/op	   33194 B/op	     133 allocs/op
BenchmarkBatchWorkSweep/factor=1/channels-8          	      55	   2204800 ns/op	   33235 B/op	     133 allocs/op
BenchmarkBatchWorkSweep/factor=1/channels-8          	      46	   2208344 ns/op	   33219 B/op	     133 allocs/op
BenchmarkBatchWorkSweep/factor=1/pipeline-8          	      62	   1815308 ns/op	   65538 B/op	     785 allocs/op
BenchmarkBatchWorkSweep/factor=1/pipeline-8          	      67	   1732192 ns/op	   65528 B/op	     785 allocs/op
BenchmarkBatchWorkSweep/factor=1/pipeline-8          	      68	   1769046 ns/op	   65551 B/op	     786 allocs/op
BenchmarkBatchWorkSweep/factor=1/pipeline-8          	      60	   1756278 ns/op	   65498 B/op	     785 allocs/op
BenchmarkBatchWorkSweep/factor=1/pipeline-8          	      67	   1763420 ns/op	   65490 B/op	     785 allocs/op
BenchmarkBatchWorkSweep/factor=2/channels-8          	      43	   2547199 ns/op	   33203 B/op	     133 allocs/op
BenchmarkBatchWorkSweep/factor=2/channels-8          	      40	   2910657 ns/op	   33226 B/op	     133 allocs/op
BenchmarkBatchWorkSweep/factor=2/channels-8          	      43	   2553736 ns/op	   33194 B/op	     133 allocs/op
BenchmarkBatchWorkSweep/factor=2/channels-8          	      37	   2852229 ns/op	   33195 B/op	     133 allocs/op
BenchmarkBatchWorkSweep/factor=2/channels-8          	      40	   2548994 ns/op	   33194 B/op	     133 allocs/op
BenchmarkBatchWorkSweep/factor=2/pipeline-8          	      52	   2238410 ns/op	   65464 B/op	     785 allocs/op
BenchmarkBatchWorkSweep/factor=2/pipeline-8          	      54	   2237519 ns/op	   65505 B/op	     785 allocs/op
BenchmarkBatchWorkSweep/factor=2/pipeline-8          	      46	   2345327 ns/op	   65510 B/op	     785 allocs/op
BenchmarkBatchWorkSweep/factor=2/pipeline-8          	      52	   2215953 ns/op	   65475 B/op	     785 allocs/op
BenchmarkBatchWorkSweep/factor=2/pipeline-8          	      46	   2237337 ns/op	   65537 B/op	     785 allocs/op
BenchmarkBatchWorkSweep/factor=4/channels-8          	      31	   3613550 ns/op	   33195 B/op	     133 allocs/op
BenchmarkBatchWorkSweep/factor=4/channels-8          	      31	   3584577 ns/op	   33195 B/op	     133 allocs/op
BenchmarkBatchWorkSweep/factor=4/channels-8          	      30	   3594293 ns/op	   33198 B/op	     133 allocs/op
BenchmarkBatchWorkSweep/factor=4/channels-8          	      30	   3988586 ns/op	   33195 B/op	     133 allocs/op
BenchmarkBatchWorkSweep/factor=4/channels-8          	      28	   3593381 ns/op	   33247 B/op	     133 allocs/op
BenchmarkBatchWorkSweep/factor=4/pipeline-8          	      32	   3400811 ns/op	   65535 B/op	     785 allocs/op
BenchmarkBatchWorkSweep/factor=4/pipeline-8          	      34	   3322070 ns/op	   65484 B/op	     785 allocs/op
BenchmarkBatchWorkSweep/factor=4/pipeline-8          	      31	   3366632 ns/op	   65458 B/op	     785 allocs/op
BenchmarkBatchWorkSweep/factor=4/pipeline-8          	      31	   3372550 ns/op	   65652 B/op	     787 allocs/op
BenchmarkBatchWorkSweep/factor=4/pipeline-8          	      30	   3714465 ns/op	   65499 B/op	     785 allocs/op
BenchmarkBatchWorkSweep/factor=8/channels-8          	      18	   5761623 ns/op	   33198 B/op	     133 allocs/op
BenchmarkBatchWorkSweep/factor=8/channels-8          	      19	   5760936 ns/op	   33197 B/op	     133 allocs/op
BenchmarkBatchWorkSweep/factor=8/channels-8          	      19	   5750502 ns/op	   33197 B/op	     133 allocs/op
BenchmarkBatchWorkSweep/factor=8/channels-8          	      18	   5785072 ns/op	   33224 B/op	     133 allocs/op
BenchmarkBatchWorkSweep/factor=8/channels-8          	      19	   5772822 ns/op	   33223 B/op	     133 allocs/op
BenchmarkBatchWorkSweep/factor=8/pipeline-8          	      20	   6279854 ns/op	   65582 B/op	     786 allocs/op
BenchmarkBatchWorkSweep/factor=8/pipeline-8          	      20	   5607021 ns/op	   65544 B/op	     786 allocs/op
BenchmarkBatchWorkSweep/factor=8/pipeline-8          	      19	   5727103 ns/op	   65475 B/op	     785 allocs/op
BenchmarkBatchWorkSweep/factor=8/pipeline-8          	      20	   5778273 ns/op	   65473 B/op	     785 allocs/op
BenchmarkBatchWorkSweep/factor=8/pipeline-8          	      20	   5811408 ns/op	   65615 B/op	     786 allocs/op
BenchmarkBatchChanWorkSweep/factor=1/channels-8      	      25	   4523302 ns/op	   14535 B/op	     132 allocs/op
BenchmarkBatchChanWorkSweep/factor=1/channels-8      	      22	   4625936 ns/op	   14517 B/op	     132 allocs/op
BenchmarkBatchChanWorkSweep/factor=1/channels-8      	      22	   4561396 ns/op	   14509 B/op	     132 allocs/op
BenchmarkBatchChanWorkSweep/factor=1/channels-8      	      25	   4554043 ns/op	   14508 B/op	     132 allocs/op
BenchmarkBatchChanWorkSweep/factor=1/channels-8      	      25	   4575015 ns/op	   14512 B/op	     132 allocs/op
BenchmarkBatchChanWorkSweep/factor=1/pipeline-8      	      31	   3835950 ns/op	   15291 B/op	     145 allocs/op
BenchmarkBatchChanWorkSweep/factor=1/pipeline-8      	      30	   3755421 ns/op	   15292 B/op	     145 allocs/op
BenchmarkBatchChanWorkSweep/factor=1/pipeline-8      	      31	   4001948 ns/op	   15390 B/op	     146 allocs/op
BenchmarkBatchChanWorkSweep/factor=1/pipeline-8      	      31	   3645159 ns/op	   15291 B/op	     145 allocs/op
BenchmarkBatchChanWorkSweep/factor=1/pipeline-8      	      31	   3749183 ns/op	   15316 B/op	     145 allocs/op
BenchmarkBatchChanWorkSweep/factor=2/channels-8      	      19	   5448686 ns/op	   14509 B/op	     132 allocs/op
BenchmarkBatchChanWorkSweep/factor=2/channels-8      	      20	   5438302 ns/op	   14509 B/op	     132 allocs/op
BenchmarkBatchChanWorkSweep/factor=2/channels-8      	      20	   5459683 ns/op	   14509 B/op	     132 allocs/op
BenchmarkBatchChanWorkSweep/factor=2/channels-8      	      21	   5727270 ns/op	   14509 B/op	     132 allocs/op
BenchmarkBatchChanWorkSweep/factor=2/channels-8      	      21	   5367433 ns/op	   14509 B/op	     132 allocs/op
BenchmarkBatchChanWorkSweep/factor=2/pipeline-8      	      25	   4581233 ns/op	   15505 B/op	     147 allocs/op
BenchmarkBatchChanWorkSweep/factor=2/pipeline-8      	      25	   4545740 ns/op	   15348 B/op	     145 allocs/op
BenchmarkBatchChanWorkSweep/factor=2/pipeline-8      	      25	   4590595 ns/op	   15317 B/op	     145 allocs/op
BenchmarkBatchChanWorkSweep/factor=2/pipeline-8      	      21	   5012841 ns/op	   15375 B/op	     145 allocs/op
BenchmarkBatchChanWorkSweep/factor=2/pipeline-8      	      22	   4563970 ns/op	   15296 B/op	     145 allocs/op
BenchmarkBatchChanWorkSweep/factor=4/channels-8      	      16	   6967307 ns/op	   14511 B/op	     132 allocs/op
BenchmarkBatchChanWorkSweep/factor=4/channels-8      	      16	   6961260 ns/op	   14511 B/op	     132 allocs/op
BenchmarkBatchChanWorkSweep/factor=4/channels-8      	      15	   6911053 ns/op	   14511 B/op	     132 allocs/op
BenchmarkBatchChanWorkSweep/factor=4/channels-8      	      16	   7559656 ns/op	   14511 B/op	     132 allocs/op
BenchmarkBatchChanWorkSweep/factor=4/channels-8      	      16	   6960469 ns/op	   14511 B/op	     132 allocs/op
BenchmarkBatchChanWorkSweep/factor=4/pipeline-8      	      19	   6039741 ns/op	   15299 B/op	     145 allocs/op
BenchmarkBatchChanWorkSweep/factor=4/pipeline-8      	      18	   6055287 ns/op	   15572 B/op	     148 allocs/op
BenchmarkBatchChanWorkSweep/factor=4/pipeline-8      	      18	   5981060 ns/op	   15476 B/op	     147 allocs/op
BenchmarkBatchChanWorkSweep/factor=4/pipeline-8      	      18	   6496127 ns/op	   15401 B/op	     146 allocs/op
BenchmarkBatchChanWorkSweep/factor=4/pipeline-8      	      18	   6300963 ns/op	   15300 B/op	     145 allocs/op
BenchmarkBatchChanWorkSweep/factor=8/channels-8      	      10	  10048208 ns/op	   14515 B/op	     132 allocs/op
BenchmarkBatchChanWorkSweep/factor=8/channels-8      	      12	   9870906 ns/op	   14513 B/op	     132 allocs/op
BenchmarkBatchChanWorkSweep/factor=8/channels-8      	      12	   9973577 ns/op	   14513 B/op	     132 allocs/op
BenchmarkBatchChanWorkSweep/factor=8/channels-8      	      10	  10082712 ns/op	   14515 B/op	     132 allocs/op
BenchmarkBatchChanWorkSweep/factor=8/channels-8      	      12	   9860583 ns/op	   14513 B/op	     132 allocs/op
BenchmarkBatchChanWorkSweep/factor=8/pipeline-8      	      12	   8784472 ns/op	   15310 B/op	     145 allocs/op
BenchmarkBatchChanWorkSweep/factor=8/pipeline-8      	      12	   8792174 ns/op	   15310 B/op	     145 allocs/op
BenchmarkBatchChanWorkSweep/factor=8/pipeline-8      	      12	   8762820 ns/op	   15310 B/op	     145 allocs/op
BenchmarkBatchChanWorkSweep/factor=8/pipeline-8      	      12	   9552788 ns/op	   15310 B/op	     145 allocs/op
BenchmarkBatchChanWorkSweep/factor=8/pipeline-8      	      13	   8728087 ns/op	   15308 B/op	     145 allocs/op
BenchmarkSplitByWorkSweep/factor=1/channels-8        	      10	  10118787 ns/op	     491 B/op	      12 allocs/op
BenchmarkSplitByWorkSweep/factor=1/channels-8        	      10	  10850634 ns/op	     491 B/op	      12 allocs/op
BenchmarkSplitByWorkSweep/factor=1/channels-8        	      12	  10749246 ns/op	     577 B/op	      13 allocs/op
BenchmarkSplitByWorkSweep/factor=1/channels-8        	      10	  10823812 ns/op	     491 B/op	      12 allocs/op
BenchmarkSplitByWorkSweep/factor=1/channels-8        	      10	  10428392 ns/op	     520 B/op	      12 allocs/op
BenchmarkSplitByWorkSweep/factor=1/pipeline-8        	       7	  14368018 ns/op	    1579 B/op	      29 allocs/op
BenchmarkSplitByWorkSweep/factor=1/pipeline-8        	       7	  14459744 ns/op	    2224 B/op	      35 allocs/op
BenchmarkSplitByWorkSweep/factor=1/pipeline-8        	       7	  14442357 ns/op	    1661 B/op	      29 allocs/op
BenchmarkSplitByWorkSweep/factor=1/pipeline-8        	       8	  13639385 ns/op	    1674 B/op	      30 allocs/op
BenchmarkSplitByWorkSweep/factor=1/pipeline-8        	       8	  13752573 ns/op	    1578 B/op	      29 allocs/op
BenchmarkSplitByWorkSweep/factor=2/channels-8        	      10	  10774221 ns/op	     548 B/op	      12 allocs/op
BenchmarkSplitByWorkSweep/factor=2/channels-8        	       9	  11505537 ns/op	     727 B/op	      14 allocs/op
BenchmarkSplitByWorkSweep/factor=2/channels-8        	      10	  10813808 ns/op	     769 B/op	      15 allocs/op
BenchmarkSplitByWorkSweep/factor=2/channels-8        	      10	  10755612 ns/op	     734 B/op	      13 allocs/op
BenchmarkSplitByWorkSweep/factor=2/channels-8        	       9	  11544444 ns/op	     727 B/op	      14 allocs/op
BenchmarkSplitByWorkSweep/factor=2/pipeline-8        	       7	  14958738 ns/op	    2128 B/op	      34 allocs/op
BenchmarkSplitByWorkSweep/factor=2/pipeline-8        	       7	  14890470 ns/op	    1936 B/op	      32 allocs/op
BenchmarkSplitByWorkSweep/factor=2/pipeline-8        	       7	  14790256 ns/op	    2141 B/op	      34 allocs/op
BenchmarkSplitByWorkSweep/factor=2/pipeline-8        	       7	  14666679 ns/op	    1634 B/op	      29 allocs/op
BenchmarkSplitByWorkSweep/factor=2/pipeline-8        	       7	  14767589 ns/op	    1524 B/op	      28 allocs/op
BenchmarkSplitByWorkSweep/factor=4/channels-8        	       9	  13465019 ns/op	     492 B/op	      12 allocs/op
BenchmarkSplitByWorkSweep/factor=4/channels-8        	       8	  12652260 ns/op	     530 B/op	      12 allocs/op
BenchmarkSplitByWorkSweep/factor=4/channels-8        	       8	  13355989 ns/op	     734 B/op	      14 allocs/op
BenchmarkSplitByWorkSweep/factor=4/channels-8        	       8	  13334438 ns/op	     494 B/op	      12 allocs/op
BenchmarkSplitByWorkSweep/factor=4/channels-8        	       8	  12569682 ns/op	     614 B/op	      13 allocs/op
BenchmarkSplitByWorkSweep/factor=4/pipeline-8        	       7	  15653774 ns/op	    1524 B/op	      28 allocs/op
BenchmarkSplitByWorkSweep/factor=4/pipeline-8        	       7	  16917423 ns/op	    1634 B/op	      29 allocs/op
BenchmarkSplitByWorkSweep/factor=4/pipeline-8        	       6	  17102826 ns/op	    1997 B/op	      33 allocs/op
BenchmarkSplitByWorkSweep/factor=4/pipeline-8        	       7	  15790833 ns/op	    1853 B/op	      31 allocs/op
BenchmarkSplitByWorkSweep/factor=4/pipeline-8        	       7	  15646744 ns/op	    1771 B/op	      31 allocs/op
BenchmarkSplitByWorkSweep/factor=8/channels-8        	       7	  15630506 ns/op	     496 B/op	      12 allocs/op
BenchmarkSplitByWorkSweep/factor=8/channels-8        	       7	  14987970 ns/op	     496 B/op	      12 allocs/op
BenchmarkSplitByWorkSweep/factor=8/channels-8        	       7	  16149357 ns/op	     496 B/op	      12 allocs/op
BenchmarkSplitByWorkSweep/factor=8/channels-8        	       7	  16063881 ns/op	     701 B/op	      14 allocs/op
BenchmarkSplitByWorkSweep/factor=8/channels-8        	       7	  15590411 ns/op	     578 B/op	      13 allocs/op
BenchmarkSplitByWorkSweep/factor=8/pipeline-8        	       6	  17513500 ns/op	    2381 B/op	      37 allocs/op
BenchmarkSplitByWorkSweep/factor=8/pipeline-8        	       6	  17205514 ns/op	    1821 B/op	      31 allocs/op
BenchmarkSplitByWorkSweep/factor=8/pipeline-8        	       6	  17366979 ns/op	    1533 B/op	      28 allocs/op
BenchmarkSplitByWorkSweep/factor=8/pipeline-8        	       6	  17388979 ns/op	    2045 B/op	      33 allocs/op
BenchmarkSplitByWorkSweep/factor=8/pipeline-8        	       6	  17393111 ns/op	    2557 B/op	      39 allocs/op
BenchmarkOverheadStepSweep/steps=1/channels-8        	      27	   4324105 ns/op	     204 B/op	       6 allocs/op
BenchmarkOverheadStepSweep/steps=1/channels-8        	      24	   4325983 ns/op	     204 B/op	       6 allocs/op
BenchmarkOverheadStepSweep/steps=1/channels-8        	      26	   4329561 ns/op	     204 B/op	       6 allocs/op
BenchmarkOverheadStepSweep/steps=1/channels-8        	      26	   4716336 ns/op	     204 B/op	       6 allocs/op
BenchmarkOverheadStepSweep/steps=1/channels-8        	      24	   4338509 ns/op	     204 B/op	       6 allocs/op
BenchmarkOverheadStepSweep/steps=1/pipeline-8        	      27	   4239813 ns/op	     765 B/op	      16 allocs/op
BenchmarkOverheadStepSweep/steps=1/pipeline-8        	      25	   4295230 ns/op	     766 B/op	      16 allocs/op
BenchmarkOverheadStepSweep/steps=1/pipeline-8        	      27	   4281841 ns/op	     786 B/op	      16 allocs/op
BenchmarkOverheadStepSweep/steps=1/pipeline-8        	      26	   4651659 ns/op	     825 B/op	      16 allocs/op
BenchmarkOverheadStepSweep/steps=1/pipeline-8        	      25	   4292977 ns/op	     820 B/op	      16 allocs/op
BenchmarkOverheadStepSweep/steps=2/channels-8        	      15	   7451047 ns/op	     401 B/op	      11 allocs/op
BenchmarkOverheadStepSweep/steps=2/channels-8        	      14	   7443958 ns/op	     392 B/op	      11 allocs/op
BenchmarkOverheadStepSweep/steps=2/channels-8        	      15	   7357147 ns/op	     394 B/op	      11 allocs/op
BenchmarkOverheadStepSweep/steps=2/channels-8        	      15	   7966594 ns/op	     375 B/op	      11 allocs/op
BenchmarkOverheadStepSweep/steps=2/channels-8        	      15	   7362658 ns/op	     343 B/op	      11 allocs/op
BenchmarkOverheadStepSweep/steps=2/pipeline-8        	      14	   7657473 ns/op	     924 B/op	      18 allocs/op
BenchmarkOverheadStepSweep/steps=2/pipeline-8        	      14	   7632703 ns/op	    1075 B/op	      20 allocs/op
BenchmarkOverheadStepSweep/steps=2/pipeline-8        	      14	   7617726 ns/op	     890 B/op	      18 allocs/op
BenchmarkOverheadStepSweep/steps=2/pipeline-8        	      14	   8296970 ns/op	     890 B/op	      18 allocs/op
BenchmarkOverheadStepSweep/steps=2/pipeline-8        	      14	   7662681 ns/op	     890 B/op	      18 allocs/op
BenchmarkOverheadStepSweep/steps=4/channels-8        	       8	  13679396 ns/op	     622 B/op	      21 allocs/op
BenchmarkOverheadStepSweep/steps=4/channels-8        	       8	  13818500 ns/op	     934 B/op	      24 allocs/op
BenchmarkOverheadStepSweep/steps=4/channels-8        	       8	  13911818 ns/op	     826 B/op	      23 allocs/op
BenchmarkOverheadStepSweep/steps=4/channels-8        	       8	  14064968 ns/op	     814 B/op	      23 allocs/op
BenchmarkOverheadStepSweep/steps=4/channels-8        	       8	  13556896 ns/op	     730 B/op	      22 allocs/op
BenchmarkOverheadStepSweep/steps=4/pipeline-8        	       7	  14309286 ns/op	    1291 B/op	      24 allocs/op
BenchmarkOverheadStepSweep/steps=4/pipeline-8        	       8	  14150755 ns/op	    1134 B/op	      22 allocs/op
BenchmarkOverheadStepSweep/steps=4/pipeline-8        	       7	  14601548 ns/op	    1469 B/op	      25 allocs/op
BenchmarkOverheadStepSweep/steps=4/pipeline-8        	       7	  14704934 ns/op	    1209 B/op	      23 allocs/op
BenchmarkOverheadStepSweep/steps=4/pipeline-8        	       7	  14603304 ns/op	    1318 B/op	      24 allocs/op
BenchmarkOverheadStepSweep/steps=8/channels-8        	       4	  28752458 ns/op	    2212 B/op	      52 allocs/op
BenchmarkOverheadStepSweep/steps=8/channels-8        	       4	  27756208 ns/op	    1180 B/op	      41 allocs/op
BenchmarkOverheadStepSweep/steps=8/channels-8        	       4	  28648667 ns/op	    1468 B/op	      44 allocs/op
BenchmarkOverheadStepSweep/steps=8/channels-8        	       4	  28834542 ns/op	    1372 B/op	      43 allocs/op
BenchmarkOverheadStepSweep/steps=8/channels-8        	       4	  28069010 ns/op	    1708 B/op	      46 allocs/op
BenchmarkOverheadStepSweep/steps=8/pipeline-8        	       4	  29113948 ns/op	    2108 B/op	      35 allocs/op
BenchmarkOverheadStepSweep/steps=8/pipeline-8        	       4	  29543583 ns/op	    2156 B/op	      36 allocs/op
BenchmarkOverheadStepSweep/steps=8/pipeline-8        	       4	  29674906 ns/op	    2108 B/op	      35 allocs/op
BenchmarkOverheadStepSweep/steps=8/pipeline-8        	       4	  29163167 ns/op	    1940 B/op	      34 allocs/op
BenchmarkOverheadStepSweep/steps=8/pipeline-8        	       4	  29279656 ns/op	    1628 B/op	      30 allocs/op
BenchmarkOverheadStepSweep/steps=16/channels-8       	       2	  59553812 ns/op	    2344 B/op	      82 allocs/op
BenchmarkOverheadStepSweep/steps=16/channels-8       	       2	  61303563 ns/op	    2872 B/op	      87 allocs/op
BenchmarkOverheadStepSweep/steps=16/channels-8       	       2	  59799125 ns/op	    4024 B/op	      99 allocs/op
BenchmarkOverheadStepSweep/steps=16/channels-8       	       2	  67543270 ns/op	    4168 B/op	     101 allocs/op
BenchmarkOverheadStepSweep/steps=16/channels-8       	       2	  63286563 ns/op	    4072 B/op	     100 allocs/op
BenchmarkOverheadStepSweep/steps=16/pipeline-8       	       2	  62620896 ns/op	    4056 B/op	      62 allocs/op
BenchmarkOverheadStepSweep/steps=16/pipeline-8       	       2	  64360833 ns/op	    3672 B/op	      58 allocs/op
BenchmarkOverheadStepSweep/steps=16/pipeline-8       	       2	  64523375 ns/op	    3864 B/op	      60 allocs/op
BenchmarkOverheadStepSweep/steps=16/pipeline-8       	       2	  64111124 ns/op	    3000 B/op	      51 allocs/op
BenchmarkOverheadStepSweep/steps=16/pipeline-8       	       2	  65775314 ns/op	    3288 B/op	      54 allocs/op
PASS
ok  	github.com/askiada/go-pipeline/v2/pkg/pipeline	99.442s
```

## Channel vs pipeline diff

Diff is percent vs channels (medians from count=5).

| Benchmark | Channels ns/op | Pipeline ns/op | Diff |
| --- | ---: | ---: | ---: |
| Batch/items=1k/conc=1 | 578,497.0 | 462,539.0 | -20.0% |
| Batch/items=1k/conc=4 | 797,209.0 | 703,570.0 | -11.7% |
| BatchChan/items=1k/conc=1 | 1,213,216.0 | 939,407.0 | -22.6% |
| BatchChan/items=1k/conc=4 | 1,179,499.0 | 1,019,721.0 | -13.5% |
| BatchChanWorkSweep/factor=1 | 4,561,396.0 | 3,755,421.0 | -17.7% |
| BatchChanWorkSweep/factor=2 | 5,448,686.0 | 4,581,233.0 | -15.9% |
| BatchChanWorkSweep/factor=4 | 6,961,260.0 | 6,055,287.0 | -13.0% |
| BatchChanWorkSweep/factor=8 | 9,973,577.0 | 8,784,472.0 | -11.9% |
| BatchWorkSweep/factor=1 | 2,168,483.0 | 1,763,420.0 | -18.7% |
| BatchWorkSweep/factor=2 | 2,553,736.0 | 2,237,519.0 | -12.4% |
| BatchWorkSweep/factor=4 | 3,594,293.0 | 3,372,550.0 | -6.2% |
| BatchWorkSweep/factor=8 | 5,761,623.0 | 5,778,273.0 | +0.3% |
| FromChan/items=1k/conc=1 | 1,127,093.0 | 1,491,809.0 | +32.4% |
| FromChan/items=1k/conc=4 | 1,895,687.0 | 1,599,455.0 | -15.6% |
| FromChanWorkSweep/factor=1 | 4,420,887.0 | 5,738,482.0 | +29.8% |
| FromChanWorkSweep/factor=2 | 5,151,691.0 | 6,395,116.0 | +24.1% |
| FromChanWorkSweep/factor=4 | 6,748,008.0 | 8,052,804.0 | +19.3% |
| FromChanWorkSweep/factor=8 | 10,281,517.0 | 10,895,350.0 | +6.0% |
| OneToMany/items=1k/conc=1 | 1,581,893.0 | 1,834,863.0 | +16.0% |
| OneToMany/items=1k/conc=4 | 2,244,566.0 | 2,676,108.0 | +19.2% |
| OneToManyWorkSweep/factor=1 | 6,126,184.0 | 6,862,700.0 | +12.0% |
| OneToManyWorkSweep/factor=2 | 6,793,672.0 | 7,172,238.0 | +5.6% |
| OneToManyWorkSweep/factor=4 | 8,359,990.0 | 8,379,518.0 | +0.2% |
| OneToManyWorkSweep/factor=8 | 10,264,475.0 | 11,151,894.0 | +8.6% |
| OneToOne/items=1k/conc=1 | 1,120,653.0 | 1,156,478.0 | +3.2% |
| OneToOne/items=1k/conc=4 | 1,854,374.0 | 1,753,778.0 | -5.4% |
| OneToOneOrZero/items=1k/conc=1 | 816,273.0 | 799,262.0 | -2.1% |
| OneToOneOrZero/items=1k/conc=4 | 1,351,353.0 | 1,290,364.0 | -4.5% |
| OneToOneOrZeroWorkSweep/factor=1 | 3,220,096.0 | 3,145,031.0 | -2.3% |
| OneToOneOrZeroWorkSweep/factor=2 | 3,588,079.0 | 3,576,448.0 | -0.3% |
| OneToOneOrZeroWorkSweep/factor=4 | 4,492,696.0 | 4,371,854.0 | -2.7% |
| OneToOneOrZeroWorkSweep/factor=8 | 6,225,919.0 | 6,074,450.0 | -2.4% |
| OverheadStepSweep/steps=1 | 4,329,561.0 | 4,292,977.0 | -0.8% |
| OverheadStepSweep/steps=16 | 61,303,563.0 | 64,360,833.0 | +5.0% |
| OverheadStepSweep/steps=2 | 7,443,958.0 | 7,657,473.0 | +2.9% |
| OverheadStepSweep/steps=4 | 13,818,500.0 | 14,601,548.0 | +5.7% |
| OverheadStepSweep/steps=8 | 28,648,667.0 | 29,279,656.0 | +2.2% |
| OverheadWorkSweep/factor=1 | 4,473,539.0 | 4,579,849.0 | +2.4% |
| OverheadWorkSweep/factor=2 | 5,318,976.0 | 5,103,612.0 | -4.0% |
| OverheadWorkSweep/factor=4 | 6,803,221.0 | 6,535,336.0 | -3.9% |
| OverheadWorkSweep/factor=8 | 10,235,113.0 | 9,509,302.0 | -7.1% |
| Sink/items=1k/conc=1 | 579,421.0 | 540,172.0 | -6.8% |
| Sink/items=1k/conc=4 | 857,835.0 | 821,149.0 | -4.3% |
| SinkFromChan/items=1k/conc=1 | 598,736.0 | 878,671.0 | +46.8% |
| SinkFromChan/items=1k/conc=4 | 850,781.0 | 1,296,477.0 | +52.4% |
| SinkFromChanWorkSweep/factor=1 | 2,240,549.0 | 3,452,372.0 | +54.1% |
| SinkFromChanWorkSweep/factor=2 | 2,955,536.0 | 4,276,623.0 | +44.7% |
| SinkFromChanWorkSweep/factor=4 | 5,515,577.0 | 5,707,933.0 | +3.5% |
| SinkFromChanWorkSweep/factor=8 | 7,285,953.0 | 8,534,436.0 | +17.1% |
| SinkWorkSweep/factor=1 | 2,238,275.0 | 2,096,890.0 | -6.3% |
| SinkWorkSweep/factor=2 | 2,949,466.0 | 2,813,242.0 | -4.6% |
| SinkWorkSweep/factor=4 | 5,491,714.0 | 5,202,028.0 | -5.3% |
| SinkWorkSweep/factor=8 | 7,246,186.0 | 7,309,739.0 | +0.9% |
| SplitBy/items=1k/conc=1 | 2,685,569.0 | 3,708,220.0 | +38.1% |
| SplitBy/items=1k/conc=4 | 3,189,650.0 | 3,567,639.0 | +11.9% |
| SplitByWorkSweep/factor=1 | 10,749,246.0 | 14,368,018.0 | +33.7% |
| SplitByWorkSweep/factor=2 | 10,813,808.0 | 14,790,256.0 | +36.8% |
| SplitByWorkSweep/factor=4 | 13,334,438.0 | 15,790,833.0 | +18.4% |
| SplitByWorkSweep/factor=8 | 15,630,506.0 | 17,388,979.0 | +11.3% |
| SplitMerge/items=1k/conc=1 | 4,699,600.0 | 6,585,203.0 | +40.1% |
| SplitMerge/items=1k/conc=4 | 6,916,969.0 | 6,122,125.0 | -11.5% |
| TwoStage/items=1k/conc=1 | 1,923,739.0 | 2,053,027.0 | +6.7% |
| TwoStage/items=1k/conc=4 | 3,782,303.0 | 2,909,745.0 | -23.1% |
