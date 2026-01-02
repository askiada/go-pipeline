# Performance

go-pipeline adds orchestration overhead on top of channel operations. For
performance-sensitive workloads, refer to the benchmarks and overhead guidance
to decide when the trade-offs are worthwhile.

- `benchmarks.md` contains benchmark scenarios and overhead tables.
- `step-options.md` covers buffering, max in-flight, and rate limiting
  controls that affect throughput and memory.
