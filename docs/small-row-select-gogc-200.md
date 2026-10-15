# GOGC=200 with the default memory limit

On `test`, reran the same 10 GB seeded database with 100 independent readers, CLOCK, and the frozen scaling-optimized binary. Only `GOGC=200` is set. `GOMEMLIMIT` and `GOMAXPROCS` are unset; Go 1.26.0 uses all 60 execution slots. Earlier GC-headroom measurements set both `GOGC=200` and `GOMEMLIMIT=46GiB`; this follow-up isolates the GC setting.

Each cache uses a fresh process, checked stride-eight warmup and three consecutive 30-second samples. The table reports medians; RSS is the process maximum, including warmup and shutdown. Cache budgets and RSS use decimal units.

| Cache | Default GC SELECTs/s | GOGC=200 SELECTs/s | Change | CPU cores | Host CPU | p99 µs | Peak RSS GB |
|---|---:|---:|---:|---:|---:|---:|---:|
| 100 MB | 1,290,306 | 1,514,581 | +17.4% | 42.4 | 70.6% | 1114.1 | 0.83 |
| 1 GB | 1,494,712 | 1,721,687 | +15.2% | 46.4 | 77.3% | 852.0 | 4.93 |
| 10 GB | 2,527,890 | 3,472,815 | +37.4% | 54.2 | 90.3% | 426.0 | 45.41 |
| 20 GB | 2,262,230 | 2,918,204 | +29.0% | 39.3 | 65.4% | 114.7 | 47.51 |

The 10 GB and 20 GB results are within 0.4% of the earlier runs with both `GOGC=200` and `GOMEMLIMIT=46GiB`. In these measurements, leaving the memory limit at its runtime default gives similar throughput. Compared with default GC, peak RSS rises from 30.04 to 45.41 GB at 10 GB cache and from 31.61 to 47.51 GB at 20 GB cache. CPU use improves substantially at 10 GB; the fully fitting 20 GB case still uses only about 65% of the host.

These are sequential measurements on one fixture, rather than independent replications establishing statistical significance. Latency percentiles are histogram upper bounds. CPU includes query validation and Go GC. Linux filesystem caching stays enabled, so these results measure the engine cache/decoding/CPU workload, rather than cold-device random I/O.

All answers were checked. All 1,226 SSTables match the original seed before and after the matrix, sequence numbers stayed unchanged, and cache occupancy stayed within each budget. The executable SHA256 matches the earlier default-GC and combined GC/memory-limit matrices. No engine code or runtime defaults were changed for this experiment.

Full sample data, commands, source-binary identity and integrity evidence are in [the saved results](benchmarks/select-scaling/2026-10-15/gogc-200-only.json). The earlier implementation and runtime comparisons are in [the scaling report](small-row-select-scaling.md).

Reproduce on `test`:

```bash
env -u GOMEMLIMIT -u GOMAXPROCS GOGC=200 python3 \
  ~/slopdb-bench/select-scaling-20261015/select_cache_matrix.py \
  --binary ~/slopdb-bench/select-scaling-20261015/select-final \
  --dir ~/slopdb-small-row-10gb --policy clock \
  --output ~/slopdb-bench/select-scaling-20261015/gogc-200-only-clock-new
```

Raw reports and resource counters are under `~/slopdb-bench/select-scaling-20261015/gogc-200-only-clock/` on `test`; the home-directory report is `~/slopdb-bench/select-scaling-20261015/gogc-200-only-report.md`.
