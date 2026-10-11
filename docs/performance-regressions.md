# Performance regression checks and repeatable measurements

Ordinary Go tests now enforce bounded access using actual EXPLAIN ANALYZE read
counters. Equivalent scalar/composite ranges and IN sets must return independently
expected answers while examining at most a small fixed number of rows/index
entries. This detects accidental full scans without machine-dependent timing
thresholds. Go benchmarks cover 10,000/100,000-row point reads, scalar/composite
ranges, payload sorting and aggregates; typed Q1/Q6 kernel benchmarks remain
available. Loading, statement preparation and warmups are outside measured loops.

```sh
go test ./... -count=1 -timeout 10m
go test ./sql ./tests/regression -run '^$' \
  -bench 'Benchmark(TypedQ[16]Kernel|ReadPlans)$' -benchmem -count 3
python3 scripts/run_benchmarks.py \
  --output /tmp/slopdb-bench/performance-example \
  --workers 1,4,10 --warehouses 2,8 --seconds 15 --repetitions 3
```

The matrix runner uses sequential subprocesses, fresh databases, prepared
statements and seed 42. WAL remains enabled and WAL fsync disabled. It preserves
every raw report/log, records revision/dirty state, Go version and host, checks
all 30 TPC-C consistency assertions and fails on unexpected errors. Timeouts and
failed runs remain visible instead of being discarded. The output directory must
not exist; the runner never deletes a previous database or report. Artifacts can
be large because databases are preserved outside the repository.

TPC-C JSON now includes Go allocated bytes/allocations, process CPU and whole
process peak RSS (Linux), plus storage-counter snapshots before/after the run.
Resource deltas cover the measured window and shutdown drain, excluding loading,
preparation, validation and close. Peak RSS is a process high-water mark and can
include loading. SlopDB's storage counters report installed flush/compaction
counts and SSTable input/output sizes, cache hits/misses and write batches/groups.
They describe logical file work, not physical device I/O, and reset on opening.
Native adapters can supply the optional metrics interface; absent storage fields
mean unavailable, not zero work.

For analytical measurements, repeat `--tpch-data` with generated DBGEN directories
at different scales. All 22 queries run with one warmup and the configured number
of repetitions. Existing TPC-H reports include CPU, allocations, query samples,
input hashes and results. Use `cmd/tpch/verify.py` with independent native answers
to qualify new datasets; successful execution alone is not independent correctness
verification. No OS cache resets are performed by the matrix runner.

GitHub Actions runs builds, vet, ordinary and race suites on the declared Go
baseline and current stable Go. The manual performance workflow runs the matrix
and retains raw JSON/log artifacts even after failures. Timing results are
descriptive; portable correctness/path invariants gate changes. Compare repeated
runs on the same otherwise idle host, scale, durability and toolchain, and use
statistical comparison tools such as benchstat for Go benchmark text.
