# Conditional COMMIT index cleanup

Ordinary DML commits now skip the index-cleanup metadata iterator and its exclusive catalog lock. A private session hint requests cleanup after an index drop, including transactional drops committed by COMMIT or outermost RELEASE. Cleanup failures retain the hint for retry; rollback can leave one harmless empty cleanup. Startup and online-build cleanup still inspect persisted markers independently.

Measured on SSH host `test`, 2026-10-24. The baseline restores the three production files from `11b39400f6631d46fe7d9f6357e66b9f4f9f0a4b` through a Go build overlay. Both binaries use the same current benchmark harness; the fixed binary contains the conditional cleanup change. There are three serial samples per cell, with the second repetition reversing mode and version order.

| Durability | Version | Median tx/s | Run range, tx/s | p50, ms | p95, ms | p99, ms |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| sync | baseline | 4,856 | 4,768–4,966 | 23.810 | 41.110 | 53.910 |
| sync | fixed | 4,626 | 4,566–4,649 | 25.370 | 43.940 | 59.130 |
| async | baseline | 32,235 | 32,162–32,553 | 3.571 | 7.351 | 11.690 |
| async | fixed | 75,455 | 74,234–75,512 | 1.182 | 4.304 | 10.030 |

Async median throughput increased 2.34 times, with p50 falling from 3.571 to 1.182 ms and p99 from 11.690 to 10.030 ms. Sync median throughput fell 4.7%, and its p99 rose from 53.910 to 59.130 ms. The medians describe these runs; three samples do not establish statistical significance. This fix removes a common-path catalog convoy. Rotation fsync and read-quorum admission remain, so the previous latency investigation still applies to the remaining tails.

## Conditions and validation

- One full SQL leader and two logs-only voters; quorum of two, 128 clients, one process on one 60-vCPU ARM machine. TCP loopback for replication, TLS off; SQL uses independent native sessions.
- Each client runs BEGIN, one prepared primary-key increment on its own counter row, then COMMIT. Latency measures the entire transaction. All samples use the same 128-row fixture and verify every committed counter.
- Go 1.22.2, GOMAXPROCS=60, GOGC=100, default 4 MiB write buffers. Databases use disk-backed ext4 through TMPDIR=/var/tmp/sdb-rj24-check. Binaries and reports use /tmp outside the repository.
- WAL recording and fsync are enabled. Async mode retains normal 100 ms periodic certification and the 1 s lag limit; sync mode retains per-commit durable replication. Read-quorum admission remains enabled. No tracing, profiling or diagnostic durability flags are set.
- Each sync sample times 25,000 transactions; each async sample times 250,000. Setup, preparation, validation, close and recovery are outside timing.
- All 12 measured samples passed exact row/total checks, integrity, independent reconstruction of both journal replicas, and sync cold restart or async fenced stable-point recovery. Including calibration there are 48 independent journal reconstructions.
- Full normal and race suites, root build, go vet ./..., and git diff --check passed. Permanent tests cover the idle catalog-lock bypass, DML metadata-scan bypass, independent sessions, commit/release boundaries, rollback, reopen, and retry after a committed cleanup failure.

## WAL rotation

The current rotation path calls syncWAL before closing the old WAL. Foreground rotation fsync can be avoided with a generation-aware durability handoff: retain sealed WAL files, make each durable barrier cover every earlier unsynced generation (or await their durable SSTable and manifest publication), and retire those files only after their replacement is stable. Syncing only the current WAL could otherwise issue a receipt for a prefix whose older records are still volatile. This patch preserves rotation synchronization; it changes only automatic index-cleanup admission.

The subsequent [background WAL synchronization change](wal-background-sync.md)
implements this handoff and measures it separately against the cleanup fix.

## Reproduce

Build an ordinary test binary, create a disk-backed temporary directory and run samples serially:

```sh
mkdir -p /var/tmp/sdb-rbench /tmp/slopdb-bench
go test -c -o /tmp/slopdb-bench/replication.test ./replication
TMPDIR=/var/tmp/sdb-rbench GOMAXPROCS=60 GOGC=100 \
  /tmp/slopdb-bench/replication.test -test.run "^$" \
  -test.bench "^BenchmarkRaftLogsOnlyTransactions$/^raft3-async$/^workers128$" \
  -test.benchtime=250000x -test.count=3 -test.timeout=10m
```

For sync mode select `raft3` and `-test.benchtime=25000x`. The measured runner alternates versions and reverses the second round; all exact commands, source fingerprints, binary hashes and per-run values are in [the machine-readable results](commit-index-cleanup-results.json). Raw logs, original source files and the baseline overlay are retained outside the repository under `/tmp/slopdb-bench/commit-index-cleanup-20261024/` locally and on `test`.
