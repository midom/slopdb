# One full Raft node and two logs-only voters

Measured on SSH host `test`, 2026-10-24, using production commit `11b39400f663` plus the benchmark extension in `replication/throughput_benchmark_test.go`. Three repetitions per cell; throughput and latency columns are medians of the per-run statistics.

The requested topology uses one full SQL leader and two logs-only voters: three voters, a majority of two. Each sample verifies that membership before timing. A matched three-full-node quorum uses the same fixture, iteration count, durability mode, worker count and runtime settings.

| Mode | Clients | One full + two journals, tx/s | Run range, tx/s | p50, ms | p95, ms | p99, ms | Three full, tx/s | Difference |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| sync | 1 | 212 | 206–217 | 4.662 | 5.451 | 5.944 | 216 | -1.7% |
| sync | 16 | 834 | 826–836 | 18.240 | 31.730 | 39.060 | 825 | +1.1% |
| sync | 64 | 2,761 | 2,692–2,816 | 20.710 | 42.150 | 51.900 | 2,863 | -3.6% |
| sync | 128 | 4,778 | 4,605–4,801 | 24.260 | 44.890 | 59.450 | 4,660 | +2.5% |
| async | 1 | 1,609 | 1,608–1,619 | 0.580 | 0.676 | 2.307 | 1,617 | -0.5% |
| async | 16 | 15,491 | 15,408–15,503 | 0.907 | 1.574 | 4.848 | 15,337 | +1.0% |
| async | 64 | 27,036 | 26,911–27,257 | 2.124 | 4.479 | 7.781 | 26,484 | +2.1% |
| async | 128 | 32,431 | 32,215–32,756 | 3.538 | 7.340 | 11.820 | 32,005 | +1.3% |

The small row-update workload shows similar throughput with the two topologies. The observed median differences span -3.6% to +2.5%. Three repetitions describe run variation; they do not establish statistical significance.

## Workload and machine

- 60 ARM Neoverse-N1 vCPUs, about 59 GiB RAM; Linux arm64, Go 1.22.2.
- All three Raft members share one Go process on one machine; authenticated TCP loopback, TLS off. SQL runs through independent native sessions.
- `GOMAXPROCS=60`, `GOGC=100`, default storage options, WAL recording and fsync capability enabled on every application and consensus store.
- Databases use `TMPDIR=/home/ubuntu/slopdb-bench-data/raft-journals-20261024` on ext4 `/dev/sda1`. The host’s `/tmp` is tmpfs and was used only for binaries/reports, not timed databases.
- Fixture: 128 counters with integer primary keys, initially zero. Every client repeatedly executes one explicit transaction with one prepared primary-key increment, on a row private to that client. Latency includes BEGIN, UPDATE and COMMIT.
- Sync commits wait for the normal durable Raft quorum and leader application. Async commits use the normal replication/application acknowledgment path with periodic durability certification (100 ms interval, 1 s maximum lag). A final stable barrier is outside timing.
- Fresh fixture/cluster for each measured sample; startup, promotion, rollback warmup, SQL preparation, validation, shutdown and recovery are outside timing. Samples run serially. The second repetition reverses cell and topology order.

## Validation

Every measured sample passed exact per-row and total committed-update checks and logical integrity checks. Full followers match the leader snapshot. Each journal is separately rebuilt from its own opaque snapshot and retained committed WAL, then its full SQL data and integrity are checked; the async hash chain and certified endpoint are also verified. Sync cold restart and async fenced stable-point recovery preserve the totals. There were no failed measured samples.

Before timing, role/proposal tests and benchmark smoke cases passed with the race detector, and `go vet ./replication` and `git diff --check` passed. Unix-socket validation uses a short disk-backed TMPDIR to stay within socket path limits. Timed runs use an ordinary non-race binary.

## Reproduce

Build a test binary from this benchmark source, create a disk-backed temporary directory, and run matching cases serially. For example:

```sh
mkdir -p /var/tmp/sdb-rbench /tmp/slopdb-bench
go test -c -o /tmp/slopdb-bench/replication.test ./replication
TMPDIR=/var/tmp/sdb-rbench GOMAXPROCS=60 GOGC=100 \
  /tmp/slopdb-bench/replication.test -test.run "^$" \
  -test.bench "^BenchmarkRaftLogsOnlyTransactions$/^raft3-async$/^workers128$" \
  -test.benchtime=250000x -test.count=3 -test.timeout=10m
```

For the full-node control select `BenchmarkRaftTransactions`; sync mode is `raft3`, async mode is `raft3-async`. The measured iteration counts per sample were:

| Clients | Sync iterations | Async iterations |
| ---: | ---: | ---: |
| 1 | 1,600 | 12,000 |
| 16 | 18,000 | 160,000 |
| 64 | 25,000 | 250,000 |
| 128 | 25,000 | 250,000 |

Timed binary SHA256: `a33c22894cab89573c77600b66abc7f3ca4d06c92fb3a40bf2c033593a22ac56`.

[Machine metadata, commands, source fingerprints and all 48 samples](logs-only-results.json). Raw logs and build/source artifacts are retained outside the repository under `/tmp/slopdb-bench/raft-journals-20261024/` locally and `/tmp/slopdb-bench/slopdb-raft-journals-20261024/` on `test`.
