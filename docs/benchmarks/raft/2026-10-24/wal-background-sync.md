# Background WAL rotation fsync

Memtable rotation now transfers the sealed WAL to a background fsync/close job. That job holds no storage writer or publication lock during I/O. Deferred writes can enter the new generation while an older sync remains pending. Durable writes, explicit WAL barriers and Flush cover the active WAL and every earlier sealed generation, including generations that finish out of order.

Background failures stop subsequent writes and fail durability confirmation. Close joins all jobs and reports their errors. SSTable and manifest publication still synchronize before retiring WAL paths; the background job retains its open file until sync and close finish. The first WAL sync also synchronizes its directory entry. Background sync alone does not advance the certified Raft durable position.

## Matched measurements

On SSH host `test`, 2026-10-24. Both versions already include the conditional COMMIT cleanup fix. The baseline is the exact binary from that follow-up; its hash is checked against the retained results. The fixed binary adds background rotation sync, prefix barriers and initial WAL directory-entry synchronization. The benchmark harness is identical.

| Durability | Version | Median tx/s | Run range, tx/s | p50, ms | p95, ms | p99, ms |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| sync | baseline | 4,629 | 4,414–4,656 | 24.450 | 46.930 | 59.630 |
| sync | fixed | 4,450 | 4,437–4,713 | 25.700 | 46.610 | 59.200 |
| async | baseline | 75,042 | 74,293–76,706 | 1.171 | 4.189 | 10.310 |
| async | fixed | 74,503 | 74,432–78,425 | 1.212 | 4.085 | 10.430 |

Async median throughput changed -0.7%; p99 changed from 10.310 to 10.430 ms.

Sync median throughput changed -3.9%; p99 changed from 59.630 to 59.200 ms.

Three samples describe run variation and do not establish statistical significance. The deterministic gated-I/O tests separately prove that an old WAL sync cannot hold up deferred writes, reads or SSTable publication. Storage backpressure, manifest publication, read-quorum checks and durable certification remain possible waits.

## Conditions

- One full SQL leader plus two logs-only voters, majority of two, 128 independent clients. All members share one Go process on one 60-vCPU ARM host; replication uses TCP loopback and TLS is off. SQL uses native sessions.
- BEGIN, one prepared primary-key counter increment on a private row, COMMIT. The fixture has 128 counters; every row and the total committed count are checked. Latency covers the full transaction and uses the existing sorted per-transaction quantiles.
- Go 1.22.2, GOMAXPROCS=60, GOGC=100. WAL recording and fsync enabled, default 4 MiB application and consensus write buffers. Async mode uses normal 100 ms certification and a 1 s lag limit; read-quorum admission remains enabled.
- Disk-backed ext4 /dev/sda1 via TMPDIR=/var/tmp/sdb-rj24-check. Reports and binaries use /tmp outside the repository. No race instrumentation, profiling, traces, special overlays or diagnostic durability flags are enabled in timed runs.
- Three serial samples per version/mode; the second round reverses mode and version order. Each sync sample times 25,000 transactions; each async sample times 250,000. Setup, preparation, validation, shutdown and recovery are outside timing.
- All 12 samples passed exact counters, integrity, independent reconstruction of each journal from its own snapshot/WAL and sync cold restart or async fenced stable recovery. There are 1,650,000 timed transactions and 24 measured journal reconstructions, plus 24 calibration reconstructions.

## Permanent validation

All checks ran on `test`: root build, full normal suite, full race suite, go vet ./..., and whitespace validation. Targeted rotation tests also passed three race repetitions. The Linux directory syscall test passed separately with and without the race detector.

`storage/wal_sync_test.go` gates actual file sync calls to verify foreground progress, SST publication during a pending WAL sync, three-generation barriers, out-of-order completion, failure propagation, explicit Flush error handling, shutdown joins, disabled-fsync behavior and abrupt-exit recovery after a barrier. `storage/wal_sync_linux_test.go` checks that the first sync persists both WAL data and its directory entry, with later syncs persisting new records without repeating the directory sync.

`replication/wal_rotation_test.go` crosses consensus WAL rotations with a full leader and two journal voters, certifies a durable prefix, removes the primary recovery payload and reconstructs every exact row and value from journal evidence. Integrity is checked after recovery.

## Reproduce

Build an ordinary test binary and use a disk-backed temporary directory:

```sh
mkdir -p /var/tmp/sdb-rbench /tmp/slopdb-bench
go test -c -o /tmp/slopdb-bench/replication.test ./replication
TMPDIR=/var/tmp/sdb-rbench GOMAXPROCS=60 GOGC=100 \
  /tmp/slopdb-bench/replication.test -test.run "^$" \
  -test.bench "^BenchmarkRaftLogsOnlyTransactions$/^raft3-async$/^workers128$" \
  -test.benchtime=250000x -test.count=3 -test.timeout=10m
```

For sync mode select `raft3` and `-test.benchtime=25000x`. All measured commands, source fingerprints, binary hashes and individual results are in [the machine-readable report](wal-background-sync-results.json). Raw logs, source snapshots and the runner remain outside the repository under `/tmp/slopdb-bench/wal-background-sync-20261024/` locally and on `test`.
