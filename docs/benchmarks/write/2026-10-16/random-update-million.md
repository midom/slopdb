# Random UPDATE above one million transactions/s

## Result

The target passed at 400 and 600 concurrent writers. All three 30-second samples at each writer count exceeded 1,000,000 committed transactions/s. Each transaction changes one row. WAL is enabled; WAL fsync is disabled for the main table.

| Writers | Median transactions/s | Three timed samples | Mean busy cores | Transactions/WAL group |
|---:|---:|---|---:|---:|
| 96 | 737,267 | 747,223 / 735,512 / 737,267 | 15.47 | 42.1 |
| 200 | 942,113 | 942,113 / 928,714 / 949,316 | 19.20 | 87.3 |
| 400 | 1,022,749 | 1,022,749 / 1,036,446 / 1,016,982 | 20.47 | 177.2 |
| 600 | 1,052,145 | 1,053,128 / 1,043,737 / 1,052,145 | 21.12 | 254.0 |

At 200 writers, the median rose from 405,660 before this goal to 942,113. The write buffer changed from 4 to 8 MiB. Both measurements use three 30-second samples, a 10-second warm-up, and the same saved data and update range. The 200-writer result remains below one million/s; the target requires more writers in this test.

At 600 writers, WAL groups average near the 256-transaction limit. More writers give little extra throughput over 400 writers. The ordered publication path and caller wake cost still limit scaling.

## Test conditions

- Machine: 96 virtual AMD EPYC Genoa CPUs; Go 1.26.7.
- `GOMAXPROCS=96`, `GOGC=200`. No run sets GOGC above 200.
- 20 GiB maximum mmap block cache, 8 MiB write buffer, eight background compaction jobs.
- Rows have an 8-byte ID, an 8-byte value, and a 64-byte BLOB: 80 logical bytes, 84 bytes in the SQL encoding.
- One session per writer. Each writer uses a shared prepared SQL statement with private bindings.
- SQL: `UPDATE bench SET v=v+1 WHERE id=?`. Keys are random existing integer IDs from 1 through 1,000,000.
- Each case uses a fresh Btrfs copy of the saved 10 GB database, with 138,864,149 total rows. The one-million-row update range is read into the cache before timing.
- The main table has three consecutive 30-second samples after a 10-second warm-up. The samples share one database for that case.
- Sample time includes completion of transactions in progress at the stop signal. It excludes warm-up and close/reopen verification.
- Each SQL autocommit transaction changes one row. WAL grouping combines commits internally; SQL transaction batches are not used.
- SSTable and manifest sync remains enabled. The main table skips WAL fsync and does not measure power-loss durability.
- Runs are serial. No profiler, tests, or build run during the primary samples.

Original data: `/tmp/slopdb-select-bench-20261014/database-small`. All 145 original SSTable names and sizes, and the fixture metadata, remain unchanged. The benchmark copies and raw records remain under `/tmp/slopdb-write-bench-20261016/`.

The rates above use the hash memtable. It is now optional and off by default.
Add `--hash-memtable` to repeat these tests. The default skip list supports scans
without sorting the entire memtable.

## WAL fsync control

The 200-writer control reached **158,178 committed updates/s** with WAL fsync enabled. It has one 20-second sample after a 3-second warm-up. Each committed WAL group is synced. It passed close/reopen checks; the test does not simulate a power failure.

## Latency, allocation, and writer progress

Mean latency uses one sample per 64 successful transactions per writer. The p99 column gives the largest histogram-bin upper bound across the three intervals. It is not an exact percentile. Peak RSS covers setup, warm-up, timing, close, reopen, and verification. Allocation bytes and GC counts cover timing only.

| Writers | Mean latency, µs | p99 upper bound, µs | Allocated bytes/transaction | GC cycles | Peak RSS, GiB | External retries |
|---:|---:|---:|---:|---:|---:|---:|
| 96 | 130 | 262 | 1,927 | 117 | 11.73 | 0 |
| 200 | 213 | 524 | 1,798 | 136 | 12.21 | 0 |
| 400 | 389 | 1,049 | 1,752 | 141 | 12.26 | 0 |
| 600 | 574 | 4,194 | 1,752 | 141 | 12.49 | 0 |

All configured writers made progress in every interval. Internal lock waits are part of transaction time. Per-writer minimum and maximum counts, flushes, compaction output, and individual samples are in the JSON file.

## Changes that raised the rate

1. Replace memtable sorted-list insertion with hash chains and an append log. Point writes no longer search the sorted list. Scans sort a stable copy. Frozen tables reuse that sorted copy.
2. Allocate memtable nodes and row bytes in Go memory chunks. Reuse the WAL encoding buffer. Remove temporary sorted batch entries from publication.
3. Replace the global queue mutex with an ordered atomic queue. Queue nodes remain separate from pooled requests to prevent reuse races. One caller leads each WAL group and transfers leadership before completed callers wake. The 256-transaction and 1 MiB estimated group limits remain.
4. Reuse prepared update row maps, input/output buffers, the private execution object, and caller-owned results. Borrow BLOB bytes only during the restricted point-update evaluation; committed storage and transaction writes keep their own copies.
5. Use sharded read counters for latest point reads and online index protection.
6. Remove the unused duplicate lock ownership map. Exclusive locks use one owner pointer. Resources without lock values avoid a separate bucket allocation. Reuse exclusive lock entries only after the owner journal drops all references.
7. Size the hash table for the configured write buffer. Stop point search at the first visible matching version when installation follows sequence order. Out-of-order direct memtable writes use the full search. Equal-sequence scans put the latest insertion first.

The storage file format is unchanged. Automatic IDs keep their previously configured persistence mode. The cache keeps its heap/mmap options.

The main tradeoff is scan cost on a changing memtable. A first scan or range estimate must sort a copy of its append log. Point writes get the lower cost; frozen tables cache the sort.

## Trials removed from the final code

- A dedicated queue worker made smaller WAL groups and reduced throughput.
- Shared completion channels and asynchronous notification also reduced throughput.
- Added group collection windows reduced throughput. The final code adds no collection delay.
- Lower Go processor counts did not give a useful rate increase.
- Buffers above 16 MiB reduced throughput in the tested cases. The final matrix uses 8 MiB.

All pilot and profile runs are excluded from the main table. Their rate summaries are kept in the JSON file.

## Verification

Every final case passed after database close/reopen. The verifier reads all one million target rows, checks all 64 payload bytes, and checks that the total value increase equals every committed update, including warm-up. All five final cases together verified 379,452,093 committed updates.

Full tests passed:

```sh
GOGC=200 GOMAXPROCS=16 go test ./...
GOGC=200 GOMAXPROCS=16 go test -race ./db ./sql ./storage -count=1
```

Tests cover concurrent publication, retained snapshots, hash collisions, out-of-order and equal-sequence versions, scans in both directions, owned row/range bytes, 600 queued writers, group byte limits, large atomic batches, producer gaps, WAL failures, generated IDs, lock upgrades, transaction abort policy, and online indexes.

## Reproduce

Build the current tool, then use a new destination directory:

```sh
GOGC=200 GOMAXPROCS=16 go build -o /tmp/slopdb-write-bench ./cmd/write-bench
cp --reflink=always -a /tmp/slopdb-select-bench-20261014/database-small \
  /tmp/slopdb-update-million
GOGC=200 /tmp/slopdb-write-bench --db /tmp/slopdb-update-million \
  --workload update-random --hash-memtable --writers 400 --procs 96 --mem-mib 8 \
  --hot 1000000 --warmup 10 --seconds 30 --samples 3
```

Use 600 writers for the higher measured rate. Add `--durable` for WAL fsync. Use a fresh copy for each run; the verifier expects the saved starting values.

[Raw results, commands, source/binary hashes, and pilot summaries](random-update-million-results.json). The code changes are uncommitted.
