# Concurrent writes: 96 and 200 writers

## Main results

See [the later random UPDATE results](random-update-million.md) for the one-million-transactions/s goal.

Each transaction commits one row change. Rows have 80 logical bytes. WAL is enabled; WAL fsync is disabled in this table. Values are medians of three consecutive 30-second samples after a 10-second warm-up. All samples use the same growing database for that case.

| Operation | 96 writers, transactions/s | 200 writers, transactions/s |
|---|---:|---:|
| INSERT, random keys | 239,271 | 270,261 |
| INSERT, persistent automatic IDs | 29,695 | 21,534 |
| INSERT, restart-reuse automatic IDs | 330,265 | 344,354 |
| UPDATE, random IDs | 329,629 | 405,660 |
| UPDATE, sequential IDs | 368,340 | 439,884 |

Random inserts slow as the database grows. At 96 writers, the three samples are 251,232, 239,271, 215,472 transactions/s. At 200 writers, they are 297,355, 270,261, 200,883. The random insert CPU use rises while throughput falls. Flushes and compaction are included in timing. These runs do not establish a long-term steady write rate.

The restart-reuse ID samples also fall in the last interval: 334,223 / 330,265 / 285,783 at 96 writers, and 344,354 / 344,648 / 265,288 at 200 writers. The medians must be read with this decline in mind.

## Settings and data

- Machine: 96 virtual AMD EPYC Genoa CPUs; Go 1.26.7. All 96 CPUs are available to the process.
- `GOMAXPROCS=96`, `GOGC=200`. GOGC was never set above 200.
- 20 GiB maximum mmap block cache, 4 MiB write buffer, eight background compaction jobs.
- The cache size is a limit. The test does not fill all 20 GiB before timing.
- Row: 8-byte integer ID, 8-byte integer value, 64-byte BLOB; 80 logical bytes, 84 bytes in the SQL row encoding. This excludes storage keys and WAL metadata.
- One session per writer; one shared prepared statement. No SQL transaction batches. WAL groups can contain several independent transactions.
- INSERT uses a new database for each case. Random keys use a deterministic permutation with no duplicate keys. Automatic inserts use a real `INTEGER PRIMARY KEY AUTOINCREMENT` column and omit the ID.
- UPDATE uses a Btrfs copy of the saved 10 GB small-row database: 138,864,149 rows. Each case updates IDs 1 through 1,000,000. That range is read into the cache before timing. Thus the update result measures a resident subset of the 10 GB database.
- Random updates choose existing IDs. Sequential updates visit existing IDs in insertion order. They use `UPDATE bench SET v=v+1 WHERE id=?`.
- Each case runs alone. Primary samples have no profiler or concurrent build/test process.
- Time includes completion of transactions in progress at the stop signal. Warm-up and verification are excluded.
- WAL fsync is disabled for the main table. SSTable and manifest sync remains enabled. The main results do not give a power-loss durability rate.

Original data: `/tmp/slopdb-select-bench-20261014/database-small`. Its 145 SSTable names and sizes, and its fixture metadata, are unchanged. INSERT databases and UPDATE copies remain under `/tmp/slopdb-write-bench-20261016/`.

## Reference build

Reference: commit `a0769eaa8c6b263f6db482e31f9db74b895d84fb`. Both binaries use the same final benchmark code and padded worker counters. The reference build only omits the new option assignment. Each reference case has two 12-second samples after a 3-second warm-up. The new build has longer runs and larger final databases. These values show observed rates at different durations; they are not matched long-term speed ratios.

| Operation | Reference, 96 writers | New, 96 writers | Reference, 200 writers | New, 200 writers |
|---|---:|---:|---:|---:|
| INSERT, random keys | 160,441 | 239,271 | 163,715 | 270,261 |
| INSERT, persistent automatic IDs | 8,395 | 29,695 | 4,120 | 21,534 |
| UPDATE, random IDs | 193,311 | 329,629 | 197,123 | 405,660 |
| UPDATE, sequential IDs | 197,775 | 368,340 | 195,078 | 439,884 |

## WAL fsync enabled: 200 writers

Each case has one 20-second sample after a 3-second warm-up. These shorter controls use WAL fsync on every committed group. Close/reopen checks passed; the test does not simulate a power failure.

| Operation | Reference transactions/s | New transactions/s |
|---|---:|---:|
| INSERT, random keys | 92,506 | 123,818 |
| INSERT, persistent automatic IDs | 1,752 | 19,327 |
| INSERT, restart-reuse automatic IDs | — | 153,162 |
| UPDATE, random IDs | 91,788 | 133,093 |
| UPDATE, sequential IDs | 106,221 | 155,425 |

Persistent automatic IDs improve from 1,752 to 19,327 transactions/s in this matched durable control. Counter snapshots enter the WAL queue in order. The catalog publication mutex is released after queue insertion, so other transactions can share the fsync. With fsync disabled, the shorter commit path is faster and remains in use.

## CPU, latency, and groups

CPU is the mean number of busy cores during timing. Mean latency uses one sample per 64 successful transactions per writer. The p99 column is the largest histogram-bin upper bound across the three samples; it is not an exact p99. Groups are total timed transactions divided by total timed WAL groups.

| Operation | Writers | Busy cores | Mean latency, µs | p99 upper bound, µs | Transactions/group |
|---|---:|---:|---:|---:|---:|
| INSERT, random keys | 96 | 21.57 | 407 | 2,097 | 40.7 |
| INSERT, random keys | 200 | 28.99 | 801 | 16,777 | 88.2 |
| INSERT, persistent automatic IDs | 96 | 5.13 | 3,237 | 33,554 | 1.0 |
| INSERT, persistent automatic IDs | 200 | 5.68 | 9,229 | 67,109 | 1.0 |
| INSERT, restart-reuse automatic IDs | 96 | 13.66 | 302 | 2,097 | 39.9 |
| INSERT, restart-reuse automatic IDs | 200 | 14.05 | 626 | 8,389 | 56.9 |
| UPDATE, random IDs | 96 | 10.31 | 293 | 1,049 | 46.7 |
| UPDATE, random IDs | 200 | 12.60 | 496 | 2,097 | 98.2 |
| UPDATE, sequential IDs | 96 | 10.47 | 259 | 524 | 46.4 |
| UPDATE, sequential IDs | 200 | 12.38 | 444 | 1,049 | 98.1 |

All configured writers made progress in every sample. There were no external lock-timeout or conflict retries. Internal lock waits are included in transaction time. Exact per-writer minimum and maximum counts are in the JSON file.

## Flushes, compaction, and memory

Flush and compaction counts cover the three timed samples. Compaction output is cumulative output, not live database size. Peak RSS covers the entire process, including setup, warm-up, close, reopen, and verification. Go allocation bytes count all allocations during timing, not retained heap bytes.

| Operation | Writers | Flushes | Compactions | Compaction output, GiB | Peak RSS, GiB | Allocated bytes/transaction | GC cycles |
|---|---:|---:|---:|---:|---:|---:|---:|
| INSERT, random keys | 96 | 687 | 5 | 3.18 | 3.92 | 7,194 | 733 |
| INSERT, random keys | 200 | 747 | 4 | 1.90 | 4.36 | 7,555 | 734 |
| INSERT, persistent automatic IDs | 96 | 106 | 94 | 1.44 | 0.37 | 16,497 | 945 |
| INSERT, persistent automatic IDs | 200 | 74 | 71 | 0.83 | 0.41 | 15,662 | 729 |
| INSERT, restart-reuse automatic IDs | 96 | 924 | 1456 | 7.56 | 3.48 | 9,848 | 1388 |
| INSERT, restart-reuse automatic IDs | 200 | 928 | 1429 | 7.66 | 3.53 | 9,664 | 1320 |
| UPDATE, random IDs | 96 | 957 | 39 | 2.69 | 8.69 | 4,831 | 145 |
| UPDATE, random IDs | 200 | 1174 | 36 | 2.49 | 9.29 | 4,584 | 167 |
| UPDATE, sequential IDs | 96 | 1083 | 39 | 2.68 | 6.22 | 4,730 | 168 |
| UPDATE, sequential IDs | 200 | 1312 | 38 | 2.60 | 5.97 | 4,541 | 194 |

## Changes

- Add `storage.Options.DisableAutoIncrementPersistence`. Default: false. The CLI flag is `--no-autoincrement-persistence`; the benchmark flag is `--volatile-ids`.
- In this mode, automatic-ID reservations stay in memory. After a restart, allocation starts above the highest surviving row ID. Deleted high IDs can be used again. IDs are not reused during the same open instance. Previously stored counters are ignored while the option is enabled. Row WAL and fsync settings still apply.
- Protect runtime ID reservations with a separate mutex. Automatic allocation no longer takes the exclusive catalog publication lock. Remove repeated reservation scans and locks for IDs already reserved by the allocator.
- Use sharded shared catalog counters for autocommit inserts and point updates. Row and predicate locks remain in the normal lock manager. Queued DDL stops new shared grants and appears in lock diagnostics. Explicit transactions keep the normal catalog lock path.
- Use separate reader and writer wait conditions for catalog publication. Wake one writer at a time; wake readers when queued writers finish.
- Raise the WAL group limit from 64 to 256 transactions, with a 1 MiB estimated byte limit. A large caller batch stays atomic and runs alone. There is no added wait to collect a group.
- Allow durable automatic-ID counter commits to join the ordered WAL queue before they release the catalog publication mutex. Merge complete counter snapshots in queue order and publish counters only after success.
- Check signed ID exhaustion. Concurrent allocation after deleted high IDs must start above the persistent floor.
- Add `cmd/write-bench` with row, payload, value-total, and reopen checks.

## Remaining limits

The engine still has one ordered WAL and memtable publication path. Extra writers can join larger groups, but they cannot publish several groups in parallel. At 200 writers, random updates average 98 transactions/group and use about 12.6 cores. Automatic inserts also use a shared ID allocator. Persistent automatic inserts without fsync still serialize counter metadata publication and average one transaction/group.

Earlier, separate profile runs found catalog wait and wake costs in automatic inserts, and catalog lock-manager contention in updates. After the shared catalog path changed, an intermediate 200-writer update profile showed memtable search, Bloom checks, insertion, and scheduler work among the main costs. The old 64-transaction group limit was full. These profiles guided the changes; they are not profiles of the final build and are excluded from the result tables.

Further gains need less work in the serial publication path, fewer SQL row allocations and copies, and less compaction work for growing random-key databases. Parallel publication would need a design that preserves WAL order, transaction atomicity, and reader visibility. Increasing writers alone will not use all 96 cores.

## Verification and reproduction

All 27 cases passed verification after close/reopen. INSERT checks every row, all 64 payload bytes, missing or duplicate tickets, and random-key mapping. UPDATE checks all one million target rows, all payload bytes, and that the sum of value changes equals all committed transactions, including warm-up.

The full Go test suite passed. The race test selection passed for catalog publication, generated IDs, WAL queue ordering, lock waits, shutdown, online indexes, and prepared execution. New tests cover 200 concurrent ID writers, default persistent deleted IDs, restart reuse, ID exhaustion, table drop/recreate, inactive table counters, DDL exclusion, and WAL failures.

```sh
GOGC=200 GOMAXPROCS=16 go test ./...
GOGC=200 GOMAXPROCS=16 go test -race ./db ./sql ./storage \
  -run 'TestPointCatalog|TestCatalog|Test.*GeneratedIDs|TestAutoIncrement|TestQueued|TestLockWaits|TestConcurrentShutdown|TestOnline|TestPrepared' -count=1
```

See [the benchmark tool](../../../../cmd/write-bench/README.md) for build and single-case commands. Exact final commands, samples, source and binary SHA256 values, and verification results are in [write-results.json](write-results.json). The serial matrix runner and raw process records remain under `/tmp/slopdb-write-bench-20261016/`. Pilot and profile runs are excluded from this JSON and all result tables. SQLite is excluded from this benchmark.
