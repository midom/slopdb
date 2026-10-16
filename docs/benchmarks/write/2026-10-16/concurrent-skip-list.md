# Concurrent skip list and parallel memtable installation

## Implementation

The default sorted memtable now supports concurrent inserts. Insertions publish the bottom link with atomic compare-and-swap, then add upper links with compare-and-swap. Failed operations retry from a predecessor that is still before the key. Nodes own immutable key/value bytes in one allocation. Height, entry counts, and publication sequence use atomic operations. Sorted private batches can still reuse predecessors.

The production commit queue uses the concurrent path. After WAL encoding/write/fsync when enabled, the leader assigns a disjoint sequence span to each transaction. It wakes the queued callers to install their own mutations in parallel. Those callers use their existing goroutines; no new worker pool or group collection delay is added. One barrier waits for all rows before the leader accounts and publishes the whole group. No caller returns success before publication.

```mermaid
flowchart LR
    Q[Queued transactions] --> W[Order WAL and assign sequences]
    W --> I[Writers install rows with CAS]
    I --> B[Wait for every writer]
    B --> P[Publish commit watermark]
    P --> R[Return to callers]
```

Readers retain the old committed view until publication, including while only part of a group has physical links. WAL, rotation, and group publication remain ordered under the writer lock. Flush and close wait for installation to finish. Range tombstone metadata still uses its own mutex. Hash memtables remain optional and off by default; their installation remains serial. Scans of the default memtable seek and traverse atomic links without sorting a copy.

## WAL disabled: matched median transactions/s

| Operation | Previous, 96 writers | Concurrent, 96 writers | Previous, 200 writers | Concurrent, 200 writers |
|---|---:|---:|---:|---:|
| INSERT, random keys | 242,978 | 415,607 | 278,099 | 467,613 |
| INSERT, persistent automatic IDs | 32,159 | 31,988 | 22,570 | 22,146 |
| INSERT, restart-reuse automatic IDs | 358,636 | 360,652 | 351,289 | 343,902 |
| UPDATE, random existing IDs | 329,992 | 534,603 | 390,298 | 715,046 |

Random INSERT medians rise by 71.0% at 96 writers and 68.1% at 200 writers. Random UPDATE medians rise by 62.0% and 83.2%, respectively.

Automatic-ID inserts do not show a useful gain. Persistent counters still serialize metadata publication in this WAL-disabled mode. Restart-reuse inserts still share the ID allocator. At 200 writers, both automatic-ID medians are about 2% lower than before. The new skip list targets parallel memtable insertion; it does not remove those shared SQL costs.

Both builds use the same 8 MiB write buffer, 20 GiB mmap cache, 96 Go processors, GOGC=200, WAL disabled, and three 30-second intervals after 10 seconds of warm-up. Each INSERT case starts empty. The faster build inserts more rows and reaches a larger database during the same elapsed time. These comparisons do not hold database size fixed and do not establish a long-term steady insert rate.

## Individual samples

| Operation | Writers | Three intervals, transactions/s | Mean busy cores | Transactions/publication group |
|---|---:|---|---:|---:|
| INSERT, random keys | 96 | 420,269 / 415,607 / 301,941 | 40.24 | 29.9 |
| INSERT, persistent automatic IDs | 96 | 32,098 / 31,868 / 31,988 | 5.34 | 1.0 |
| INSERT, restart-reuse automatic IDs | 96 | 364,477 / 360,652 / 272,436 | 14.92 | 38.1 |
| INSERT, random keys | 200 | 553,869 / 467,613 / 262,033 | 57.50 | 75.0 |
| INSERT, persistent automatic IDs | 200 | 22,363 / 22,146 / 21,928 | 5.77 | 1.0 |
| INSERT, restart-reuse automatic IDs | 200 | 360,674 / 343,902 / 277,774 | 15.02 | 49.9 |
| UPDATE, random existing IDs | 96 | 534,674 / 525,351 / 534,603 | 14.96 | 46.3 |
| UPDATE, random existing IDs | 200 | 715,046 / 717,682 / 707,073 | 20.10 | 98.4 |

Random inserts still slow as the databases grow. At 200 writers, the concurrent skip list runs at 553,869 / 467,613 / 262,033 transactions/s. Its last interval is close to the previous skip list, although it has inserted many more rows. At 96 writers, the last interval rises from 227,719 to 301,941. Automatic restart-reuse inserts also slow in the last interval.

The random INSERT case uses up to 66.0 busy cores at 200 writers. Flushes, compactions, SQL lookups, and allocation are included in timing. These final runs do not contain a CPU profile to assign their relative cost.

## WAL fsync control

The 200-writer random INSERT control reaches 142,474 committed inserts/s with WAL enabled and fsync on each committed group. It uses one 20-second interval after 3 seconds of warm-up. It passed all row/payload checks after close and reopen. This shorter control is excluded from the main table. It does not test power-loss persistence.

## Test settings and verification

- Machine: 96 virtual AMD EPYC Genoa CPUs, Go 1.26.7.
- Rows: 8-byte ID, 8-byte value, 64-byte BLOB; 80 logical bytes, 84 encoded bytes. One row change per SQL transaction.
- `GOMAXPROCS=96`, `GOGC=200`. No run uses GOGC above 200.
- 20 GiB mmap cache limit, 8 MiB write buffer, eight background compaction jobs.
- One session per writer, shared prepared statement, private bindings and result buffers.
- Primary cases disable WAL entirely. SSTable and manifest sync remains enabled.
- INSERT uses a new database for each case. Random keys are a deterministic permutation without duplicates. Automatic IDs use SQL AUTOINCREMENT; restart reuse adds `--volatile-ids`.
- UPDATE uses fresh Btrfs copies of the saved 10 GB database with 138,864,149 rows. Random keys select one million existing cached rows, IDs 1 through 1,000,000.
- Cases run serially without concurrent builds, tests, or profilers. The eight primary cases each have three 30-second intervals after 10 seconds of warm-up.
- Samples include completion of transactions already in progress at the stop signal. They exclude warm-up, close, reopen, and verification.

All nine final cases passed row and payload checks after clean close and reopen. INSERT checks every row, positive IDs, unique transaction tickets, random-key mapping, payload bytes, and row totals. UPDATE checks every one of the million target rows, all payload bytes, and the total value increase. The cases verified 282,467,179 committed row changes, including warm-up. Every configured writer made progress in every interval, with no external lock retries. The original 145 SSTable names and sizes and fixture metadata are unchanged.

WAL-disabled clean close persists data to SSTables. A crash can lose unflushed changes; these cases do not test crash or power-loss persistence.

Full tests and race checks passed before the final benchmarks:

```sh
GOGC=200 GOMAXPROCS=16 go test ./...
GOGC=200 GOMAXPROCS=16 go test -race ./db ./sql ./storage -count=1
```

New tests check concurrent point insertion and version ordering, every level of the skip list, reverse scans, retained snapshots, private batch predecessor reuse, a paused partial installation that stays invisible, and multirow queued transactions under concurrent snapshot readers, rotation, WAL on/off, and reopen. Existing tests cover hash mode, WAL recovery/failures, queue limits and gaps, lock waits, aborts, catalog grants, and online indexes.

Short matched pilots guided the full runs. They are excluded from all primary tables and retained in the JSON file. The previous results are [the INSERT retry](insert-retry.md) and [the default skip-list WAL-disabled UPDATE tests](optional-memtable-no-wal.md).

## Reproduce

```sh
GOGC=200 GOMAXPROCS=16 go build -o /tmp/slopdb-write-bench ./cmd/write-bench
GOGC=200 /tmp/slopdb-write-bench --db /tmp/fresh-concurrent-skip-insert \
  --workload insert-random --writers 200 --procs 96 --mem-mib 8 \
  --no-wal --warmup 10 --seconds 30 --samples 3
```

The concurrent skip list is the default; no memtable flag is needed. Use `--workload insert-auto` for persistent automatic IDs and add `--volatile-ids` for restart reuse. For UPDATE, use a new copy of the saved fixture and `--workload update-random --hot 1000000`. Use a new database or copy for each case.

[Raw results, commands, source hashes, binary hash, and excluded pilots](concurrent-skip-list-results.json). Full database files and artifacts remain under `/tmp/slopdb-write-bench-20261016/concurrent-skip-final-*`. The implementation and reports are uncommitted.
