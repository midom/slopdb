# INSERT performance with 100 rows per transaction

## Matched median transactions/s

| Keys | Writers | Before | Concurrent skip list | Change | Current rows/s |
|---|---:|---:|---:|---:|---:|
| random | 96 | 4,328 | 3,433 | -20.7% | 343,312 |
| random | 200 | 4,443 | 3,719 | -16.3% | 371,863 |
| sequential | 96 | 4,350 | 3,480 | -20.0% | 347,974 |
| sequential | 200 | 4,131 | 3,786 | -8.3% | 378,621 |

The concurrent skip list reduces the median rate in all four 100-row cases. The faster first interval does not carry through later intervals. The one-row gains in the previous report do not apply to these transactions.

## Individual intervals

| Keys | Writers | Build | Three intervals, transactions/s | Mean busy cores | Transactions/group | Verified rows, including warm-up |
|---|---:|---|---|---:|---:|---:|
| random | 96 | before | 6,333 / 4,328 / 2,542 | 49.97 | 15.6 | 46,426,300 |
| random | 96 | after | 7,411 / 3,433 / 2,322 | 62.83 | 5.8 | 49,101,800 |
| random | 200 | before | 7,295 / 4,443 / 2,478 | 61.23 | 53.7 | 49,843,900 |
| random | 200 | after | 7,680 / 3,719 / 2,405 | 70.26 | 34.4 | 51,497,300 |
| sequential | 96 | before | 8,977 / 4,350 / 1,392 | 20.73 | 22.2 | 55,131,700 |
| sequential | 96 | after | 10,332 / 3,480 / 702 | 27.33 | 4.9 | 56,206,300 |
| sequential | 200 | before | 9,322 / 4,131 / 720 | 21.33 | 61.8 | 53,448,800 |
| sequential | 200 | after | 9,924 / 3,786 / 2,558 | 31.95 | 24.6 | 61,927,900 |

Each case starts empty and grows throughout timing. The builds reach different row counts during the same elapsed time. These comparisons do not establish a steady rate at a fixed database size. The last 200-writer sequential interval is much faster with the new build, although its three-interval median is lower.

## Single-writer controls

These shorter controls use two 10-second intervals after 3 seconds of warm-up. They are excluded from the primary table.

| Keys | Before transactions/s | Current transactions/s | Change |
|---|---:|---:|---:|
| random | 1,184 | 1,166 | -1.5% |
| sequential | 2,179 | 2,108 | -3.3% |

## Interpretation

The parallel installer searches from the head for each row. The old group path sorts mutations and reuses predecessors. The new path can lose this batch benefit and adds link CAS operations. The measurements also show more CPU use and smaller publication groups in the new build. These runs do not isolate the cost of each change; there is no CPU profile or separate experiment with predecessor reuse enabled.

A single writer keeps the sorted batch path. Its short controls show a smaller loss. A useful next experiment is sorted private insertion with predecessor reuse inside each parallel transaction, compared with serial sorted group installation.

## Settings and verification

- Before engine: `762221ca4ff25b4ad6d28e7eb942dd0a153f4420`; after engine: `10412fc`. Both binaries use the exact same extended benchmark source. No engine code was changed for these tests.
- Machine: 96 virtual AMD EPYC Genoa CPUs, Go 1.26.7.
- Each SQL INSERT statement contains 100 VALUES tuples. One successful statement is one committed transaction. Each result must report exactly 100 changed rows.
- Row: 8-byte ID, 8-byte value, 64-byte BLOB; 80 logical bytes, 84 encoded bytes.
- Random explicit keys use a deterministic permutation without duplicates. Sequential explicit keys are contiguous within each transaction; transaction ranges interleave writers. SQL automatic ID allocation is not used.
- `GOMAXPROCS=96`, `GOGC=200`, 20 GiB mmap cache limit, 8 MiB write buffer, eight background compaction jobs, default sorted memtable.
- WAL is disabled entirely. SSTable and manifest sync remains enabled.
- Eight primary cases use three 30-second intervals after 10 seconds of warm-up. All cases run serially without concurrent builds, tests, or profiles.
- Timing includes completion of transactions in progress at the stop signal. Warm-up, close, reopen, and verification are excluded.
- Each writer owns a session and uses a shared prepared statement with private bindings and payloads.
- All 12 cases pass full row and payload checks after close/reopen. Checks include positive IDs, unique row tickets, expected transaction ranges, key mapping, all payload bytes, and exact row totals. Every configured writer makes progress in every interval, with no external lock retries.
- Verified 439,032,800 rows from 4,390,328 committed transactions, including warm-up. Full artifacts and databases remain under `/tmp/slopdb-batch100-20261016/`. Original fixtures are not used or changed.
- Source and binary hashes match the saved metadata.
- WAL-disabled verification tests a clean close. A crash can lose unflushed changes; crash and power-loss persistence are not tested.

## Reproduce

These measurements predate the option. The after build used parallel
installation by default. It is now off by default; add
`--parallel-memtable-writes` to reproduce the after mode. Omit it for serial
installation. Saved commands and hashes describe the original measured builds.

```sh
GOGC=200 GOMAXPROCS=16 go build -o /tmp/slopdb-write-bench ./cmd/write-bench
GOGC=200 /tmp/slopdb-write-bench --db /tmp/fresh-batch100-random \
  --workload insert-random --rows-per-tx 100 --writers 200 --procs 96 \
  --parallel-memtable-writes --no-wal --mem-mib 8 --warmup 10 --seconds 30 --samples 3
```

Use `--workload insert-sequential` for sequential explicit keys. Use a new database for each case. The tool now supports `--rows-per-tx`, sequential INSERT, rows/s, and full multirow verification. Engine code remains unchanged.

[Raw results, commands, source and binary hashes, and runner scripts](batch100-inserts-results.json).
