# Optional hash memtables and WAL-disabled random UPDATE

## Changes

The default memtable is again a sorted skip list. Its scans seek and traverse atomic links without sorting all entries. Hash memtables are optional: set `storage.Options.HashMemTable = true`, or pass `--hash-memtable` to the CLI or write benchmark. The option is off by default.

The option applies to startup, WAL recovery, automatic rotation, explicit flush, and clean close. It does not change the database file format. The previous hash node/byte chunks and point-read path remain available when the option is enabled. The sorted mode keeps sorted batch insertion with reused predecessors and bounded skip-list sampling for range estimates.

The benchmark now accepts `--no-wal`. It sets `storage.Options.DisableWAL = true`; it skips WAL encoding, writes, and fsync. Each timed sample records the selected mode and confirms that no WAL is active. WAL-disabled cases left no WAL files after clean close. The ordinary commit queue still groups and publishes transactions.

## WAL disabled: median committed updates/s

| Writers | Default skip list | Hash option |
|---:|---:|---:|
| 96 | 329,992 | 871,324 |
| 200 | 390,298 | 1,017,139 |
| 400 | 455,670 | 1,078,508 |
| 600 | 485,465 | 1,025,051 |

At 200 writers, the hash median exceeds one million updates/s. Its three samples are 962,481 / 1,017,139 / 1,017,694. Two of three samples exceed one million/s. All three hash samples at 400 and 600 writers exceed one million/s.

## Matched WAL controls

WAL-enabled controls skip WAL fsync. They use the same binary, settings, sample lengths, and original data as the WAL-disabled cases.

| Writers | Memtable | WAL on, fsync off | WAL entirely off | Rate increase |
|---:|---|---:|---:|---:|
| 96 | Default skip list | 310,318 | 329,992 | 6.3% |
| 96 | Hash | 726,167 | 871,324 | 20.0% |
| 200 | Default skip list | 374,253 | 390,298 | 4.3% |
| 200 | Hash | 891,621 | 1,017,139 | 14.1% |

Disabling WAL raises the hash rate by about 14% at 200 writers. It does not remove ordered memtable publication, queue handoff, transaction locking, or background flush/compaction work. The highest WAL-disabled hash median is at 400 writers. At 600 writers, it falls by about 5%. The data here does not identify the remaining cost with a new CPU profile.

## Individual WAL-disabled samples

| Writers | Memtable | Three samples, updates/s | Mean busy cores | Updates/publication group | Allocated bytes/update |
|---:|---|---|---:|---:|---:|
| 96 | skip | 337,466 / 329,992 / 328,854 | 8.89 | 47.7 | 2,726 |
| 96 | hash | 870,145 / 871,324 / 876,984 | 17.51 | 36.3 | 1,838 |
| 200 | skip | 395,943 / 390,298 / 380,098 | 10.09 | 99.6 | 2,528 |
| 200 | hash | 962,481 / 1,017,139 / 1,017,694 | 20.05 | 76.9 | 1,768 |
| 400 | skip | 444,413 / 455,670 / 455,673 | 11.23 | 198.1 | 2,355 |
| 400 | hash | 1,050,191 / 1,078,508 / 1,089,130 | 21.01 | 168.0 | 1,738 |
| 600 | skip | 485,465 / 482,800 / 486,364 | 11.92 | 256.0 | 2,300 |
| 600 | hash | 1,041,235 / 1,025,051 / 1,023,648 | 20.36 | 250.7 | 1,755 |

All configured writers made progress in every interval. No case reported an external lock retry. Internal lock waits are included in the transaction time. Complete latency, RSS, GC, flush, and compaction records are in the JSON file.

## Test conditions

- Machine: 96 virtual AMD EPYC Genoa CPUs, Go 1.26.7.
- `GOMAXPROCS=96`, `GOGC=200`. No run used GOGC above 200.
- 20 GiB mmap block cache, 8 MiB write buffer, eight background compaction jobs.
- 80 logical bytes per row: 8-byte ID, 8-byte value, 64-byte BLOB. SQL storage encoding is 84 bytes.
- SQL: `UPDATE bench SET v=v+1 WHERE id=?`; one committed row change per autocommit transaction.
- One session per writer, shared prepared statement, private bindings and result buffers.
- Random existing IDs 1 through 1,000,000, cached before timing. This measures a cached update set within a 10 GB database with 138,864,149 rows.
- Each case uses a fresh Btrfs copy of `/tmp/slopdb-select-bench-20261014/database-small`.
- Three consecutive 30-second intervals after 10 seconds of warm-up. The samples share one copy for that case.
- Sample time includes completion of transactions that are in progress at the stop signal. It excludes warm-up and close/reopen verification.
- Cases run in table order: 96, 200, 400, 600 writers; WAL off before WAL on; skip list before hash. WAL controls are at 96 and 200 writers.
- All cases run one at a time. No profiler, build, or test runs during the primary samples.
- SSTable and manifest sync remains enabled in both WAL modes.

## Verification

All twelve cases passed after clean close and reopen. The verifier reads every one of the million target rows, checks all 64 payload bytes, and checks that the total value increase equals all committed updates, including warm-up. These cases verified 793,828,844 committed updates. The original 145 SSTable names and sizes and fixture metadata are unchanged.

WAL-disabled results measure clean-close persistence. A crash can lose unflushed changes. The tests do not measure crash or power-loss persistence.

Full tests and race checks passed before the primary benchmark:

```sh
GOGC=200 GOMAXPROCS=16 go test ./...
GOGC=200 GOMAXPROCS=16 go test -race ./db ./sql ./storage -count=1
```

Tests cover both memtable modes, default scans without an append-log sort, concurrent publication, retained snapshots, collisions, out-of-order and equal-sequence versions, reverse scans, owned keys/values/range bounds, flush mode retention, mode changes on reopen, and WAL recovery into either mode. WAL-disabled tests check flush and clean-close persistence and the absence of WAL files.

## Reproduce

```sh
GOGC=200 GOMAXPROCS=16 go build -o /tmp/slopdb-write-bench ./cmd/write-bench
cp --reflink=always -a /tmp/slopdb-select-bench-20261014/database-small \
  /tmp/slopdb-update-no-wal
GOGC=200 /tmp/slopdb-write-bench --db /tmp/slopdb-update-no-wal \
  --workload update-random --writers 200 --procs 96 --mem-mib 8 --no-wal \
  --hot 1000000 --warmup 10 --seconds 30 --samples 3
```

Add `--hash-memtable` for the hash result. Omit `--no-wal` for the WAL control; its default skips WAL fsync. Use a fresh copy for every case. `--no-wal` and `--durable` cannot be combined.

[Raw results, commands, source hashes, and binary hash](optional-memtable-no-wal-results.json). Full local artifacts remain under `/tmp/slopdb-write-bench-20261016/optional-update-random-*`. The code changes are uncommitted.
