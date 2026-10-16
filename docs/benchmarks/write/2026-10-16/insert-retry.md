# INSERT retry with optional hash memtables and WAL disabled

## Median committed inserts/s

| INSERT type | Skip list, 96 writers | Hash, 96 writers | Skip list, 200 writers | Hash, 200 writers |
|---|---:|---:|---:|---:|
| Random keys | 242,978 | 433,893 | 278,099 | 420,679 |
| Persistent automatic IDs | 32,159 | 33,921 | 22,570 | 22,201 |
| Automatic IDs, restart reuse | 358,636 | 406,108 | 351,289 | 416,711 |

Each number is the median of three consecutive 30-second intervals after a 10-second warm-up. Each transaction inserts one 80-byte row. WAL is disabled entirely. The default memtable is the skip list; hash mode requires `--hash-memtable`. All cases start with a new, empty database and retain the same growing database throughout their intervals.

## Growth during the run

Random-key inserts slow sharply as the database grows. Hash at 200 writers starts at 624,901/s and ends at 253,077/s. At 96 writers, it starts at 567,508/s and ends at 243,024/s. Hash mode inserts more total rows during the same elapsed time, so it reaches a larger database than the skip list. These medians do not establish a sustained rate for a fixed database size.

At 200 random-key hash writers, CPU use reaches 67.6 busy cores in the middle interval. More writers raise the initial random INSERT rate but do not prevent its decline. The samples include flush, compaction, SQL lookup, and allocation work; no new CPU profile is used to assign the relative cost of each.

Restart-reuse automatic IDs also slow in the last interval. At 200 hash writers, the three rates are 416,711 / 417,170 / 312,997. Persistent automatic IDs remain much slower and average about one transaction per publication group. Their metadata publication remains serialized in this WAL-disabled mode. The hash option has little effect on that case.

## Individual samples and database growth

| Writers | INSERT type | Memtable | Three intervals, inserts/s | Verified rows including warm-up | Database file bytes, GiB |
|---:|---|---|---|---:|---:|
| 96 | Random keys | skip | 261,189 / 242,978 / 227,719 | 24,789,478 | 2.04 |
| 96 | Random keys | hash | 567,508 / 433,893 / 243,024 | 43,265,200 | 3.81 |
| 96 | Persistent automatic IDs | skip | 31,930 / 32,272 / 32,159 | 3,206,217 | 0.22 |
| 96 | Persistent automatic IDs | hash | 34,197 / 33,921 / 33,473 | 3,387,991 | 0.24 |
| 96 | Automatic IDs, restart reuse | skip | 361,524 / 358,636 / 283,312 | 33,781,214 | 2.42 |
| 96 | Automatic IDs, restart reuse | hash | 418,020 / 406,108 / 310,432 | 38,244,308 | 2.75 |
| 200 | Random keys | skip | 307,980 / 278,099 / 257,964 | 28,506,528 | 2.38 |
| 200 | Random keys | hash | 624,901 / 420,679 / 253,077 | 45,964,443 | 3.88 |
| 200 | Persistent automatic IDs | skip | 22,518 / 22,570 / 22,830 | 2,261,262 | 0.16 |
| 200 | Persistent automatic IDs | hash | 22,005 / 22,299 / 22,201 | 2,217,782 | 0.15 |
| 200 | Automatic IDs, restart reuse | skip | 351,289 / 361,830 / 291,679 | 33,866,255 | 2.43 |
| 200 | Automatic IDs, restart reuse | hash | 416,711 / 417,170 / 312,997 | 38,552,243 | 2.77 |

Database file sizes are sums of file lengths after close/reopen verification. They include SSTables and metadata. They are not peak allocated storage or total compaction output. Verification can wait for background work to finish after timing.

## CPU, allocation, and publication groups

| Writers | INSERT type | Memtable | Mean busy cores | Inserts/publication group | Allocated bytes/insert | GC cycles | Peak RSS, GiB |
|---:|---|---|---:|---:|---:|---:|---:|
| 96 | Random keys | skip | 16.27 | 44.5 | 6,245 | 600 | 4.06 |
| 96 | Random keys | hash | 48.10 | 18.2 | 6,321 | 622 | 6.62 |
| 96 | Persistent automatic IDs | skip | 5.37 | 1.0 | 15,045 | 765 | 0.45 |
| 96 | Persistent automatic IDs | hash | 5.29 | 1.0 | 15,436 | 808 | 0.46 |
| 96 | Automatic IDs, restart reuse | skip | 13.55 | 35.6 | 9,193 | 1166 | 3.65 |
| 96 | Automatic IDs, restart reuse | hash | 14.07 | 15.7 | 9,236 | 1135 | 4.13 |
| 200 | Random keys | skip | 21.46 | 94.3 | 6,019 | 602 | 4.67 |
| 200 | Random keys | hash | 53.97 | 62.2 | 6,789 | 611 | 7.24 |
| 200 | Persistent automatic IDs | skip | 5.78 | 1.0 | 14,151 | 548 | 0.46 |
| 200 | Persistent automatic IDs | hash | 5.77 | 1.0 | 14,302 | 563 | 0.45 |
| 200 | Automatic IDs, restart reuse | skip | 13.83 | 45.7 | 9,276 | 1131 | 3.69 |
| 200 | Automatic IDs, restart reuse | hash | 14.55 | 15.7 | 9,205 | 1108 | 4.18 |

CPU, allocation, and GC figures cover timing. Peak RSS covers setup, warm-up, timing, close, reopen, and verification. Every configured writer made progress in each interval. No case reported an external lock retry. Internal waits are included in transaction time. Complete sampled latency, writer counts, flushes, and compaction output are in the JSON file.

## Settings

- Engine commit: `762221ca4ff25b4ad6d28e7eb942dd0a153f4420`. Storage, SQL execution, and lock code are unchanged from that commit.
- Machine: 96 virtual AMD EPYC Genoa CPUs; Go 1.26.7.
- `GOMAXPROCS=96`, `GOGC=200`. No run uses GOGC above 200.
- 20 GiB mmap cache limit, 8 MiB write buffer, eight background compaction jobs. The cache limit does not imply a full cache.
- WAL encoding, writes, and fsync are disabled with `--no-wal`. SSTable and manifest sync remains enabled.
- Row: 8-byte integer ID, 8-byte integer value, 64-byte BLOB; 80 logical bytes, 84 encoded bytes. Storage keys and metadata are extra.
- One session per writer, shared prepared statement, private bindings and reused result. No SQL transaction batches; the engine can group independent commits internally.
- Random keys use a deterministic permutation with no duplicate keys. Automatic inserts omit the ID and use `INTEGER PRIMARY KEY AUTOINCREMENT`.
- Persistent counters are the default. `--volatile-ids` keeps reservations in memory; after reopen, allocation starts above the highest surviving ID. Deleted high IDs can be reused after restart.
- Each case uses an empty database. Prior benchmark databases and the saved 10 GB SELECT fixture remain unchanged.
- Cases run serially: 96 before 200 writers; random, persistent automatic IDs, then restart-reuse IDs; skip list before hash.
- Three consecutive 30-second intervals after 10 seconds of warm-up. Time includes completion of transactions already in progress at the stop signal.
- No profiler, test, or build runs during primary samples.

## Reporting fix and verification

A short check found a benchmark reporting fault when an interval had no latency samples: division by zero produced NaN, which JSON cannot encode. The tool now records the latency sample count and writes null mean/p99 values for an empty sample set. Percentile ranks round up. This changes reporting only; engine code and the timed transaction loop are unchanged. All primary intervals have latency samples.

All six short workload/memtable checks passed after the fix. A further two-interval check reached zero sampled latency and verified valid JSON null fields. Short checks and the failed reporting-fault run are excluded from the primary tables.

All twelve primary cases passed checks after clean close and reopen. The verifier reads every inserted row, checks the deterministic 64-byte payload, positive ID, unique transaction ticket, random-key mapping where applicable, and total row count. It verified 298,042,921 committed inserts, including warm-up. No WAL file was left in any primary database. The original 145 SSTable names and sizes and fixture metadata remain unchanged.

WAL-disabled results prove persistence after a clean close. A crash can lose unflushed data. These runs do not test crash or power-loss persistence.

## Relation to the previous INSERT results

[The previous INSERT report](concurrent-writers.md) used WAL enabled with fsync disabled, a 4 MiB buffer, the skip list, and an earlier engine. This retry uses WAL disabled and an 8 MiB buffer. It also tests optional hash memtables. Direct rate ratios between those reports do not isolate one change.

## Reproduce

```sh
GOGC=200 GOMAXPROCS=16 go build -o /tmp/slopdb-write-bench ./cmd/write-bench
GOGC=200 /tmp/slopdb-write-bench --db /tmp/fresh-random-insert-db \
  --workload insert-random --writers 200 --procs 96 --mem-mib 8 \
  --no-wal --hash-memtable --warmup 10 --seconds 30 --samples 3
```

Omit `--hash-memtable` for the default skip list. Use `--workload insert-auto` for persistent automatic IDs; add `--volatile-ids` for restart reuse. Use a new database directory for each case.

[Raw results, commands, source hashes, binary hash, and excluded short checks](insert-retry-results.json). Full artifacts and database files remain under `/tmp/slopdb-write-bench-20261016/retry-insert-*`. The reporting fix and new reports are uncommitted.
