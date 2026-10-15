# SELECT concurrency after counter and reference changes

## Changes

- Prepared plan statistics use 128 padded counter shards. Each private bound AST selects a shard at creation and keeps it through plan invalidation. PlanStats sums all shards. No global counter is changed on a plan hit.
- Cached pages use aligned atomic reference counts inside the mmap arena. Normal pins and releases do not take the arena mutex. The last release takes the mutex to free and coalesce the allocation. CLOCK reference bits also use atomics. The mapping slice header stays fixed after allocation, with a separate unmap state for safe concurrent close.
- Prepared point SELECTs decode only fields needed by the projection and WHERE predicate. Unneeded text and BLOB fields are skipped.
- Session.ExecPreparedInto reuses caller-owned column records, row maps, and BLOB buffers. It follows the normal execution, snapshot, transaction, and cancellation paths. The current ExecPrepared API still returns independent results that callers can retain.

## Method

These measurements used mmap cache memory before the memory selector and the
final block validation error fix were added.
The raw artifacts contain the exact measured binaries and source. Mmap remains
the default; heap mode can now be selected through `Options.BlockCacheMemory`.

Same database: 138,864,149 rows, 145 unchanged SSTables, 10,000,026,972 bytes on disk. Each encoded SQL row is 84 bytes (80 bytes of user data). The decoded page working set is 12.64 GiB. No data was regenerated. The benchmark uses uniform random keys and the prepared query `SELECT id,v,payload FROM bench WHERE id=?`. Each reader has its own session; readers share the prepared statement. Every returned row is checked.

Machine: 96 AMD EPYC virtual CPUs, Go 1.26.7. Every Go process used GOGC=200 and GOMAXPROCS=96. GOMEMLIMIT was not explicitly set. Caches use LRU. Operating system file pages were warm.

For the 20 GiB comparison, each process warms each reader count for 10 seconds and alternates 96 and 200 readers over three 15-second timed samples per count. The old binary already has the mmap arena; the comparison measures the new counter, reference, and result changes. Throughput runs have no profiling enabled. Resource columns show separate medians across the samples.

## 20 GiB cache

| Code and API | Readers | Million SELECT/s | Sample range | Busy cores | Mean µs | p99 µs | Allocated bytes/SELECT | Allocations/SELECT |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| Before, current API | 96 | 7.26 | 6.63–8.10 | 70.5 | 13.0 | 29.4 | 1857 | 15.00 |
| Before, current API | 200 | 6.73 | 6.01–6.80 | 61.2 | 29.4 | 67.2 | 1857 | 15.00 |
| After, current API | 96 | 8.12 | 7.87–8.31 | 69.3 | 11.5 | 29.1 | 1857 | 15.00 |
| After, current API | 200 | 7.03 | 6.89–7.31 | 61.0 | 28.2 | 35.8 | 1857 | 15.00 |
| After, buffer reuse | 96 | 14.41 | 14.34–14.44 | 91.0 | 6.4 | 12.8 | 169 | 8.00 |
| After, buffer reuse | 200 | 14.62 | 14.60–14.76 | 91.6 | 13.4 | 19.0 | 169 | 8.00 |

At 200 readers, the current API increased median throughput by 4.4%. Buffer reuse increased it by 117.2% against the old binary, and 108.0% against the new current API. Allocated bytes per SELECT fell by 90.9%.

With buffer reuse, 200 readers increased throughput by 1.5% over 96 readers. CPU use was about 91–92 of the 96 cores. The cache had no measured misses or allocation failures during these fitting-cache runs.

The old control had visible variation at 96 readers (6.63–8.10 million/s). The smaller gains with the current API should be read with that variation in mind. Buffer reuse has a much larger gain and a narrow sample range. It is a separate API that must be selected by the caller; its gains do not apply automatically to retained results from ExecPrepared.

## 200 readers with smaller caches

The 1 and 10 GiB cases each have a fresh process, a 10-second warmup, and two 15-second timed samples per API. The 20 GiB row uses the three-sample comparison above.

| Cache GiB | Current API, million SELECT/s | Buffer reuse, million SELECT/s | Gain from reuse | Busy cores with reuse | Reuse mean µs | Reuse p99 µs |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 4.82 | 6.89 | 42.8% | 91.6 | 28.8 | 554.2 |
| 10 | 6.48 | 11.37 | 75.5% | 91.6 | 17.3 | 332.0 |
| 20 | 7.03 | 14.62 | 108.0% | 91.6 | 13.4 | 19.0 |

## Use the reusable destination

```go
var result db.ExecResult
_, err := session.ExecPreparedInto(&result, stmt, id)
```

Use one destination per reader. Consume the result before the next call that uses that destination. Do not retain aliases to its rows, column slice, or BLOBs across calls. Old contents are given up on entry; contents are unspecified on error. General SELECTs and writes use their normal executor and replace the destination. Context and option variants are documented in `docs/prepared-statements.md`.

## Verification

All query checks passed. There were no arena allocation failures. The SSTable names and sizes still match the prior inventory.

- `GOGC=200 GOMAXPROCS=96 go test ./... -count=1 -timeout 10m` passed.
- `GOGC=200 GOMAXPROCS=96 go test -race ./... -count=1 -timeout 10m` passed.
- `GOGC=200 GOMAXPROCS=96 go vet ./...` passed.
- `git diff --check` passed.

The new tests cover 200 concurrent readers with exact counter totals, references while the allocator mutex is held, eviction and close, destination reuse across missing rows and query changes, retained ordinary results, catalog invalidation, cancellation, NULL and empty BLOBs, skipped fields, and predicates that read fields outside the projection.

The final commit review found that returning a failed reader to its pool cleared
its validation error before the error was returned. The fix saves the error
before release. A new test rejects malformed raw, Snappy, and Zstd blocks with
valid checksums. It failed before the fix. The storage suite, storage race suite,
and storage vet passed after the fix with GOGC=200. This error path does not run
for the valid benchmark rows; throughput was not measured again after the fix.

The first full race run found a race when close cleared the mapping slice header. The fixed header and separate unmap state passed five repeated close race tests and the final full race suite. The failed initial log is kept with the raw artifacts.

## Profiles

Separate 10-second profiles at 200 readers and a 20 GiB cache are excluded from throughput tables. They include CPU, mutex, block, and allocation samples. Allocation profiles also include startup and warmup.

- The arena pin and nonfinal release mutex waits are gone. Remaining application lock waits mainly come from snapshot registration and release, with a smaller contribution from read-view registration.
- `runtime.mallocgc`, including allocation refill and sweep work, accounts for 29.33% of CPU samples with the current API and 3.36% with buffer reuse. These inclusive costs overlap with their callers.
- The buffer reuse allocation profile is dominated by the query cancellation context. Row maps and copied result column records are no longer allocated on each warm point SELECT.
- Table index search uses 7.17% of CPU samples in the reuse profile; block entry header decoding uses 6.94% flat CPU. These and snapshot bookkeeping are remaining costs after the requested changes.

## Artifacts

Raw logs, JSON samples, profiles, scripts, binary hashes, source archive, and fixture inventory: `/tmp/slopdb-select-bench-20261014/arena/readers200-improved`.

Before binary SHA-256: `d7c89eec4ec3db7fe0b801f3625532903f8bfad8bfd5c4799399de771037c38b`.
After binary SHA-256: `968b5a5b7f9e45df4ed6dcff9e64b737d86c8b2b1d8403a62c1560de02c8ad8d`.

The tracked base for these measurements is main `6143e41`. `changed-source.zip` includes the new files as well as tracked modifications. The benchmark uses the same Go engine for current and reuse APIs; `-reuse-result` selects the new method in the harness.
