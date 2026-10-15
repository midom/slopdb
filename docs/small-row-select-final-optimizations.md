# Further small-row SELECT improvements

This follows the [scaling changes](small-row-select-scaling.md) and [GOGC=200-only measurements](small-row-select-gogc-200.md). The baseline is their frozen optimized binary, rather than the original Git HEAD. All comparisons retain the existing prepared SQL API and independently owned result maps, column slices and BLOBs.

The fixture remains the same 39,062,500 rows / 10,000,000,000 logical bytes on `test`. All 1,226 L6 SSTables match the original seed by SHA256. No loading, VACUUM, writes or persistent-format migration occur during these measurements.

## Results

Median checked SELECTs/s: 100 independent sessions and a fresh process for each cache size. The final GOGC=200 matrix uses three consecutive 30-second samples. Final default-GC CLOCK/LRU rechecks use three 15-second samples; complete three-30-second matrices from the candidate before the final context-allocation fix are retained separately in the saved summary. Differences in sample duration limit the default-GC comparison. Decimal cache budgets, 4 KiB physical pages, uniformly random prepared full-row primary-key reads, unchanged deterministic seed and page-spaced warmup.

| Cache | Default-GC CLOCK before | After | Gain | Default-GC LRU after | GOGC=200 CLOCK before | After | Gain |
|---|---:|---:|---:|---:|---:|---:|---:|
| 100 MB | 1,290,306 | 1,952,413 | +51.3% | 1,957,211 | 1,514,581 | 2,166,803 | +43.1% |
| 1 GB | 1,494,712 | 1,856,402 | +24.2% | 1,848,762 | 1,721,687 | 2,354,411 | +36.8% |
| 10 GB | 2,527,890 | 3,421,919 | +35.4% | 3,208,616 | 3,472,815 | 4,342,559 | +25.0% |
| 20 GB | 2,262,230 | 3,115,958 | +37.7% | 3,107,382 | 2,918,204 | 3,753,984 | +28.6% |

`GOGC=200` is set alone; `GOMEMLIMIT` and `GOMAXPROCS` are unset. Default-runtime measurements unset all three. The runtime uses 60 execution slots on the 60-core ARM Neoverse-N1 host with approximately 58 GiB RAM. These are sequential descriptive measurements on one fixture, rather than independent replications establishing statistical significance.

CLOCK CPU, tail latency and resident memory:

| Cache | Default CPU cores | GOGC=200 CPU cores | Default p99 µs | GOGC=200 p99 µs | Default peak RSS GB | GOGC=200 peak RSS GB |
|---|---:|---:|---:|---:|---:|---:|
| 100 MB | 45.1 | 47.4 | 819.2 | 786.4 | 0.88 | 1.36 |
| 1 GB | 45.8 | 50.5 | 983.0 | 753.7 | 3.71 | 5.25 |
| 10 GB | 46.6 | 53.9 | 188.4 | 81.9 | 29.29 | 44.31 |
| 20 GB | 37.1 | 42.2 | 86.0 | 77.8 | 30.48 | 46.32 |

CPU measurements include result validation and Go GC. Percentiles are histogram upper bounds. Peak RSS includes startup, warmup and close. The Linux filesystem cache remains enabled and is never reset; this measures engine-cache, decoding and CPU costs rather than cold-device random I/O.

The default-GC, fully fitting 20 GB CLOCK scaling sweep uses two 15-second samples per fresh process, matching the previous sweep:

| Readers | Before SELECTs/s | After SELECTs/s | Gain | Before CPU cores | After CPU cores |
|---|---:|---:|---:|---:|---:|
| 1 | 103,767 | 124,286 | +19.8% | 1.1 | 1.1 |
| 8 | 755,058 | 909,484 | +20.5% | 8.6 | 8.3 |
| 16 | 1,480,202 | 1,754,424 | +18.5% | 17.1 | 16.9 |
| 32 | 2,150,348 | 2,788,611 | +29.7% | 27.6 | 29.0 |
| 60 | 2,256,217 | 2,976,347 | +31.9% | 33.8 | 34.8 |
| 100 | 2,299,330 | 3,107,880 | +35.2% | 34.3 | 36.9 |

## Changes

1. **Predecode immutable SSTable indexes.** Build pointer-free key and block-handle arrays once per reader, avoiding repeated prefix reconstruction and handle-varint decoding on every point read. Compare the common file key prefix once, then search precomputed leading bytes. Equal fingerprints always fall back to full key comparison, preserving ordering for embedded NULs, short keys and duplicate separators. Older versions spanning multiple pages still traverse subsequent handles. Index checksums and malformed-handle/order checks remain active. Iterators retain the existing representation and persistent format.

2. **Cheaper block header decoding.** Decode the common one-byte prefix lengths and one/two-byte value length together, falling back to the full checked varint decoder for larger values. Skip a redundant restart-key search when the block has only one restart. Truncation, length overflow and shared-prefix checks remain active.

3. **Bounded private lookup buffers.** Prepared plans reuse their primary-key encoding and raw row-value buffers. Storage copies into caller-supplied private scratch through `GetAtForReaderInto`; SQL still constructs independent result maps and copies returned BLOBs/TEXT. Transactions retain their overlay path. Scratch over 64 KiB is discarded when the binding is released. Catalog changes invalidate plans and their scratch normally.

4. **Avoid queued cache admissions.** A decoded page can already satisfy its read. If its shard is busy, skip cache admission instead of queuing a writer that blocks subsequent readers. Approximate LRU refreshes also skip contention. CLOCK reference bits retain the reader-lock path, so eviction sweeps stay bounded. `BlockCacheAdmissionSkips` exposes the bypass count; byte budgets remain exact and partitioned across 128 shards.

5. **Compact cache records.** Replace a separate generic list node and cache record with typed intrusive links; avoid storing a duplicate page-data slice. This removes one heap object per resident page and reduces metadata scanning. Reusing an eviction record never modifies a retained immutable BlockReader. Both LRU and CLOCK use the same structure.

6. **Distribute catalog/snapshot reader registration.** Replace the remaining global committed-catalog RWMutex reader counter with 128 padded reader counters selected by stable session snapshot IDs. Writers publish intent, exclude every distributed and legacy reader, and retain preference over new readers. This protects the same committed catalog, row-ID reservations and snapshot boundary.

7. **Avoid allocation in common cancellation checks.** Uncontended session gates and SQL interruption checks use context errors without calling `Done()`. Contended calls retain the cancellable select path. Immutable default memory-budget contexts are shared for Background/TODO roots, while each call retains independent cancellation. Explicit zero overrides, nonzero budgets, caller values/deadlines, shutdown and transaction-abort registration preserve their behavior.

8. **Remove a discarded result allocation.** Allocate an empty ExecResult only when the statement list produces no result, rather than allocate one before every SELECT and immediately replace it. Empty input still returns a non-nil result.

These changes are measured together; the matrix does not isolate individual speedups.

## Memory and remaining costs

The predecoded index adds approximately 153.7 MB of resident array capacity for this fixture, reported as `PointIndexBytes` for the current file view. This is additional SSTable-reader metadata outside the decoded-page byte budget; older pinned views can temporarily retain more. The RSS table includes that cost. The compact cache records save separate list-node objects, but additional index metadata remains a real memory tradeoff, particularly with a small page cache.

At the fully fitting cache, timed allocation is approximately 2,032 bytes/query and 15.0 allocations/query with GOGC=200. Owned result maps and the public ColumnDef slices still impose a material allocation floor. GC work, memory access and synchronization remain in the retained profiles; improved throughput does not establish linear scaling to all 60 cores.

At the fully fitting cache, measured Go allocation volume is approximately 7.61 GB/s before and 7.63 GB/s after, despite the higher query rate. Together with the retained allocation/runtime profiles, this is consistent with allocation throughput remaining a limit; it is not a measurement of physical memory bandwidth. The final context changes remove a further 160 bytes and two allocations per query relative to the preceding candidate.

The saved [CPU](benchmarks/select-fix-all/2026-10-15/profile-runtime-final-20000000000-cpu.txt), [allocation](benchmarks/select-fix-all/2026-10-15/profile-runtime-final-20000000000-alloc.txt) and [blocking](benchmarks/select-fix-all/2026-10-15/profile-runtime-final-20000000000-block.txt) reports retain the remaining costs. Heap allocation profiles include startup and warmup; timed bytes/query comes from sample counters. Diagnostic throughput is excluded from the result tables.

Skipped admission can affect hit rates for workloads with different locality. The complete samples retain hit rates, bypass fractions, cache occupancy, GC counts, latency, allocations and CPU cost so that throughput changes can be assessed alongside these effects. The engine does not change application-global GC settings.

## Experiments and validation

A lock-free immutable-bucket cache prototype was rejected. In matched short GOGC=200 pilots it added admission allocations and metadata: throughput fell from about 1.93M to 1.86M SELECTs/s at 100 MB, from 3.56M to 3.35M at 10 GB and from 3.42M to 3.30M at 20 GB. Adding admission bypass to that prototype was worse at 100 MB. The retained implementation uses Go maps, shared read locks, compact reusable records and best-effort writer admission. Pilot timings are diagnostic and excluded from the result tables.

Catalog write controls compare the compact-cache pilot immediately before the catalog gate replacement against the candidate immediately after the gate replacement and before the final context-allocation fix, using existing BenchmarkCatalogWorkloads cases with one/128 tables, 16/60 execution slots and three 300 ms samples. Every benchmark validates exact committed updates and integrity. Point-update median times differ by about 0.5–3.3%; explicit transaction-update throughput increases by about 3.4–43.1%. These short controls are not a statistical proof of write performance across workloads.

Permanent tests cover varint boundaries and deterministic malformed headers, long-key/value forward/reverse seeks, NUL-containing/truncated/equal index fingerprints, duplicate index separators with snapshots across pages, malformed handles and order, caller-buffer ownership and missing/closed reads, non-blocking admission, cancellation during contended session waits, and colliding/legacy catalog readers with writer preference. Existing tests retain prepared result ownership, transaction overlays, schema invalidation, concurrent writes/compaction, shutdown and crash recovery. Database artifacts use `t.TempDir()`.

Required validation passed with local Go 1.22.2: root build, complete ordinary tests, complete race tests, go vet and git diff --check. Benchmark binaries use Go 1.26.0, matching the previous benchmark compiler and CGO build setting. The saved summary records production-source fingerprints and binary/report hashes. A separate 30-second malformed-header fuzz run passed 1,315,146 executions.

## Artifacts and reproduction

Complete compact samples, comparisons, commands and integrity evidence: [summary.json](benchmarks/select-fix-all/2026-10-15/summary.json). Raw reports, profiles, resources and the final source archive are on `test` under `~/slopdb-bench/select-fix-all-20261015/`, including `report.md`. Existing home-directory reports and source snapshots remain available.

```bash
env -u GOMEMLIMIT -u GOMAXPROCS GOGC=200 python3 \
  ~/slopdb-bench/select-fix-all-20261015/select_cache_matrix.py \
  --binary ~/slopdb-bench/select-fix-all-20261015/select-runtime-final \
  --dir ~/slopdb-small-row-10gb --policy clock \
  --output ~/slopdb-bench/select-fix-all-20261015/reproduce-clock-gogc200
```
