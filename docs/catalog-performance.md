# Catalog performance and concurrent renames

Measurements on 2026-10-12 compare the committed name-prefixed catalog at
`30e6b39`, the object-ID catalog before tuning, and the tuned implementation.
Production-source SHA256 fingerprints and complete samples are in
[the saved reports](benchmarks/catalog/2026-10-12/sources.json).

The catalog-ID change alone did not show a material regression. Three-sample
microbenchmark medians for prepared point reads, secondary reads, indexed ranges,
scans, joins and updates were generally within a few percent of the old catalog.
Both 1-table and 128-table catalogs were tested, with one and four workers.
Short microbenchmarks vary between runs; small differences are not evidence of
statistical significance.

## Pure point SELECT

The query is `SELECT v FROM point_000 WHERE id=?`, repeatedly binding the integer
primary key 1. Every returned row must equal the seeded value 17. Timed execution
contains only SELECTs: no writes, loading, transaction commits, SQL parsing or
explicit BEGIN/COMMIT. Each worker uses an independent session and shares a
prepared statement safely. Sessions and an initial execution are created before
measurement. Fresh databases, preparation and close are outside the timing.

This deliberately uses a warm memtable and one repeated key, making storage work
small and exposing the relative cost of catalog, snapshot and lifecycle
synchronization. These measurements do not predict cold disk I/O performance.
Go 1.22.2, Linux ARM64, four available CPUs, `GOMAXPROCS=4`, default 4 KiB pages;
three separate one-second samples per configuration, medians below.

| Catalog tables | Workers | Old catalog SELECT/s | ID catalog before tuning | After tuning | Gain over ID catalog |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1 | 1 | 120,656 | 119,460 | 171,292 | 1.43x |
| 1 | 10 | 255,493 | 252,270 | 319,591 | 1.27x |
| 1 | 32 | 241,955 | 239,349 | 310,078 | 1.30x |
| 128 | 1 | 110,509 | 116,469 | 167,616 | 1.44x |
| 128 | 10 | 247,647 | 279,642 | 351,370 | 1.26x |
| 128 | 32 | 249,875 | 274,650 | 342,700 | 1.25x |

These are aggregate completed SELECTs per wall-clock second, not per-request
latency. With ten workers, allocation falls from 43 to 24 allocations/query and
from about 4,650 to 3,593 bytes/query. Increasing the catalog size did not produce
linear per-query overhead. Differences between the two catalog sizes also
include scheduling and allocator variation.

Reproduce:

```sh
go test ./db -run '^$' -bench '^BenchmarkSelectPoint$' \
  -benchmem -benchtime=1s -count=3 -cpu=4

go test ./db -run '^$' -bench '^BenchmarkCatalogWorkloads$' \
  -benchmem -benchtime=200ms -count=3 -cpu=1,4
```

The broader write benchmarks also assert that the final sum equals the exact
number of successful updates, then run integrity checking.

## Profile findings and changes

CPU, mutex, block and allocation profiles isolated repeated SELECT planning and
shared lifecycle synchronization. Prepared primary-key reads were constructing
scan/OR candidates and discarding them before using their existing point reader.
That work is now skipped for complete prepared PK predicates. The ordinary
reader still validates types, reads the statement snapshot and transaction
overlay, and evaluates residual predicates.

Cancellation registration now uses a session-local mutex rather than the database
lifecycle mutex. Registration publishes the cancel function before checking
close flags, so concurrent Close either sees the callback or the operation
cancels itself. The shutdown gate uses an atomic reader fast path; queued writers
still block new readers, and cancelled waiters wake successors. The original
mutex-protected path remains for waiting. Statement catalog snapshots and
logical DDL/writer locks remain intact.

The final SELECT profile no longer shows material session-registration or
shutdown-gate mutex contention. Allocation and GC for caller-owned row maps and
result metadata remain substantial CPU costs. Profile timings are diagnostic;
throughput figures above come from separate unprofiled runs. Go benchmark
profiles also include calibration and fixture setup, so residual storage-write
profile samples are not writes in the timed SELECT workload.

TPC-C profiling additionally found unnecessary row-ID reservations on inserts
whose keys are already explicit primary keys. Those inserts no longer advance a
shared hidden-row-ID counter or repeatedly acquire the catalog publication mutex.
Hidden-row-ID tables and AUTOINCREMENT retain allocation/reservation and durability
checks. INSERT SELECT follows the same rule. `LastID` remains the integer primary
key or generated hidden ID; composite/noninteger clustered keys return zero
rather than allocating an unrelated unused ID.

## Transactional measurements

All three one-minute runs use ten workers, the default seeded TPC-C-inspired
fixture, prepared statements, 4 KiB pages, WAL enabled and WAL fsync disabled.
Loading, preparation, validation and close/reopen are outside the timing.

| Version | Committed tx/s | Mean ms | p50 ms | p99 ms | Failed transactions |
| --- | ---: | ---: | ---: | ---: | ---: |
| Old name catalog | 2,071 | 4.80 | 3.23 | 24.51 | 0 |
| ID catalog before tuning | 2,057 | 4.83 | 3.23 | 25.10 | 0 |
| Final tuned code | 2,371 | 4.20 | 1.59 | 34.55 | 0 |

Each run passed all 15 consistency assertions both immediately and after reopen.
Expected invalid-item rollbacks and shutdown cancellations are reported separately.
The catalog change alone is -0.6% in this pair; tuning increases committed
throughput 15.2% and reduces the median transaction time about 51%.

The aggregate p99 increased. These are single fresh, growing one-minute runs;
higher throughput creates more orders and index entries during the same period.
Delivery and payment tails remain bottlenecks, so this result is not a claim that
every transaction class or percentile improved. An intermediate tuning run
recorded 2,450 tx/s with p99 31.12 ms; it is retained alongside the final run.
Repeated microbenchmark results and raw transaction reports remain available;
small differences between individual runs should not be treated as definitive.

The final 15-second diagnostic profile reduces aggregate sampled mutex delay
from about 51.4 seconds to 10.0 seconds across workers. This is summed delay,
not wall-clock time, and profiling adds overhead. The unnecessary row-ID
allocator/record-write contention disappears. Session creation now uses a shared
catalog lock and atomic snapshot-reader IDs. Deadlock detection locks only shards
represented in its frozen wait graph, preserving a coherent cycle check without
blocking unrelated resources. Detector-attributed delay falls from 4.25 seconds
in the intermediate profile to 0.44 seconds in the final profile. Remaining
contention is primarily in actual row/unique lock ownership, alongside allocation,
GC and storage iteration work.

Reproduce a throughput run and a separate diagnostic run:

```sh
go run ./cmd/tpcc -workers 10 -duration 1m -json /tmp/slopdb-tpcc.json
go run ./cmd/tpcc -workers 10 -duration 15s \
  -profile-dir /tmp/slopdb-bench/tpcc-profiles \
  -json /tmp/slopdb-bench/tpcc-profile.json
go tool pprof -top /tmp/slopdb-bench/tpcc-profiles/mutex.pprof
```

## Consistency and rename verification

Permanent tests cover:

- An active writer finishes and commits before a queued rename publishes;
  acknowledged rows and indexes survive reopen.
- Writers queued behind a rename refresh the catalog on commit or rollback.
  Warmed prepared statements cannot silently write through an obsolete storage
  mapping, including after the old logical name is reused.
- Ten writers update through a stable view while its base table repeatedly
  renames. Both disjoint rows and one contended row retain all 500 updates.
- A point-read cursor pinned before rename continues reading its original
  snapshot; its next statement sees the new catalog and reused logical name.
- A child process crashes without Close after 500 acknowledged inserts alongside
  repeated renames, with another rename uncommitted. Reopen retains every row,
  its exact value, the committed catalog and secondary index.
- Execution-gate reader/writer exclusion, writer preference and cancellation,
  plus the full transaction, foreign-key, storage and recovery suites under the
  race detector.

Crash tests use the default synced WAL. Performance runs keep WAL enabled and
WAL fsync disabled; these are intentionally different durability settings.
