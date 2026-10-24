# Raft tail latency investigation

Follow-up to [the journal-voter benchmark](logs-only.md), on the same `test` host
and production base `11b39400f663`. One full leader, two logs-only voters,
128 clients, TCP loopback, ext4 databases, Go 1.22.2, `GOMAXPROCS=60`.
The workload remains one explicit transaction with one prepared increment of a
private row. Production source paths were examined through isolated build
overlays; the repository changes add opt-in benchmark diagnostics.

## Findings

The large tail is predominantly time **blocked on protocol completion and
storage/catalog locks**. It is not explained by an unspecified scheduling delay.
In the slowest approximately 1% of traced transactions, synchronization waits
account for 98.2% of sync elapsed time and 93.8% of async elapsed time.
Ready-to-run scheduler time accounts for 1.3% and 4.6%, respectively. Trace
instrumentation changes timings; these fractions describe the diagnostic traces.

1. **Async still performs synchronous I/O at memtable rotation and flush.**
   `storage/lsm.go:822` calls `syncWAL()` before rotating an old WAL, while holding
   the writer and LSM mutexes. The background flush releases the LSM mutex to
   collect immutable rows. `writeTableLocked()` also releases it around SSTable
   build/sync (`storage/lsm.go:1059`), then reacquires it. Manifest publication
   remains under the mutex (`storage/lsm.go:1266`), so its file/directory syncs
   block writers. Writers can also wait for the single immutable table to flush.
   `diskStore.StoreLogs()` holds its own mutex while entering storage, propagating those stalls into Raft replication and the leader loop.

   With periodic durability flushing disabled for the timed window and GC off,
   `strace` still recorded **116 timed fsync/fdatasync calls** with 4 MiB buffers.
   Journal consensus-WAL syncs reached **18.0 ms and 21.9 ms** on nodes 2 and 3;
   SST syncs reached 17.2 ms. About **90% of the slowest 1%** of transactions in
   that instrumented run overlapped a sync call, versus **4.1% of all transactions**.
   These syscall durations include tracing effects.

   A matched, untraced control changed only application and consensus buffers
   from **4 MiB to 128 MiB**, keeping cleanup bypass, periodic-flush bypass and
   GC-off settings identical. Its p99 fell from **9.85 ms to 2.50 ms**, while
   median latency stayed near 1.2 ms. A separate large-buffer syscall trace
   recorded **zero** fsync/fdatasync calls inside timing. The larger buffers
   postpone rotation in this short workload; they are not a long-run fix.

2. **Sync UPDATE admission performs an additional fresh quorum round.**
   `Node.checkSQL()` calls `VerifyLeader()` before UPDATE (`replication/node.go:561`)
   even though COMMIT subsequently performs actual Raft consensus. Verification
   requests queue through the Raft leader loop alongside log persistence.
   The sync trace attributes about 67.6% of tail synchronization wait to this
   admission path and about 30.8% to the commit proposal path. Native-application
   catch-up in `waitApplied()` contributes little in these profiles.

   An isolated probe bypassing only the extra statement-admission verification
   reduced diagnostic sync p99 from **51.54 ms to 19.57 ms**; actual commit quorum
   replication and fsync remained enabled. This probe changes read admission and
   must not be deployed as a general fix. A safe optimization must preserve
   freshness, for example by batching appropriate fresh verification requests.

3. **Every COMMIT performs index cleanup under the global catalog write lock.**
   `db/database.go:382` calls `CleanupIndexGarbage()` after COMMIT/RELEASE, and
   `sql/index_cleanup.go:78` takes the exclusive catalog gate before checking
   whether cleanup metadata exists. The fixture has no secondary indexes or
   pending cleanup. This unnecessarily serializes catalog snapshot acquisition.
   The async trace shows substantial waits in `StartStatement()` and the cleanup
   gate. Skipping this no-op only for the controlled fixture raised throughput
   from **32.5k to 76.4k tx/s** and reduced median latency from **3.56 ms to 1.16 ms**.
   Its p99 remained **10.75 ms**, so it explains the common-path bottleneck but
   does not remove the rotation/flush tail by itself.

Disabling GC alone changed p99 only from 11.35 ms to 11.16 ms. Moving periodic
certification outside the measured window changed it to 10.76 ms. Neither
removed the tail. The storage rotation/flush critical sections remain active
in both controls.

## Stage measurements

These runs record every transaction without execution tracing or contention
profiling. Sync uses 25,000 transactions; async uses 250,000.
The tail columns are averages across the slowest approximately 1% of each run,
not sums of independently calculated stage percentiles.

| Mode | Transaction p99 | Tail mean total | BEGIN share | UPDATE share | COMMIT share |
| --- | ---: | ---: | ---: | ---: | ---: |
| Sync | 51.54 ms | 63.77 ms | 0.4% | 66.5% | 33.2% |
| Async | 11.35 ms | 14.92 ms | 7.1% | 38.2% | 54.7% |

## Diagnostic controls

Single probes on the same fixture, with stage timing enabled. They isolate
mechanisms rather than establish statistically significant performance scores.
All cases passed exact row totals, integrity and journal-reconstruction checks.
The preceding three-repeat benchmark remains the reference default-settings result.

| Mode | Probe | tx/s | p50, ms | p99, ms |
| --- | --- | ---: | ---: | ---: |
| Sync | Default diagnostic baseline | 5,031 | 23.427 | 51.544 |
| Sync | Skip empty cleanup | 4,519 | 24.536 | 66.598 |
| Sync | Bypass extra admission quorum | 9,203 | 13.567 | 19.575 |
| Async | Default diagnostic baseline | 32,472 | 3.556 | 11.347 |
| Async | Skip empty cleanup | 76,436 | 1.157 | 10.750 |
| Async | GC off | 33,273 | 3.557 | 11.163 |
| Async | Skip cleanup + GC off | 81,562 | 1.159 | 10.435 |
| Async | Bypass extra admission quorum | 37,855 | 3.086 | 9.808 |
| Async | No periodic flush in timed window | 33,849 | 3.546 | 10.756 |
| Async | Skip cleanup + no periodic flush | 80,383 | 1.172 | 10.328 |
| Async | Skip cleanup + no periodic flush + GC off | 84,468 | 1.149 | 9.846 |
| Async | Same triple control + 128 MiB buffers | 97,440 | 1.221 | 2.498 |

The no-periodic-flush overlay uses a one-hour interval and two-hour lag bound;
startup and the final explicit stable barrier still run. This changes the
configured durability window and is only a diagnostic control. No general
read-consistency or durability relaxation is included in production code.

## Tooling and validation

`replication/latency_benchmark_test.go` adds three opt-in flags, intended for
one benchmark case and one sample:

- `-raft-bench-latencies /outside/repo/stages.json`: per-transaction monotonic
  BEGIN/UPDATE/COMMIT timings, worker IDs and offsets, plus epoch window bounds.
- `-raft-bench-trace /outside/repo/runtime.trace`: timed execution trace with
  transaction and statement regions, associated with worker tasks.
- `-raft-bench-contention /outside/repo/prefix`: timed block and mutex profiles.

The existing `-raft-bench-timed-cpu` flag captures CPU profiles. Timers stop
before profile serialization, SQL integrity checks and recovery. Collection
bookkeeping was tightened after the investigation so CPU profiling also stops
before diagnostic serialization. The initial combined CPU profiles include
serialization overhead and were not used for the conclusions here; stage data,
trace regions, synchronization profiles and syscall evidence support them.
The results distinguish measured source hashes from the final tooling hashes;
measured sources are archived outside the repository. Diagnostic
build overlays remain outside the repository in
`/tmp/slopdb-bench/raft-latency-20261024/overlay/` locally and
`/tmp/slopdb-bench/slopdb-raft-latency-20261024/overlay/` on `test`.
Their hashes and invocation arguments are recorded in the results JSON.

All replication tests and a diagnostics-enabled benchmark smoke case passed
with `-race` on `test`; `go vet ./replication` and whitespace checks passed.
The smoke output was additionally checked for record count, stage-sum equality,
all worker IDs, and readable trace/profile files. Runtime controls and syscall
tracing are separate from the ordinary benchmark score. Raw traces, profiles,
per-transaction data and command logs remain outside the repository under
`/tmp/slopdb-bench/slopdb-raft-latency-20261024/` on `test`, with summaries and
selected raw data copied to `/tmp/slopdb-bench/raft-latency-20261024/` locally.

[Summaries, source/overlay hashes, commands and validation evidence](latency-investigation-results.json).

## Production targets

Move manifest sync work outside the LSM publication mutex using serialized,
durable version publication; preserve the existing unlocked SSTable build/sync.
Decouple asynchronous WAL-generation synchronization from foreground admission while ensuring durability certificates
cover every relevant generation. Skip index-cleanup locking/scans when no work
is pending. Reduce fresh verification overhead without serving stale leader
reads. Larger buffers and bypassed checks demonstrate the costs; they do not
replace these correctness requirements.

The [conditional COMMIT cleanup follow-up](commit-index-cleanup.md) implements
the cleanup fast path with transactional drops, rollback and error retries
preserved, and measures it against the original production code.
The [background WAL synchronization follow-up](wal-background-sync.md) moves
rotation fsync outside storage locks while making durable barriers cover every
earlier generation, with deterministic blocking tests and matched measurements.
