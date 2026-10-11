# Page-size measurement protocol

Measure the requested 16 KiB, 64 KiB and 1 MiB settings with fresh databases.
Keep the source data, schema, indexes, compression, cache byte budgets,
prepared execution and durability settings constant within each engine.
Page-size comparisons use the same executable revision; earlier storage reports
remain historical evidence, not a controlled baseline for new code.

## Supported sizes and interpretation

| Engine | 16 KiB | 64 KiB | 1 MiB | Meaning |
| --- | --- | --- | --- | --- |
| SlopDB | Supported | Supported | Supported | Target uncompressed SSTable data-block size |
| SQLite | Supported | Supported | Unsupported | Fixed database page size |
| DuckDB | Supported | Supported | Unsupported | Fixed persistent database block size |

SlopDB data blocks contain whole entries and restart metadata, and compression
can reduce their stored size. They are not padded fixed-size pages. Record both
the configured target and measured physical storage. A larger block can reduce
index/framing overhead and improve compression while making a point read decode
more data. Its WAL framing remains independent of SSTable block size.

SQLite accepts power-of-two pages from 512 through 65,536 bytes. Set
`PRAGMA page_size` before creating the database or enabling WAL, then verify the
actual value after reopening. Invalid values can be silently ignored; an accepted
SQL statement is insufficient evidence. SQLite cannot change page size while in
WAL mode. See [SQLite page-size documentation](https://sqlite.org/pragma.html#pragma_page_size)
and [WAL restrictions](https://sqlite.org/wal.html).

DuckDB accepts power-of-two blocks from 16,384 through 262,144 bytes when
creating a file. Configure the new file through `default_block_size` or
`ATTACH ... (BLOCK_SIZE ...)`, and read its actual block size from database
metadata after reopening. The setting does not rewrite an existing file.
See [DuckDB block sizes](https://duckdb.org/docs/current/configuration/pragmas#block-sizes)
and [ATTACH options](https://duckdb.org/docs/current/sql/statements/attach#options).

Record unsupported engine/size pairs explicitly. Do not substitute a supported
size or report a successful measurement for an ignored setting.

## Datasets

The large TPC-H fixture uses the same hash-verified DBGEN SF=0.2 inputs as
[the large storage comparison](storage-large-comparison.md): eight tables,
1,731,999 rows, fourteen secondary indexes, ANALYZE and the Q15 view.
Preserve all input hashes in the report. Compare every result from all 22 queries
with independent SQLite/DuckDB or native qualification answers, including after
reopening and maintenance.

The large TPC-C storage fixture uses the pinned committed snapshot from the
earlier comparison. Its nine tables contain 83,165 source rows. Replicate each
table 128 times except the shared 1,000-row item catalog, producing 10,518,120
rows across 256 warehouses. Shift `w_id` and every column ending in `_w_id` by
`replica * 2`; shift `h_id` by `replica * (maximum source h_id + 1)`. Preserve
the source JSON hash and declared keys plus `orders_customer` index. Compare
every column of every row with the shifted source, not just row counts.
Repeated values influence compression; label this as a replicated storage
fixture rather than a fresh transactional benchmark.

TPC-C timed runs use a separate fresh seeded load. The existing consistency
checks assume that initial state, so the previously mutated storage snapshot
cannot be substituted without redefining the baseline. Use ten workers,
one minute, seed 42, prepared statements and WAL fsync disabled. Hold warehouse,
district, customer and item counts constant and report them explicitly. Keep
loading, preparation, validation, close and reopen outside the timed window.
Require all fifteen consistency checks both immediately after execution and
after reopening. Report expected rollbacks, shutdown cancellations, retries and
unexpected failures separately. Databases from equal-duration runs contain
different numbers of committed rows; their final sizes are workload outcomes,
not equal-data space-efficiency comparisons.

## Memory, maintenance and timing controls

SlopDB defaults to a 4 MiB memtable and a 128 MiB decoded-block cache. Keep
these byte budgets and auto compression fixed across page sizes. The cache has
sixteen shards, so each default shard retains at most 8 MiB of decoded data.
Report its hits and misses together with allocations and throughput; larger
pages reduce the number of retained blocks without increasing the cache budget.

For SQLite, set any cache budget in KiB with a negative `cache_size` after the
page size is established, on every connection. A positive page count would
increase the memory budget with the page size. Also report `wal_autocheckpoint`:
the default thousand-page threshold changes its byte size with the page size.
Either fix its approximate byte threshold for every variant or identify the
default checkpoint policy as part of the tested configuration.
See [SQLite cache sizing](https://sqlite.org/pragma.html#pragma_cache_size) and
[automatic checkpoints](https://sqlite.org/pragma.html#pragma_wal_autocheckpoint).

Keep DuckDB thread count and memory limit constant, and report the native driver
or Python version used for each operation. WAL fsync bypass must apply only to
the exact benchmark WAL path; checkpoint and database-file synchronization stay
enabled. Do not substitute WAL disabling for WAL fsync disabling.

Record sizes after clean close, and again after the stated maintenance operation:
SlopDB VACUUM, SQLite VACUUM, and DuckDB VACUUM followed by CHECKPOINT. These
operations have different semantics. In particular, DuckDB VACUUM itself does
not reclaim file space; see [its documentation](https://duckdb.org/docs/current/sql/statements/vacuum#reclaiming-space).
Do not describe them as identical physical compactions.

Sum every regular file in the dedicated database directory, including indexes,
catalog metadata, locks and any remaining WAL. Keep inputs, logs, reports,
binaries, profiles and query spill files outside that directory. Report apparent
file bytes and filesystem-allocated bytes separately; verify the latter against
`du -s -B1`, accounting for directory allocation. Preserve a per-file inventory,
actual page/block metadata, row counts, validation status and exact operations.

Run loads, maintenance, full tests and timed workloads serially on the same
machine. Use fresh cases in a recorded order; report OS-cache conditions rather
than claiming cold reads without resetting them. For TPC-H, one warmup and
multiple measured executions per query distinguish initialization from repeated
execution. A timeout is an incomplete query, not a timing sample. A single
TPC-C run per size is descriptive evidence, not statistical proof.

With limited disk space, finish a case's validation, measurements and saved
reports before removing only that case's task-created database. Preserve the
pinned source inputs and previous databases. Leave room for WAL, compaction,
VACUUM copies and query spills; do not schedule all variants concurrently.
