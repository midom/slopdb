# Page-size measurement protocol

Storage comparisons replay one frozen canonical `.sql` file per dataset into
fresh SlopDB, SQLite and DuckDB databases. Every engine/page case receives the
same SQL bytes, schema, indexes, views, INSERT values and transaction boundaries.
The SQL SHA256 and byte length must match its sidecar manifest before replay.
There is no benchmark run, per-engine data/schema rewrite, native COPY shortcut
or query performance test in this storage matrix. Earlier snapshots and timings
remain historical evidence rather than current comparison cells.

Use `scripts/canonical_storage_fixture.py` to dump the pinned source once, then
`scripts/storage_sql_matrix.py --sql DATA.sql --manifest DATA.sql.manifest.json`
to replay it at 16 KiB, 64 KiB and 1 MiB plus default controls. Native engines
reject 1 MiB; unsupported requests remain explicit. SlopDB's Zstd maximum window
matches its configured block target (minimum 1 KiB). All fresh cases use one
compiled SlopDB revision; every native case is reloaded, not reused.

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

## Canonical datasets and equality gates

The TPC-H dump originates from the pinned DBGEN SF=0.2 inputs: eight tables,
1,731,999 rows, fourteen secondary indexes, ANALYZE and the Q15 view. The TPC-C
storage dump originates from the pinned 83,165-row committed snapshot, with
128 replicas except its shared 1,000-row item catalog: 10,518,120 rows across
256 warehouses. The generator shifts warehouse/history keys once before
freezing the SQL. Repeated values influence compression; this is a replicated
storage fixture, not a fresh transactional benchmark.

Both dumps use identical portable `BIGINT`, `DOUBLE` and `TEXT` declarations
across engines, including explicit NOT NULL primary keys. Do not replace DuckDB
INTEGER/REAL types during replay. Multi-row INSERTs contain at most 256 rows;
transactions contain at most 4,096 rows. Native runners stream statements with
quote/comment-aware splitting and execute the original text. SlopDB also streams
and executes those statements through its normal SQL path. SQL, source hashes,
generation configuration and expected fingerprints remain frozen in manifests.

After clean close/reopen, read every declared column of every table in its
canonical key order and compute the exact `slopdb-canonical-sql-v1` SHA256:

- A row starts with ASCII `R` and a big-endian uint32 column count.
- NULL is tag 0; its logical payload size is zero.
- int64 is tag 1 plus eight signed big-endian bytes; comparisons remain exact.
- float64 is tag 2 plus eight big-endian IEEE-754 bytes. Values must be finite;
  signed zero is normalized to positive zero. Other floating values are exact.
- Text is tag 3 plus a big-endian uint64 UTF-8 length and the exact UTF-8 bytes.

Each table report must match the manifest's row count, SHA256, `logical_bytes`
(non-NULL payload: integer/real eight bytes, text UTF-8 bytes) and `encoded_bytes`
(payload plus hash framing). Dataset row and logical-byte totals must also match.
Independent Python and Go encoders verify results. Repeat the complete check after
maintenance and another close/reopen. Neither row counts alone nor aggregate
TPC-H answers establish logical-data equality. No size result is published until
both full-value checks pass. The SQL hash is checked again during/after replay.

TPC-C throughput fixtures remain separate. Equal-duration benchmark runs commit
different row counts and values, so their final sizes are not equal-data storage
comparisons. No TPC-C or TPC-H workload executes in the canonical storage matrix,
including at 1 MiB. A full-column validation scan verifies storage rather than
measuring query throughput.

## Memory, maintenance and timing controls

SlopDB defaults to a 4 MiB memtable and a 128 MiB decoded-block cache. Keep
these byte budgets, compression codec/level and window-to-page policy fixed
across page sizes. Canonical loads use the same bounded multi-row SQL INSERTs
and transaction boundaries in every engine. Set the Zstd window
to the configured data-block target: 4 KiB, 16 KiB, 64 KiB or 1 MiB, respectively.
Independent blocks do not share compression history. Earlier explicit 8 MiB
windows are a different configuration and must be labeled historical. The cache has
sixteen shards, so each default shard retains at most 8 MiB of decoded data.
Larger pages reduce the number of retained blocks without increasing the cache
budget. The storage matrix does not collect query throughput or cache-hit samples.

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

Record pre-maintenance sizes after clean close as diagnostic inventories. Report
SlopDB’s final comparison size only after VACUUM atomically merges and rewrites
all SSTables, including files already in L6, directly into L6 using the current
page/window settings. Verify every L0–L5 file count and byte count is zero after
reopening, and retain all seven level inventories plus the explicit
`full_bottom_level_rewrite` operation marker. Merely promoting old files to L6
does not meet this requirement. Multiple output files are expected: L6 is the
lowest level, not a requirement to create one enormous SSTable.

Native maintenance remains SQLite VACUUM, and DuckDB VACUUM followed by
CHECKPOINT. Every native cell is freshly replayed from the same canonical SQL.
These operations have different semantics. In particular, DuckDB VACUUM itself does
not reclaim file space; see [its documentation](https://duckdb.org/docs/current/sql/statements/vacuum#reclaiming-space).
Do not describe them as identical physical compactions.

Sum every regular file in the dedicated database directory, including indexes,
catalog metadata, locks and any remaining WAL. Keep inputs, logs, reports,
binaries, profiles and query spill files outside that directory. Report apparent
file bytes and filesystem-allocated bytes separately, including directory
allocation. The matrix obtains allocated bytes from filesystem `st_blocks`;
`du -s -B1` can independently check them. Preserve a per-file inventory,
actual page/block metadata, row counts, validation status and exact operations.

Run loads, maintenance and full tests serially on the same machine. Use fresh
cases in a recorded order and preserve host/version/build metadata. OS caches are
not reset. Loading durations are diagnostics, with SQL hashing/open/verification
boundaries reported by the runner; they are not certified benchmark scores. No
query or transaction timing samples are collected by this storage matrix.

With limited disk space, finish a case's validation, measurements and saved
reports before removing only that case's task-created database. Preserve the
pinned source inputs and previous databases. Leave room for WAL, compaction,
VACUUM copies and query spills; do not schedule all variants concurrently.
