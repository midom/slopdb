# Canonical SQL storage measurements

This command loads the same immutable `.sql` file used by the native SQLite and
DuckDB storage comparisons. It executes bounded SQL statements through the normal
SlopDB SQL API, including the dump's multi-row INSERTs and transaction boundaries.
It does not run a transactional workload or a TPC-H query benchmark.

```sh
go build -o /tmp/slopdb-bench/storage-sql ./cmd/storage-sql
/tmp/slopdb-bench/storage-sql \
  -sql /tmp/slopdb-bench/canonical/tpch.sql \
  -manifest /tmp/slopdb-bench/canonical/tpch-manifest.json \
  -dir /tmp/slopdb-bench/canonical/slopdb-tpch-16384 \
  -page-size 16384 \
  -json /tmp/slopdb-bench/canonical/slopdb-tpch-16384.json
```

The database directory and output report must not already exist. Reports must be
outside the database directory. Existing directories are never removed. A failed
load or verification leaves its database available for diagnosis and writes no
success report.

The manifest uses `slopdb-canonical-sql-v1`: a SHA-256 and byte length for the whole
SQL file, plus independently generated per-table row counts, payload byte counts,
typed row SHA-256 hashes, column types and deterministic `order_by` columns. The SQL
hash is checked before opening the database and again while executing the dump.
The bounded statement reader recognizes doubled quotes, quoted identifiers,
multiline strings, and line/block comments; statements require a final semicolon
and cannot exceed 8 MiB. This format covers benchmark tables, indexes and views;
it is not a general trigger-body SQL script parser.

Each phase closes and reopens the database, streams every projected column in the
manifest's order, and checks the complete typed fingerprint against the manifest.
Row hash framing is `R`, a big-endian uint32 column count, then cells: NULL=`0`,
int64=`1` + signed big-endian 8 bytes, float64=`2` + big-endian IEEE-754 8 bytes,
TEXT=`3` + big-endian uint64 UTF-8 byte count + UTF-8. REAL signed zeros normalize
to positive zero; nonfinite values are rejected. Payload byte counts exclude tags
and framing, and retain duplicate rows and NULL positions in the hash.

The `closed` phase measures all database files after loading and verifying the
reopened state. The `after_maintenance` phase runs `VACUUM`, closes, reopens and
verifies every value again before measurement. Success additionally requires empty
L0–L5 inventories and an exact match between L6's file numbers/sizes and every
physical SSTable. Apparent file bytes and filesystem allocated bytes are reported
separately; directory allocations are included in allocated totals. No WAL may
remain after clean close.

WAL stays enabled and its fsync is disabled; SSTable and MANIFEST synchronization
retain their normal settings. Zstd windows follow the configured physical block
target. Loading and maintenance times include their final close; the independently
reported verification times cover logical row hashing only. Loading times exclude
the initial SQL file hash check. The 1 MiB case is load/storage-only like every
case in this command.
