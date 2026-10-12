# Canonical TPC-C storage evidence

All ten supported engine/page cases freshly replayed one frozen SQL file and
passed complete typed fingerprints after close/reopen and after maintenance.
The nine tables contain 10,518,120 rows and exactly 713,759,360 logical payload
bytes in every case. The canonical file has 321,378,587 bytes and SHA256
`8772c9bfcb2672d1b18bfb6aa98059489d26962083c94568f089be211eccd543`.
Every supported case executes the same 46,242 SQL statements.

| Engine | Page/block | Final MiB | Allocated MiB |
| --- | --- | ---: | ---: |
| slopdb | 4 KiB | 182.50 | 182.84 |
| slopdb | 16 KiB | 148.83 | 149.25 |
| slopdb | 64 KiB | 145.44 | 145.91 |
| slopdb | 1 MiB | 150.04 | 150.41 |
| sqlite | 4 KiB | 449.99 | 450.00 |
| sqlite | 16 KiB | 447.91 | 447.92 |
| sqlite | 64 KiB | 448.50 | 448.52 |
| sqlite | 1 MiB | Unsupported | — |
| duckdb | 16 KiB | 574.11 | 574.12 |
| duckdb | 64 KiB | 268.01 | 268.02 |
| duckdb | 256 KiB | 277.76 | 277.77 |
| duckdb | 1 MiB | Unsupported | — |

SlopDB windows match block targets; final reports prove every SSTable was fully
rewritten into L6, no files remain in L0–L5, and the physical inventory matches
L6 metadata. Native maintenance is SQLite VACUUM or DuckDB VACUUM + CHECKPOINT.
WAL stays enabled with fsync disabled; SQLite synchronous=OFF additionally
disables data/checkpoint sync. No transaction/query workload ran.
The frozen dump and raw CPU profiles remain outside Git. Loading and verification
CPU samples from the first DuckDB case are preserved separately in the parent
archive. Complete manifests, per-table hashes, exact byte inventories and timing
boundaries are in comparison.json and each cell's raw reports.
