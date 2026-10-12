# Canonical SQL storage evidence

The `tpcc/` and `tpch/` directories retain fresh serial storage matrices,
canonical SQL sidecar manifests, successful per-cell reports and logs.
No workload or performance test ran. Every successful phase matches every table's
canonical exact typed SHA256, row count and logical/framed bytes.
SlopDB final inventories additionally prove full L6 rewriting and matching windows.

The large `.sql` files are retained outside Git at the paths recorded by
`manifest.json`. Regenerate them with `scripts/canonical_storage_fixture.py`
using the pinned source hashes and generation settings in their sidecars.
Replay with `scripts/storage_sql_matrix.py`; all engines execute the same SQL.
Binary/build metadata and commands are in each `comparison.json`.
Artifact SHA256 values and sizes are in `manifest.json` (which does not hash itself).
