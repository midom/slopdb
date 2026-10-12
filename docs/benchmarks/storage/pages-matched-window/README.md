# Matched-window, fully compacted SlopDB storage evidence

Eight fresh storage-only cases use the original pinned TPC-C snapshot and TPC-H
SF=0.2 inputs, at 4 KiB, 16 KiB, 64 KiB and 1 MiB. Zstd encoder windows match
the target blocks. Every final inventory follows an atomic rewrite of all
SSTables into L6; reopened inventories explicitly prove L0–L5 are empty.

TPC-C checks all 10,518,120 rows and every column before and after maintenance.
TPC-H checks hash-verified inputs and persisted table counts at all sizes;
no TPC-H query pack or timed TPC-C workload runs in this refresh.

[Storage manifest](storage-comparison.json) preserves per-file inventories,
levels, actual window/page metadata and binary build metadata. The
[archive manifest](manifest.json) hashes every copied report and log and records
the immutable native-storage archive reused in the comparison tables. Native
engine storage and all performance figures remain explicitly earlier measurements.

See [the updated comparison](../../../page-size-comparison.md) and
[measurement protocol](../../../page-size-protocol.md).
