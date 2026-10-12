# Fresh page-size comparison evidence

See [the measured comparison](../../../page-size-comparison.md) for datasets,
results, load boundaries, code-based diagnostics and limitations.

- [Storage manifest](storage-comparison.json): twenty successful large storage
  cells plus four unsupported native 1 MiB requests; complete file inventories,
  input hashes, configurations and binary build metadata.
- [TPC-C manifest](tpcc-comparison.json): nine completed one-minute attempts,
  including four clean and five failed contention outcomes, one skipped SlopDB
  1 MiB request and two unsupported native requests. Every completed attempt
  passed all thirty consistency assertions.
- [Per-query medians](query-medians.json): 198 single measured TPC-H samples,
  covering all twenty-two queries at nine benchmarked engine/page configurations.
  SlopDB 1 MiB has no query execution or answer checks.
- [Archive manifest](manifest.json): hashes of the copied reports/logs, compiled
  revisions and TPC-C binary build metadata.

The `storage/` and `tpcc/` subdirectories retain full raw reports and logs,
including failed outcomes. Copies preserve their original external artifact
paths. No metadata sanitization was needed. The earlier stopped page matrix
is superseded and contributes no numbers to this archive.
