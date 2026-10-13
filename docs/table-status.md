# Table status

`SHOW TABLE STATUS` returns MySQL-style metadata for tables and views in `main`.
Results are sorted by name. Use `FROM main` or `IN main`, and either `LIKE` to
match names or `WHERE` to filter the output columns:

```sql
SHOW TABLE STATUS;
SHOW TABLE STATUS FROM main LIKE 'orders%';
SHOW TABLE STATUS WHERE Name = 'orders' AND Rows > 0;
SHOW TABLE STATUS WHERE Data_length + Index_length > 1048576;
```

`WHERE` supports prepared parameters. `LIKE` requires a string literal and uses
the same escaping rules as `SHOW TABLES`. `FULL` is not supported for this command.

| Column | Meaning in SlopDB |
| --- | --- |
| Name | Table or view name |
| Engine | `SlopDB` for tables; NULL for views |
| Row_format | `LSM` for tables |
| Rows | Estimated visible row count at the statement snapshot |
| Avg_row_length | `Data_length / Rows`, rounded down; zero when Rows is zero |
| Data_length | Estimated compressed SSTable data-block bytes plus encoded unflushed records for the table's row-key range |
| Index_length | The same size estimate summed over the table's physical secondary indexes, including automatic constraint indexes |
| Auto_increment | Next available AUTOINCREMENT value, including shared reservations; NULL for tables without AUTOINCREMENT or exhausted int64 values |
| Collation | Effective database collation |
| Create_options | Empty string for tables |
| Comment | Estimation description for tables; `VIEW` for views |

`Version`, `Max_data_length`, `Data_free`, `Create_time`, `Update_time`,
`Check_time`, and `Checksum` are NULL because SlopDB does not track equivalent
per-table metadata. Views return NULL except for `Name` and `Comment`.
The clustered primary key is included in `Data_length`. Constraint keys without
physical secondary entries do not add to `Index_length`.

## Estimation and cost

SSTable file bounds reject unrelated files. Index seeks locate the first and last
blocks of each key range. Their byte offsets account for all interior data blocks
without reading those blocks. Up to two boundary blocks per overlapping file are
read to attribute shared blocks in proportion to their encoded entry bytes.
Compression trailers and restart overhead are distributed in the same proportion.
Size-only index estimates need no interior reads.

Row statistics additionally sample eight random interior block offsets per
file, spread across eight equal byte intervals. Sampling offsets weights blocks
by their compressed byte sizes; estimates
use rows per compressed byte rather than assuming a uniform page size or
compression ratio. Each sampled block selects up to eight value records and
checks their winning MVCC versions using bounded point reads. Old versions,
deletes and range tombstones therefore do not automatically count as live rows.
Point reads can consult additional overlapping files. The estimator does not
construct an SSTable range iterator or decode every table row.

For active and immutable memtables, a short prefix of at most 256 records is
accounted for directly. Larger ranges use skip-list promotion levels to sample
roughly 256–1024 nodes across the whole buffer, weighted by their promotion
probability. Visibility probes are bounded separately. Narrow ranges can be
accounted for exactly even in a large memtable. Sampling varies between calls;
these figures are estimates, especially for highly skewed values or many retained
versions. Use `SELECT COUNT(*)` for an exact row count.

Sizes include retained versions and tombstones, so an empty table can still use
space until maintenance reclaims it. Shared SSTable filters, index blocks,
metadata, footers, catalog records, WALs and allocator overhead are excluded;
these estimates are not the database directory's total disk usage. Flushing
changes the memory/disk mix, and compression or `VACUUM` can change the result.
Different tables are estimated from independently pinned storage views, so
concurrent writes and compaction can change physical sizes during the command.
Visible row samples use the statement's MVCC snapshot, and the session's latest
private writes adjust the estimates inside an explicit transaction.

The storage API exposes `LSMTree.EstimateRange(ctx, lower, upper, snapshot)` and
`EstimateRangeBytes(ctx, lower, upper)`. Bounds are half-open, with NULL (`nil`)
bounds unbounded; `math.MaxUint64` selects the current committed watermark.
Older snapshots must remain pinned for the call. Visibility checks borrow immutable
values while the view is pinned, avoiding copies of potentially large row payloads.
Sizes-only calls skip visibility probes. Both paths pin file readers, honor
context cancellation, and return
sampled-block checksum/read errors rather than treating them as empty ranges.
