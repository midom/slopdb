# MySQL CDC and replication source

SlopDB can stream its committed changes using the MySQL binary-log protocol.
This is a replication **source**, backed by a durable journal in the native LSM.
It does not run another storage engine or accept replication from a MySQL source.

`SHOW MASTER STATUS`, `SHOW BINARY LOG STATUS`, a binlog dump, or a replication
identity-variable query activates the journal. Embedders can call
`Database.EnableCDC(ctx)` explicitly. Activation and subsequent records use the
configured WAL/replication durability. Activation records the initial catalog;
row history before activation is unavailable. Once enabled, logging continues
across restarts, including without an active MySQL listener.

Each journal record corresponds to one committed transaction. Group commit keeps
the original transaction boundaries. Records contain before/after row images
and catalog changes in the same atomic storage publication as the data. Rolled
back writes, secondary-index maintenance and WAL-less temporary tables are
excluded. Source identity, sequence numbers and timestamps survive recovery,
backup and native Raft snapshot transfers. Raft replicas receive identical journal
bytes despite private temporary-table activity on individual nodes.

## Wire interface

The source supports `COM_BINLOG_DUMP`, GTID dump requests with executed-interval
filtering, and `COM_REGISTER_REPLICA`. Events include format descriptions,
previous GTIDs, table maps, full-row version-2 inserts/updates/deletes, query
events, transaction commits, rotation and heartbeats. Events carry CRC32
checksums. Consumers can resume using a GTID set or an actual file byte offset;
files use names such as `slopdb-bin.000001`. `SHOW BINARY LOGS` reports file sizes.
An offset inside an event is rejected.

Signed/unsigned integer widths, ENUM, binary and UTF-8 strings, fractional
temporal values, packed DECIMAL and MySQL binary JSON are encoded directly.
Source cascades appear as explicit row changes; row-event flags prevent a
destination from applying those cascades twice. A persistent UUID identifies the
GTID source. The offset index keeps transaction boundaries rather than row
payloads and is rebuilt deterministically from the journal after restart. Live
streams wait on commit notifications and heartbeat timers.

The root schema in events is always `main`. It remains stable if the configured
MySQL login database alias changes. Snapshot clients can `USE main`; other
virtual schemas retain their names. Create the destination's `main` database
when starting replication from an initially empty source. For an existing
database, the consumer must arrange a consistent initial snapshot and matching
CDC position. `SHOW MASTER STATUS` alone does not make an ordinary concurrent
SQL dump consistent. FTWRL and automatic snapshot provisioning are unavailable.

Interoperability tests include independent `mysqlbinlog` decoding and an actual
isolated Oracle MySQL 8 replica applying CREATE TABLE, uint64, DECIMAL, JSON,
large Unicode TEXT, prefix indexes, cascades, INSERT/UPDATE/DELETE and receiver
restart with GTID auto-positioning. The external-server test is
optional; raw protocol and recovery tests run in the ordinary Go suite.

## Typed API

`Database.NewCDCReader(ctx, afterSequence)` resumes immediately after the supplied
transaction sequence. `Read(ctx, limit)` returns owned `CDCTransaction` values
with typed rows, table definitions, schema changes and namespace deltas.
`Position()` advances only after successful decoding. `Changes()` supplies a
commit notification channel; capture it before reading, then wait if no records
are returned. Readers retain no transaction snapshot or execution lock between
calls. `Database.CDCPosition(ctx)` returns the current sequence.
Reads use a soft 16 MiB page budget without splitting a transaction; the first
transaction is returned whole. Journal records and expanded catalog checkpoints
have an approximately 256 MiB encoded limit. An oversized change fails before
WAL publication rather than committing data without its CDC record.

## Boundaries

The MySQL stream supports pure row transactions and a single compatible table or
schema DDL event per commit. Native SlopDB permits atomic transactions containing
multiple DDL statements or both DDL and rows; MySQL's implicit-DDL transaction
model cannot preserve that boundary. Such records return error 1236 instead of
being split or silently changed. ALTER operations that rewrite rows currently
fall into this boundary. Ambiguous combined column rename/type changes are also
rejected rather than translated into a destructive DROP/ADD. The typed API
retains those transactions in full.

An unsupported retained record also prevents rebuilding the MySQL offset index
past that record, including status and later resume requests. Plan native DDL
accordingly when the journal is active. Views, triggers and other native catalog
objects are not projected as MySQL DDL; their resulting row mutations are logged.
Destination schema constraints and collations must be compatible. Generated
table DDL uses UTF-8 binary collation; arbitrary SlopDB Unicode normalization
collations have no exact MySQL equivalent.

JSON numbers outside MySQL's exact DECIMAL limits and TIMESTAMP values outside
the MySQL uint32 Unix range produce errors rather than lossy events. Binary-log
files have uint32 positions and transactions cannot cross the file-size limit.
There is no journal purge/retention configuration, RESET BINARY LOGS, GTID tags,
semi-synchronous MySQL replication, or MySQL receiver/applier. Journal storage
and the in-memory transaction-position index grow with retained history.
CDC observes local committed publications under the selected durability mode.
If offline recovery discards previously published asynchronous commits, downstream
consumers need a fresh snapshot; the stream cannot undo an already applied tail.
