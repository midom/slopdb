# Raft replication

SlopDB can run a replicated database using HashiCorp Raft. The leader ships
atomic native WAL mutation batches containing row, index, catalog and allocation
changes. The default `sync` mode acknowledges after a voting majority has durably
stored its Raft entry and the leader has durably applied it to the native database.
The optional `async` mode acknowledges after replication and application, then
flushes a certified prefix periodically. It can lose recent acknowledged writes
after a whole-cluster failure. Followers apply asynchronously. SQL is served by the ready leader; followers and learners
reject SQL with a leader hint. There is no automatic SQL proxy or follower-read
mode.

Replication and clone/control connections support TCP and Unix sockets. They
use separate listeners from the MySQL/PostgreSQL SQL listeners. A shared cluster
token is mandatory; TLS and CA-verified mutual TLS are supported on both internal
listeners, including Unix sockets. SQL authentication and TLS use their existing,
separate protocol flags.

## Start a cluster

Use a new **node directory** for each member. It contains local identity, native
database generations, a durable Raft log and consensus snapshots. Do not pass an
existing standalone database directory as the node directory.

```sh
export SLOPDB_RAFT_TOKEN='replace-with-a-long-random-shared-secret'

./slopdb --raft-id n1 --raft-bootstrap \
  --raft-listen tcp://127.0.0.1:9400 \
  --raft-control tcp://127.0.0.1:9401 \
  --raft-token-env SLOPDB_RAFT_TOKEN \
  --mysql-tcp 127.0.0.1:3306 ./node-1
```

Wait for `Ready: true` in the authenticated status response before cloning or
connecting SQL clients. JSON field names are case-sensitive:

```sh
curl -H "Authorization: Bearer $SLOPDB_RAFT_TOKEN" \
  http://127.0.0.1:9401/v1/status
```

Start additional members with unique IDs and directories:

```sh
./slopdb --raft-id n2 \
  --raft-listen tcp://127.0.0.1:9410 \
  --raft-control tcp://127.0.0.1:9411 \
  --raft-join tcp://127.0.0.1:9401 \
  --raft-token-env SLOPDB_RAFT_TOKEN \
  --mysql-tcp 127.0.0.1:3307 ./node-2

./slopdb --raft-id n3 \
  --raft-listen tcp://127.0.0.1:9420 \
  --raft-control tcp://127.0.0.1:9421 \
  --raft-join tcp://127.0.0.1:9401 \
  --raft-token-env SLOPDB_RAFT_TOKEN \
  --mysql-tcp 127.0.0.1:3308 ./node-3
```

A joining node first downloads and verifies a committed database snapshot and
its Raft membership metadata. The leader adds it as a **nonvoter**. Snapshot and
WAL catch-up then proceed asynchronously without adding it to the voting quorum.
By default, the leader commits a catch-up fence, checks the target's durable
application checkpoint, and promotes it to a voter only after that fence is
applied. Promotion intent is replicated and is resumed by a new leader. An
unreachable learner remains a nonvoter. A three-voter cluster can continue with
one voter unavailable; a two-voter cluster needs both voters.

Use `--raft-auto-promote=false` on the joining node to keep it a learner until
explicit promotion. `--raft-bootstrap` is for the first member of a new cluster.
On restart, retain the node directory, ID, Raft endpoint and control endpoint;
omit the bootstrap and seed flags. Async restarts must contact a live quorum as
described below; `--raft-join` can supply a leader discovery address. Bind port `0` is useful for tests, but
restart must use the actual persisted port. Wildcard listeners require explicit
`--raft-advertise` / `--raft-control-advertise` reachable endpoints.

To copy an existing standalone database into a new cluster, add
`--raft-seed /path/to/existing-db` to the first bootstrap command. SlopDB opens
that database exclusively, upgrades it if necessary, and takes a consistent
backup into the new node directory. The seed is included in the initial Raft
snapshot before learners or SQL clients are admitted. The original database
remains a separate standalone database.

Replace `--mysql-tcp` with `--postgres-tcp` or a protocol socket flag to use
PostgreSQL emulation or Unix SQL connections. With no SQL listener flags the
process runs only the Raft/control services until SIGINT or SIGTERM; replicated
mode does not run the stdin CLI or dot commands.

## Unix sockets

All internal endpoint flags accept `unix:///absolute/path`:

```sh
mkdir -p /tmp/slopdb-n1 /tmp/slopdb-n2
./slopdb --raft-id n1 --raft-bootstrap \
  --raft-listen unix:///tmp/slopdb-n1/raft.sock \
  --raft-control unix:///tmp/slopdb-n1/control.sock \
  --raft-token-env SLOPDB_RAFT_TOKEN \
  --postgres-socket /tmp/slopdb-n1/.s.PGSQL.5432 ./node-1

./slopdb --raft-id n2 \
  --raft-listen unix:///tmp/slopdb-n2/raft.sock \
  --raft-control unix:///tmp/slopdb-n2/control.sock \
  --raft-join unix:///tmp/slopdb-n1/control.sock \
  --raft-token-env SLOPDB_RAFT_TOKEN ./node-2
```

Socket files have mode `0600`. On restart, a socket left by an unclean exit is
reclaimed only if it refuses connections. Live sockets, regular files and
symlinks are preserved. Socket parent directories must already exist.

## TLS and certificates

Supply these flags to every node:

```sh
--raft-tls-cert node.pem --raft-tls-key node-key.pem --raft-tls-ca cluster-ca.pem
```

The CA bundle verifies outbound servers and requires inbound client certificates
signed by that CA. Each node certificate must support both TLS server and client
authentication, and its SANs must match the endpoints other nodes dial. The same
node certificate is used in both directions. TLS 1.2 is the minimum. For Unix
sockets the default verified server name is `localhost`; use
`--raft-tls-server-name cluster.example` to select a common certificate DNS name.
The override applies to all outbound cluster connections.

A certificate/key without a CA bundle enables server TLS with normal system-root
verification outbound; the cluster token still authenticates Raft/control
access. A custom/private CA needs the CA flag. Without TLS, token authentication
does not encrypt database contents or HTTP control traffic; use plaintext only
on trusted transports.

For control requests over TLS, use the HTTPS URL and supply the cluster CA,
client certificate and key to curl. For Unix control sockets use curl's
`--unix-socket /path/control.sock` with an HTTP(S) localhost URL.

## Membership and status

Every control endpoint requires `Authorization: Bearer <cluster-token>`.
Membership changes and snapshot exports must be sent to the ready leader.

| Method and path | Body / response |
| --- | --- |
| `GET /v1/status` | Identity, cluster ID, role, leader, readiness, applied index/term, durability mode, durable index/term, recovery state and membership. Available on learners and followers. |
| `GET /v1/snapshot` | Stream of Raft snapshot metadata and checksummed native state, used by clone bootstrap. |
| `POST /v1/join` | `{"ID":"n2","Address":"tcp://host:9410","Control":"tcp://host:9411","Cluster":"<cluster-ID>","AutoPromote":true}`; target must already be running with matching identity. Normally called by `--raft-join`. |
| `POST /v1/promote` | `{"ID":"n2"}`; waits for durable catch-up, then requests voting membership. |
| `POST /v1/transfer` | `{"ID":"n2"}`; transfers leadership to an existing voter. |
| `POST /v1/remove` | `{"ID":"n2"}`; removes a member. Transfer leadership before removing the current leader. |
| `POST /v1/durability/wait` | `{"Cluster":"<cluster-ID>","Index":123,"Term":4}`; waits for a certified durable prefix covering the token. Send to the ready leader. |

For example, explicitly promote a learner:

```sh
curl -H "Authorization: Bearer $SLOPDB_RAFT_TOKEN" \
  -H 'Content-Type: application/json' -d '{"ID":"n2"}' \
  http://127.0.0.1:9401/v1/promote
```

Duplicate joins preserve existing voting membership. A node ID or Raft address
already assigned to a different identity/address is rejected. Removing a member
does not delete its directory. Stop a removed process and use a new identity and
node directory when replacing it. There is no automatic address replacement or
automatic quorum override. Async clusters have the explicit offline recovery
procedure below.

## Asynchronous durability

Set `--raft-durability async` on the bootstrap node. Joining members inherit the
mode from the source; restarts inherit the persisted mode. Every member must use
the same mode. An existing node cannot change its mode in place; seed a new
cluster from a standalone backup to change modes.

```sh
./slopdb --raft-id n1 --raft-bootstrap \
  --raft-durability async --raft-durability-interval 100ms \
  --raft-max-durability-lag 1s \
  --raft-listen tcp://127.0.0.1:9400 \
  --raft-control tcp://127.0.0.1:9401 \
  --raft-token-env SLOPDB_RAFT_TOKEN \
  --postgres-tcp 127.0.0.1:5432 ./node-1
```

Two positions describe the log: **R**, the live applied prefix, and **D**, the
certified durable prefix. Ordinary async-mode commits wait for Raft replication
to a voting majority and leader application, without a per-commit WAL fsync.
`COMMIT ASYNC` explicitly requests this behavior. Plain `COMMIT` and autocommit
use the cluster's mode. In a sync cluster, `COMMIT ASYNC` retains sync guarantees.

`COMMIT DURABLE` waits until D covers the committed transaction and all preceding
entries. It is supported over both MySQL and PostgreSQL, including prepared
statements. Use it for transactions that must survive a full-cluster restart:

```sql
BEGIN;
UPDATE accounts SET balance = balance - 10 WHERE id = 1;
COMMIT DURABLE;
```

The Go API also provides `Tx.CommitDurable[Context]`, `Node.CurrentToken`,
`Node.DurableToken`, `Node.WaitDurable(ctx, token)` and `Node.FlushDurable(ctx)`.
Tokens identify a cluster incarnation and log prefix. Store a durable token
outside the cluster if disaster recovery must enforce a minimum restore point.
`COMMIT DURABLE` returning `db.ErrDurabilityUnconfirmed` / SQLSTATE `40003` means
the transaction committed but its durability confirmation failed. Rollback
cannot undo it; reconcile its outcome before retrying.

Every flush commits a fence, synchronizes matching consensus-log prefixes on a
voting majority, and stores a quorum certificate on a voting majority before
advancing D. Certificates bind the cluster ID, term, membership and chained log
hash. Native state can be reconstructed from a synchronized snapshot and this
certified log. A regular flush holds membership fixed while later writes can
continue beyond its captured prefix. Snapshot export also pauses proposals until
its exact prefix has been certified and captured. Term/vote records and membership
log records remain synchronously durable. WAL and its synchronization capability
must stay enabled; the ordinary
`--no-wal-fsync` and `--no-wal` flags remain invalid for replication.

The default group-flush interval is 100 ms. The default maximum durability lag
is the greater of one second and ten flush intervals. Once an unstable prefix
exceeds that age, new writes are rejected until flushing succeeds. This limits
continued admission during storage trouble; it is **not** a guaranteed recovery
point deadline. Existing tentative writes remain at risk until D advances.
Status exposes `DurabilityError` when periodic flushing fails.

An async process that restarts enters `Recovering` and cannot vote, elect itself,
or serve SQL. It must download a freshly certified snapshot from a ready leader
that verifies a live quorum before starting Raft again. Discovery uses control
addresses in the last certificate and the optional `--raft-join` address.
This prevents a restarted member from voting using a log that forgot updates.
Membership promotion and exported snapshots also require a stable prefix.

## Whole-cluster stable-point recovery

If no live quorum remains, async nodes stay in recovery even when their files
appear intact. This also applies to a clean stop of every async member. Recovery
is explicit and offline. To preserve all rows during planned shutdown, first
stop SQL writes and membership changes, call `FlushDurable`, and record its
durable token. A clean `Close` flushes local files but does not certify a new D.

1. Stop or otherwise fence **all original members**, including originals of any
   copied directories. Prevent old clients and processes from rejoining.
2. Gather offline node directories (or complete offline copies) from every voter
   in the latest certified configuration. Keep the original shared token. Recovery
   takes exclusive local locks but cannot fence remote originals for you.
3. Export a new standalone seed. The positional directory is the first source;
   `--raft-recovery-sources` lists the others:

   ```sh
   ./slopdb --raft-token-env SLOPDB_RAFT_TOKEN \
     --raft-recover-to ./recovered-seed \
     --raft-recovery-sources ./node-2,./node-3 \
     --raft-recovery-fenced \
     --raft-recovery-minimum-token '{"Cluster":"OLD-ID","Index":123,"Term":4}' \
     ./node-1
   ```

   The minimum token is optional. Supply the token you recorded, not the example
   values. Recovery selects the highest valid certificate available, reconstructs
   exactly that prefix, checks its log hash, membership and database integrity,
   and refuses to restore behind the requested minimum. If no source has enough
   intact snapshot/log history, recovery fails and preserves its evidence.
4. Bootstrap a **new node directory** with `--raft-seed ./recovered-seed`, then
   clone new members normally. The resulting cluster has a fresh incarnation ID;
   old durability tokens are rejected. The export contains `RECOVERED_FROM.json`
   and recovery marks supplied old roots `RETIRED` so they cannot be reopened as
   members. Keep them for investigation; never bypass the retirement marker.

The recovered database contains a complete transaction prefix, including rows,
indexes, catalog changes and allocation state. Tentative entries after D are
discarded even if some happened to survive locally. Durable certificates assume
crash failures and trusted token holders; this is not Byzantine consensus.

## Recovery and consistency

Each committed batch includes its cluster/index/term application checkpoint in the
same native WAL record. Replaying an already applied index performs no write.
Range tombstones are replicated alongside that checkpoint. Page-size changes
through `PRAGMA page_size` are replicated; local compression and cache settings
may differ. `--page-size` is rejected in replicated mode because it would make a
local override outside consensus. Replication also rejects disabled WAL, disabled
WAL fsync, disabled durable automatic-ID counters and index repair on open.

Snapshots pin a committed MVCC view and stream sorted raw keys with bounded
batches. A SHA256 digest, ordering, cluster identity and replay checkpoint are
verified before a fresh native generation becomes visible. The generation is
fully flushed and synchronized, then `CURRENT` is atomically published. Async
exports first seal their exact prefix. A crash
before publication leaves the old generation usable; a crash after publication
leaves the complete new generation. Sync-mode startup restores the consensus snapshot and
replays subsequent committed Raft entries. A lagging learner whose logs have
been compacted automatically receives a new snapshot through Raft.

Followers do not run local catalog migrations or index cleanup. A new leader
first drains old SQL, commits a fence, reloads the persisted catalog and performs
abandoned index cleanup through Raft before admitting clients. Existing sessions,
open transactions and prepared plans are invalidated by leadership transitions;
clients should reconnect and prepare again. Statements that access data capture
Raft's committed index, verify leadership with a fresh quorum, and wait for actual
native application of that prefix before execution. Read-only statements do not
append log entries. Each data-access admission checks a quorum; no time-based leadership
lease is used. Initial leader admission still commits a fence in the current term.

Transaction control (`BEGIN`, `COMMIT`, `ROLLBACK` and savepoints) checks the ready
leader role and operates on session state. Every commit write still waits for
Raft quorum replication and native application through the storage hook. A
successful transaction-control response alone does not confirm quorum health;
data-access statements and write acknowledgments continue to require a quorum.

Already committed WAL commands can share one native WAL record and, in sync mode,
one fsync. The group's final checkpoint is published atomically with all its
mutations before any of its commit futures complete. Control and membership
changes remain ordered boundaries. Grouping adds no waiting timer and preserves
individual Raft entries and SQL transaction boundaries.

Independent proposals encode outside the enqueue gate and enter Raft
concurrently. SQL row, catalog and counter locks preserve dependent-write order;
flushes and snapshots take the exclusive gate after preceding enqueues and their
callbacks finish. Commands larger than 64 KiB are limited to one unresolved
proposal at a time. The existing unbuffered Raft enqueue behavior is retained.

WAL commands and application checkpoints use specialized JSON writers/readers
for their canonical shape, with standard JSON decoding for other representations.
Their encoded bytes match the standard library, preserving existing transport
and persisted history formats.

Local consensus-log records use a versioned binary encoding. Existing JSON log
records remain readable and can coexist with new records in the same directory;
the Raft transport, command payloads and history hash are unchanged. Older
binaries cannot read newly written binary log records.

A lost-quorum node cannot acknowledge a write or serve SQL reads. A client that
loses its connection or receives a leadership error during a commit can have an
**unknown commit outcome**: the entry may still commit. Reconnect to the leader
and reconcile using an application transaction/request identifier rather than
blindly retrying a non-idempotent operation. Query cancellation does not retract
an already proposed WAL batch.

This is single-database, single-leader replication. There is no multi-leader
merge, sharding, automatic SQL routing, request deduplication or automatic
disaster recovery. An encoded WAL command is limited to 64 MiB (JSON base64 framing
adds overhead); an individual snapshot key/value record is limited to 256 MiB.
The node's native data files need not be byte-identical because local MVCC
sequence numbers, compression and compaction are independent. The replicated
logical row/index/catalog bytes and transaction boundaries are preserved.

## Go API

`replication.Open(ctx, replication.Config{...})` owns the database and listeners.
Use `node.Database()` for SQL, `WaitReady` for leader admission, `Status` for
monitoring, and `Join`, `Promote`, `TransferLeadership` and `Remove` for membership.
`Done` / `Err` report fatal application errors. Check `node.Close()` for storage
flush failures; do not independently close or modify its database or native
storage. `StorageOptions` configure local storage, and optional `RaftConfig`
allows timing tuning; node identity, protocol version and snapshot recovery are
controlled by the replication layer. Use the same SQL quoting mode on all nodes.

The lower-level storage and database replication hooks are for the replication
owner, not application writes. A native standalone backup is not a replacement
for a node's consensus state. Never copy another node's stable Raft log, vote,
identity or node directory to create a member; use the clone/join path.
