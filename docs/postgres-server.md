# PostgreSQL protocol emulation

SlopDB can serve one database through the PostgreSQL **3.0 wire protocol**, over
TCP, a Unix socket, or both. This is a compatibility interface to the SlopDB
engine, not a PostgreSQL engine or a full implementation of PostgreSQL SQL,
system catalogs, or extensions. It supports lib/pq and pgx connections, including
pgx's default prepared-statement cache.

```sh
go build -o slopdb .
./slopdb --postgres-tcp 127.0.0.1:5432 ./example-db
psql 'host=127.0.0.1 port=5432 user=slopdb dbname=slopdb sslmode=disable'
```

Either listener flag enters server mode without reading SQL from stdin. To use
standard PostgreSQL clients with a Unix socket, choose the conventional socket
filename in a directory owned by the server user:

```sh
mkdir -m 700 /tmp/slopdb-postgres
./slopdb --postgres-tcp 127.0.0.1:5432 \
  --postgres-socket /tmp/slopdb-postgres/.s.PGSQL.5432 ./example-db
psql 'host=/tmp/slopdb-postgres port=5432 user=slopdb dbname=slopdb sslmode=disable'
```

`--postgres-socket` is a complete socket path. Libpq-style clients take its
parent directory as `host` and derive `.s.PGSQL.<port>` from the port option.
The parent directory must already exist. The socket uses permissions `0600` and
is removed on clean shutdown. Existing paths are never overwritten. Listener
startup errors close any listeners already created. SIGINT and SIGTERM stop
listeners, cancel work, roll back open transactions and close/flush the database;
flush errors exit nonzero. Storage, compression, query timeout, lock timeout,
WAL and durability CLI flags apply normally.

CLI MySQL and PostgreSQL server modes are mutually exclusive. PostgreSQL mode
uses standard SQL quoting, including double-quoted identifiers, and rejects
`--mysql-quoting=true`. The embedded `postgresserver` package likewise requires
a database opened with standard quoting. Each database directory must have one
owning process; do not run separate servers against the same directory.

## Login, TLS and client certificates

The default user and database are `slopdb`. With no configured password, the
server uses trust authentication for that username: it does not request or
validate a password. Set a nonempty password through an environment variable to
require SCRAM-SHA-256 authentication. Passwords are not sent in cleartext or
retained in the server configuration after the SCRAM verifier is constructed.

```sh
export SLOPDB_POSTGRES_PASSWORD='choose-a-password'
./slopdb --postgres-tcp 127.0.0.1:5432 \
  --postgres-user app --postgres-database app \
  --postgres-password-env SLOPDB_POSTGRES_PASSWORD ./example-db
psql 'host=127.0.0.1 port=5432 user=app dbname=app sslmode=disable' -W
```

A named password environment variable must exist. An empty value selects trust
authentication. There is one account with access to the whole database; roles,
GRANTs and `pg_hba.conf` are not emulated. SCRAM-SHA-256 is supported; MD5,
cleartext-password authentication and SCRAM channel binding (`-PLUS`) are not.

TLS uses PostgreSQL's SSLRequest negotiation with TLS 1.2 or newer. Supply a PEM
server certificate chain and matching key. Add `--postgres-require-tls` to reject
plaintext logins; a certificate/key alone permits both plaintext and TLS.

To require a client certificate signed by a specific CA, supply the client CA
bundle as well:

```sh
./slopdb --postgres-tcp 127.0.0.1:5432 \
  --postgres-socket /tmp/slopdb-postgres/.s.PGSQL.5432 \
  --postgres-tls-cert server-chain.pem --postgres-tls-key server-key.pem \
  --postgres-tls-ca client-ca.pem ./example-db

psql 'host=127.0.0.1 port=5432 user=slopdb dbname=slopdb sslmode=verify-full sslrootcert=server-ca.pem sslcert=client-chain.pem sslkey=client-key.pem'
```

`--postgres-tls-ca` requires TLS and a verified client certificate on **both**
transports. It trusts only the supplied client CA bundle, not system roots.
Certificate chains, validity periods and client-authentication usage are checked
by Go's TLS/X.509 implementation. The client's `sslrootcert` trusts the server,
whose DNS/IP SAN must match the client's server name. Server and client CAs may
be different. Username and configured SCRAM password checks still apply after
certificate verification. Any certificate trusted by the client CA can access
the configured account; certificate subjects are not mapped to separate roles.
Leaving the password empty with a required client CA gives certificate-only
login for the configured username.

TLS files load at startup. Incomplete certificate/key options, unreadable files,
mismatched keys, invalid CA PEM, or a CA/required TLS without a server certificate
are errors before the CLI opens the database. Restart to reload credentials.
The handshake deadline covers startup, TLS and SCRAM. Direct TLS negotiation
without SSLRequest, GSS encryption, and TLS channel binding are unsupported.
Some clients disable TLS automatically for Unix sockets; a client must explicitly
negotiate TLS there when the server requires it (pgx can be configured to do so).

## Queries and transactions

Each connection owns an independent SlopDB session. Supported protocol messages
include Query, Parse, Bind, Describe, Execute, Close, Flush, Sync and Terminate.
Named and unnamed statements and portals, PostgreSQL `$1` parameters, repeated
or reordered parameters, text/binary parameter formats, and text/binary results
are supported. Describe reads catalog metadata without executing user SELECTs,
DML, volatile expressions or locking reads. Portals execute once and materialize
the result; repeated Execute fetches resume that result, including bounded
fetches and PortalSuspended. Closing a statement leaves already-bound portals
usable. Portals expire at transaction end/Sync outside an explicit transaction.
ROLLBACK TO also closes all portals; cursor survival across savepoint rollback
is not emulated.

Use SlopDB SQL declarations: INTEGER/BIGINT, REAL/DOUBLE, TEXT/VARCHAR and
BLOB/VARBINARY, DECIMAL/NUMERIC and JSON/JSONB. Signed integers are int64 and REALs
are float64. Unsigned values cover uint64. Result columns
use the corresponding PostgreSQL int8, float8, text and bytea wire types;
unsigned values and DECIMAL use exact PostgreSQL numeric encoding and JSON uses
the JSON wire type. JSONB input accepts its version-1 wire wrapper, storing the
same validated JSON value as JSON.
Boolean, int2, int4, int8, float4, float8, text/varchar and bytea parameter OIDs
are accepted. Binary bytea preserves arbitrary bytes, including empty values;
text bytea uses `\x` hex encoding. Legacy octal bytea escapes are unsupported. Mixed/uncertain expression
types use text output to preserve exact values rather than round large integers.
SlopDB predicates normally produce integers, not PostgreSQL boolean columns.

Common casts on parameters, such as `$1::bigint`, `$1::text`, `$1::bytea` and
`$1::float8`, `$1::numeric` and `$1::json`, translate to native CAST expressions.
`CREATE SCHEMA`, qualified names, `SET search_path` and `current_schema()` resolve
virtual schemas inside the same physical database. Other PostgreSQL cast syntax,
dollar-quoted SQL, E-string literals and PostgreSQL-specific data types
are not generally implemented. Casts apply SlopDB type/range semantics.
Parameters are never substituted into SQL strings. Use explicit casts for
ambiguous parameters; unspecified types with no useful context default to text.

Prepared metadata covers ordinary tables, views, joins, derived tables,
nonrecursive CTEs, compound queries and DML RETURNING. Unsupported metadata
shapes, including recursive queries and native EXPLAIN/PRAGMA/SHOW introspection,
require the simple query protocol. Basic PostgreSQL SHOW helpers below also work
through the extended protocol. Reprepare after schema changes; an incompatible
cached result descriptor produces an error, and its uncommitted writes roll
back. For pgx, `QueryExecModeSimpleProtocol` selects the simple protocol. Lib/pq
uses it for unparameterized, unprepared queries.

BEGIN/COMMIT/ROLLBACK, READ COMMITTED and savepoints use native sessions. A SQL
error inside an explicit transaction changes ReadyForQuery to `E`; further work
fails with `25P02` until ROLLBACK or a successful ROLLBACK TO. COMMIT of an aborted
transaction rolls it back. Disconnects and shutdown discard pending work.
Ordinary extended-query cycles use an implicit transaction committed at Sync,
or rolled back on an error. Errors discard pending extended messages through
Sync; explicit transactions retain their failed state. Simple multi-statement
batches without explicit transaction commands run atomically. Batches that mix
explicit transaction commands follow the native statement-by-statement behavior.
A deadlock or other engine-initiated whole-transaction rollback also retains
that failed PostgreSQL transaction state until the client rolls back.
READ COMMITTED remains the default. REPEATABLE READ selects the native retained
snapshot behavior: consistent SELECTs use a transaction snapshot while locking
reads and mutations use current data, following MySQL's isolation semantics.
SERIALIZABLE and read-only transaction requests fail. DDL uses SlopDB's transactional semantics.
Extended cycles build indexes transactionally; a standalone simple Query uses
the native online-index build path.

Replicated commits report `slopdb_commit_position` using ParameterStatus, after
an implicit commit or with explicit COMMIT completion. Command tags remain
unchanged. `SHOW LAST COMMIT POSITION`, `SHOW REPLICATION POSITION` and prepared
`WAIT FOR POSITION $1 TIMEOUT $2` provide logical positions and local visibility
waits. See [logical commit positions](replication.md#logical-commit-positions-and-causal-waits).
Parameter reporting uses the [PostgreSQL ParameterStatus format](https://www.postgresql.org/docs/18/protocol-message-formats.html#PROTOCOL-MESSAGE-FORMATS-PARAMETERSTATUS).

`SET READ BARRIER PREVIOUS` / `NEXT` select certified historical data for simple
and prepared reads; `DEFAULT` restores the node default and `COMMITTED` restores
ordinary admission. See [durable read barriers](replication.md#reading-durable-barriers).

CancelRequest authenticates with the random backend key and interrupts only the
current operation, leaving the session reusable. When TLS is required,
cancellation requests must also negotiate TLS (and present a client certificate
when required). Clients that send unencrypted cancellation packets in this mode
must close their SQL connection to cancel work instead. Closing a connection
always cancels active execution and rolls back its transaction.

Session helpers include `SELECT version()`, `SELECT current_database()`,
`SELECT current_user`, `SELECT session_user`, `SELECT pg_backend_pid()`, and SHOW
for server/client encoding, server version, DateStyle, TimeZone, application_name,
standard_conforming_strings and transaction isolation. SET supports UTF8 client
encoding, UTC timezone, ISO/MDY DateStyle, standard_conforming_strings=on,
application_name, search_path and READ COMMITTED / REPEATABLE READ isolation. DISCARD ALL resets statements,
portals and the application name outside a transaction. Unsupported settings fail.
Startup accepts the corresponding fixed encoding/date/precision options and an
application name; unsupported startup options fail instead of being ignored.

The server advertises a compatibility server version of 15.0. It does not emulate
PostgreSQL catalogs, `information_schema`, roles, sequences/SERIAL,
COPY, PostgreSQL replication, notifications, stored procedures, arrays or UUID types.
Tools that require those features, including psql catalog meta-commands, may fail.
The implemented wire version is 3.0; newer wire versions are rejected.

## Embedding

Package `slopdb/postgresserver` accepts caller-created listeners:

```go
server, err := postgresserver.New(database, postgresserver.Config{
    Username: "app", Password: password, Database: "app",
    TLSConfig: tlsConfig,
})
if err != nil { return err }
defer server.Close()
listener, err := net.Listen("tcp", "127.0.0.1:5432")
if err != nil { return err }
return server.Serve(listener)
```

Call Serve concurrently on separate TCP and Unix listeners to expose both.
Close stops listeners and clients and waits for sessions to roll back; the caller
then closes the database and checks the flush error. Embedded callers control
Unix socket permissions. TLSConfig and its CA pool are cloned. The TLS minimum
is 1.2; RequireTLS rejects plaintext. `tls.RequireAndVerifyClientCert` with an
explicit ClientCAs pool automatically requires TLS. GetConfigForClient is
unsupported; GetCertificate may select server certificates without replacing the
verification policy.

Default limits are 64 MiB per incoming message, 1,024 prepared statements and
1,024 portals per connection, and 10 seconds to authenticate. Config can override
these limits. Query results are materialized, subject to the engine's usual
soft query memory budget rather than a process-wide memory limit.

Protocol references: PostgreSQL's [message flow](https://www.postgresql.org/docs/current/protocol-flow.html),
[message formats](https://www.postgresql.org/docs/current/protocol-message-formats.html)
and [error fields](https://www.postgresql.org/docs/current/protocol-error-fields.html).
