# MySQL protocol server

SlopDB can serve an existing database directory to MySQL clients over TCP, a
Unix socket, or both. The wire protocol does not change SlopDB's SQL semantics.
One process owns the database; use client connections instead of opening that
directory from another process while the server is running.

```sh
go build -o slopdb .
./slopdb --mysql-tcp 127.0.0.1:3306 ./example-db
mysql --protocol=TCP -h 127.0.0.1 -P 3306 -u slopdb slopdb
```

For Unix sockets, or both transports together:

```sh
./slopdb --mysql-socket /tmp/slopdb.sock ./example-db
mysql --protocol=SOCKET --socket=/tmp/slopdb.sock -u slopdb slopdb

./slopdb --mysql-tcp 127.0.0.1:3306 --mysql-socket /tmp/slopdb.sock ./example-db
```

Specifying either listener flag starts server mode, without reading SQL from
stdin. Unix sockets are created with permissions `0600` and removed on clean
shutdown. An existing socket or file is never removed to make room for a new
listener. Remove a stale socket yourself after verifying its server has stopped.
Listener startup errors close any listeners already created and exit nonzero.
SIGINT and SIGTERM disconnect clients, cancel queries, roll back pending
transactions, and close and flush the database. Flush failures exit nonzero.

The default login is `slopdb` with an empty password; the exposed database name
is `slopdb`. Set a password without placing it in command-line arguments:

```sh
export SLOPDB_MYSQL_PASSWORD='choose-a-password'
./slopdb --mysql-tcp 127.0.0.1:3306 \
  --mysql-user app --mysql-database app \
  --mysql-password-env SLOPDB_MYSQL_PASSWORD ./example-db
mysql --protocol=TCP -h 127.0.0.1 -P 3306 -u app -p app
```

A named password environment variable must exist; an explicitly empty value
permits passwordless login. Authentication uses `mysql_native_password`. There
is one configured account, with access to the whole database. MySQL account/grant
management is not implemented. TLS encryption and CA-verified client certificate
authentication can be enabled as described below.

Server mode enables [MySQL quoting](sql-quoting.md) by default. Pass
`--mysql-quoting=false` to use SlopDB's ordinary quoting rules. Other CLI storage,
query timeout, lock timeout, compression and durability flags apply normally;
WAL and WAL fsync remain enabled by default.

## TLS and client certificate authentication

The server supports MySQL's SSLRequest negotiation with TLS 1.2 and 1.3 over
both TCP and Unix sockets. Supply a PEM server certificate chain and matching
private key to enable TLS. Add `--mysql-require-tls` to reject plaintext logins:

```sh
./slopdb --mysql-tcp 127.0.0.1:3306 \
  --mysql-tls-cert server-chain.pem --mysql-tls-key server-key.pem \
  --mysql-require-tls ./example-db
```

Without `--mysql-require-tls`, a server configured with only a certificate/key
accepts both encrypted and plaintext connections. Without any TLS configuration,
it accepts plaintext connections and does not advertise TLS.

To require a client certificate signed by a specific CA, add `--mysql-tls-ca`:

```sh
./slopdb --mysql-tcp 127.0.0.1:3306 --mysql-socket /tmp/slopdb.sock \
  --mysql-tls-cert server-chain.pem --mysql-tls-key server-key.pem \
  --mysql-tls-ca client-ca.pem ./example-db

mysql --protocol=TCP -h 127.0.0.1 -P 3306 -u slopdb \
  --ssl-mode=VERIFY_IDENTITY --ssl-ca=server-ca.pem \
  --ssl-cert=client-chain.pem --ssl-key=client-key.pem slopdb
```

`--mysql-tls-ca` is the server's trust bundle for **client** certificates. It
implies required TLS and required certificate verification on every connection,
including Unix sockets. Plaintext cannot bypass certificate authentication.
Only chains trusted by that bundle are accepted; system roots are not added.
Certificates must be within their validity period and valid for client
authentication. Clients should send any intermediate certificates in their
certificate chain. The client's `--ssl-ca`, in contrast, trusts the **server**
certificate, whose DNS name or IP SAN must match the connection hostname.
The two CA bundles can contain different issuers.

A verified certificate is an additional login requirement. Clients must still
use the configured username and password. To use certificate authentication
without a password, leave the server password empty and provide a trusted client
certificate. Any valid certificate trusted by the configured client CA can
access the configured account; subjects are not mapped to separate accounts.

Certificate/key flags must be supplied together. A client CA or required TLS
without a server certificate/key is an error, as are unreadable certificates,
mismatched keys and an invalid CA PEM file. These CLI errors are reported before
opening the database or creating listeners. Certificates and trust bundles are
loaded at startup; restart the server to change them. The authentication timeout
covers SSLRequest, TLS negotiation and the encrypted MySQL login. Shutdown also
interrupts incomplete TLS handshakes.

## Queries and connections

Each authenticated connection owns an independent SlopDB session. Supported
commands include text queries, ping, database selection, binary prepared
statements with typed parameters, long parameter data, statement reset/close,
and connection reset. Results carry column metadata, NULLs, binary values,
affected-row counts, generated IDs and transaction status. Clients may connect
without selecting a database; tables still refer to the single exposed database.
`USE` and the handshake database name must select that configured name.

`BEGIN` / `START TRANSACTION`, `COMMIT`, `ROLLBACK` and savepoints use the normal
SlopDB transaction implementation. Disconnect and connection reset discard
uncommitted writes. `SET [SESSION] autocommit=0` starts transactions lazily;
`COMMIT` and `ROLLBACK` finish them, and the next SQL statement starts a new one.
Setting autocommit back to `1` commits pending work. The default is autocommit.
`SET [SESSION] TRANSACTION ISOLATION LEVEL READ COMMITTED` is accepted; other
isolation levels and unsupported settings return errors. DDL follows SlopDB's
transactional behavior and does not implicitly commit an explicit transaction.

Replicated commits include `slopdb_commit_position` in the OK packet's info
field and, when `CLIENT_SESSION_TRACK` is negotiated, its system-variable state
block. `SHOW LAST COMMIT POSITION`, `SHOW REPLICATION POSITION` and prepared
`WAIT FOR POSITION ? TIMEOUT ?` expose the same logical-position workflow.
See [logical commit positions](replication.md#logical-commit-positions-and-causal-waits).
The framing follows the [MySQL OK packet specification](https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_basic_ok_packet.html).

`SET READ BARRIER PREVIOUS` / `NEXT` select certified historical data for reads;
`DEFAULT` restores the node default and `COMMITTED` restores ordinary admission.
See [durable read barriers](replication.md#reading-durable-barriers).

Basic client helpers include `SHOW DATABASES`, `SELECT DATABASE()`,
`SELECT VERSION()`, `SELECT CONNECTION_ID()`, `SELECT @@autocommit`, and
`SET NAMES utf8` / `utf8mb3` / `utf8mb4`. Other queries run through the SlopDB SQL
engine, including its existing `SHOW`, `DESCRIBE` and introspection support.
MySQL system schemas and general `@@` variables are not emulated.

Prepared parameters use anonymous `?` placeholders. Result metadata is sent at
execution; prepare reports parameter metadata and no result columns. Clients
that require result metadata before execution are not supported. Statements
are never executed to discover metadata during prepare. MySQL cursors, multiple
statements in a single query, multiple result sets, compression, replication,
LOCAL INFILE, stored procedures, and administrative MySQL commands are not
implemented or advertised. Unsupported commands return protocol errors.

SlopDB validates stored column types. MySQL wire string parameters bind as TEXT;
wire BLOB parameters bind as BLOB. Some drivers, including Go's MySQL driver,
encode both strings and byte slices as wire strings. Use `CAST(? AS BLOB)` for
BLOB assignments with those drivers, including empty byte slices. SQL BLOB
literals such as `X'00ff'` also work. Unsigned parameters above `MaxInt64` are
rejected, consistent with SlopDB's native integer range. REAL values retain
float64 precision on the wire. Mixed-type expression columns use a string wire
type to preserve their values without rounding large integers.

## Go clients and embedding

A standard MySQL driver can connect with a DSN such as:

```text
slopdb@tcp(127.0.0.1:3306)/slopdb
slopdb@unix(/tmp/slopdb.sock)/slopdb
```

For embedding, package `slopdb/mysqlserver` accepts caller-created listeners:

```go
server, err := mysqlserver.New(database, mysqlserver.Config{
    Username: "app", Password: password, Database: "app",
})
if err != nil { return err }
defer server.Close()
listener, err := net.Listen("tcp", "127.0.0.1:3306")
if err != nil { return err }
return server.Serve(listener)
```

Call `Serve` concurrently with separate TCP and Unix listeners to expose both.
`Server.Close` closes its listeners and clients and waits for session cleanup;
the caller then closes the database and checks its flush error. The caller
controls Unix socket permissions when embedding. Default limits are 64 MiB per
incoming logical packet or accumulated long parameters, 1,024 prepared statements
per connection, and 10 seconds to authenticate. `mysqlserver.Config` can override
these limits.

Embedded servers enable TLS with `mysqlserver.Config.TLSConfig`, a `*tls.Config`
containing server certificates, and can set `RequireTLS: true`. For mutual TLS,
set `TLSConfig.ClientAuth = tls.RequireAndVerifyClientCert` and set
`TLSConfig.ClientCAs` to an explicit `x509.CertPool`. This automatically requires
TLS, even when `RequireTLS` is false. `New` clones the TLS configuration and CA
pool and enforces a minimum of TLS 1.2. `GetConfigForClient` is unsupported so
dynamic configurations cannot override the verification policy; use
`GetCertificate` for certificate selection. Embedded servers retain their
database's existing SQL quoting options.

Protocol layout references: MySQL's [TLS upgrade](https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_connection_phase_packets_protocol_ssl_request.html),
[connection handshake](https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_connection_phase_packets_protocol_handshake_v10.html),
[prepared execution](https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_stmt_execute.html),
and [binary result sets](https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_binary_resultset.html).
