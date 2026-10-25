# Live session activity

MySQL, PostgreSQL and native sessions share one activity registry. It also
includes the native default session. Public backend IDs come from one allocator
and match MySQL `CONNECTION_ID()` and PostgreSQL `pg_backend_pid()`; they survive
native catalog reloads. `slopdb_session_id` identifies the native lock owner.
All sessions are visible while permissions remain deferred.

```sql
SHOW PROCESSLIST;
SHOW FULL PROCESSLIST;
SELECT id, user, db, command, time, state, info
FROM information_schema.processlist
WHERE command = 'Query';

SELECT pid, usename, datname, application_name, state, query,
       wait_event_type, wait_event
FROM pg_catalog.pg_stat_activity
WHERE state <> 'idle';
```

Unqualified `pg_stat_activity` is available in PostgreSQL mode. The qualified
`information_schema.processlist` relation is available in all dialects. Both
relations describe their own typed columns through information_schema and
PostgreSQL's catalog projections. Preparing a query reads static metadata and
never calls the live activity provider.

The views describe active statements, idle sessions, idle transactions and
PostgreSQL failed transactions. PostgreSQL `query` retains the most recent
statement while idle; MySQL `info` is NULL when idle. Query text is source SQL,
including prepared placeholders, without expanding bound parameter values.
Retention is bounded to 64 KiB at a Unicode boundary. Normal SHOW limits `Info`
to 100 Unicode code points; FULL displays all retained text. MySQL `time` is
whole seconds since the active query began or the idle state changed, sampled
with the result. It is zero for an unknown or future timestamp.

MySQL `host` uses the peer host and port. PostgreSQL `client_addr` contains the
numeric address, `client_port` contains the TCP port or -1 for Unix sockets,
and `client_hostname` remains NULL because no reverse DNS lookup is performed.
Until an INET type and codec exist, `client_addr` has a truthful TEXT descriptor.
Activity timestamps have TIMESTAMPTZ metadata; `pid` and `client_port` use int4.
A native backend ID outside the signed int4 range leaves `pid` NULL and remains
available unabridged in the uint64 `slopdb_backend_id` extension.

Actual native lock waits appear as PostgreSQL `wait_event_type = 'Lock'`, with
the native lock kind in `wait_event`. Both providers expose
`slopdb_wait_kind` and `slopdb_wait_resource`; the resource is an opaque native
lock identity. MySQL also displays a waiting state. These facts describe real
waits and do not implement the complete PostgreSQL pg_locks relation. Missing
catalog/user IDs, leader PIDs, transaction IDs and query IDs remain NULL.

Activity aliases and joins in the same SELECT source list share one sampled
registry view. Results are live observations, independent of transaction data
snapshots, and can change immediately after collection. Collection never takes
another session's SQL gate or scans user rows. Simple activity inspections and
SHOW PROCESSLIST remain available during follower or recovery states. Queries
that reference ordinary data, subqueries or mixed data joins retain normal
replication admission; an activity relation does not bypass that policy.
