# PostgreSQL compatibility coverage

PostgreSQL mode uses the same native Go LSM engine, indexes, constraints,
transactions, WAL and virtual namespaces as the other interfaces. It adds its
own SQL semantics and PostgreSQL wire codecs. No alternate storage engine,
permission enforcement or stored-routine implementation is introduced.

## Types and SQL

BOOLEAN, integer widths and aliases, NUMERIC/DECIMAL, JSON/JSONB, BYTEA, UUID,
DATE/TIME/TIMESTAMP/TIMESTAMPTZ/INTERVAL and text declarations are supported.
General/chained `::` casts, typed literals, escape strings, dollar quotes, ILIKE
and null-safe DISTINCT FROM predicates work with prepared execution too.
PostgreSQL INTEGER is 32-bit, BIGINT is 64-bit, REAL is float4 and DOUBLE
PRECISION is float8. Native and MySQL sessions retain their own dialect rules.
Values remain native encoded rows; PostgreSQL declarations persist separately.

SERIAL aliases currently require a single integer primary key and use the
native automatic-ID allocator. There is no sequence object, nextval/currval,
independent identity generator or sequence name to return from
pg_get_serial_sequence. Temporal input is ISO-oriented with years 1 through 9999 and supported infinity
values; BCE/large years, broad DateStyle/named-timezone input, ISO 8601 interval
syntax and general interval arithmetic remain open. RowDescription type modifiers
currently report -1. SQL literal coercion is incomplete; use explicit casts when
comparing UUID/temporal columns with strings. REGCLASS text output currently uses
numeric OIDs. Arrays, composites, enums/domains, ranges, spatial,
network and full-text PostgreSQL types remain open.

Read committed is the default. REPEATABLE READ retains the existing native
MySQL-style snapshot rule: SELECT uses the transaction snapshot; locking reads
and updates use current committed data. This does not implement PostgreSQL
snapshot-isolation update-conflict behavior. SERIALIZABLE, predicate locking
and read-only transaction modes remain unsupported.

## Functions

A separate PostgreSQL registry implements string and byte operations, quoting,
FORMAT `%s`/`%I`/`%L`, concatenation with PostgreSQL NULL rules, regex functions,
JSON/JSONB builders and scalar access/update functions, exact NUMERIC operations,
mathematical functions, date parts/truncation/formatting, transaction/statement/
wall clocks and interruptible PG_SLEEP. Prepared Describe reads static function
metadata and never executes functions.

Transaction timestamps stay stable through statements and savepoints; wall
clock timestamps advance. JSON builders distinguish SQL NULL, JSON null,
booleans and exact numbers. JSON path array arguments use PostgreSQL textual
array notation; this does not provide a general SQL array type.

Regex matching uses Go RE2 with supported PostgreSQL flags; unsupported regular
expression constructs and flags fail. Locale, timezone, collation and formatting
coverage is a subset. In particular the date template engine does not implement
all PostgreSQL numeric/interval formatting and normalization rules. Native SQL
functions outside the PostgreSQL overrides can retain native semantics. The 222
checked-in PostgreSQL 16.15 scalar reference cases define the independently verified subset;
this is not a claim of complete PostgreSQL function equivalence. NUMERIC
transcendental overloads currently use double precision, RANDOM retains the
native integer behavior, and SETSEED is unsupported.

## Catalogs

The following virtual relations are generated from the executing session's
catalog view and support ordinary predicates, joins, grouping and ordering:

- `pg_namespace`, `pg_class`, `pg_attribute`, `pg_type`, `pg_index`
- `pg_constraint`, `pg_attrdef`, `pg_tables`, `pg_indexes`, `pg_views`
- `pg_database`, `pg_am`
- `information_schema.schemata`, `tables`, `columns`, `views`
- `information_schema.table_constraints`, `key_column_usage`, `referential_constraints`

Unqualified system relation names and `pg_catalog.` names are supported. The
native root schema appears as `public`. Ordinary table/index OIDs derive from
persisted object IDs; other virtual objects use deterministic synthetic OIDs.
Primary keys link to their maintained native indexes; native clustered keys
without a separate index entry have a logical catalog-index projection. Access methods report LSM. These catalogs are read-only
and expose a documented subset of PostgreSQL columns rather than a copy of every
PostgreSQL internal relation. Index vectors/constraint column lists are textual
metadata, not general PostgreSQL array/vector values. ACLs remain NULL and no
permissions are inferred from owner labels.

Helpers include to_regclass, to_regnamespace, format_type, pg_table_is_visible,
pg_get_indexdef, pg_get_constraintdef, pg_get_expr, pg_get_viewdef,
pg_encoding_to_char, pg_get_userbyid, pg_client_encoding and current_setting for
supported settings. Expression metadata stores native SQL text, not PostgreSQL
internal node trees. Name casts and catalog helpers resolve this session's
namespace view without executing a user query. Live psql schema/table listing (`\dn`, `\dt`) is verified. Full `\d`/ORM introspection,
pg_description/pg_depend/pg_proc and extension catalogs remain open.

## COPY

COPY FROM STDIN and TO STDOUT support text, CSV and PostgreSQL binary formats,
selected columns/defaults, query output, headers, delimiter/null/quote/escape
options, UTF8, and FORCE_QUOTE/FORCE_NULL/FORCE_NOT_NULL. Both simple and extended
protocol paths are tested with actual pgx and lib/pq drivers. Binary codecs
include exact NUMERIC, JSONB, UUID and temporal types.

FROM runs atomically inside its own transaction or a savepoint in the caller's
transaction. Errors, cancellation, disconnects and malformed streams do not
publish partial imports. TO uses native streaming query snapshots and bounded
output chunks. Rows are decoded incrementally, but pending native transaction
writes consume memory proportional to an import; COPY is not a memory-bounded
bulk transaction engine. Input currently inserts each row through the native
prepared DML path, so an immediate foreign key cannot refer forward to a later
row in the same COPY unless the constraint is deferred.

Server-side filenames and PROGRAM, FREEZE, WHERE/default-marker options,
parameterized COPY queries and non-UTF8 transcoding remain unsupported.

## Other remaining gaps

Standalone sequence/identity APIs, arrays and set-returning functions,
PostgreSQL logical/physical replication, LISTEN/NOTIFY, extensions, partitions,
materialized views and complete PostgreSQL SQL/system catalogs remain open.
Protocol compression is not part of this PostgreSQL 3.0 implementation. The
MySQL compressed wire mode and MySQL binlog source do not imply PostgreSQL
replication support. Authentication/TLS remain available; authorization and
stored routines stay deferred as requested.

See [server operation and protocol details](postgres-server.md). Catalog layout
and behavior were checked against PostgreSQL 16 documentation for
[pg_class](https://www.postgresql.org/docs/16/catalog-pg-class.html) and
[information_schema.columns](https://www.postgresql.org/docs/16/infoschema-columns.html).
