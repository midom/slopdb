# MediaWiki compatibility

SlopDB's MySQL endpoint can install and run the unmodified MediaWiki 1.46.2
release. The compatibility work was checked on the `test` machine with PHP
8.5.4, a fresh database, the stock installer and the stock MySQL driver.

Every table uses SlopDB's LSM storage. `CREATE TABLE ... ENGINE=...` and
`ALTER TABLE ... ENGINE=...` accept and ignore the requested name, including
unrecognized engine names. `SHOW ENGINES`, `SHOW TABLE STATUS`, MySQL
`SHOW CREATE TABLE` and `information_schema.tables` report `LSM`. There are no
InnoDB or MyISAM storage implementations or engine-dependent search modes.

## Implemented compatibility

The core schema's 64 tables now work, including integer type declarations and
unsigned bounds, `AUTO_INCREMENT`, fixed padded `BINARY`, `VARBINARY`, the
TEXT/BLOB families, `ENUM`, floating point declarations and fractional temporal
values. MySQL sessions convert strings to binary columns, apply the supported
strict/nonstrict assignment rules and preserve native API behavior outside
MySQL mode. Insert-ignore warnings and MySQL changed/matched row counts are
available through the protocol.

Query additions include `IF`, `GREATEST`/`LEAST`, MySQL casts, character-set
conversion, `CONCAT`, `SUBSTRING`, fractional timestamps, `UNIX_TIMESTAMP`,
ordered/distinct/multiple-argument `GROUP_CONCAT`, Unicode general-ci expression
collations, grouped joins, index hints and `STRAIGHT_JOIN`. Joined UPDATE and
DELETE collect distinct physical target rows before applying normal constraints,
triggers and index maintenance. `CREATE TABLE LIKE` copies schema definitions
and visible index names into independently owned catalog objects.

Session-private temporary tables support ordinary queries, joins, indexes and
transactional row changes. Definitions survive transaction rollback and disappear
when the session closes or resets. Their rows and indexes occupy a reserved
range in the same LSM, bypass the WAL and replication, and are removed on
shutdown and on restart. They may spill to SSTables; they are not a separate
in-memory storage engine.

`FULLTEXT` declarations persist and `MATCH ... AGAINST` executes natural-language
and Boolean searches, including required/excluded terms, prefixes, phrases and
groups. Searches respect statement snapshots, private transaction changes,
cancellation and query budgets. Search terms use a four-character minimum and
the documented stopword vocabulary in the source. An inverted index stores term
postings and positions in the LSM. Selective searches fetch candidate documents
from those postings; corpus statistics use lightweight document markers.
Relevance scores are SlopDB's own scores, not an emulation of either MySQL
storage engine's scoring algorithm.

The endpoint also supplies the installer/session settings, named locks with real
cross-connection ownership and waits, warning packets and `SHOW WARNINGS`,
`CLIENT_FOUND_ROWS`, declared prepared-result types, MySQL-shaped EXPLAIN rows,
and the system-catalog views used by MediaWiki. Login accounts and password
hashes persist; permission checks are disabled. Additional databases are virtual
schemas in the same physical LSM, and `USE` changes default name resolution.
Qualified joins and transactionally switching schemas are supported. See
[MySQL server](mysql-server.md) for protocol and replication details.

## Verification

The permanent `TestMediaWikiCoreSchemaAndWorkflows` regression loads the complete
release schema from [the checked-in factual fixture](../tests/regression/testdata/mediawiki/README.md),
checks binary prefix searches, group concatenation, fulltext joins and quoted
revision-ID deletion/restoration, then closes and reopens the database and checks
integrity. Other ordinary Go tests cover parser errors, typed prepared execution,
assignment failures, temporary isolation, joined mutations, schema copies,
collation weights, account/grant persistence and named-lock concurrency.

The optional HTTP runner verifies actual application results:

```sh
python3 scripts/mediawiki_acceptance.py \
  --base-url http://127.0.0.1:33882 --username Admin \
  --password-file /private/admin-password --report /tmp/mediawiki-acceptance.json
```

It creates a uniquely named page and account. Checks include login, two edits,
revision content, body and prefix search, category membership, recent changes,
protection, move, deletion, complete restoration, watchlists, administrator group
names, contributions, backlinks, account creation and seven special pages. The
report removes password/token fields. The runner needs a separately installed
wiki and is not required by `go test ./...`.

On the test machine, installation and the HTTP checks passed against a fresh
SlopDB database. The stock `maintenance/run.php update --quick` and `runJobs --maxjobs=100`
commands also passed, followed by the HTTP checks after a server restart. Binaries, databases, private logs and reports
remain outside this repository.

## Boundaries

This covers the core application's audited requirements, not complete MySQL
compatibility or arbitrary extensions. Signed integers retain exact int64 values;
unsigned integers cover the complete uint64 range. Unique and nonunique prefix
indexes use character prefixes for TEXT and byte prefixes for binary columns,
including uniqueness checks and residual query predicates. Unicode charset
conversion maps utf8, utf8mb3 and utf8mb4 to full Unicode, without imposing the
three-byte restriction; invalid UTF-8 is still rejected. Other character
encodings are not silently rewritten. DECIMAL has exact decimal arithmetic and
up to 65 digits; JSON validates documents and supplies functions and path accessors.

MySQL sessions default to REPEATABLE READ: ordinary SELECTs retain the first
consistent-read snapshot, while locking reads and mutations use current committed
data plus private writes. READ COMMITTED remains available and is the native API
default. Ordinary DDL retains SlopDB's
transactional semantics. Joined mutations require physical source tables;
derived mutation sources are rejected. Temporary foreign keys are unsupported.
Persistent foreign keys support composite/self references, CASCADE, SET NULL,
SET DEFAULT and deferred checks. MATCH FULL/PARTIAL and MySQL gap locks are not
implemented.
Stored routines, general MySQL system schemas and other protocol limits are
listed in [MySQL server](mysql-server.md).
