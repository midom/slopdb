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
when the session closes or resets; temporary rows and catalog entries are never
written to the persistent database.

`FULLTEXT` declarations persist and `MATCH ... AGAINST` executes natural-language
and Boolean searches, including required/excluded terms, prefixes, phrases and
groups. Searches respect statement snapshots, private transaction changes,
cancellation and query budgets. Search terms use a four-character minimum and
the documented stopword vocabulary in the source. Fulltext evaluation currently
scans documents; the maintained document indexes do not provide inverted posting
lists. Relevance scores are SlopDB's own scores, not an emulation of either MySQL
storage engine's scoring algorithm.

The endpoint also supplies the installer/session settings, named locks with real
cross-connection ownership and waits, warning packets and `SHOW WARNINGS`,
`CLIENT_FOUND_ROWS`, declared prepared-result types, MySQL-shaped EXPLAIN rows,
and the system-catalog views used by MediaWiki. Accounts, password hashes and
global/database grants persist. Additional databases have separate native
storage and `USE` selects their sessions. See [MySQL server](mysql-server.md)
for the supported administration commands and database/backup boundaries.

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
SlopDB database. Maintenance job execution and close/reopen checks are part of
the application acceptance run. Binaries, databases, private logs and reports
remain outside this repository.

## Boundaries

This covers the core application's audited requirements, not complete MySQL
compatibility or arbitrary extensions. Integers remain signed int64 internally;
unsigned values above `MaxInt64` are rejected. Nonunique prefix indexes preserve
their requested lengths in metadata and use full-value physical keys. Unique
prefix indexes are rejected. Unicode charset conversion supports utf8, utf8mb3
and utf8mb4; other character encodings are not silently rewritten.

Only READ COMMITTED isolation is supported. Ordinary DDL retains SlopDB's
transactional semantics. Joined mutations require physical source tables;
derived mutation sources are rejected. Temporary foreign keys are unsupported.
Cross-database joins and switching databases within a transaction are unsupported.
Stored routines, general MySQL system schemas and other protocol limits are
listed in [MySQL server](mysql-server.md).
