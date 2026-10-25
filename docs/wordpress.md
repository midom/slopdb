# WordPress on SlopDB

Stock WordPress 7.1.3 was installed on `test` on 2026-10-25 through SlopDB's
MySQL listener. The [official WordPress package](https://wordpress.org/download/)
passed `wp core verify-checksums`. Application source and database code were
not patched. The fresh single-site installation has its normal twelve tables.

## Access and processes

The installation is `/home/ubuntu/slopdb-wordpress-20261025` on `test`.

| Item | Location |
| --- | --- |
| Site | `http://127.0.0.1:33886/` |
| Administration | `http://127.0.0.1:33886/wp-admin/` |
| MySQL | `127.0.0.1:33374`, user/database `wordpress` |
| Application / data | `wordpress/`, `database-verified/` |
| Backend binary | `slopdb-release` |
| Admin user | `admin` |
| Admin password | `artifacts/admin-password`, mode 0600 |
| Process IDs | `artifacts/slopdb.pid`, `artifacts/http.pid` |
| Private reports and smoke scripts | `artifacts/`, mode 0700 |

Both listeners bind loopback. Run `ssh -L 33886:127.0.0.1:33886 test` and open
`http://127.0.0.1:33886/` locally. Read the password file over SSH; credentials
are excluded from reports and repository files. The backend uses:

```sh
./slopdb-release --mysql-tcp 127.0.0.1:33374 \
  --mysql-user wordpress --mysql-database wordpress database-verified
```

PHP 8.5.4 serves the normal document root using its development server and
`artifacts/router.php` for non-file requests. mysqli, mbstring, XML, curl,
fileinfo and GD are available; GD thumbnail creation was verified. Production
HTTP/TLS hosting and outbound mail are not configured. Existing MediaWiki
services use separate data and ports; their HTTP endpoint still returned 200.

## Recorded verification

The tested binary was built from clean revision `12c881d`; its SHA256 is
`d70c5d6b683d9606c9f8f33003b5eb163a963789c3a043eed003931073a352cc`.
`artifacts/release-buildinfo.txt` retains its build metadata.

Sixteen real HTTP/authenticated REST checks passed: homepage, admin login,
block editor, category creation, draft creation, publishing/content updates,
comment creation/readback, REST search, published page, HTTP search, PNG upload
and image metadata, thumbnail generation, media download, attachment metadata
and settings updates. Retained content includes post 19, comment 4 and image 21.
A normal core `wp_scheduled_delete` cron event also ran successfully.

After clean shutdown/restart, six checks verified the published post, comment,
image metadata/thumbnails, taxonomy, settings and HTTP page. They passed again
after integrity verification and full compaction. Integrity reported 216 rows
and 498 index entries before and after maintenance. Five SSTables were rewritten
into one 46,527-byte L6 SSTable, with L0–L5 empty. These are fixture observations,
not storage or throughput benchmarks. Live `SHOW LSM STATUS` reported that
layout; `@@read_only=0` and `@@log_bin=0` confirmed writable standalone operation
without activating CDC.

Final HTTP, restart and maintenance logs contained no WordPress database errors,
PHP warnings or PHP fatal errors. Reports include `http-check-release.log`,
`http-results.json`, `persistence-results.json`, `cron-check.log`,
`integrity-vacuum.log` and `storage-maintained.tsv`. Integrity used SlopDB's
`.integrity` command with `--allow-dot-commands` while the server was stopped;
`PRAGMA integrity_check` is not a supported alias.

## Fixes covered by permanent tests

The installer/web queries required nonreserved `count` identifiers, stored
Unicode table/column collations, table-local index names such as `meta_key`,
declared `ID` spelling, bare qualified-column labels and duplicate result slots.
Qualified stars (`SELECT t.*, tt.*`) now share typed projection/prepare logic.
`SQL_CALC_FOUND_ROWS` counts once, at the statement snapshot, before pagination.
Grouping by a complete primary key permits dependent base columns in SELECT
and ORDER BY, while arbitrary joined-table columns still fail validation.

Regression sources include `db/mysql_wordpress_columns_test.go`,
`db/mysql_unicode_collation_test.go`, `db/mysql_local_indexes_test.go`,
`db/qualified_star_test.go`, `db/wordpress_query_semantics_test.go`,
`mysqlserver/qualified_labels_test.go` and `mysqlserver/found_rows_test.go`.
Ordinary Go tests need no WordPress, PHP or external database.

## Remaining boundaries

This validates the stock single-site workflows listed above. Multisite,
arbitrary plugins/themes, mail delivery and the complete upstream MySQL
administrative toolchain are not certified. Exact historic MySQL UCA tailoring
remains a [collation limit](text-collation.md). Storage always uses LSM; ENGINE
clauses do not select InnoDB/MyISAM. Authorization and stored routines remain
deferred. See [MySQL server](mysql-server.md), [functions](mysql-functions.md)
and [shared catalogs](catalogs.md) for other compatibility boundaries.
