# Shared catalog relations

`information_schema` uses the session's native immutable catalog view for both
MySQL and PostgreSQL, and is also queryable through the native API. It includes
private temporary tables and uncommitted DDL visible to that session. A rollback
or subsequent statement sees the appropriate new catalog view. It does not scan
user rows or execute column defaults. Prepared metadata uses the same static,
typed table layouts as execution.

Available relations are `schemata`, `tables`, `columns`, `views`, `statistics`,
`table_indexes`, `table_constraints`, `key_column_usage`,
`referential_constraints`, `check_constraints`, `constraint_column_usage`,
`triggers`, `character_sets`, `collations`,
`collation_character_set_applicability`, `engines`, `routines`, and `parameters`.
The last two are empty because stored routines are unsupported. `table_indexes`
and `slopdb_*` columns are native extensions. Native object IDs are independent
of PostgreSQL wire OIDs. `statistics` describes logical primary/unique keys as
well as maintained indexes, including prefix lengths and fulltext indexes.

Naming is an adapter concern: the root namespace is `main` through the native
API, the configured root database through MySQL, and `public` through PostgreSQL.
MySQL's catalog name is `def`; PostgreSQL uses the configured database name.
The common relation layouts include SQL standard columns and MySQL extensions.
PostgreSQL columns retain PostgreSQL type names and SQL default-expression text;
MySQL columns expose literal defaults as values and expression defaults as SQL.
`slopdb_create_sql` exposes the same rendered definition as SHOW CREATE for
native tables, views and triggers. It is populated on demand; virtual system
views have no persisted CREATE definition. System catalogs describe their own
static table/column layouts as SYSTEM VIEW entries.

`slopdb_default_expression` always contains recorded SQL rather than an evaluated
value. `slopdb_declared_type` preserves the native declared spelling.

SHOW DATABASES, TABLES, COLUMNS, INDEX and TRIGGERS project the same information
schema frames. SHOW DATABASES lists actual virtual databases; the schemata
relation additionally describes system namespaces. SHOW TABLE STATUS projects
the same table facts and sampled storage estimates. Selecting table size,
cardinality or auto-increment fields requests the same bounded native estimates;
name-only catalog queries avoid storage work. Runtime SHOW commands
use separate runtime providers. SHOW CREATE renders the session's catalog
objects with the selected dialect's SQL spelling.

Simple literal equalities on schema/table names filter relations before column
or view metadata is derived. Other predicates use the ordinary SELECT pipeline.
Catalog results are currently materialized, and capturing the catalog view
copies metadata map headers; this is not a constant-memory catalog cursor.
Explicitly requested table cardinalities are sampled and storage sizes include
retained versions; they are estimates rather than exact row counts. Unknown
sizes, creation
timestamps, ownership and privileges remain NULL; introspection does not invent
a permissions model. Complete upstream catalogs, arrays and PostgreSQL internal
expression binary representations remain unsupported.
