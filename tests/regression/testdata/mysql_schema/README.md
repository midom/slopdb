# MySQL schema fixture

`schema.sql` contains 64 relational table definitions used by
`TestMySQLSchemaWorkflows`. It exercises signed and unsigned declarations,
AUTO_INCREMENT, binary/text lengths and defaults, composite keys, prefix
indexes and a FULLTEXT search table.

The regression loads the entire schema, inserts fixed rows, verifies binary
predicates, ordered aggregation, fulltext joins and deletion/restoration, then
reopens the database and checks integrity. Expected answers follow from the
fixed inputs; they are never generated from SlopDB. No external application,
SQL server or scripting runtime is required.
