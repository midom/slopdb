# SQL quoting options

SlopDB keeps its existing quoting rules by default: single quotes delimit strings,
double quotes delimit identifiers, and backslashes in strings remain literal.
Backticks and square brackets can also delimit identifiers.

Enable MySQL default quoting when opening the database:

```sh
./slopdb --mysql-quoting ./example-db
```

```go
options := storage.DefaultOptions()
options.MySQLQuoting = true
database, err := db.Open("./example-db", options)
```

The option applies to SQL execution, preparation and streaming queries through
`Database`, `Session`, and `Tx`. It is copied at `Open` and remains fixed for that
open database. Choose it again when reopening; it is not a persisted database
setting. `.open` in the CLI carries the selected option to the next database,
and `.status` displays it.

With the option enabled, both single and double quotes delimit strings. Use
backticks for identifiers, doubling an embedded backtick. Existing square-bracket
identifier quoting remains available. Quotes can also be doubled inside strings.
Adjacent string literals concatenate, and SELECT aliases can use string quotes.

```sql
CREATE TABLE `orders` (`id` INTEGER PRIMARY KEY, `note` TEXT);
INSERT INTO `orders` VALUES (1, "Customer said \"hello\"; delivered\n");
SELECT `note`, "string literal" AS "label" FROM `orders`;
SELECT 'first ' "second" AS combined;
```

MySQL-mode strings interpret `\0`, `\b`, `\n`, `\r`, `\t`, and `\Z` as NUL,
backspace, newline, carriage return, tab, and ASCII 26. `\\`, `\'`, and `\"`
represent a backslash, single quote, and double quote. Other escaped characters
lose the backslash. `\%` and `\_` preserve the backslash for LIKE pattern matching;
MySQL-mode LIKE/NOT LIKE expressions use backslash as their default escape.
An explicit one-byte `ESCAPE` overrides that default. These rules follow
[MySQL string literals](https://dev.mysql.com/doc/refman/8.0/en/string-literals.html).

```sql
SELECT "a_b" LIKE "a\_b" AS literal_underscore;
SELECT "100%" LIKE "100\%" AS literal_percent;
SELECT "C:\\data\\file" AS path;
```

Prepared bindings contain values rather than SQL syntax: a bound Go string
containing `\n` retains those two characters. Only quoted text in the SQL statement
is unescaped. Strings and quoted identifiers containing `?` do not create
parameters.

## Schema persistence and SQL output

DEFAULT, CHECK, views and trigger bodies persist canonical SQL after the input
has been parsed. Their meaning remains the same after reopen, renames and other
schema changes, including when reopening with the default quoting rules.

`Dump`, the schema SQL getters, and `SHOW CREATE TABLE/VIEW/TRIGGER` render SQL
using the open database's quoting option. MySQL-mode output uses backtick
identifiers and escapes string bytes, so it can be restored into another database
opened with `MySQLQuoting: true`. The CLI's `.dump`, `.save`, `.schema`, `.read`,
`.import`, `.plan`, and `.explain` follow the selected mode. Pipe/file statement
splitting and REPL completion account for escaped quotes and quoted semicolons.

## Direct parsing

Applications using the SQL package directly can choose the same input rules:

```go
options := sql.ParseOptions{MySQLQuoting: true}
statements, err := sql.ParseWithOptions(text, options)
prepared, err := sql.PrepareWithOptions(text, options)
```

`NewTokenizerWithOptions` uses the same option. Existing `Parse`, `Prepare`, and
`NewTokenizer` calls retain their default behavior. `Database.ParseOptions()`
returns the options to use when independently parsing SQL for that database.
