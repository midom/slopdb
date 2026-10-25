# MediaWiki schema fixture

`schema-1.46.2.sql` contains the factual core table definitions from the
unmodified MediaWiki 1.46.2 release's `sql/mysql/tables-generated.sql`.
Installer placeholders and comments are removed and table names are quoted as
the installer does; names, types, defaults,
constraints and index prefix lengths are retained. No expected query answers
are generated from SlopDB.

Source: <https://releases.wikimedia.org/mediawiki/1.46/mediawiki-1.46.2.tar.gz>

Release SHA256:
`8f7f937f8bbc1acd4cef1c5362a95f248dcd242020f2588798eb2f73ac2e69ba`

The release schema has 64 core table definitions, including its FULLTEXT
search table.
