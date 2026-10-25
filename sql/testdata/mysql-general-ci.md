# Independent MySQL general_ci weights

`../mysql_general_ci_weights.go` stores factual primary weights for the legacy
`utf8mb4_general_ci` collation. The nonidentity map contains 1,108 entries.
All other valid BMP code points sort as themselves; supplementary characters
have weight `FFFD`. PAD SPACE comparison trims trailing ASCII spaces; LIKE
matching does not trim them.

The values were generated independently on MariaDB 11.8.6 on 2026-10-25 with
`regenerate_mysql_general_ci.php`, using queries of the form:

```sql
SELECT HEX(WEIGHT_STRING(CONVERT(UNHEX('c3a9') USING utf8mb4)
                       COLLATE utf8mb4_general_ci));
```

The generator queries all 63,488 valid BMP Unicode code points and samples
supplementary values. It needs PHP mysqli/mbstring and a disposable reference
server; ordinary Go tests need neither. The generator only executes SELECTs.
The JSON output's SHA256 was
`b63f3c6a8dd67d392e76e8f8f7b1da71fd506040dcaa923078f5de2ee1073f8f`.

The generated mapping was also compared with all primary weights in MySQL
8.4's `my_unicase_pages_default`; they match exactly. See the primary source
at <https://github.com/mysql/mysql-server/blob/8.4/strings/ctype-utf8.cc>.
No MySQL server implementation source is included or linked into SlopDB.

This is deliberately not NFD/case folding: the general_ci collation gives
sharp-s the single weight S, keeps combining marks as separate weights, and
collapses supplementary code points. For example, `ß = s`, `ß != ss`, and
precomposed `é != e` followed by a combining acute accent in this collation.
