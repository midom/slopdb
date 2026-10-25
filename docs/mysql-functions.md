# MySQL functions

MySQL function validation, execution, prepared-result metadata and optimizer
volatility share a function registry. Functions work in expressions, predicates,
prepared statements and persisted views/defaults where those expressions are
allowed. Native SQLite-style functions retain their existing behavior outside
MySQL mode; MySQL sessions use MySQL-specific LENGTH, HEX, ROUND and ABS rules.

## Function coverage

| Family | Added functions and syntax |
| --- | --- |
| Strings | CONCAT_WS, LEFT, RIGHT, LPAD, RPAD, REPEAT, REVERSE, SPACE, INSERT, SUBSTRING_INDEX, MID, LOCATE, POSITION, STRCMP, FIELD, FIND_IN_SET, ELT, MAKE_SET, EXPORT_SET, SOUNDEX, LCASE, UCASE |
| Lengths and encodings | ASCII, ORD, CHAR, CHAR_LENGTH, CHARACTER_LENGTH, OCTET_LENGTH, BIT_LENGTH, UNHEX, TO_BASE64, FROM_BASE64 |
| Regular expressions | REGEXP_LIKE, REGEXP_INSTR, REGEXP_SUBSTR, REGEXP_REPLACE |
| Exact numbers and formatting | CEIL, CEILING, FLOOR, SIGN, TRUNCATE, MOD, CONV, BIN, OCT, BIT_COUNT, CRC32, FORMAT, exact MySQL ROUND and ABS |
| Math | PI, POW, POWER, SQRT, EXP, LN, LOG, LOG2, LOG10, SIN, COS, TAN, ASIN, ACOS, ATAN, ATAN2, COT, DEGREES, RADIANS, RAND |
| Date components | YEAR, MONTH, DAY, DAYOFMONTH, DAYOFWEEK, DAYOFYEAR, WEEKDAY, QUARTER, WEEK, WEEKOFYEAR, YEARWEEK, DAYNAME, MONTHNAME, LAST_DAY |
| Date arithmetic | DATE_ADD, DATE_SUB, ADDDATE, SUBDATE, TIMESTAMPADD, TIMESTAMPDIFF, DATEDIFF, PERIOD_ADD, PERIOD_DIFF, EXTRACT |
| Time values | HOUR, MINUTE, SECOND, MICROSECOND, ADDTIME, SUBTIME, TIMEDIFF, TIME_TO_SEC, SEC_TO_TIME, MAKETIME |
| Formatting and calendars | DATE_FORMAT, TIME_FORMAT, STR_TO_DATE, MAKEDATE, FROM_DAYS, TO_DAYS, TO_SECONDS, FROM_UNIXTIME, GET_FORMAT, CONVERT_TZ |
| Clocks | CURDATE, CURRENT_DATE, UTC_DATE, CURTIME, CURRENT_TIME, UTC_TIME, UTC_TIMESTAMP, LOCALTIME, LOCALTIMESTAMP |
| Digests and encryption | MD5, SHA, SHA1, SHA2, RANDOM_BYTES, AES_ENCRYPT, AES_DECRYPT |
| Compression | COMPRESS, UNCOMPRESS, UNCOMPRESSED_LENGTH |
| UUIDs | UUID, IS_UUID, UUID_TO_BIN, BIN_TO_UUID |
| Network addresses | INET_ATON, INET_NTOA, INET6_ATON, INET6_NTOA, IS_IPV4, IS_IPV6, IS_IPV4_COMPAT, IS_IPV4_MAPPED |
| Session and utility functions | USER, SESSION_USER, SYSTEM_USER, SCHEMA, LAST_INSERT_ID, ROW_COUNT, FOUND_ROWS, IS_USED_LOCK, RELEASE_ALL_LOCKS, ISNULL, CHARSET, COLLATION, SLEEP, BENCHMARK, NAME_CONST |

These extend the previously supported IF, GREATEST, LEAST, CONCAT, SUBSTRING,
NOW, SYSDATE, CURRENT_TIMESTAMP, UNIX_TIMESTAMP, CONVERT, named locks and JSON
functions. SQL forms include `POSITION(x IN y)`, `SUBSTRING(x FROM n FOR count)`,
`TRIM(BOTH x FROM y)`, `CHAR(... USING utf8mb4)`,
`EXTRACT(YEAR_MONTH FROM value)` and `DATE_ADD(value, INTERVAL n MONTH)`.
Canonical persisted expressions reparse through the same function validation.

## Behavior

TEXT string positions count Unicode code points; BLOB operations count bytes.
Function-specific NULL and binary-result rules are preserved. UTF-8 charset
aliases utf8, utf8mb3 and utf8mb4 all accept full Unicode, as elsewhere in SlopDB.
Invalid UTF-8 conversions still fail. Explicit collations and SlopDB's configured
text collation continue to control comparisons.

DECIMAL rounding and truncation stay exact, including values beyond float64's
integer precision. Exact decimal ROUND ties round away from zero; approximate
DOUBLE ties use nearest-even. Scientific literals remain approximate in MySQL
mode. Integer arguments distinguish exact numbers, approximate numbers and
integer prefixes of strings. FORMAT supports the documented MySQL locale names,
with checked-in punctuation/grouping answers from an independent MySQL server.

Calendar arithmetic clamps month-end dates, handles leap years and supports all
eight WEEK modes. Time arithmetic preserves signed durations up to the MySQL
838-hour range and handles fractional seconds. The session timezone remains UTC.
Temporal results use SlopDB's existing TEXT representation.

RAND uses MySQL's seed/generator rules. Constant seeded expressions advance
privately across rows; column-dependent seeds are applied per row. Statement
state is released after success or failure and prepared executions start a fresh
seeded sequence. Random, clock, locking, session and blocking functions are
classified as volatile, preventing constant caching or unsafe predicate reuse.

LAST_INSERT_ID records the first generated ID of a multi-row insert, preserves
uint64 values, ignores explicitly assigned IDs, isolates trigger changes and
survives transaction rollback. LAST_INSERT_ID(expr) updates the connection value.
ROW_COUNT reports affected rows or -1 for a result-producing statement.
FOUND_ROWS reports the preceding SELECT's returned row count; SQL_CALC_FOUND_ROWS
is not implemented. SLEEP waits on a timer; SLEEP and BENCHMARK honor native query
cancellation and deadlines.

Generated strings, padding, formatting and decompression check the query memory
budget before allocating expanded results. Invalid arities fail even on empty
inputs. Hashes and UUID/IP conversion return text or binary values with matching
prepared metadata. AES functions currently implement MySQL's default
aes-128-ecb behavior and padding; other block modes and KDF arguments are not
implemented. COMPRESS uses MySQL's length-prefix/zlib format and reads MySQL
compressed values; compressed byte sequences may differ between zlib encoders.

Regular-expression functions support character positions, occurrence selection,
capture replacements and match flags, with a bounded statement-local compiled
pattern cache. They use RE2. Lookarounds and pattern backreferences produce an
unsupported-pattern error. Unicode line-ending and end-anchor rules retain RE2
semantics; anchored SUBSTR/REPLACE patterns at a noninitial position are rejected.
This is not a complete implementation of MySQL's ICU regular-expression engine.

## Verification and boundaries

Permanent tests cover NULLs, Unicode and binary values, exact decimals, uint64,
coercion, prepared calls, metadata, cancellation, memory budgets and persisted
views. Oracle MySQL 8.0.46 supplies independent fixed and generated reference
answers. The normal suite uses checked-in answers; it does not require MySQL.
The independent scalar fixture covers 360 cases through both text and prepared
MySQL protocol execution. Regenerate it against an isolated Oracle MySQL server
using `scripts/regenerate_mysql_function_references.py --socket /path/to/socket`.
The generator obtains answers only from that server.

This is an implementation of the listed functions, not a complete emulation of
every MySQL function, sql_mode, warning, collation or temporal coercion. Native
invalid-value, overflow and cancellation policies remain in effect where noted.
Locale-dependent date-name settings, automatic numeric-context
conversion of temporal TEXT values, uncommon year-zero arithmetic and additional
cryptographic modes remain outside this function set.

`JSON_SEARCH` supports one/all modes, path restrictions and LIKE patterns.
`CONVERT_TZ` accepts UTC/GMT, fixed offsets and available IANA time zones,
including daylight-saving transitions; unknown zones return NULL. Named zones
use the host time-zone database rather than MySQL timezone tables. `GET_FORMAT`
provides the documented DATE/TIME/DATETIME format families. Additional independent
MySQL 8.0.46/PG16.15 facts are checked into `db/testdata/mysql_extra_functions.json`
and the PostgreSQL scalar fixture, with direct and prepared checks.
