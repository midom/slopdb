package sql

import (
	"strings"
	"sync"
	"unicode/utf8"

	"golang.org/x/text/collate"
	"golang.org/x/text/language"
)

func mysqlCollation(name string) (Collation, bool) {
	switch strings.ToUpper(name) {
	case "UTF8_UNICODE_CI", "UTF8MB3_UNICODE_CI", "UTF8MB4_UNICODE_CI", "UTF8_UNICODE_520_CI", "UTF8MB3_UNICODE_520_CI", "UTF8MB4_UNICODE_520_CI", "UTF8MB4_0900_AI_CI":
		return Collation(strings.ToUpper(name)), true
	case "UTF8_GENERAL_CI", "UTF8MB3_GENERAL_CI", "UTF8MB4_GENERAL_CI":
		return Collation(strings.ToUpper(name)), true
	case "UTF8_BIN", "UTF8MB3_BIN", "UTF8MB4_BIN":
		return Collation(strings.ToUpper(name)), true
	}
	return "", false
}

func unicodeMySQLCollation(c Collation) bool {
	name := string(c)
	return strings.Contains(name, "_UNICODE_") || name == "UTF8MB4_0900_AI_CI"
}

// Compatibility names use the root Unicode collation tables shipped with
// x/text. This supports full Unicode, canonical equivalence and primary-weight
// expansions; it does not emulate every historical MySQL UCA table revision.
var unicodeCollators = sync.Pool{New: func() any { return collate.New(language.Und, collate.IgnoreCase, collate.IgnoreDiacritics) }}

func unicodeMySQLKey(c Collation, value string, pad bool) string {
	if pad && c != "UTF8MB4_0900_AI_CI" {
		value = strings.TrimRight(value, " ")
	}
	if !utf8.ValidString(value) {
		return value
	}
	engine := unicodeCollators.Get().(*collate.Collator)
	var buffer collate.Buffer
	key := string(engine.KeyFromString(&buffer, value))
	unicodeCollators.Put(engine)
	return key
}

// MySQLGeneralCIKey uses one primary weight per Unicode code point, matching
// utf8mb4_general_ci rather than applying Unicode normalization or full case
// folding. In particular, combining marks and expansions retain MySQL's legacy
// behavior. Keys are UTF-8 encodings of weight code points, so byte comparison
// preserves weight order and ASCII LIKE wildcard tokens remain recognizable.
func MySQLGeneralCIKey(value string) string { return mysqlGeneralCIKey(strings.TrimRight(value, " ")) }

func mysqlGeneralCIKey(value string) string {
	if !utf8.ValidString(value) {
		return value
	}
	var key strings.Builder
	for _, r := range value {
		if r > 0xffff {
			r = 0xfffd
		} else if weight, ok := mysqlGeneralCIWeights[uint16(r)]; ok {
			r = rune(weight)
		}
		key.WriteRune(r)
	}
	return key.String()
}

func mysqlCollationKey(c Collation, value string) (string, bool) {
	if _, ok := mysqlCollation(string(c)); !ok {
		return "", false
	}
	if unicodeMySQLCollation(c) {
		return unicodeMySQLKey(c, value, true), true
	}
	if strings.HasSuffix(string(c), "_GENERAL_CI") {
		return MySQLGeneralCIKey(value), true
	}
	return strings.TrimRight(value, " "), true
}

func explicitValueCollation(a, b Value) Collation {
	if a.Collation != "" {
		return a.Collation
	}
	return b.Collation
}

func valueTextKey(v Value, fallback Collation) string {
	if v.Collation != "" {
		return v.Collation.Key(v.Text)
	}
	if v.Binary {
		return v.Text
	}
	return fallback.Key(v.Text)
}
