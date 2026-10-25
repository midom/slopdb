package sql

import (
	"strings"
	"unicode/utf8"
)

func mysqlCollation(name string) (Collation, bool) {
	switch strings.ToUpper(name) {
	case "UTF8_GENERAL_CI", "UTF8MB3_GENERAL_CI", "UTF8MB4_GENERAL_CI":
		return Collation(strings.ToUpper(name)), true
	case "UTF8_BIN", "UTF8MB3_BIN", "UTF8MB4_BIN":
		return Collation(strings.ToUpper(name)), true
	}
	return "", false
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
