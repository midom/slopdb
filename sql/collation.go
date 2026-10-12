package sql

import (
	"fmt"
	"strings"
	"unicode/utf8"

	"golang.org/x/text/cases"
	"golang.org/x/text/unicode/norm"
)

// Collation controls comparison and key encoding of TEXT values. BINARY is the
// legacy bytewise behavior and remains the default for existing databases.
type Collation string

const BinaryCollation Collation = "BINARY"

// ParseCollation accepts a Unicode normalization form, optionally followed by
// _CI for Unicode case folding. The spelling is stable in persisted catalogs.
func ParseCollation(name string) (Collation, error) {
	mode := Collation(strings.ToUpper(strings.TrimSpace(name)))
	switch mode {
	case BinaryCollation, "NFC", "NFD", "NFKC", "NFKD",
		"NFC_CI", "NFD_CI", "NFKC_CI", "NFKD_CI":
		return mode, nil
	default:
		return "", fmt.Errorf("unsupported collation %q", name)
	}
}

// Key returns the representation used by TEXT equality, ordering, and index
// keys. Case folding precedes a second normalization pass because folding can
// introduce decomposed code points.
func (c Collation) Key(value string) string {
	if c == "" || c == BinaryCollation || !utf8.ValidString(value) {
		return value
	}
	form := strings.TrimSuffix(string(c), "_CI")
	var normalizer norm.Form
	switch form {
	case "NFD":
		normalizer = norm.NFD
	case "NFKC":
		normalizer = norm.NFKC
	case "NFKD":
		normalizer = norm.NFKD
	default:
		normalizer = norm.NFC
	}
	value = normalizer.String(value)
	if strings.HasSuffix(string(c), "_CI") {
		value = normalizer.String(cases.Fold().String(value))
	}
	return value
}

func (c Collation) Compare(a, b string) int {
	return strings.Compare(c.Key(a), c.Key(b))
}
