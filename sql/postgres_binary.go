package sql

import (
	"encoding/hex"
	"fmt"
	"strings"
)

// ParsePostgresBytea accepts PostgreSQL's hex and legacy escape text formats.
func ParsePostgresBytea(text string) ([]byte, error) {
	invalid := func() ([]byte, error) { return nil, fmt.Errorf("invalid input syntax for type BYTEA") }
	out := make([]byte, 0, len(text))
	if strings.HasPrefix(text, "\\x") {
		for i := 2; i < len(text); {
			if text[i] == ' ' || text[i] == '\t' || text[i] == '\n' || text[i] == '\r' {
				i++
				continue
			}
			if i+1 >= len(text) {
				return invalid()
			}
			var value [1]byte
			if _, e := hex.Decode(value[:], []byte(text[i:i+2])); e != nil {
				return invalid()
			}
			out = append(out, value[0])
			i += 2
		}
		return out, nil
	}
	for i := 0; i < len(text); i++ {
		if text[i] != '\\' {
			out = append(out, text[i])
			continue
		}
		i++
		if i >= len(text) {
			return invalid()
		}
		if text[i] == '\\' {
			out = append(out, '\\')
			continue
		}
		if i+2 >= len(text) || text[i] < '0' || text[i] > '3' || text[i+1] < '0' || text[i+1] > '7' || text[i+2] < '0' || text[i+2] > '7' {
			return invalid()
		}
		out = append(out, (text[i]-'0')*64+(text[i+1]-'0')*8+text[i+2]-'0')
		i += 2
	}
	return out, nil
}
