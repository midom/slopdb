package sql

import (
	"fmt"
	"strconv"
	"strings"
)

// PostgreSQL's text[] paths choose an object key or array index from the
// current JSON container. Numeric object keys must remain object keys.
func pgJSONPathValue(document any, parts []string) []any {
	value := document
	for _, part := range parts {
		switch container := value.(type) {
		case map[string]any:
			next, found := container[part]
			if !found {
				return nil
			}
			value = next
		case []any:
			index, err := strconv.ParseInt(part, 10, 64)
			if err != nil {
				return nil
			}
			if index < 0 {
				index += int64(len(container))
			}
			if index < 0 || index >= int64(len(container)) {
				return nil
			}
			value = container[int(index)]
		default:
			return nil
		}
	}
	return []any{value}
}

func parsePGJSONPath(text string) ([]string, error) {
	text = strings.TrimSpace(text)
	if len(text) < 2 || text[0] != '{' || text[len(text)-1] != '}' {
		return nil, fmt.Errorf("invalid JSON path array")
	}
	inner := text[1 : len(text)-1]
	if strings.TrimSpace(inner) == "" {
		return []string{}, nil
	}
	var parts []string
	for pos := 0; pos < len(inner); {
		for pos < len(inner) && (inner[pos] == ' ' || inner[pos] == '\t' || inner[pos] == '\r' || inner[pos] == '\n') {
			pos++
		}
		if pos == len(inner) {
			return nil, fmt.Errorf("invalid empty JSON path element")
		}
		quoted := inner[pos] == '"'
		if quoted {
			pos++
		}
		var out strings.Builder
		closed := !quoted
		for pos < len(inner) {
			c := inner[pos]
			if c == '\\' {
				pos++
				if pos == len(inner) {
					return nil, fmt.Errorf("invalid JSON path escape")
				}
				out.WriteByte(inner[pos])
				pos++
				continue
			}
			if quoted {
				pos++
				if c == '"' {
					closed = true
					break
				}
				out.WriteByte(c)
				continue
			}
			if c == ',' {
				break
			}
			if c == '"' || c == '{' || c == '}' {
				return nil, fmt.Errorf("invalid JSON path element")
			}
			out.WriteByte(c)
			pos++
		}
		if !closed {
			return nil, fmt.Errorf("unterminated JSON path element")
		}
		part := out.String()
		if !quoted {
			part = strings.TrimSpace(part)
			if part == "" {
				return nil, fmt.Errorf("invalid empty JSON path element")
			}
		}
		parts = append(parts, part)
		for pos < len(inner) && (inner[pos] == ' ' || inner[pos] == '\t' || inner[pos] == '\r' || inner[pos] == '\n') {
			pos++
		}
		if pos == len(inner) {
			break
		}
		if inner[pos] != ',' {
			return nil, fmt.Errorf("invalid JSON path separator")
		}
		pos++
		if pos == len(inner) {
			return nil, fmt.Errorf("invalid empty JSON path element")
		}
	}
	return parts, nil
}
