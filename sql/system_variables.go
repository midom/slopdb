package sql

import "strings"

// SystemVariableLike matches ASCII-insensitive system names with SQL %, _ and
// backslash escapes. Dynamic programming bounds work for adversarial patterns.
func SystemVariableLike(value, pattern string) bool {
	value = strings.ToLower(value)
	pattern = strings.ToLower(pattern)
	previous := make([]bool, len(value)+1)
	previous[0] = true
	for j := 0; j < len(pattern); j++ {
		c := pattern[j]
		escaped := false
		if c == '\\' {
			j++
			if j >= len(pattern) {
				return false
			}
			c = pattern[j]
			escaped = true
		}
		next := make([]bool, len(value)+1)
		if c == '%' && !escaped {
			next[0] = previous[0]
			for i := 1; i <= len(value); i++ {
				next[i] = previous[i] || next[i-1]
			}
		} else {
			for i := 1; i <= len(value); i++ {
				next[i] = previous[i-1] && ((c == '_' && !escaped) || c == value[i-1])
			}
		}
		previous = next
	}
	return previous[len(value)]
}
