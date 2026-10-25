package sql

import (
	"strings"
	"unicode/utf8"
)

type likeToken struct {
	r    rune
	wild bool
}
type likePatternKey struct {
	pattern   string
	escape    byte
	sensitive bool
}
type likePattern struct {
	tokens    []likeToken
	valid     bool
	sensitive bool
}

func foldLikeRune(r rune) rune {
	if r >= 'A' && r <= 'Z' {
		return r + ('a' - 'A')
	}
	return r
}

func compileLike(pattern string, escape byte) likePattern {
	return compileLikeMode(pattern, escape, false)
}

func compileLikeMode(pattern string, escape byte, sensitive bool) likePattern {
	p := likePattern{valid: true, sensitive: sensitive}
	for pos := 0; pos < len(pattern); {
		r, n := utf8.DecodeRuneInString(pattern[pos:])
		pos += n
		if escape != 0 && r == rune(escape) {
			if pos == len(pattern) {
				p.valid = false
				return p
			}
			r, n = utf8.DecodeRuneInString(pattern[pos:])
			pos += n
			p.tokens = append(p.tokens, likeToken{r: p.fold(r)})
		} else {
			p.tokens = append(p.tokens, likeToken{r: p.fold(r), wild: r == '%' || r == '_'})
		}
	}
	return p
}

// Greedy wildcard matching uses byte positions at rune boundaries. It retains
// ASCII-only case folding, Unicode '_', escapes, and matching across newlines,
// without allocating a dynamic-programming matrix for every input string.
func (p likePattern) fold(r rune) rune {
	if p.sensitive {
		return r
	}
	return foldLikeRune(r)
}

func (p likePattern) match(input string) bool {
	if !p.valid {
		return false
	}
	si, ti := 0, 0
	star, mark := -1, 0
	for si < len(input) {
		if ti < len(p.tokens) && p.tokens[ti].wild && p.tokens[ti].r == '%' {
			star = ti
			ti++
			mark = si
			continue
		}
		r, n := utf8.DecodeRuneInString(input[si:])
		if ti < len(p.tokens) && (p.tokens[ti].wild && p.tokens[ti].r == '_' || !p.tokens[ti].wild && p.tokens[ti].r == p.fold(r)) {
			si += n
			ti++
			continue
		}
		if star < 0 || mark == len(input) {
			return false
		}
		_, n = utf8.DecodeRuneInString(input[mark:])
		mark += n
		si = mark
		ti = star + 1
	}
	for ti < len(p.tokens) && p.tokens[ti].wild && p.tokens[ti].r == '%' {
		ti++
	}
	return ti == len(p.tokens)
}

func (s *Schema) matchLike(input, pattern string, escape byte) bool {
	return s.matchLikeMode(input, pattern, escape, false)
}

func (s *Schema) matchLikeMode(input, pattern string, escape byte, sensitive bool) bool {
	if s.selectDepth == 0 {
		return compileLikeMode(pattern, escape, sensitive).match(input)
	}
	key := likePatternKey{pattern, escape, sensitive}
	p, ok := s.likePatterns[key]
	if !ok {
		p = compileLikeMode(pattern, escape, sensitive)
		if len(s.likePatterns) < 4096 {
			if s.likePatterns == nil {
				s.likePatterns = make(map[likePatternKey]likePattern)
			}
			s.likePatterns[key] = p
		}
	}
	return p.match(input)
}

func (s *Schema) matchLikeValues(input, pattern Value, escape byte) bool {
	left, right := input.String(), pattern.String()
	if s.MySQLMode() {
		left, right = mysqlStringValue(input), mysqlStringValue(pattern)
	}
	collation := explicitValueCollation(input, pattern)
	if collation == "" && !input.Binary && !pattern.Binary {
		collation = s.collation
	}
	sensitive := false
	if _, ok := mysqlCollation(string(collation)); ok {
		sensitive = strings.HasSuffix(string(collation), "_BIN")
		if !sensitive {
			left = mysqlGeneralCIKey(left)
			right = mysqlGeneralCIKey(right)
		}
	} else if collation != "" {
		left = collation.Key(left)
		right = collation.Key(right)
	} else if _, ok := MySQLSession(s.ctx); ok && (input.Binary || pattern.Binary) {
		sensitive = true
	}
	return s.matchLikeMode(left, right, escape, sensitive)
}
