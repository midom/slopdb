package sql

import "unicode/utf8"

type likeToken struct {
	r    rune
	wild bool
}
type likePatternKey struct {
	pattern string
	escape  byte
}
type likePattern struct {
	tokens []likeToken
	valid  bool
}

func foldLikeRune(r rune) rune {
	if r >= 'A' && r <= 'Z' {
		return r + ('a' - 'A')
	}
	return r
}

func compileLike(pattern string, escape byte) likePattern {
	p := likePattern{valid: true}
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
			p.tokens = append(p.tokens, likeToken{r: foldLikeRune(r)})
		} else {
			p.tokens = append(p.tokens, likeToken{r: foldLikeRune(r), wild: r == '%' || r == '_'})
		}
	}
	return p
}

// Greedy wildcard matching uses byte positions at rune boundaries. It retains
// ASCII-only case folding, Unicode '_', escapes, and matching across newlines,
// without allocating a dynamic-programming matrix for every input string.
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
		if ti < len(p.tokens) && (p.tokens[ti].wild && p.tokens[ti].r == '_' || !p.tokens[ti].wild && p.tokens[ti].r == foldLikeRune(r)) {
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
	if s.selectDepth == 0 {
		return compileLike(pattern, escape).match(input)
	}
	key := likePatternKey{pattern, escape}
	p, ok := s.likePatterns[key]
	if !ok {
		p = compileLike(pattern, escape)
		if len(s.likePatterns) < 4096 {
			if s.likePatterns == nil {
				s.likePatterns = make(map[likePatternKey]likePattern)
			}
			s.likePatterns[key] = p
		}
	}
	return p.match(input)
}
