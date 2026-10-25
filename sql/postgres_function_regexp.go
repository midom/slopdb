package sql

import (
	"fmt"
	"regexp"
	"regexp/syntax"
	"strings"
	"unicode"
	"unicode/utf8"
)

func postgresRegexpFunctions() map[string]mysqlFunctionSpec {
	out := map[string]mysqlFunctionSpec{}
	for _, name := range strings.Fields("REGEXP_LIKE REGEXP_COUNT REGEXP_INSTR REGEXP_SUBSTR REGEXP_REPLACE") {
		name := name
		min, max, typ := 2, 3, ColInteger
		switch name {
		case "REGEXP_COUNT":
			max = 4
		case "REGEXP_INSTR":
			max = 7
		case "REGEXP_SUBSTR":
			max, typ = 6, ColText
		case "REGEXP_REPLACE":
			min, max, typ = 3, 6, ColText
		}
		spec := mysqlFunctionSpec{Min: min, Max: max, Return: typ, Eval: func(s *Schema, a []Value) (Value, error) { return postgresRegexpEval(s, name, a) }}
		if typ == ColInteger {
			decl := "INTEGER"
			if name == "REGEXP_LIKE" {
				decl = "BOOLEAN"
			}
			spec.Metadata = func([]ColumnDef) ColumnDef { return ColumnDef{ColType: ColInteger, Declared: decl, Postgres: true} }
		}
		out[name] = spec
	}
	for _, name := range []string{"SUBSTR", "SUBSTRING"} {
		name := name
		out[name] = mysqlFunctionSpec{Min: 2, Max: 3, Return: ColText, Metadata: func(a []ColumnDef) ColumnDef {
			if len(a) > 0 && a[0].ColType == ColBlob {
				return ColumnDef{ColType: ColBlob, Declared: "BYTEA", Postgres: true}
			}
			return ColumnDef{ColType: ColText}
		}, Eval: func(s *Schema, a []Value) (Value, error) {
			if mysqlAnyNull(a) {
				return NewNullValue(), nil
			}
			if name == "SUBSTRING" && a[1].Type == ColText {
				if len(a) != 2 {
					return NewNullValue(), fmt.Errorf("substring SQL regular expression escape syntax is unsupported")
				}
				p, e := s.postgresRegexpCompile(postgresText(a[1]), "")
				if e != nil {
					return NewNullValue(), e
				}
				text := postgresText(a[0])
				m := p.expression.FindStringSubmatchIndex(text)
				if m == nil {
					return NewNullValue(), nil
				}
				group := 0
				if len(m) > 2 {
					group = 1
				}
				if m[2*group] < 0 {
					return NewNullValue(), nil
				}
				return NewTextValue(text[m[2*group]:m[2*group+1]]), nil
			}
			start, e := CastPostgresValue(a[1], "INTEGER")
			if e != nil {
				return NewNullValue(), e
			}
			n := int64(utf8.RuneCountInString(postgresText(a[0])))
			binary := a[0].Type == ColBlob
			if binary {
				n = int64(len(a[0].Blob))
			}
			lo := start.Int - 1
			hi := n
			if len(a) == 3 {
				count, e := CastPostgresValue(a[2], "INTEGER")
				if e != nil {
					return NewNullValue(), e
				}
				if count.Int < 0 {
					return NewNullValue(), fmt.Errorf("negative substring length not allowed")
				}
				hi = lo + count.Int
			}
			if lo < 0 {
				lo = 0
			}
			if lo > n {
				lo = n
			}
			if hi < lo {
				hi = lo
			}
			if hi > n {
				hi = n
			}
			if binary {
				return NewBlobValue(append([]byte(nil), a[0].Blob[lo:hi]...)), nil
			}
			return NewTextValue(string([]rune(postgresText(a[0]))[lo:hi])), nil
		}}
	}
	out["POSITION"] = mysqlFunctionSpec{Min: 2, Max: 2, Return: ColInteger, Metadata: func([]ColumnDef) ColumnDef {
		return ColumnDef{ColType: ColInteger, Declared: "INTEGER", Postgres: true}
	}, Eval: func(s *Schema, a []Value) (Value, error) {
		if mysqlAnyNull(a) {
			return NewNullValue(), nil
		}
		needle, haystack := postgresText(a[0]), postgresText(a[1])
		binary := a[0].Type == ColBlob && a[1].Type == ColBlob
		if binary {
			needle, haystack = string(a[0].Blob), string(a[1].Blob)
		}
		i := strings.Index(haystack, needle)
		if i < 0 {
			return NewIntValue(0), nil
		}
		if !binary {
			i = utf8.RuneCountInString(haystack[:i])
		}
		return NewIntValue(int64(i + 1)), nil
	}}
	out["OVERLAY"] = mysqlFunctionSpec{Min: 3, Max: 4, Return: ColText, Metadata: func(a []ColumnDef) ColumnDef {
		if len(a) > 0 && a[0].ColType == ColBlob {
			return ColumnDef{ColType: ColBlob, Declared: "BYTEA", Postgres: true}
		}
		return ColumnDef{ColType: ColText}
	}, Eval: postgresOverlay}
	return out
}
func postgresOverlay(s *Schema, a []Value) (Value, error) {
	if mysqlAnyNull(a) {
		return NewNullValue(), nil
	}
	start, e := CastPostgresValue(a[2], "INTEGER")
	if e != nil {
		return NewNullValue(), e
	}
	if start.Int <= 0 {
		return NewNullValue(), fmt.Errorf("negative substring length not allowed")
	}
	binary := a[0].Type == ColBlob && a[1].Type == ColBlob
	source, replacement := []rune(postgresText(a[0])), []rune(postgresText(a[1]))
	if binary {
		source, replacement = []rune(string(a[0].Blob)), []rune(string(a[1].Blob))
		source = make([]rune, len(a[0].Blob))
		replacement = make([]rune, len(a[1].Blob))
		for i, b := range a[0].Blob {
			source[i] = rune(b)
		}
		for i, b := range a[1].Blob {
			replacement[i] = rune(b)
		}
	}
	count := int64(len(replacement))
	if len(a) > 3 {
		c, e := CastPostgresValue(a[3], "INTEGER")
		if e != nil {
			return NewNullValue(), e
		}
		count = c.Int
	}
	if count < 0 {
		return NewNullValue(), fmt.Errorf("negative substring length not allowed")
	}
	lo, hi := start.Int-1, start.Int-1+count
	if lo > int64(len(source)) {
		lo = int64(len(source))
	}
	if hi > int64(len(source)) {
		hi = int64(len(source))
	}
	size := uint64(len(source)) + uint64(len(replacement))
	if e = mysqlFunctionSize(s, size*4); e != nil {
		return NewNullValue(), e
	}
	r := append(append(append([]rune{}, source[:lo]...), replacement...), source[hi:]...)
	if binary {
		data := make([]byte, len(r))
		for i, c := range r {
			data[i] = byte(c)
		}
		return NewBlobValue(data), nil
	}
	return NewTextValue(string(r)), nil
}
func (s *Schema) postgresRegexpCompile(pattern, flags string) (*mysqlRegexpProgram, error) {
	if e := mysqlFunctionSize(s, uint64(len(pattern))*64); e != nil {
		return nil, e
	}
	sensitive, dotall, multiline, expanded, literal := true, true, false, false, false
	if strings.HasPrefix(pattern, "***=") {
		literal = true
		pattern = pattern[4:]
	} else if strings.HasPrefix(pattern, "***:") {
		pattern = pattern[4:]
	}
	if strings.HasPrefix(pattern, "(?") {
		if end := strings.IndexByte(pattern, ')'); end > 2 {
			candidate := pattern[2:end]
			allLetters := true
			for _, r := range candidate {
				if !unicode.IsLetter(r) {
					allLetters = false
				}
			}
			if allLetters {
				flags += candidate
				pattern = pattern[end+1:]
			}
		}
	}
	for _, flag := range flags {
		switch flag {
		case 'c':
			sensitive = true
		case 'i':
			sensitive = false
		case 's':
			dotall, multiline = true, false
		case 'n', 'm':
			dotall, multiline = false, true
		case 'p':
			dotall, multiline = false, false
		case 'w':
			dotall, multiline = true, true
		case 'x':
			expanded = true
		case 't':
			expanded = false
		case 'q':
			literal = true
		case 'e':
		case 'b':
			return nil, fmt.Errorf("basic PostgreSQL regular expressions are unsupported")
		default:
			return nil, fmt.Errorf("invalid PostgreSQL regular expression option %q", string(flag))
		}
	}
	if literal {
		pattern = regexp.QuoteMeta(pattern)
	} else {
		var b strings.Builder
		class := false
		for i := 0; i < len(pattern); i++ {
			c := pattern[i]
			if c == '\\' && i+1 < len(pattern) {
				i++
				switch pattern[i] {
				case 'y':
					b.WriteString(`\b`)
				case 'Y':
					b.WriteString(`\B`)
				case 'b':
					b.WriteString(`\x08`)
				case 'B':
					b.WriteString(`\\`)
				case 'Z':
					b.WriteString(`\z`)
				case 'm', 'M':
					return nil, fmt.Errorf("directional PostgreSQL word constraints are unsupported")
				default:
					b.WriteByte('\\')
					b.WriteByte(pattern[i])
				}
				continue
			}
			if expanded && !class {
				if unicode.IsSpace(rune(c)) {
					continue
				}
				if c == '#' {
					for i < len(pattern) && pattern[i] != '\n' {
						i++
					}
					continue
				}
			}
			if c == '[' {
				class = true
				b.WriteByte(c)
				if !dotall && i+1 < len(pattern) && pattern[i+1] == '^' {
					b.WriteString("^\\n")
					i++
				}
				continue
			}
			if c == ']' {
				class = false
			}
			b.WriteByte(c)
		}
		pattern = b.String()
		if strings.Contains(pattern, "*?") || strings.Contains(pattern, "+?") || strings.Contains(pattern, "}?") {
			return nil, fmt.Errorf("non-greedy PostgreSQL regular expressions are unsupported")
		}
	}
	prefix := ""
	if !sensitive {
		prefix += "i"
	}
	if dotall {
		prefix += "s"
	}
	if multiline {
		prefix += "m"
	}
	if prefix != "" {
		pattern = "(?" + prefix + ")" + pattern
	}
	key := "__postgres:" + pattern
	if cached := s.mysqlRegexpPrograms[key]; cached != nil {
		return cached, nil
	}
	r, e := regexp.Compile(pattern)
	if e != nil {
		return nil, fmt.Errorf("unsupported or invalid PostgreSQL regular expression (RE2 subset): %w", e)
	}
	r.Longest()
	parsed, e := syntax.Parse(pattern, syntax.Perl)
	if e != nil {
		return nil, e
	}
	program, e := syntax.Compile(parsed.Simplify())
	if e != nil {
		return nil, e
	}
	p := &mysqlRegexpProgram{expression: r, program: program}
	for _, inst := range program.Inst {
		if inst.Op == syntax.InstEmptyWidth && syntax.EmptyOp(inst.Arg)&(syntax.EmptyBeginLine|syntax.EmptyBeginText|syntax.EmptyWordBoundary|syntax.EmptyNoWordBoundary) != 0 {
			p.anchored = true
		}
	}
	if s.statementActive && len(s.mysqlRegexpPrograms) < 128 {
		if s.mysqlRegexpPrograms == nil {
			s.mysqlRegexpPrograms = map[string]*mysqlRegexpProgram{}
		}
		s.mysqlRegexpPrograms[key] = p
	}
	return p, nil
}
func postgresRegexpEval(s *Schema, name string, a []Value) (Value, error) {
	if mysqlAnyNull(a) {
		return NewNullValue(), nil
	}
	text, pattern := postgresText(a[0]), postgresText(a[1])
	start, n, after, group := int64(1), int64(1), int64(0), int64(0)
	flags := ""
	explicitN := false
	integer := func(index int) (int64, error) { v, e := CastPostgresValue(a[index], "INTEGER"); return v.Int, e }
	var e error
	if name == "REGEXP_LIKE" {
		if len(a) > 2 {
			flags = postgresText(a[2])
		}
	} else if name == "REGEXP_REPLACE" {
		if len(a) > 3 {
			if a[3].Type == ColText && len(a) == 4 {
				flags = postgresText(a[3])
			} else {
				start, e = integer(3)
				if e != nil {
					return NewNullValue(), e
				}
				if len(a) > 4 {
					n, e = integer(4)
					if e != nil {
						return NewNullValue(), e
					}
					explicitN = true
				}
				if len(a) > 5 {
					flags = postgresText(a[5])
				}
			}
		}
		if !explicitN && strings.Contains(flags, "g") {
			n = 0
		}
		flags = strings.ReplaceAll(flags, "g", "")
	} else {
		if len(a) > 2 {
			start, e = integer(2)
			if e != nil {
				return NewNullValue(), e
			}
		}
		switch name {
		case "REGEXP_COUNT":
			if len(a) > 3 {
				flags = postgresText(a[3])
			}
		case "REGEXP_INSTR":
			if len(a) > 3 {
				n, e = integer(3)
			}
			if e == nil && len(a) > 4 {
				after, e = integer(4)
			}
			if len(a) > 5 {
				flags = postgresText(a[5])
			}
			if e == nil && len(a) > 6 {
				group, e = integer(6)
			}
		case "REGEXP_SUBSTR":
			if len(a) > 3 {
				n, e = integer(3)
			}
			if len(a) > 4 {
				flags = postgresText(a[4])
			}
			if e == nil && len(a) > 5 {
				group, e = integer(5)
			}
		}
		if e != nil {
			return NewNullValue(), e
		}
	}
	if start < 1 || n < 0 || n == 0 && name != "REGEXP_REPLACE" || group < 0 || group > 9 || after < 0 || after > 1 {
		return NewNullValue(), fmt.Errorf("invalid regular expression argument")
	}
	p, e := s.postgresRegexpCompile(pattern, flags)
	if e != nil {
		return NewNullValue(), e
	}
	if name == "REGEXP_LIKE" {
		result := int64(0)
		if p.expression.MatchString(text) {
			result = 1
		}
		return NewIntValue(result), nil
	}
	length := int64(utf8.RuneCountInString(text))
	if start > length+1 {
		switch name {
		case "REGEXP_COUNT", "REGEXP_INSTR":
			return NewIntValue(0), nil
		case "REGEXP_REPLACE":
			return NewTextValue(text), nil
		default:
			return NewNullValue(), nil
		}
	}
	offset := 0
	for count := int64(1); count < start; count++ {
		_, n := utf8.DecodeRuneInString(text[offset:])
		offset += n
	}
	if offset > 0 && p.anchored {
		return NewNullValue(), fmt.Errorf("regular expressions with position-sensitive constraints and non-default start are unsupported")
	}
	subject := text[offset:]
	wanted := n
	if name == "REGEXP_COUNT" {
		wanted = 0
	}
	matches, e := s.mysqlRegexpMatches(p, subject, wanted)
	if e != nil {
		return NewNullValue(), e
	}
	// Restore subcaptures for empty matches after nonempty matches, which Go's
	// FindAll suppresses but PostgreSQL counts.
	for _, m := range matches {
		if m[0] == m[1] && len(m) > 2 {
			empty := p.expression.FindStringSubmatchIndex(subject[m[0]:])
			if len(empty) == len(m) && empty[0] == 0 && empty[1] == 0 {
				for i := 2; i < len(m); i++ {
					if empty[i] >= 0 {
						m[i] = m[0] + empty[i]
					}
				}
			}
		}
	}
	if name == "REGEXP_COUNT" {
		return NewIntValue(int64(len(matches))), nil
	}
	if name != "REGEXP_REPLACE" {
		if n > int64(len(matches)) || group > int64(p.expression.NumSubexp()) {
			if name == "REGEXP_INSTR" {
				return NewIntValue(0), nil
			}
			return NewNullValue(), nil
		}
		m := matches[n-1]
		lo, hi := m[2*group], m[2*group+1]
		if lo < 0 {
			if name == "REGEXP_INSTR" {
				return NewIntValue(0), nil
			}
			return NewNullValue(), nil
		}
		if name == "REGEXP_SUBSTR" {
			return NewTextValue(subject[lo:hi]), nil
		}
		at := lo
		if after == 1 {
			at = hi
		}
		return NewIntValue(start + int64(utf8.RuneCountInString(subject[:at]))), nil
	}
	var out strings.Builder
	out.WriteString(text[:offset])
	last := 0
	replacement := postgresText(a[2])
	for i, m := range matches {
		if n > 0 && int64(i+1) != n {
			continue
		}
		out.WriteString(subject[last:m[0]])
		for j := 0; j < len(replacement); j++ {
			if replacement[j] == '\\' && j+1 < len(replacement) {
				c := replacement[j+1]
				if c == '\\' {
					out.WriteByte('\\')
					j++
					continue
				}
				g := -1
				if c == '&' {
					g = 0
				} else if c >= '1' && c <= '9' {
					g = int(c - '0')
				}
				if g >= 0 {
					if g <= p.expression.NumSubexp() && m[2*g] >= 0 {
						out.WriteString(subject[m[2*g]:m[2*g+1]])
					}
					j++
					continue
				}
			}
			out.WriteByte(replacement[j])
		}
		last = m[1]
		if e = mysqlFunctionSize(s, uint64(out.Len())); e != nil {
			return NewNullValue(), e
		}
	}
	out.WriteString(subject[last:])
	if e = mysqlFunctionSize(s, uint64(out.Len())); e != nil {
		return NewNullValue(), e
	}
	return NewTextValue(out.String()), nil
}
