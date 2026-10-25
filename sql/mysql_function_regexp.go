package sql

import (
	"fmt"
	"regexp"
	"regexp/syntax"
	"strconv"
	"strings"
	"unicode/utf8"
)

type mysqlRegexpProgram struct {
	expression *regexp.Regexp
	program    *syntax.Prog
	anchored   bool
}

func mysqlRegexpFunctions() map[string]FunctionSpec {
	out := make(map[string]FunctionSpec)
	for _, name := range []string{"REGEXP_LIKE", "REGEXP_INSTR", "REGEXP_SUBSTR", "REGEXP_REPLACE"} {
		name := name
		minimum, maximum, typ := 2, 3, ColInteger
		switch name {
		case "REGEXP_INSTR":
			maximum = 6
		case "REGEXP_SUBSTR":
			maximum = 5
			typ = ColText
		case "REGEXP_REPLACE":
			minimum, maximum, typ = 3, 6, ColText
		}
		out[name] = FunctionSpec{Min: minimum, Max: maximum, Return: typ, Eval: func(s *Schema, a []Value) (Value, error) { return mysqlRegexpEval(s, name, a) }}
	}
	return out
}

func (s *Schema) mysqlRegexpCompile(pattern, flags string, insensitive bool) (*mysqlRegexpProgram, error) {
	if pattern == "" {
		return nil, fmt.Errorf("empty MySQL regular expression")
	}
	if err := functionResultSize(s, uint64(len(pattern))*64); err != nil {
		return nil, err
	}
	multiline, dotall := false, false
	for _, flag := range flags {
		switch flag {
		case 'c':
			insensitive = false
		case 'i':
			insensitive = true
		case 'm':
			multiline = true
		case 'n':
			dotall = true
		case 'u':
		default:
			return nil, fmt.Errorf("invalid MySQL regular expression match_type %q", flags)
		}
	}
	prefix := ""
	if insensitive {
		prefix += "i"
	}
	if multiline {
		prefix += "m"
	}
	if dotall {
		prefix += "s"
	}
	if prefix != "" {
		pattern = "(?" + prefix + ")" + pattern
	}
	if cached := s.mysqlRegexpPrograms[pattern]; cached != nil {
		return cached, nil
	}
	expression, err := regexp.Compile(pattern)
	if err != nil {
		return nil, fmt.Errorf("unsupported or invalid MySQL regular expression (RE2): %w", err)
	}
	parsed, err := syntax.Parse(pattern, syntax.Perl)
	if err != nil {
		return nil, err
	}
	program, err := syntax.Compile(parsed.Simplify())
	if err != nil {
		return nil, err
	}
	compiled := &mysqlRegexpProgram{expression: expression, program: program}
	for _, instruction := range program.Inst {
		if instruction.Op == syntax.InstEmptyWidth && syntax.EmptyOp(instruction.Arg)&(syntax.EmptyBeginLine|syntax.EmptyBeginText) != 0 {
			compiled.anchored = true
		}
	}
	if s.statementActive && len(s.mysqlRegexpPrograms) < 128 {
		used := len(pattern)
		for key := range s.mysqlRegexpPrograms {
			used += len(key)
		}
		if uint64(used)*64 <= uint64(s.memoryBudget()) {
			if s.mysqlRegexpPrograms == nil {
				s.mysqlRegexpPrograms = make(map[string]*mysqlRegexpProgram)
			}
			s.mysqlRegexpPrograms[pattern] = compiled
		}
	}
	return compiled, nil
}

// RE2's FindAll skips an empty match directly after a nonempty match. ICU
// includes it. Test the program's epsilon paths at that boundary to restore
// the MySQL occurrence sequence without searching every prefix again.
func mysqlRegexpEmptyAt(program *syntax.Prog, text string, at int, visited []uint32, generation uint32, stack []uint32) bool {
	before, after := rune(-1), rune(-1)
	if at > 0 {
		before, _ = utf8.DecodeLastRuneInString(text[:at])
	}
	if at < len(text) {
		after, _ = utf8.DecodeRuneInString(text[at:])
	}
	context := syntax.EmptyOpContext(before, after)
	stack = append(stack[:0], uint32(program.Start))
	for len(stack) > 0 {
		pc := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if visited[pc] == generation {
			continue
		}
		visited[pc] = generation
		instruction := program.Inst[pc]
		switch instruction.Op {
		case syntax.InstMatch:
			return true
		case syntax.InstAlt, syntax.InstAltMatch:
			stack = append(stack, instruction.Arg, instruction.Out)
		case syntax.InstCapture, syntax.InstNop:
			stack = append(stack, instruction.Out)
		case syntax.InstEmptyWidth:
			if syntax.EmptyOp(instruction.Arg)&context == syntax.EmptyOp(instruction.Arg) {
				stack = append(stack, instruction.Out)
			}
		}
	}
	return false
}

func (s *Schema) mysqlRegexpMatches(program *mysqlRegexpProgram, text string, wanted int64) ([][]int, error) {
	width := 2 * (program.expression.NumSubexp() + 1)
	unit := uint64(width*8+32) * 4
	limit := int(uint64(s.memoryBudget()) / unit)
	if limit < 1 {
		return nil, fmt.Errorf("regular expression match state exceeds query memory budget")
	}
	readLimit := limit + 1
	if wanted > 0 && wanted <= int64(limit) {
		limit = int(wanted)
		readLimit = limit
	}
	matches := program.expression.FindAllStringSubmatchIndex(text, readLimit)
	if len(matches) > limit {
		return nil, fmt.Errorf("regular expression match state exceeds query memory budget")
	}
	out := make([][]int, 0, len(matches))
	visited := make([]uint32, len(program.program.Inst))
	stack := make([]uint32, 0, len(program.program.Inst))
	generation := uint32(0)
	for i, match := range matches {
		if err := s.checkInterrupt(); err != nil {
			return nil, err
		}
		out = append(out, match)
		if wanted > 0 && int64(len(out)) >= wanted {
			return out, nil
		}
		end := match[1]
		if match[0] == end || i+1 < len(matches) && matches[i+1][0] == end {
			continue
		}
		generation++
		if mysqlRegexpEmptyAt(program.program, text, end, visited, generation, stack) {
			empty := make([]int, width)
			for j := range empty {
				empty[j] = -1
			}
			empty[0], empty[1] = end, end
			out = append(out, empty)
			if wanted > 0 && int64(len(out)) >= wanted {
				return out, nil
			}
		}
	}
	return out, nil
}

type mysqlRegexpReplacementPart struct {
	literal string
	group   int
}

func mysqlRegexpReplacement(text string, groups int) ([]mysqlRegexpReplacementPart, error) {
	var out []mysqlRegexpReplacementPart
	var literal strings.Builder
	flush := func() {
		if literal.Len() > 0 {
			out = append(out, mysqlRegexpReplacementPart{literal: literal.String(), group: -1})
			literal.Reset()
		}
	}
	for at := 0; at < len(text); {
		if text[at] == '\\' {
			at++
			if at == len(text) {
				return nil, fmt.Errorf("trailing escape in regular expression replacement")
			}
			literal.WriteByte(text[at])
			at++
			continue
		}
		if text[at] != '$' {
			literal.WriteByte(text[at])
			at++
			continue
		}
		flush()
		at++
		start := at
		for at < len(text) && text[at] >= '0' && text[at] <= '9' {
			at++
		}
		if start == at {
			return nil, fmt.Errorf("invalid capture group in regular expression replacement")
		}
		group, err := strconv.Atoi(text[start:at])
		if err != nil || group > groups {
			return nil, fmt.Errorf("regular expression replacement capture group out of range")
		}
		out = append(out, mysqlRegexpReplacementPart{group: group})
	}
	flush()
	return out, nil
}

func mysqlRegexpEval(s *Schema, name string, a []Value) (Value, error) {
	if functionAnyNull(a) {
		return NewNullValue(), nil
	}
	for i, v := range a {
		if i < 2 || name == "REGEXP_REPLACE" && i == 2 {
			if v.Type == ColBlob {
				return NewNullValue(), fmt.Errorf("binary strings are unsupported by MySQL regular expression functions")
			}
		}
	}
	text, pattern := mysqlStringValue(a[0]), mysqlStringValue(a[1])
	if !utf8.ValidString(text) || !utf8.ValidString(pattern) {
		return NewNullValue(), fmt.Errorf("invalid UTF-8 in regular expression")
	}
	position, occurrence, after := int64(1), int64(1), int64(0)
	flags := ""
	switch name {
	case "REGEXP_LIKE":
		if len(a) > 2 {
			flags = mysqlStringValue(a[2])
		}
	case "REGEXP_INSTR":
		if len(a) > 2 {
			position = mysqlStringCount(a[2])
		}
		if len(a) > 3 {
			occurrence = mysqlStringCount(a[3])
		}
		if len(a) > 4 {
			after = mysqlStringCount(a[4])
			if after != 0 && after != 1 {
				return NewNullValue(), fmt.Errorf("REGEXP_INSTR return_option must be 0 or 1")
			}
		}
		if len(a) > 5 {
			flags = mysqlStringValue(a[5])
		}
	case "REGEXP_SUBSTR":
		if len(a) > 2 {
			position = mysqlStringCount(a[2])
		}
		if len(a) > 3 {
			occurrence = mysqlStringCount(a[3])
		}
		if len(a) > 4 {
			flags = mysqlStringValue(a[4])
		}
	case "REGEXP_REPLACE":
		occurrence = 0
		if len(a) > 3 {
			position = mysqlStringCount(a[3])
		}
		if len(a) > 4 {
			occurrence = mysqlStringCount(a[4])
		}
		if len(a) > 5 {
			flags = mysqlStringValue(a[5])
		}
	}
	if occurrence < 0 || occurrence == 0 && name != "REGEXP_REPLACE" {
		occurrence = 1
	}
	length := int64(utf8.RuneCountInString(text))
	if position < 1 || position > length && !(text == "" && position == 1) {
		return NewNullValue(), fmt.Errorf("regular expression position is out of bounds")
	}
	start := len(mysqlStringSlice(text, 0, position-1, false))
	subject := text[start:]
	collation := explicitValueCollation(a[0], a[1])
	if collation == "" {
		collation = s.collation
	}
	program, err := s.mysqlRegexpCompile(pattern, flags, strings.HasSuffix(string(collation), "_CI"))
	if err != nil {
		return NewNullValue(), err
	}
	if name == "REGEXP_LIKE" {
		if program.expression.MatchString(text) {
			return NewIntValue(1), nil
		}
		return NewIntValue(0), nil
	}
	if start > 0 && program.anchored && name != "REGEXP_INSTR" {
		return NewNullValue(), fmt.Errorf("anchored regular expression with non-default position is unsupported")
	}
	if name == "REGEXP_REPLACE" && text == "" {
		return NewTextValue(""), nil
	}
	matches, err := s.mysqlRegexpMatches(program, subject, occurrence)
	if err != nil {
		return NewNullValue(), err
	}
	if name != "REGEXP_REPLACE" {
		if occurrence > int64(len(matches)) {
			if name == "REGEXP_INSTR" {
				return NewIntValue(0), nil
			}
			return NewNullValue(), nil
		}
		match := matches[occurrence-1]
		if name == "REGEXP_SUBSTR" {
			return NewTextValue(subject[match[0]:match[1]]), nil
		}
		return NewIntValue(position + int64(utf8.RuneCountInString(subject[:match[after]]))), nil
	}
	parts, err := mysqlRegexpReplacement(mysqlStringValue(a[2]), program.expression.NumSubexp())
	if err != nil {
		return NewNullValue(), err
	}
	var result strings.Builder
	last := 0
	write := func(part string) error {
		if err := functionResultSize(s, uint64(result.Len())+uint64(len(part))); err != nil {
			return err
		}
		result.WriteString(part)
		return nil
	}
	if err = write(text[:start]); err != nil {
		return NewNullValue(), err
	}
	for i, match := range matches {
		if occurrence > 0 && int64(i+1) != occurrence {
			continue
		}
		if err = write(subject[last:match[0]]); err != nil {
			return NewNullValue(), err
		}
		for _, part := range parts {
			value := part.literal
			if part.group >= 0 {
				lo, hi := match[2*part.group], match[2*part.group+1]
				value = ""
				if lo >= 0 {
					value = subject[lo:hi]
				}
			}
			if err = write(value); err != nil {
				return NewNullValue(), err
			}
		}
		last = match[1]
	}
	if err = write(subject[last:]); err != nil {
		return NewNullValue(), err
	}
	return NewTextValue(result.String()), nil
}
