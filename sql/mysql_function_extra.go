package sql

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"
)

func mysqlExtraFunctions() map[string]FunctionSpec {
	out := map[string]FunctionSpec{}
	out["GET_FORMAT"] = FunctionSpec{Min: 2, Max: 2, Return: ColText, Eval: func(s *Schema, a []Value) (Value, error) {
		if functionAnyNull(a) {
			return NewNullValue(), nil
		}
		formats := map[string][3]string{
			"USA":      {"%m.%d.%Y", "%h:%i:%s %p", "%Y-%m-%d %H.%i.%s"},
			"JIS":      {"%Y-%m-%d", "%H:%i:%s", "%Y-%m-%d %H:%i:%s"},
			"ISO":      {"%Y-%m-%d", "%H:%i:%s", "%Y-%m-%d %H:%i:%s"},
			"EUR":      {"%d.%m.%Y", "%H.%i.%s", "%Y-%m-%d %H.%i.%s"},
			"INTERNAL": {"%Y%m%d", "%H%i%s", "%Y%m%d%H%i%s"},
		}
		f, ok := formats[strings.ToUpper(mysqlStringValue(a[1]))]
		if !ok {
			return NewNullValue(), nil
		}
		i := 0
		switch strings.ToUpper(mysqlStringValue(a[0])) {
		case "DATE":
		case "TIME":
			i = 1
		case "DATETIME", "TIMESTAMP":
			i = 2
		default:
			return NewNullValue(), fmt.Errorf("invalid GET_FORMAT temporal type")
		}
		return NewTextValue(f[i]), nil
	}}
	out["CONVERT_TZ"] = FunctionSpec{Min: 3, Max: 3, Return: ColText, Eval: func(s *Schema, a []Value) (Value, error) {
		if functionAnyNull(a) {
			return NewNullValue(), nil
		}
		civil := mysqlParseCivil(a[0])
		if !civil.valid {
			return NewNullValue(), nil
		}
		from, ok := mysqlTimeZone(mysqlStringValue(a[1]))
		if !ok {
			return NewNullValue(), nil
		}
		to, ok := mysqlTimeZone(mysqlStringValue(a[2]))
		if !ok {
			return NewNullValue(), nil
		}
		t := civil.t
		instant := time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second(), t.Nanosecond(), from)
		// MySQL's 64-bit conversion range starts just after the Unix epoch and
		// ends in year3001. Out-of-range datetimes retain their civil value.
		if instant.Unix() < 1 || instant.Unix() > 32536771199 {
			return NewTextValue(mysqlFormatDate(t, false, civil.fsp)), nil
		}
		converted := instant.In(to)
		if converted.Year() < 1 || converted.Year() > 9999 {
			return NewNullValue(), nil
		}
		return NewTextValue(mysqlFormatDate(converted, false, civil.fsp)), nil
	}}
	out["NAME_CONST"] = FunctionSpec{Min: 2, Max: 2, Dialects: OnlyMySQL, Metadata: func(a []ColumnDef) ColumnDef {
		if len(a) > 1 {
			return a[1]
		}
		return ColumnDef{ColType: ColUnknown}
	}, EvalExpr: func(s *Schema, e *FuncCallExpr, row map[string]Value) (Value, error) {
		for _, arg := range e.Args {
			if !mysqlNameConstLiteral(arg) {
				return NewNullValue(), fmt.Errorf("incorrect arguments to NAME_CONST")
			}
		}
		name, err := s.evalExpr(e.Args[0], row)
		if err != nil {
			return NewNullValue(), err
		}
		if name.Null {
			return NewNullValue(), fmt.Errorf("NAME_CONST name must not be null")
		}
		return s.evalExpr(e.Args[1], row)
	}}
	out["JSON_SEARCH"] = FunctionSpec{Min: 3, Max: -1, Return: ColJSON, Eval: mysqlJSONSearch}
	return out
}
func mysqlTimeZone(name string) (*time.Location, bool) {
	if len(name) == 6 && (name[0] == '+' || name[0] == '-') && name[3] == ':' {
		h, e := strconv.Atoi(name[1:3])
		m, f := strconv.Atoi(name[4:])
		if e != nil || f != nil || m > 59 || h > 14 || (h == 14 && m > 0) || (name[0] == '-' && h == 14) {
			return nil, false
		}
		seconds := (h*60 + m) * 60
		if name[0] == '-' {
			seconds = -seconds
		}
		return time.FixedZone(name, seconds), true
	}
	zone, e := time.LoadLocation(name)
	return zone, e == nil
}

type jsonSearchNode struct {
	value any
	path  string
}

func jsonSearchChildren(n jsonSearchNode) []jsonSearchNode {
	var children []jsonSearchNode
	switch v := n.value.(type) {
	case []any:
		for i, x := range v {
			children = append(children, jsonSearchNode{x, n.path + "[" + strconv.Itoa(i) + "]"})
		}
	case map[string]any:
		keys := make([]string, 0, len(v))
		for k := range v {
			keys = append(keys, k)
		}
		sort.Slice(keys, func(i, j int) bool {
			if len(keys[i]) != len(keys[j]) {
				return len(keys[i]) < len(keys[j])
			}
			return keys[i] < keys[j]
		})
		for _, k := range keys {
			key := k
			for i, r := range k {
				if !(r == '_' || r == '$' || unicode.IsLetter(r) || (i > 0 && unicode.IsDigit(r))) {
					key = strconv.Quote(k)
					break
				}
			}
			if key == "" {
				key = "\"\""
			}
			children = append(children, jsonSearchNode{v[k], n.path + "." + key})
		}
	}
	return children
}
func mysqlJSONSearch(s *Schema, a []Value) (Value, error) {
	if a[0].Null || a[1].Null || a[2].Null {
		return NewNullValue(), nil
	}
	mode := mysqlStringValue(a[1])
	if mode != "one" && mode != "all" {
		return NewNullValue(), fmt.Errorf("JSON_SEARCH mode must be one or all")
	}
	document, e := decodeJSONDocument(a[0])
	if e != nil {
		return NewNullValue(), e
	}
	escape := rune('\\')
	if len(a) > 3 && !a[3].Null {
		text := mysqlStringValue(a[3])
		if utf8.RuneCountInString(text) > 1 {
			return NewNullValue(), fmt.Errorf("JSON_SEARCH escape must be empty or one character")
		}
		escape = 0
		if text != "" {
			escape, _ = utf8.DecodeRuneInString(text)
		}
	}
	roots := []jsonSearchNode{{document, "$"}}
	if len(a) > 4 {
		roots = nil
		for _, path := range a[4:] {
			if path.Null {
				return NewNullValue(), nil
			}
			legs, e := parseJSONPath(mysqlStringValue(path), false)
			if e != nil {
				return NewNullValue(), e
			}
			var selectRoots func(jsonSearchNode, []jsonPathLeg) error
			selectRoots = func(n jsonSearchNode, path []jsonPathLeg) error {
				if e := s.checkInterrupt(); e != nil {
					return e
				}
				if len(path) == 0 {
					roots = append(roots, n)
					return nil
				}
				leg := path[0]
				if leg.recursive {
					if e := selectRoots(n, path[1:]); e != nil {
						return e
					}
					for _, c := range jsonSearchChildren(n) {
						if e := selectRoots(c, path); e != nil {
							return e
						}
					}
					return nil
				}
				children := jsonSearchChildren(n)
				if leg.wildcard {
					for _, c := range children {
						_, array := n.value.([]any)
						if array != leg.array {
							continue
						}
						if e := selectRoots(c, path[1:]); e != nil {
							return e
						}
					}
					return nil
				}
				if leg.array {
					v, ok := n.value.([]any)
					if !ok {
						return nil
					}
					i := leg.index
					if leg.last {
						i = len(v) - 1 - i
					}
					if i >= 0 && i < len(v) {
						child := children[i]
						if leg.last {
							suffix := "last"
							if leg.index > 0 {
								suffix += "-" + strconv.Itoa(leg.index)
							}
							child.path = n.path + "[" + suffix + "]"
						}
						return selectRoots(child, path[1:])
					}
					return nil
				}
				if v, ok := n.value.(map[string]any); ok {
					if x, exists := v[leg.key]; exists {
						for _, c := range children {
							if c.value == nil && x != nil {
								continue
							}
							key := strings.TrimPrefix(c.path, n.path+".")
							if key == leg.key || key == strconv.Quote(leg.key) {
								return selectRoots(c, path[1:])
							}
						}
					}
				}
				return nil
			}
			if e = selectRoots(jsonSearchNode{document, "$"}, legs); e != nil {
				return NewNullValue(), e
			}
		}
	}
	matches := []any{}
	seen := map[string]bool{}
	var size uint64
	var visit func(jsonSearchNode) error
	visit = func(n jsonSearchNode) error {
		if e := s.checkInterrupt(); e != nil {
			return e
		}
		if value, ok := n.value.(string); ok && jsonSearchLike(value, mysqlStringValue(a[2]), escape) && !seen[n.path] {
			size += uint64(len(n.path) + 8)
			if e := functionResultSize(s, size); e != nil {
				return e
			}
			seen[n.path] = true
			matches = append(matches, n.path)
			if mode == "one" {
				return nil
			}
		}
		for _, c := range jsonSearchChildren(n) {
			if mode == "one" && len(matches) > 0 {
				return nil
			}
			if e := visit(c); e != nil {
				return e
			}
		}
		return nil
	}
	for _, root := range roots {
		if e := visit(root); e != nil {
			return NewNullValue(), e
		}
		if mode == "one" && len(matches) > 0 {
			break
		}
	}
	if len(matches) == 0 {
		return NewNullValue(), nil
	}
	if len(matches) == 1 {
		return jsonResult(matches[0])
	}
	return jsonResult(matches)
}

// LIKE matching over Unicode code points, with an optional one-character escape.
func jsonSearchLike(input, pattern string, escape rune) bool {
	type token struct {
		r    rune
		wild bool
	}
	var p []token
	raw := []rune(pattern)
	for i := 0; i < len(raw); i++ {
		r := raw[i]
		if escape != 0 && r == escape && i+1 < len(raw) {
			i++
			p = append(p, token{r: raw[i]})
		} else {
			p = append(p, token{r: r, wild: r == '%' || r == '_'})
		}
	}
	text := []rune(input)
	i, j, star, retry := 0, 0, -1, 0
	for i < len(text) {
		if j < len(p) && !(p[j].wild && p[j].r == '%') && (p[j].wild && p[j].r == '_' || p[j].r == text[i]) {
			i++
			j++
		} else if j < len(p) && p[j].wild && p[j].r == '%' {
			star = j
			j++
			retry = i
		} else if star >= 0 {
			retry++
			i = retry
			j = star + 1
		} else {
			return false
		}
	}
	for j < len(p) && p[j].wild && p[j].r == '%' {
		j++
	}
	return j == len(p)
}

func mysqlNameConstLiteral(e Expression) bool {
	switch x := e.(type) {
	case *LiteralExpr:
		return true
	case *UnaryExpr:
		return (x.Op == "+" || x.Op == "-") && mysqlNameConstLiteral(x.Right)
	}
	return false
}
