package sql

import (
	"fmt"
	"sort"
	"strings"
	"unicode/utf8"
)

func (s *Schema) evalMySQLGroupConcat(f *FuncCallExpr, rows []map[string]Value) (Value, error) {
	separator := ","
	if f.GroupConcatSeparator != nil {
		v, err := s.evalExpr(f.GroupConcatSeparator, nil)
		if err != nil {
			return NewNullValue(), err
		}
		if v.Null || v.Type != ColText && v.Type != ColBlob {
			return NewNullValue(), fmt.Errorf("GROUP_CONCAT separator must be a string")
		}
		separator = mysqlStringValue(v)
	}
	type item struct {
		text string
		keys []Value
	}
	items := make([]item, 0, len(rows))
	seen := make(map[string]bool)
	binary := false
	for _, row := range rows {
		if err := s.checkInterrupt(); err != nil {
			return NewNullValue(), err
		}
		values := make([]Value, len(f.Args))
		skip := false
		var text strings.Builder
		var identity []byte
		for i, arg := range f.Args {
			v, err := s.evalExpr(arg, row)
			if err != nil {
				return NewNullValue(), err
			}
			values[i] = v
			if v.Null {
				skip = true
				break
			}
			if v.Type == ColBlob {
				binary = true
			}
			text.WriteString(mysqlStringValue(v))
			if f.Distinct {
				identity = s.appendValueIdentity(identity, v)
			}
		}
		if skip {
			continue
		}
		if f.Distinct {
			key := string(identity)
			if seen[key] {
				continue
			}
			seen[key] = true
		}
		x := item{text: text.String(), keys: make([]Value, len(f.GroupConcatOrder))}
		for i, order := range f.GroupConcatOrder {
			if pos, ok := order.Expr.(*LiteralExpr); ok && !pos.BoundParameter && pos.Value.Type == ColInteger && !pos.Value.Null {
				if pos.Value.Int < 1 || pos.Value.Int > int64(len(values)) {
					return NewNullValue(), fmt.Errorf("GROUP_CONCAT ORDER BY position out of range")
				}
				x.keys[i] = values[pos.Value.Int-1]
			} else {
				v, err := s.evalExpr(order.Expr, row)
				if err != nil {
					return NewNullValue(), err
				}
				x.keys[i] = v
			}
		}
		items = append(items, x)
	}
	if len(items) == 0 {
		return NewNullValue(), nil
	}
	sort.SliceStable(items, func(i, j int) bool {
		for k, order := range f.GroupConcatOrder {
			cmp := s.valueCompare(items[i].keys[k], items[j].keys[k])
			if cmp != 0 {
				if order.Desc {
					return cmp > 0
				}
				return cmp < 0
			}
		}
		return false
	})
	limit := MySQLGroupConcatMaxLen(s.ctx)
	var result strings.Builder
	for i, x := range items {
		if i != 0 {
			result.WriteString(separator)
		}
		result.WriteString(x.text)
		if limit > 0 && int64(result.Len()) >= limit {
			break
		}
	}
	text := result.String()
	if limit > 0 && int64(len(text)) > limit {
		text = text[:int(limit)]
		if !binary {
			for len(text) > 0 && !utf8.ValidString(text) {
				text = text[:len(text)-1]
			}
		}
	}
	if binary {
		return NewBlobValue([]byte(text)), nil
	}
	return NewTextValue(text), nil
}

func mysqlGroupConcatSQL(f *FuncCallExpr) string {
	var parts []string
	for _, arg := range f.Args {
		parts = append(parts, formatExpr(arg))
	}
	modifier := ""
	if f.Distinct {
		modifier = "DISTINCT "
	}
	text := "GROUP_CONCAT(" + modifier + strings.Join(parts, ", ")
	if len(f.GroupConcatOrder) > 0 {
		text += " ORDER BY "
		for i, order := range f.GroupConcatOrder {
			if i > 0 {
				text += ", "
			}
			text += formatExpr(order.Expr)
			if order.Desc {
				text += " DESC"
			} else {
				text += " ASC"
			}
		}
	}
	if f.GroupConcatSeparator != nil {
		text += " SEPARATOR " + formatExpr(f.GroupConcatSeparator)
	}
	return text + ")"
}
