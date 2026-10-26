package sql

import (
	"bytes"
	"encoding/binary"
	"strings"
)

// This recipe replaces only total, bytewise TEXT comparisons. Arithmetic,
// mixed-type comparisons, functions and every unproven residual remain in the
// ordinary typed program, in their original short-circuit order. Encoded row
// bytes are consumed synchronously and are never retained by the recipe.
type typedTextPredicate struct {
	columnSlot int
	resultSlot int
	node       *typedTextNode
}

type typedTextNode struct {
	op          string
	left, right *typedTextNode
	literal     *LiteralExpr
	reverse     bool
}

func (t *typedAggregate) compileRawTextWhere(where Expression) {
	if where == nil {
		return
	}
	bindings := make(map[Expression]int)
	_ = walkExpression(where, func(expr Expression) (bool, error) {
		column, node, ok := t.rawTextNode(expr)
		if !ok {
			return false, nil
		}
		slot := t.width + len(t.rawText)
		t.rawText = append(t.rawText, typedTextPredicate{columnSlot: column, resultSlot: slot, node: node})
		bindings[expr] = slot
		return true, nil
	})
	if len(bindings) == 0 {
		return
	}
	var ok bool
	t.rawWhere, ok = compileTypedProgramBindings(where, t.slots, bindings)
	if !ok {
		t.rawText = nil
		return
	}
	t.width += len(t.rawText)
}

func (t *typedAggregate) rawTextColumn(expr Expression) (int, bool) {
	ref, ok := expr.(*ColumnRefExpr)
	if !ok {
		return 0, false
	}
	slot, ok := t.slots[strings.ToLower(ref.Name)]
	if !ok || slot >= len(t.columns) || t.columns[slot].typ != ColText {
		return 0, false
	}
	return slot, true
}

func (t *typedAggregate) rawTextNode(expr Expression) (int, *typedTextNode, bool) {
	switch e := expr.(type) {
	case *BinaryExpr:
		if e.Op == "AND" || e.Op == "OR" {
			a, left, ok := t.rawTextNode(e.Left)
			if !ok {
				return 0, nil, false
			}
			b, right, ok := t.rawTextNode(e.Right)
			if !ok || a != b {
				return 0, nil, false
			}
			return a, &typedTextNode{op: e.Op, left: left, right: right}, true
		}
		switch e.Op {
		case "=", "<>", "<", "<=", ">", ">=":
		default:
			return 0, nil, false
		}
		if column, ok := t.rawTextColumn(e.Left); ok {
			if literal, ok := e.Right.(*LiteralExpr); ok {
				return column, &typedTextNode{op: e.Op, literal: literal}, true
			}
		}
		if column, ok := t.rawTextColumn(e.Right); ok {
			if literal, ok := e.Left.(*LiteralExpr); ok {
				return column, &typedTextNode{op: e.Op, literal: literal, reverse: true}, true
			}
		}
	case *BetweenExpr:
		column, ok := t.rawTextColumn(e.Expr)
		if !ok {
			return 0, nil, false
		}
		low, ok := e.Low.(*LiteralExpr)
		if !ok {
			return 0, nil, false
		}
		high, ok := e.High.(*LiteralExpr)
		if !ok {
			return 0, nil, false
		}
		node := &typedTextNode{op: "AND", left: &typedTextNode{op: ">=", literal: low}, right: &typedTextNode{op: "<=", literal: high}}
		if e.Not {
			node = &typedTextNode{op: "NOT", left: node}
		}
		return column, node, true
	case *UnaryExpr:
		if e.Op == "NOT" {
			column, node, ok := t.rawTextNode(e.Right)
			if ok {
				return column, &typedTextNode{op: "NOT", left: node}, true
			}
		}
	}
	return 0, nil, false
}

func (n *typedTextNode) valid() bool {
	if n.literal != nil {
		v := n.literal.Value
		// Prepared bindings can change type/collation between executions. Recheck
		// this proof each time; an invalid proof uses the complete typed predicate.
		return !v.Null && v.Type == ColText && !v.EnumValue && v.NumericText == "" && v.NumericExact == "" && (v.Collation == "" || v.Collation == BinaryCollation)
	}
	return n.left != nil && n.left.valid() && (n.right == nil || n.right.valid())
}

func (n *typedTextNode) match(text []byte) bool {
	switch n.op {
	case "AND":
		return n.left.match(text) && n.right.match(text)
	case "OR":
		return n.left.match(text) || n.right.match(text)
	case "NOT":
		return !n.left.match(text)
	}
	cmp := bytes.Compare(text, []byte(n.literal.Value.Text))
	if n.reverse {
		cmp = -cmp
	}
	switch n.op {
	case "=":
		return cmp == 0
	case "<>":
		return cmp != 0
	case "<":
		return cmp < 0
	case "<=":
		return cmp <= 0
	case ">":
		return cmp > 0
	case ">=":
		return cmp >= 0
	}
	return false
}

type typedRawScan struct {
	bySlot   [][]typedTextPredicate
	decode   []bool
	postgres bool
}

func (t *typedAggregate) rawScan(s *Schema) *typedRawScan {
	if t.rawWhere == nil || s.nonbinaryDefaultCollation() {
		return nil
	}
	for _, predicate := range t.rawText {
		if !predicate.node.valid() {
			return nil
		}
	}
	scan := &typedRawScan{bySlot: make([][]typedTextPredicate, len(t.columns)), decode: make([]bool, len(t.columns)), postgres: s.expressionPostgres()}
	for _, predicate := range t.rawText {
		scan.bySlot[predicate.columnSlot] = append(scan.bySlot[predicate.columnSlot], predicate)
	}
	mark := func(program *typedProgram) {
		if program == nil {
			return
		}
		for _, instruction := range program.code {
			if instruction.op == "slot" && instruction.slot < len(scan.decode) {
				scan.decode[instruction.slot] = true
			}
		}
	}
	mark(t.rawWhere)
	for _, program := range t.group {
		mark(program)
	}
	for _, program := range t.args {
		mark(program)
	}
	for _, slot := range t.baseSlots {
		scan.decode[slot] = true
	}
	return scan
}

// decodeRow fills synthetic boolean slots while walking the encoded row. TEXT
// used only by proven predicates needs no string allocation. Unusual physical
// tags fall back per row instead of trusting a declared type blindly.
func (scan *typedRawScan) decodeRow(data []byte, layout []int, row []Value) bool {
	offset := 0
	for _, slot := range layout {
		rest := data[offset:]
		if slot < 0 {
			offset += skipTypedEncodedValue(rest)
			continue
		}
		n := 0
		if len(scan.bySlot[slot]) != 0 {
			text, null, used, ok := rawTypedText(rest)
			if !ok {
				decodeTypedRow(data, layout, row)
				return false
			}
			n = used
			for _, predicate := range scan.bySlot[slot] {
				value := NewNullValue()
				if !null {
					value = NewIntValue(0)
					if predicate.node.match(text) {
						value.Int = 1
					}
				}
				if scan.postgres {
					value = value.WithSemanticType(TypeBoolean)
				}
				row[predicate.resultSlot] = value
			}
		}
		if scan.decode[slot] {
			if len(rest) == 0 {
				row[slot] = NewNullValue()
			} else {
				var value Value
				value, n = decodeValueInto(rest, row[slot].Blob)
				row[slot] = value
			}
		} else if n == 0 {
			n = skipTypedEncodedValue(rest)
		}
		offset += n
	}
	return true
}

func rawTypedText(data []byte) (text []byte, null bool, n int, ok bool) {
	if len(data) == 0 {
		return nil, true, 0, true
	}
	if data[0] == binTypeNull {
		return nil, true, 1, true
	}
	if data[0] != binTypeText {
		return nil, false, 0, false
	}
	length, used := binary.Uvarint(data[1:])
	if used <= 0 || length > uint64(len(data)-1-used) {
		return nil, false, 0, false
	}
	end := 1 + used + int(length)
	return data[1+used : end], false, end, true
}

func skipTypedEncodedValue(data []byte) int {
	if len(data) == 0 {
		return 0
	}
	switch data[0] {
	case binTypeInt64, binTypeFloat64, binTypeUint64:
		if len(data) >= 9 {
			return 9
		}
	case binTypeText, binTypeBlob, binTypeDecimal, binTypeJSON:
		length, n := binary.Uvarint(data[1:])
		if n > 0 && length <= uint64(len(data)-1-n) {
			return 1 + n + int(length)
		}
		if n > 0 {
			return 1 + n
		}
	}
	return 1
}
