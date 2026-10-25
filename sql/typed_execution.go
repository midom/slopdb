package sql

import (
	"encoding/binary"
	"fmt"
	"math"
	"strings"
)

// A typedProgram binds columns to slots once and lowers the AST to a linear
// instruction stream. It never looks up a column name or walks an AST per row.
// Registers hold SQL Values so NULL, exact int64 and mixed numeric semantics
// remain identical to interpreted execution. Prepared private execution trees
// may retain instructions; registers are cleared when the tree is released.
type typedInstruction struct {
	op       string
	a, b, c  int
	slot     int
	value    Value
	literal  *LiteralExpr
	affinity int // 1: right literal, 2: left literal
	jump     int
	negate   bool
}

type typedProgram struct {
	mysqlMode bool
	code      []typedInstruction
	registers []Value
	result    int
}

func compileTypedProgram(expr Expression, slots map[string]int) (*typedProgram, bool) {
	p := &typedProgram{}
	var compile func(Expression) (int, bool)
	emit := func(i typedInstruction) int {
		p.code = append(p.code, i)
		return len(p.code) - 1
	}
	compile = func(expr Expression) (int, bool) {
		switch e := expr.(type) {
		case *LiteralExpr:
			return emit(typedInstruction{op: "literal", literal: e}), true
		case *ColumnRefExpr:
			if e.Name == "*" {
				return emit(typedInstruction{op: "literal", value: NewIntValue(1)}), true
			}
			slot, ok := slots[strings.ToLower(e.Name)]
			if !ok {
				return 0, false
			}
			return emit(typedInstruction{op: "slot", slot: slot}), true
		case *UnaryExpr:
			if e.Op != "+" && e.Op != "-" && e.Op != "NOT" {
				return 0, false
			}
			a, ok := compile(e.Right)
			if !ok {
				return 0, false
			}
			return emit(typedInstruction{op: "unary" + e.Op, a: a}), true
		case *BinaryExpr:
			switch e.Op {
			case "AND", "OR", "=", "<>", "<", "<=", ">", ">=", "IS", "IS NOT", "+", "-", "*", "/", "%", "||":
			default:
				return 0, false
			}
			a, ok := compile(e.Left)
			if !ok {
				return 0, false
			}
			branch := -1
			if e.Op == "AND" || e.Op == "OR" {
				branch = emit(typedInstruction{op: "branch" + e.Op, a: a})
			}
			b, ok := compile(e.Right)
			if !ok {
				return 0, false
			}
			i := typedInstruction{op: e.Op, a: a, b: b}
			if _, col := e.Left.(*ColumnRefExpr); col {
				if _, lit := e.Right.(*LiteralExpr); lit {
					i.affinity = 1
				}
			} else if _, col := e.Right.(*ColumnRefExpr); col {
				if _, lit := e.Left.(*LiteralExpr); lit {
					i.affinity = 2
				}
			}
			result := emit(i)
			if branch >= 0 {
				p.code[branch].b = result
				p.code[branch].jump = len(p.code)
			}
			return result, true
		case *BetweenExpr:
			a, ok := compile(e.Expr)
			if !ok {
				return 0, false
			}
			b, ok := compile(e.Low)
			if !ok {
				return 0, false
			}
			c, ok := compile(e.High)
			if !ok {
				return 0, false
			}
			return emit(typedInstruction{op: "between", a: a, b: b, c: c, negate: e.Not}), true
		}
		// Functions, subqueries, CASE and other expressions retain the general
		// executor. In particular, do not eagerly evaluate conditional branches.
		return 0, false
	}
	var ok bool
	p.result, ok = compile(expr)
	if !ok {
		return nil, false
	}
	p.registers = make([]Value, len(p.code))
	return p, true
}

func typedBoolean(left, right Value, and bool) Value {
	if and && ((!left.Null && !truthValue(left)) || (!right.Null && !truthValue(right))) {
		return NewIntValue(0)
	}
	if !and && (truthValue(left) || truthValue(right)) {
		return NewIntValue(1)
	}
	if left.Null || right.Null {
		return NewNullValue()
	}
	if and {
		return NewIntValue(1)
	}
	return NewIntValue(0)
}

func (p *typedProgram) eval(slots []Value) (Value, error) {
	for pc := 0; pc < len(p.code); pc++ {
		i := &p.code[pc]
		var v Value
		var err error
		switch i.op {
		case "literal":
			v = i.value
			if i.literal != nil {
				v = i.literal.Value
				if p.mysqlMode && v.NumericText != "" && !strings.ContainsAny(v.NumericText, "eE") {
					v, err = NewDecimalValue(v.NumericText)
				}
			}
			if !v.Null && v.Type == ColBlob {
				v.Blob = copyBytes(v.Blob)
			}
		case "slot":
			v = slots[i.slot]
			v.HexLiteral = false
		case "branchAND", "branchOR":
			left := p.registers[i.a]
			if i.op == "branchAND" && !left.Null && !truthValue(left) || i.op == "branchOR" && truthValue(left) {
				v = NewIntValue(0)
				if i.op == "branchOR" {
					v.Int = 1
				}
				p.registers[i.b] = v
				pc = i.jump - 1
			}
			continue
		case "AND", "OR":
			v = typedBoolean(p.registers[i.a], p.registers[i.b], i.op == "AND")
		case "unaryNOT", "unary+", "unary-":
			v = p.registers[i.a]
			if p.mysqlMode && v.HexLiteral {
				v = mysqlHexNumber(v)
			}
			if !v.Null {
				if i.op == "unaryNOT" {
					if truthValue(v) {
						v = NewIntValue(0)
					} else {
						v = NewIntValue(1)
					}
				} else if !isNumeric(v) {
					err = fmt.Errorf("unary sign requires a numeric operand")
				} else if i.op == "unary-" {
					if v.Type == ColUnsigned || v.Type == ColDecimal {
						v, err = negateExactValue(v)
					} else if v.Type == ColInteger {
						if v.Int == math.MinInt64 {
							err = fmt.Errorf("integer overflow")
						} else {
							v.Int = -v.Int
						}
					} else {
						v = negateRealValue(v)
					}
				}
			}
		case "between":
			a := compareSQLValues(p.registers[i.a], p.registers[i.b], ">=")
			b := compareSQLValues(p.registers[i.a], p.registers[i.c], "<=")
			v = typedBoolean(a, b, true)
			if i.negate && !v.Null {
				v.Int = 1 - v.Int
			}
		case "=", "<>", "<", "<=", ">", ">=":
			left, right := p.registers[i.a], p.registers[i.b]
			if i.affinity == 1 {
				right = applyAffinity(right, left.Type)
			} else if i.affinity == 2 {
				left = applyAffinity(left, right.Type)
			}
			v = compareSQLValues(left, right, i.op)
		case "IS", "IS NOT":
			left, right := p.registers[i.a], p.registers[i.b]
			same := left.Null && right.Null || !left.Null && !right.Null && ValueCompare(left, right) == 0
			if i.op == "IS NOT" {
				same = !same
			}
			v = NewIntValue(0)
			if same {
				v.Int = 1
			}
		case "||":
			left, right := p.registers[i.a], p.registers[i.b]
			v = NewNullValue()
			if !left.Null && !right.Null {
				v = NewTextValue(left.String() + right.String())
			}
		case "+", "-", "*", "/", "%":
			v, err = arithmeticValuesMode(p.mysqlMode, i.op, p.registers[i.a], p.registers[i.b], func(a, b float64) float64 {
				switch i.op {
				case "+":
					return a + b
				case "-":
					return a - b
				case "*":
					return a * b
				case "/":
					return a / b
				default:
					return math.Mod(a, b)
				}
			})
		}
		if err != nil {
			return NewNullValue(), err
		}
		p.registers[pc] = v
	}
	return p.registers[p.result], nil
}

type typedAggregate struct {
	slots     map[string]int
	width     int
	where     *typedProgram
	group     []*typedProgram
	args      []*typedProgram // nil represents COUNT(*)
	functions []*FuncCallExpr
	baseNames []string
	baseSlots []int
}

func compileTypedAggregate(info *TableInfo, table, alias string, where Expression, group []Expression, functions []*FuncCallExpr, representative map[string]bool) (*typedAggregate, bool) {
	if tableHasExplicitCollation(info) {
		return nil, false
	}
	// ENUM numeric values come from their column declaration, while this
	// compact decoder intentionally handles only ordinary storage values.
	for _, col := range info.Columns {
		if len(col.EnumValues) > 0 {
			return nil, false
		}
	}
	t := &typedAggregate{slots: make(map[string]int), functions: functions}
	// Use compact slots only for referenced columns; binding aliases never
	// increases the per-batch width.
	needed := make(map[string]bool)
	expressions := append([]Expression{where}, group...)
	for _, f := range functions {
		expressions = append(expressions, f.Args...)
	}
	for _, e := range expressions {
		_ = walkExpression(e, func(e Expression) (bool, error) {
			if c, ok := e.(*ColumnRefExpr); ok {
				needed[strings.ToLower(c.Name)] = true
			}
			return false, nil
		})
	}
	for name := range representative {
		needed[strings.ToLower(name)] = true
	}
	for _, col := range info.Columns {
		names := []string{col.Name, table + "." + col.Name, alias + "." + col.Name}
		use := false
		for _, name := range names {
			use = use || needed[strings.ToLower(name)]
		}
		if !use {
			continue
		}
		for _, name := range names {
			t.slots[strings.ToLower(name)] = t.width
		}
		t.width++
	}
	var ok bool
	if where != nil {
		t.where, ok = compileTypedProgram(where, t.slots)
		if !ok {
			return nil, false
		}
	}
	for _, expr := range group {
		p, ok := compileTypedProgram(expr, t.slots)
		if !ok {
			return nil, false
		}
		t.group = append(t.group, p)
	}
	for _, f := range functions {
		if strings.EqualFold(f.Name, "COUNT") && len(f.Args) > 0 {
			if c, ok := f.Args[0].(*ColumnRefExpr); ok && c.Name == "*" {
				t.args = append(t.args, nil)
				continue
			}
		}
		if len(f.Args) == 0 {
			return nil, false
		}
		p, ok := compileTypedProgram(f.Args[0], t.slots)
		if !ok {
			return nil, false
		}
		t.args = append(t.args, p)
	}
	for _, name := range sortedAccessNames(representative) {
		slot, ok := t.slots[strings.ToLower(name)]
		if !ok {
			// A projected alias may be computed only after aggregation. The
			// map executor likewise omits it from representative input rows.
			continue
		}
		t.baseNames = append(t.baseNames, name)
		t.baseSlots = append(t.baseSlots, slot)
	}
	return t, true
}

const typedBatchRows = 128

func (s *Schema) walkTypedAggregate(t *typedAggregate, info *TableInfo, needed map[string]bool, paths []*accessPath, groups map[string]*aggregateGroup, newGroup func() *aggregateGroup) error {
	if t.where != nil {
		t.where.mysqlMode = s.expressionMySQL()
	}
	for _, p := range t.group {
		p.mysqlMode = s.expressionMySQL()
	}
	for _, p := range t.args {
		if p != nil {
			p.mysqlMode = s.expressionMySQL()
		}
	}

	batch := make([]Value, typedBatchRows*t.width)
	rows := 0
	var groupMemory int64
	for _, g := range groups {
		groupMemory += aggregateGroupMemory(g)
	}
	var keyBuffer [128]byte
	keyScratch := keyBuffer[:0]
	flush := func() error {
		if err := s.checkInterrupt(); err != nil {
			return err
		}
		for n := 0; n < rows; n++ {
			row := batch[n*t.width : (n+1)*t.width]
			if t.where != nil {
				v, err := t.where.eval(row)
				if err != nil {
					return err
				}
				if !truthValue(v) {
					continue
				}
			}
			key := keyScratch[:0]
			for _, p := range t.group {
				v, err := p.eval(row)
				if err != nil {
					return err
				}
				key = appendValueIdentity(key, v)
			}
			// The conversion in a map lookup borrows key for the lookup only.
			// Copy it only when a new group retains the identity.
			g := groups[string(key)]
			var before int64
			if g != nil {
				before = aggregateGroupMemory(g)
			}
			if g == nil {
				g = newGroup()
				groups[string(key)] = g
			}
			if cap(key) <= typedScratchLimit {
				keyScratch = key[:0]
			}
			if g.inputs == 0 {
				g.baseValues = make([]Value, len(t.baseSlots))
				for i, slot := range t.baseSlots {
					g.baseValues[i] = row[slot]
					if row[slot].Type == ColBlob {
						g.baseValues[i].Blob = copyBytes(row[slot].Blob)
					}
				}
			}
			g.inputs++
			for i, p := range t.args {
				if p == nil {
					g.states[i].count++
					continue
				}
				v, err := p.eval(row)
				if err != nil {
					return err
				}
				if err := s.addAggregateValue(&g.states[i], t.functions[i], v); err != nil {
					return err
				}
			}
			groupMemory += aggregateGroupMemory(g) - before
			if groupMemory > s.memoryBudget() {
				return errAggregateSpill
			}
		}
		// Only this private batch may reuse BLOB storage. Group representatives
		// and MIN/MAX states own their retained values. Do not pin unusually
		// large inputs for the rest of the scan.
		resetTypedBatch(batch)
		rows = 0
		return nil
	}
	var lastLayout *scanColumn
	var layout []int
	err := s.walkAccessRaw(info, needed, paths, func(data []byte, columns []scanColumn) error {
		if len(columns) > 0 && lastLayout != &columns[0] {
			lastLayout = &columns[0]
			if cap(layout) < len(columns) {
				layout = make([]int, len(columns))
			} else {
				layout = layout[:len(columns)]
			}
			for i, col := range columns {
				layout[i] = -1
				if col.needed {
					if slot, ok := t.slots[strings.ToLower(col.name)]; ok {
						layout[i] = slot
					}
				}
			}
		}
		decodeTypedRow(data, layout, batch[rows*t.width:(rows+1)*t.width])
		rows++
		if rows == typedBatchRows {
			return flush()
		}
		return nil
	})
	if err != nil {
		return err
	}
	if rows > 0 {
		return flush()
	}
	return nil
}

const typedScratchLimit = 64 << 10

func resetTypedBatch(batch []Value) {
	retained := 0
	for i := range batch {
		buffer := batch[i].Blob
		if cap(buffer) > typedScratchLimit || retained+cap(buffer) > 8<<20 {
			buffer = nil
		}
		retained += cap(buffer)
		batch[i] = Value{Blob: buffer[:0]}
	}
}

// Decode only bound slots, skipping unused variable-width columns without
// constructing strings/blobs. Old rows missing ALTER-added columns yield NULL.
// The destination is private scratch: decoding a later row can overwrite its
// previous BLOB contents. Retained values must be copied before reusing it.
func decodeTypedRow(data []byte, layout []int, row []Value) {
	offset := 0
	for _, slot := range layout {
		if offset >= len(data) {
			if slot >= 0 {
				row[slot] = NewNullValue()
			}
			continue
		}
		if slot >= 0 {
			v, n := decodeValueInto(data[offset:], row[slot].Blob)
			offset += n
			row[slot] = v
			continue
		}
		switch data[offset] {
		case binTypeInt64, binTypeFloat64, binTypeUint64:
			if len(data)-offset >= 9 {
				offset += 9
			} else {
				offset++
			}
		case binTypeText, binTypeBlob, binTypeDecimal, binTypeJSON:
			length, n := binary.Uvarint(data[offset+1:])
			if n > 0 && length <= uint64(len(data)-offset-1-n) {
				offset += 1 + n + int(length)
			} else {
				offset++
				if n > 0 {
					offset += n
				}
			}
		default:
			offset++
		}
	}
}
