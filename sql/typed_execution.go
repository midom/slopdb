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
	boolean  bool // PostgreSQL marks boolean expression results, including NULL
}

type typedProgram struct {
	mysqlMode    bool
	postgresMode bool
	collation    Collation
	code         []typedInstruction
	registers    []Value
	result       int
	literals     []typedLiteral
	literalCount int
	literalReady bool
	outputColumn *typedSlotColumn // retained MIN/MAX of a transparent column
}

type typedLiteral struct {
	value Value
	err   error
}

func compileTypedProgram(expr Expression, slots map[string]int) (*typedProgram, bool) {
	return compileTypedProgramBindings(expr, slots, nil)
}

func compileTypedProgramBindings(expr Expression, slots map[string]int, bindings map[Expression]int) (*typedProgram, bool) {
	p := &typedProgram{}
	var compile func(Expression) (int, bool)
	emit := func(i typedInstruction) int {
		if i.literal != nil {
			i.slot = p.literalCount
			p.literalCount++
		}
		p.code = append(p.code, i)
		return len(p.code) - 1
	}
	compile = func(expr Expression) (int, bool) {
		if slot, ok := bindings[expr]; ok {
			return emit(typedInstruction{op: "slot", slot: slot}), true
		}
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
			return emit(typedInstruction{op: "unary" + e.Op, a: a, boolean: e.Op == "NOT"}), true
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
			i := typedInstruction{op: e.Op, a: a, b: b, boolean: postgresBooleanOperator(e.Op)}
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
			return emit(typedInstruction{op: "between", a: a, b: b, c: c, negate: e.Not, boolean: true}), true
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

// Instructions and bindings are immutable for one execution. Independent
// execution state owns registers and normalized literal scratch.
func (p *typedProgram) clonePrivate() *typedProgram {
	if p == nil {
		return nil
	}
	copy := *p
	copy.registers = make([]Value, len(p.registers))
	copy.literals = nil
	copy.literalReady = false
	return &copy
}

func (p *typedProgram) setDialect(s *Schema) {
	p.mysqlMode, p.postgresMode = s.expressionMySQL(), s.expressionPostgres()
	p.collation = s.collation
	if cap(p.literals) < p.literalCount {
		p.literals = make([]typedLiteral, p.literalCount)
	} else {
		p.literals = p.literals[:p.literalCount]
		clear(p.literals)
	}
	for pc := range p.code {
		instruction := &p.code[pc]
		if instruction.literal == nil {
			continue
		}
		v := instruction.literal.Value
		var err error
		if (p.mysqlMode || p.postgresMode) && v.NumericText != "" && !strings.ContainsAny(v.NumericText, "eE") {
			v, err = NewDecimalValue(v.NumericText)
		}
		// Normalization errors are delivered only if the instruction executes;
		// AND/OR must still short circuit an invalid or zero-dividing branch.
		p.literals[instruction.slot] = typedLiteral{value: v, err: err}
	}
	p.literalReady = true
}

func (p *typedProgram) clearRegisters() {
	if p != nil {
		clear(p.registers)
		clear(p.literals)
		p.literalReady = false
	}
}

func (p *typedProgram) valueCompare(a, b Value) int {
	if p.mysqlMode {
		if cmp, ok := mysqlMixedNumericCompare(a, b); ok {
			return cmp
		}
		if cmp, ok := mysqlBinaryCompare(a, b); ok {
			return cmp
		}
	}
	if a.EnumValue && (b.Type == ColInteger || b.Type == ColReal) {
		a = NewIntValue(a.EnumOrdinal)
	}
	if b.EnumValue && (a.Type == ColInteger || a.Type == ColReal) {
		b = NewIntValue(b.EnumOrdinal)
	}
	if !a.Null && !b.Null && a.Type == ColText && b.Type == ColText {
		if c := explicitValueCollation(a, b); c != "" {
			return c.Compare(a.Text, b.Text)
		}
		if !a.Binary && !b.Binary {
			return p.collation.Compare(a.Text, b.Text)
		}
	}
	return ValueCompare(a, b)
}

func (p *typedProgram) compareValues(a, b Value, op string) Value {
	if a.Null || b.Null {
		return NewNullValue()
	}
	return compareSQLValuesBy(a, b, op, p.valueCompare(a, b))
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

func (p *typedProgram) run(slots []Value) error {
	for pc := 0; pc < len(p.code); pc++ {
		i := &p.code[pc]
		var v Value
		var err error
		switch i.op {
		case "literal":
			if i.literal != nil && p.literalReady {
				literal := &p.literals[i.slot]
				if literal.err == nil && (literal.value.Type == ColInteger || literal.value.Type == ColReal || literal.value.Type == ColUnsigned) {
					p.registers[pc] = literal.value
					continue
				}
			}
			v = i.value
			if i.literal != nil {
				if p.literalReady {
					v, err = p.literals[i.slot].value, p.literals[i.slot].err
				} else {
					v = i.literal.Value
					if (p.mysqlMode || p.postgresMode) && v.NumericText != "" && !strings.ContainsAny(v.NumericText, "eE") {
						v, err = NewDecimalValue(v.NumericText)
					}
				}
			}
			if !v.Null && v.Type == ColBlob {
				v.Blob = copyBytes(v.Blob)
			}
		case "slot":
			p.registers[pc] = slots[i.slot]
			p.registers[pc].HexLiteral = false
			continue
		case "branchAND", "branchOR":
			left := p.registers[i.a]
			if i.op == "branchAND" && !left.Null && !truthValue(left) || i.op == "branchOR" && truthValue(left) {
				v = NewIntValue(0)
				if i.op == "branchOR" {
					v.Int = 1
				}
				if p.postgresMode {
					v = v.WithSemanticType(TypeBoolean)
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
			if v.EnumValue && i.op != "unaryNOT" {
				v = NewIntValue(v.EnumOrdinal)
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
							v = NewIntValue(-v.Int)
						}
					} else {
						v = negateRealValue(v)
					}
				}
			}
		case "between":
			a := p.compareValues(p.registers[i.a], p.registers[i.b], ">=")
			b := p.compareValues(p.registers[i.a], p.registers[i.c], "<=")
			v = typedBoolean(a, b, true)
			if i.negate && !v.Null {
				v.Int = 1 - v.Int
			}
		case "=", "<>", "<", "<=", ">", ">=":
			if typedIntegerCompare(&p.registers[pc], &p.registers[i.a], &p.registers[i.b], i.op, p.postgresMode) {
				continue
			}
			left, right := p.registers[i.a], p.registers[i.b]
			if i.affinity == 1 && !left.EnumValue {
				right = applyAffinity(right, left.Type)
			} else if i.affinity == 2 && !right.EnumValue {
				left = applyAffinity(left, right.Type)
			}
			v = p.compareValues(left, right, i.op)
		case "IS", "IS NOT":
			if typedIntegerCompare(&p.registers[pc], &p.registers[i.a], &p.registers[i.b], i.op, p.postgresMode) {
				continue
			}
			left, right := p.registers[i.a], p.registers[i.b]
			same := left.Null && right.Null || !left.Null && !right.Null && p.valueCompare(left, right) == 0
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
		case "%":
			if handled, e := typedIntegerModulo(&p.registers[pc], &p.registers[i.a], &p.registers[i.b]); handled {
				if e != nil {
					return e
				}
				continue
			}
			v, err = arithmeticValuesMode(p.mysqlMode, i.op, p.registers[i.a], p.registers[i.b], math.Mod)
		case "+", "-", "*", "/":
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
			return err
		}
		if p.postgresMode && i.boolean {
			v = v.WithSemanticType(TypeBoolean)
		}
		p.registers[pc] = v
	}
	return nil
}

func (p *typedProgram) eval(slots []Value) (Value, error) {
	if err := p.run(slots); err != nil {
		return NewNullValue(), err
	}
	v := p.registers[p.result]
	if column := p.outputColumn; column != nil {
		v = decorateTypedValue(v, column.kind, column.binary, column.collation, nil)
	}
	return v, nil
}

type typedAggregate struct {
	slots     map[string]int
	width     int
	columns   []typedSlotColumn // immutable physical slot metadata
	where     *typedProgram
	group     []*typedProgram
	args      []*typedProgram // nil represents COUNT(*)
	functions []*FuncCallExpr
	baseNames []string
	baseSlots []int
	rawWhere  *typedProgram
	rawText   []typedTextPredicate
}

type typedSlotColumn struct {
	kind      TypeID
	typ       ColumnType
	binary    bool
	collation Collation
}

func (t *typedAggregate) setDialect(s *Schema) {
	if t.where != nil {
		t.where.setDialect(s)
	}
	if t.rawWhere != nil {
		t.rawWhere.setDialect(s)
	}
	for _, program := range t.group {
		program.setDialect(s)
	}
	for _, program := range t.args {
		if program != nil {
			program.setDialect(s)
		}
	}
}

func (t *typedAggregate) clearRegisters() {
	t.where.clearRegisters()
	t.rawWhere.clearRegisters()
	for _, program := range t.group {
		program.clearRegisters()
	}
	for _, program := range t.args {
		program.clearRegisters()
	}
}

func (t *typedAggregate) representativeValue(slot int, value Value) Value {
	// The binary/no-ENUM proof makes physical tags sufficient for internal
	// arithmetic and predicates. Attach declared metadata only when values
	// escape into representatives or transparent MIN/MAX results, rather
	// than copying/decorating every private Value in every input batch.
	column := t.columns[slot]
	value = decorateTypedValue(value, column.kind, column.binary, column.collation, nil)
	if value.Type == ColBlob {
		value.Blob = copyBytes(value.Blob)
	}
	return value
}

func (t *typedAggregate) transparentColumn(expr Expression) *typedSlotColumn {
	if unary, ok := expr.(*UnaryExpr); ok && unary.Op == "+" {
		return t.transparentColumn(unary.Right)
	}
	if ref, ok := expr.(*ColumnRefExpr); ok {
		if slot, ok := t.slots[strings.ToLower(ref.Name)]; ok {
			return &t.columns[slot]
		}
	}
	return nil
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
		t.columns = append(t.columns, typedSlotColumn{kind: col.LogicalType().Kind, typ: col.ColType, binary: col.Binary, collation: col.Collation})
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
		if strings.EqualFold(f.Name, "MIN") || strings.EqualFold(f.Name, "MAX") {
			p.outputColumn = t.transparentColumn(f.Args[0])
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
	t.compileRawTextWhere(where)
	return t, true
}

const typedBatchRows = 128

func (s *Schema) walkTypedAggregate(t *typedAggregate, info *TableInfo, needed map[string]bool, paths []*accessPath, groups map[string]*aggregateGroup, newGroup func() *aggregateGroup) error {
	t.setDialect(s)
	defer t.clearRegisters()
	if group := groups[""]; t.scalarCountEligible(group) && aggregateGroupMemory(group) <= s.memoryBudget() {
		return s.walkTypedScalarCount(t, info, needed, paths, group)
	}
	binaryGroups := !s.nonbinaryDefaultCollation()

	batch := make([]Value, typedBatchRows*t.width)
	raw := t.rawScan(s)
	var rawRows [typedBatchRows]bool
	rows := 0
	var groupMemory int64
	for _, g := range groups {
		groupMemory += aggregateGroupMemory(g)
	}
	scalar := len(t.group) == 0 && len(t.baseSlots) == 0
	for _, f := range t.functions {
		if f.Distinct {
			scalar = false
		}
		switch strings.ToUpper(f.Name) {
		case "COUNT", "SUM", "AVG", "TOTAL", "MIN", "MAX":
		default:
			scalar = false
		}
	}
	scalarGroup := groups[""]
	scalar = scalar && scalarGroup != nil
	var keyBuffer [128]byte
	keyScratch := keyBuffer[:0]
	flush := func() error {
		if err := s.checkInterrupt(); err != nil {
			return err
		}
		for n := 0; n < rows; n++ {
			row := batch[n*t.width : (n+1)*t.width]
			if t.where != nil {
				predicate := t.where
				if rawRows[n] {
					predicate = t.rawWhere
				}
				accepted, err := predicate.evalPredicate(row)
				if err != nil {
					return err
				}
				if !accepted {
					continue
				}
			}
			g := scalarGroup
			var before int64
			if !scalar {
				key := keyScratch[:0]
				for _, p := range t.group {
					v, err := p.eval(row)
					if err != nil {
						return err
					}
					if v.Type == ColText && (!binaryGroups || v.Collation != "" && v.Collation != BinaryCollation) {
						v.Text = valueTextKey(v, s.collation)
					}
					key = appendValueIdentity(key, v)
				}
				// The conversion in a map lookup borrows key for the lookup only.
				// Copy it only when a new group retains the identity.
				g = groups[string(key)]
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
			}
			if g.inputs == 0 {
				g.baseValues = make([]Value, len(t.baseSlots))
				for i, slot := range t.baseSlots {
					g.baseValues[i] = t.representativeValue(slot, row[slot])
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
			if !scalar {
				groupMemory += aggregateGroupMemory(g) - before
			}
			if !scalar && groupMemory > s.memoryBudget() {
				return errAggregateSpill
			}
		}
		if scalar && aggregateGroupMemory(scalarGroup) > s.memoryBudget() {
			return errAggregateSpill
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
		row := batch[rows*t.width : (rows+1)*t.width]
		if raw != nil {
			rawRows[rows] = raw.decodeRow(data, layout, row)
		} else {
			decodeTypedRow(data, layout, row)
		}
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
