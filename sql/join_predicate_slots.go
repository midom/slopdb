package sql

import "strings"

// Hot, supported predicates bind each needed input once to a compact vector.
// Short equality probes stay on their existing path; unsupported expressions
// (including subqueries/functions) keep the map evaluator and its lexical row.
type joinPredicateSlots struct {
	values   []Value
	bindings [][]joinPredicateBinding
	programs [][]*typedProgram
	final    *typedProgram
}

type joinPredicateBinding struct {
	name string
	slot int
}

func (s *Schema) compileJoinPredicateSlots(plan *joinPlan, predicates [][]Expression) *joinPredicateSlots {
	eligible := typedPredicateCost(plan.where) >= 6
	for _, expressions := range predicates {
		for _, expr := range expressions {
			eligible = eligible || typedPredicateCost(expr) >= 6
		}
	}
	if !eligible {
		return nil
	}
	slots := make(map[string]int)
	bindings := make([][]joinPredicateBinding, len(plan.order))
	width := 0
	for depth, relationIndex := range plan.order {
		relation := plan.relations[relationIndex]
		for _, name := range sortedAccessNames(relation.needed) {
			bindings[depth] = append(bindings[depth], joinPredicateBinding{name, width})
			slots[strings.ToLower(relation.alias+"."+name)] = width
			if plan.firstOwner[name] == relationIndex {
				slots[strings.ToLower(name)] = width
			}
			width++
		}
	}
	result := &joinPredicateSlots{programs: make([][]*typedProgram, len(predicates))}
	used := make([]bool, width)
	count := 0
	compile := func(expr Expression) *typedProgram {
		if typedPredicateCost(expr) < 6 {
			return nil
		}
		program, ok := compileTypedProgram(expr, slots)
		// Small three-instruction equalities already benefit from hash/index
		// probes. Vector setup is reserved for arithmetic/boolean expression work.
		if !ok || len(program.code) < 6 {
			return nil
		}
		for _, instruction := range program.code {
			if instruction.op == "slot" {
				used[instruction.slot] = true
			}
		}
		program.setDialect(s)
		count++
		return program
	}
	for depth, expressions := range predicates {
		result.programs[depth] = make([]*typedProgram, len(expressions))
		for i, expr := range expressions {
			result.programs[depth][i] = compile(expr)
		}
	}
	result.final = compile(plan.where)
	if count == 0 {
		return nil
	}
	remap := make([]int, width)
	for old, needed := range used {
		remap[old] = -1
		if needed {
			remap[old] = len(result.values)
			result.values = append(result.values, NewNullValue())
		}
	}
	result.bindings = make([][]joinPredicateBinding, len(bindings))
	for depth, columns := range bindings {
		for _, column := range columns {
			if slot := remap[column.slot]; slot >= 0 {
				result.bindings[depth] = append(result.bindings[depth], joinPredicateBinding{column.name, slot})
			}
		}
	}
	patch := func(program *typedProgram) {
		if program == nil {
			return
		}
		for i := range program.code {
			if program.code[i].op == "slot" {
				program.code[i].slot = remap[program.code[i].slot]
			}
		}
	}
	for _, programs := range result.programs {
		for _, program := range programs {
			patch(program)
		}
	}
	patch(result.final)
	return result
}

// Count only operations supported by the slot interpreter, saturating at the
// useful-work threshold. Common equality joins require no compilation/scratch.
func typedPredicateCost(expr Expression) int {
	switch e := expr.(type) {
	case *LiteralExpr, *ColumnRefExpr:
		return 1
	case *UnaryExpr:
		if e.Op != "+" && e.Op != "-" && e.Op != "NOT" {
			return 0
		}
		if n := typedPredicateCost(e.Right); n > 0 {
			return min(6, n+1)
		}
	case *BinaryExpr:
		switch e.Op {
		case "AND", "OR", "=", "<>", "<", "<=", ">", ">=", "IS", "IS NOT", "+", "-", "*", "/", "%", "||":
		default:
			return 0
		}
		a, b := typedPredicateCost(e.Left), typedPredicateCost(e.Right)
		if a > 0 && b > 0 {
			n := a + b + 1
			if e.Op == "AND" || e.Op == "OR" {
				n++
			}
			return min(6, n)
		}
	case *BetweenExpr:
		a, b, c := typedPredicateCost(e.Expr), typedPredicateCost(e.Low), typedPredicateCost(e.High)
		if a > 0 && b > 0 && c > 0 {
			return min(6, a+b+c+1)
		}
	}
	return 0
}

func (v *joinPredicateSlots) bind(depth int, row map[string]Value) {
	for _, column := range v.bindings[depth] {
		value, ok := row[column.name]
		if !ok {
			value = NewNullValue()
		}
		v.values[column.slot] = value
	}
}

func (v *joinPredicateSlots) reset(depth int) {
	for _, column := range v.bindings[depth] {
		v.values[column.slot] = NewNullValue()
	}
}

func (v *joinPredicateSlots) close() {
	if v == nil {
		return
	}
	for _, programs := range v.programs {
		for _, program := range programs {
			program.clearRegisters()
		}
	}
	v.final.clearRegisters()
	clear(v.values)
}
