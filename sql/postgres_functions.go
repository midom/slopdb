package sql

import (
	"fmt"
	"strings"
)

// Registry descriptors share the engine's metadata and bounded evaluation shape.
var postgresFunctionRegistry map[string]mysqlFunctionSpec

func init() { postgresFunctionRegistry = postgresFunctions() }
func postgresFunctions() map[string]mysqlFunctionSpec {
	out := map[string]mysqlFunctionSpec{}
	for _, group := range []map[string]mysqlFunctionSpec{postgresStringFunctions(), postgresJSONFunctions(), postgresMathFunctions(), postgresClockFunctions(), postgresRegexpFunctions(), postgresCatalogFunctions()} {
		for name, spec := range group {
			if _, ok := out[name]; ok {
				panic("duplicate PostgreSQL function " + name)
			}
			out[name] = spec
		}
	}
	return out
}

// PostgresFunctionMetadata exposes immutable builtin metadata to protocol
// Describe without evaluating an expression or taking query locks.
func PostgresFunctionMetadata(name string, args []ColumnDef) (ColumnDef, bool) {
	spec, ok := postgresFunctionRegistry[strings.ToUpper(name)]
	if !ok {
		return ColumnDef{}, false
	}
	if spec.Metadata != nil {
		return spec.Metadata(args), true
	}
	return ColumnDef{ColType: spec.Return}, true
}
func validPostgresFunction(name string, n int) (bool, bool) {
	spec, ok := postgresFunctionRegistry[name]
	valid := ok && n >= spec.Min && (spec.Max < 0 || n <= spec.Max)
	if name == "JSON_BUILD_OBJECT" || name == "JSONB_BUILD_OBJECT" {
		valid = valid && n%2 == 0
	}
	return valid, ok
}
func (s *Schema) evalPostgresFunc(e *FuncCallExpr, row map[string]Value) (Value, bool, error) {
	if !s.PostgresMode() {
		return Value{}, false, nil
	}
	name := strings.ToUpper(e.Name)
	spec, ok := postgresFunctionRegistry[name]
	if !ok {
		return Value{}, false, nil
	}
	if len(e.Args) < spec.Min || spec.Max >= 0 && len(e.Args) > spec.Max {
		return NewNullValue(), true, fmt.Errorf("wrong number of arguments to %s", name)
	}
	args := make([]Value, len(e.Args))
	for i, arg := range e.Args {
		v, err := s.evalExpr(arg, row)
		if err != nil {
			return NewNullValue(), true, err
		}
		args[i] = v
	}
	v, err := spec.Eval(s, args)
	if err == nil && v.PGType == "" && spec.Metadata != nil {
		columns := make([]ColumnDef, len(args))
		for i, arg := range args {
			columns[i] = ColumnDef{ColType: arg.Type, Declared: arg.PGType, Postgres: true}
		}
		v.PGType = spec.Metadata(columns).Declared
	}
	return v, true, err
}

func (s *Schema) describePostgresFunction(e *FuncCallExpr, env map[string]ColumnDef, scope map[string][]ColumnDef, depth int) (ColumnDef, bool, error) {
	spec, ok := postgresFunctionRegistry[strings.ToUpper(e.Name)]
	if !ok {
		return ColumnDef{}, false, nil
	}
	if len(e.Args) < spec.Min || spec.Max >= 0 && len(e.Args) > spec.Max {
		return ColumnDef{}, true, fmt.Errorf("wrong number of arguments to %s", e.Name)
	}
	args := make([]ColumnDef, len(e.Args))
	for i, arg := range e.Args {
		c, err := s.describeExpression(arg, env, scope, depth+1)
		if err != nil {
			return ColumnDef{}, true, err
		}
		args[i] = c
	}
	if spec.Metadata != nil {
		return spec.Metadata(args), true, nil
	}
	return ColumnDef{ColType: spec.Return}, true, nil
}

func postgresBooleanOperator(op string) bool {
	switch strings.ToUpper(op) {
	case "=", "!=", "<>", "<", ">", "<=", ">=", "IS", "IS NOT", "AND", "OR", "LIKE", "NOT LIKE", "ILIKE", "NOT ILIKE", "GLOB", "NOT GLOB", "REGEXP", "NOT REGEXP":
		return true
	}
	return false
}
func (s *Schema) postgresBooleanValue(b bool) Value {
	v := NewIntValue(0)
	if b {
		v.Int = 1
	}
	if s.PostgresMode() {
		v.PGType = "BOOLEAN"
	}
	return v
}
func (s *Schema) booleanColumn() ColumnDef {
	if s.PostgresMode() {
		return ColumnDef{ColType: ColInteger, Declared: "BOOLEAN", Postgres: true}
	}
	return ColumnDef{ColType: ColInteger}
}
