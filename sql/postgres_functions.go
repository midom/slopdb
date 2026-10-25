package sql

import (
	"strings"
)

// Registry descriptors share the engine's metadata and bounded evaluation shape.
var postgresFunctionRegistry map[string]FunctionSpec

func postgresFunctions() map[string]FunctionSpec {
	return buildFunctionRegistry(DialectPostgreSQL, postgresStringFunctions(), postgresJSONFunctions(), postgresMathFunctions(), postgresClockFunctions(), postgresRegexpFunctions(), postgresCatalogFunctions(), postgresExtraFunctions())
}

// PostgresFunctionMetadata exposes immutable builtin metadata to protocol
// Describe without evaluating an expression or taking query locks.
func PostgresFunctionMetadata(name string, args []ColumnDef) (ColumnDef, bool) {
	c, known, e := FunctionMetadata(DialectPostgreSQL, name, args)
	return c, known && e == nil
}
func validPostgresFunction(name string, n int) (bool, bool) {
	spec, ok := LookupFunction(DialectPostgreSQL, name)
	return ok && spec.valid(n), ok
}
func (s *Schema) describePostgresFunction(e *FuncCallExpr, env map[string]ColumnDef, scope map[string][]ColumnDef, depth int) (ColumnDef, bool, error) {
	if !s.PostgresMode() {
		return ColumnDef{}, false, nil
	}
	args := make([]ColumnDef, len(e.Args))
	for i, arg := range e.Args {
		c, err := s.describeExpression(arg, env, scope, depth+1)
		if err != nil {
			return ColumnDef{}, true, err
		}
		args[i] = c
	}
	return FunctionMetadata(s.Dialect(), e.Name, args)
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
		v = v.WithSemanticType(TypeBoolean)
	}
	return v
}
func (s *Schema) booleanColumn() ColumnDef {
	if s.PostgresMode() {
		return LogicalType{Kind: TypeBoolean}.Column()
	}
	return ColumnDef{ColType: ColInteger}
}
