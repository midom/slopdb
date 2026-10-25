package sql

import (
	"fmt"
	"strings"
)

// DialectSet limits the dialects in which an implementation overrides a name.
// Zero keeps existing native extensions available to every dialect.
type DialectSet uint8

const (
	OnlyNative     DialectSet = 1 << DialectNative
	OnlyMySQL      DialectSet = 1 << DialectMySQL
	OnlyPostgreSQL DialectSet = 1 << DialectPostgreSQL
)

// FunctionSpec is the one immutable descriptor consumed by validation,
// argument inference, evaluation, result description and optimizer traits.
// EvalExpr preserves lazy/special forms; ordinary functions use Eval.
type FunctionSpec struct {
	Min, Max        int
	Return          ColumnType
	Metadata        func([]ColumnDef) ColumnDef
	Arguments       func([]ColumnDef) ([]ColumnDef, error)
	ValidArgs       func(int) bool
	Volatile        bool
	Dialects        DialectSet
	StrictArguments bool
	Eval            func(*Schema, []Value) (Value, error)
	EvalExpr        func(*Schema, *FuncCallExpr, map[string]Value) (Value, error)
}

func (f FunctionSpec) supports(d Dialect) bool { return f.Dialects == 0 || f.Dialects&(1<<d) != 0 }
func (f FunctionSpec) valid(n int) bool {
	return n >= f.Min && (f.Max < 0 || n <= f.Max) && (f.ValidArgs == nil || f.ValidArgs(n))
}
func (f FunctionSpec) result(args []ColumnDef) ColumnDef {
	if f.Metadata != nil {
		return f.Metadata(args)
	}
	return ColumnDef{ColType: f.Return, Unsigned: f.Return == ColUnsigned}
}

func buildFunctionRegistry(d Dialect, families ...map[string]FunctionSpec) map[string]FunctionSpec {
	out := map[string]FunctionSpec{}
	for _, family := range families {
		for name, spec := range family {
			if _, exists := out[name]; exists {
				panic("duplicate " + d.String() + " function: " + name)
			}
			out[name] = spec
		}
	}
	return out
}

var functionRegistries [3]map[string]FunctionSpec

func init() {
	mysqlFunctionRegistry = buildMySQLFunctionRegistry()
	postgresFunctionRegistry = postgresFunctions()
	base := nativeFunctionRegistry()
	for d := DialectNative; d <= DialectPostgreSQL; d++ {
		registry := make(map[string]FunctionSpec, len(base)+len(mysqlFunctionRegistry))
		for name, spec := range base {
			registry[name] = spec
		}
		for name, spec := range mysqlFunctionRegistry {
			if spec.supports(d) {
				registry[name] = spec
			}
		}
		// BENCHMARK is expression-based even though its metadata is a family entry.
		for _, name := range []string{"RAND", "BENCHMARK"} {
			spec := registry[name]
			spec.Volatile = true
			spec.Eval = nil
			spec.EvalExpr = legacyMySQLFunction
			registry[name] = spec
		}
		if d == DialectPostgreSQL {
			for name, spec := range postgresFunctionRegistry {
				spec.Dialects = OnlyPostgreSQL
				if name == "JSON_BUILD_OBJECT" || name == "JSONB_BUILD_OBJECT" {
					spec.ValidArgs = func(n int) bool { return n%2 == 0 }
				}
				spec.Arguments = postgresFunctionArgumentTypes(name)
				spec.StrictArguments = spec.Arguments != nil
				registry[name] = spec
			}
		}
		functionRegistries[d] = registry
	}
}

// LookupFunction returns a descriptor value, never a mutable registry map.
func LookupFunction(d Dialect, name string) (FunctionSpec, bool) {
	if !d.Valid() {
		return FunctionSpec{}, false
	}
	spec, ok := functionRegistries[d][strings.ToUpper(name)]
	return spec, ok
}
func FunctionMetadata(d Dialect, name string, args []ColumnDef) (ColumnDef, bool, error) {
	spec, ok := LookupFunction(d, name)
	if !ok {
		return ColumnDef{}, false, nil
	}
	if !spec.valid(len(args)) {
		return ColumnDef{}, true, functionArityError(name)
	}
	return spec.result(args), true, nil
}

// FunctionArgumentTypes infers constraints without evaluating an argument.
// Unknown/polymorphic positions remain ColUnknown, not guessed integer values.
func FunctionArgumentTypes(d Dialect, name string, args []ColumnDef) ([]ColumnDef, bool, error) {
	spec, ok := LookupFunction(d, name)
	if !ok {
		return nil, false, nil
	}
	if !spec.valid(len(args)) {
		return nil, true, functionArityError(name)
	}
	if spec.Arguments != nil {
		r, e := spec.Arguments(args)
		return r, true, e
	}
	result := make([]ColumnDef, len(args))
	for i := range result {
		result[i] = ColumnDef{ColType: ColUnknown}
	}
	return result, true, nil
}
func functionArityError(name string) error {
	return fmt.Errorf("wrong number of arguments to %s", strings.ToUpper(name))
}
func validateResolvedFunction(d Dialect, f *FuncCallExpr) error {
	spec, ok := LookupFunction(d, f.Name)
	if !ok {
		return fmt.Errorf("unknown function: %s", strings.ToUpper(f.Name))
	}
	if !spec.valid(len(f.Args)) {
		return functionArityError(f.Name)
	}
	return validateFunctionSyntax(f)
}
func validateAnyFunction(f *FuncCallExpr) error {
	known := false
	for d := DialectNative; d <= DialectPostgreSQL; d++ {
		spec, ok := LookupFunction(d, f.Name)
		known = known || ok
		if ok && spec.valid(len(f.Args)) {
			return validateFunctionSyntax(f)
		}
	}
	if known {
		return functionArityError(f.Name)
	}
	return fmt.Errorf("unknown function: %s", strings.ToUpper(f.Name))
}
func validateFunctionSyntax(f *FuncCallExpr) error {
	if f.Name == fulltextFunction {
		return validateFulltextFunction(f)
	}
	if !strings.EqualFold(f.Name, "CAST") || len(f.Args) != 2 {
		return nil
	}
	if literal, ok := f.Args[1].(*LiteralExpr); ok {
		name := strings.ToUpper(mysqlCastTarget(literal.Value.Text))
		switch name {
		case "INT", "INTEGER", "BIGINT", "REAL", "FLOAT", "DOUBLE", "DOUBLE PRECISION", "TEXT", "BLOB", "DECIMAL", "NUMERIC", "JSON", "JSONB", "UNSIGNED", "UNSIGNED INTEGER":
			return nil
		}
		if _, _, ok := varyingCastType(literal.Value.Text); ok || PostgresColumnOID(ColumnDef{Declared: literal.Value.Text}) != 0 || strings.HasPrefix(name, "DECIMAL(") || strings.HasPrefix(name, "NUMERIC(") {
			return nil
		}
		return fmt.Errorf("unsupported cast type: %s", literal.Value.Text)
	}
	return nil
}
func (s *Schema) evalResolvedFunction(f *FuncCallExpr, row map[string]Value) (Value, error) {
	spec, known := LookupFunction(s.Dialect(), f.Name)
	if !known {
		return NewNullValue(), fmt.Errorf("unknown function: %s", strings.ToUpper(f.Name))
	}
	if !spec.valid(len(f.Args)) {
		return NewNullValue(), functionArityError(f.Name)
	}
	if err := validateFunctionSyntax(f); err != nil {
		return NewNullValue(), err
	}
	if spec.EvalExpr != nil {
		return spec.EvalExpr(s, f, row)
	}
	args := make([]Value, len(f.Args))
	for i, e := range f.Args {
		v, err := s.evalExpr(e, row)
		if err != nil {
			return NewNullValue(), err
		}
		args[i] = v
	}
	if spec.StrictArguments {
		columns := functionValueColumns(args)
		types, err := spec.Arguments(columns)
		if err != nil {
			return NewNullValue(), err
		}
		for i, target := range types {
			if i >= len(args) {
				break
			}
			if target.ColType == ColUnknown || target.Declared == "" {
				continue
			}
			v, err := CastPostgresValue(args[i], target.Declared)
			if err != nil {
				return NewNullValue(), fmt.Errorf("%s argument %d: %w", f.Name, i+1, err)
			}
			args[i] = v
		}
	}
	if spec.Eval == nil {
		return NewNullValue(), fmt.Errorf("function %s requires its aggregate/window execution context", f.Name)
	}
	value, err := spec.Eval(s, args)
	if err == nil && value.SemanticType == TypeUnknown && value.PGType == "" && spec.Metadata != nil {
		column := spec.result(functionValueColumns(args))
		if column.SemanticType != TypeUnknown {
			value = value.WithSemanticType(column.SemanticType)
		} else if s.PostgresMode() && column.Declared != "" {
			value = value.WithDeclaredType(column.Declared)
		}
	}
	return value, err
}
func functionValueColumns(args []Value) []ColumnDef {
	columns := make([]ColumnDef, len(args))
	for i, arg := range args {
		columns[i] = arg.LogicalType().Column()
	}
	return columns
}
func legacyMySQLFunction(s *Schema, f *FuncCallExpr, row map[string]Value) (Value, error) {
	v, known, e := s.evalMySQLFunc(f, row)
	if !known && e == nil {
		return NewNullValue(), fmt.Errorf("unknown function: %s", f.Name)
	}
	return v, e
}
func FunctionDeterministic(d Dialect, name string) bool {
	spec, known := LookupFunction(d, name)
	return known && !spec.Volatile
}
func (s *Schema) deterministicFunction(name string) bool {
	return FunctionDeterministic(s.Dialect(), name)
}
