package sql

import (
	"fmt"
)

type mysqlFunctionSessionState struct {
	lastID    uint64
	rowCount  int64
	foundRows uint64
	generated bool
}

// These values belong to one connection and survive transaction rollback, like
// MySQL's informational functions. They never become catalog or WAL metadata.
func (s *Schema) RecordMySQLFunctionResult(stmt Statement, changes int64, rows uint64) {
	if !s.MySQLMode() {
		return
	}
	switch stmt.(type) {
	case *SelectStmt, *UnionStmt:
		s.mysqlFunctions.rowCount = -1
		s.mysqlFunctions.foundRows = rows
	case *ShowStmt, *ExplainStmt, *PragmaStmt:
		s.mysqlFunctions.rowCount = -1
	case *InsertStmt, *UpdateStmt, *DeleteStmt:
		s.mysqlFunctions.rowCount = changes
	default:
		s.mysqlFunctions.rowCount = 0
	}
}

func (s *Schema) noteMySQLGeneratedID(v Value) {
	if !s.MySQLMode() || s.triggerDepth != 0 || s.mysqlFunctions.generated {
		return
	}
	if v.Type == ColUnsigned {
		s.mysqlFunctions.lastID = v.Uint
	} else if v.Type == ColInteger && v.Int >= 0 {
		s.mysqlFunctions.lastID = uint64(v.Int)
	} else {
		return
	}
	s.mysqlFunctions.generated = true
}

func mysqlSessionFunctions() map[string]mysqlFunctionSpec {
	out := map[string]mysqlFunctionSpec{
		"LAST_INSERT_ID": {Min: 0, Max: 1, Return: ColUnsigned, Volatile: true, Eval: func(s *Schema, a []Value) (Value, error) {
			if len(a) > 0 {
				if a[0].Null {
					return NewNullValue(), nil
				}
				s.mysqlFunctions.lastID = mysqlUtilityUint(a[0])
			}
			return NewUnsignedValue(s.mysqlFunctions.lastID), nil
		}},
		"ROW_COUNT":  {Min: 0, Max: 0, Return: ColInteger, Volatile: true, Eval: func(s *Schema, a []Value) (Value, error) { return NewIntValue(s.mysqlFunctions.rowCount), nil }},
		"FOUND_ROWS": {Min: 0, Max: 0, Return: ColUnsigned, Volatile: true, Eval: func(s *Schema, a []Value) (Value, error) { return NewUnsignedValue(s.mysqlFunctions.foundRows), nil }},
		"BENCHMARK":  {Min: 2, Max: 2, Return: ColInteger, Volatile: true},
	}
	for _, name := range []string{"IS_USED_LOCK", "RELEASE_ALL_LOCKS"} {
		name := name
		spec := mysqlFunctionSpec{Min: 1, Max: 1, Return: ColInteger, Volatile: true}
		if name == "RELEASE_ALL_LOCKS" {
			spec.Min, spec.Max = 0, 0
		}
		spec.Eval = func(s *Schema, a []Value) (Value, error) {
			if value, ok, err := MySQLFunction(s.ctx, name, a); ok || err != nil {
				return value, err
			}
			return NewNullValue(), fmt.Errorf("%s requires a MySQL connection", name)
		}
		out[name] = spec
	}
	return out
}

func (s *Schema) evalMySQLBenchmark(e *FuncCallExpr, row map[string]Value) (Value, bool, error) {
	if len(e.Args) != 2 {
		return NewNullValue(), true, fmt.Errorf("wrong number of arguments to BENCHMARK")
	}
	value, err := s.evalExpr(e.Args[0], row)
	if err != nil {
		return NewNullValue(), true, err
	}
	if value.Null {
		return NewNullValue(), true, nil
	}
	numeric := mysqlFunctionNumber(value)
	n, _, err := valueDecimal(numeric)
	if err != nil || n.Sign() < 0 {
		return NewNullValue(), true, nil
	}
	count := mysqlUtilityUint(value)
	for i := uint64(0); i < count; i++ {
		if i&255 == 0 {
			if err := s.checkInterrupt(); err != nil {
				return NewNullValue(), true, err
			}
		}
		if _, err := s.evalExpr(e.Args[1], row); err != nil {
			return NewNullValue(), true, err
		}
	}
	return NewIntValue(0), true, nil
}
