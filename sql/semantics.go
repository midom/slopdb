package sql

import (
	"bytes"
	"fmt"
	"math"
	"strconv"
	"strings"
	"unicode/utf8"

	"slopdb/storage"
)

// Primary keys are candidate keys: every component is unique and non-NULL.
func primaryKeyColumns(info *TableInfo) error {
	seen := make(map[string]bool)
	for _, name := range info.PKColumns {
		if seen[name] {
			return fmt.Errorf("duplicate PRIMARY KEY column: %s", name)
		}
		seen[name] = true
		found := false
		for i := range info.Columns {
			if info.Columns[i].Name == name {
				info.Columns[i].PrimaryKey = true
				info.Columns[i].NotNull = true
				found = true
			}
		}
		if !found {
			return fmt.Errorf("no such PRIMARY KEY column: %s", name)
		}
	}
	for _, col := range info.Columns {
		if col.AutoIncrement && (len(info.PKColumns) != 1 || info.PKColumns[0] != col.Name || col.ColType != ColInteger) {
			return fmt.Errorf("AUTOINCREMENT requires a single INTEGER PRIMARY KEY")
		}
	}
	return nil
}

func validatePrimaryKey(info *TableInfo, row map[string]Value) error {
	for _, name := range info.PKColumns {
		value, ok := row[name]
		if !ok || value.Null {
			return fmt.Errorf("NOT NULL constraint failed: %s.%s (PRIMARY KEY)", info.Name, name)
		}
	}
	return nil
}

func validateColumnValue(table string, col ColumnDef, v Value) error {
	if !v.Null && v.Type != col.ColType {
		return fmt.Errorf("datatype mismatch for column %s.%s: expected %s, got %s", table, col.Name, col.ColType, v.Type)
	}
	if !v.Null && col.Length > 0 {
		switch col.ColType {
		case ColText:
			if int64(utf8.RuneCountInString(v.Text)) > col.Length {
				return fmt.Errorf("value too long for %s.%s: %s allows %d characters", table, col.Name, col.DeclaredType(), col.Length)
			}
		case ColBlob:
			if int64(len(v.Blob)) > col.Length {
				return fmt.Errorf("value too long for %s.%s: %s allows %d bytes", table, col.Name, col.DeclaredType(), col.Length)
			}
		}
	}
	if !v.Null && v.Type == ColReal && (math.IsNaN(v.Real) || math.IsInf(v.Real, 0)) {
		return fmt.Errorf("numeric value out of range: %s.%s", table, col.Name)
	}
	return nil
}
func validateRowTypes(info *TableInfo, row map[string]Value) error {
	return validateRowTypesPreserving(info, row, nil, nil)
}

func validateRowTypesPreserving(info *TableInfo, row, original map[string]Value, assigned map[string]bool) error {
	for _, col := range info.Columns {
		if col.AllowExistingLength && original != nil && !assigned[col.Name] &&
			bytes.Equal(encodeValue(row[col.Name]), encodeValue(original[col.Name])) {
			// The stored value predates an ALLOW EXISTING shrink. Every newly
			// supplied value still passes the current length check.
			col.Length = 0
		}
		if err := validateColumnValue(info.Name, col, row[col.Name]); err != nil {
			return err
		}
	}
	return nil
}

// Old versions stored NULL primary keys in a dedicated fallback band. Probe
// that band once when upgrading nullable catalog metadata; never hide invalid
// existing rows or introduce an O(table-size) startup scan.
func (s *Schema) upgradePrimaryKeys() error {
	changed := false
	for _, info := range s.tables {
		legacy := false
		for _, name := range info.PKColumns {
			for _, col := range info.Columns {
				if col.Name == name && !col.NotNull {
					legacy = true
				}
			}
		}
		if legacy {
			prefix := []byte(info.storagePrefix(info.Name) + "\xffnull:")
			it := s.newRangeIterator(prefix, prefixSuccessor(prefix), false)
			invalid := it.Valid() && bytes.HasPrefix(it.Key().UserKey, prefix)
			err := storage.IteratorError(it)
			it.Close()
			if err != nil {
				return err
			}
			if invalid {
				return fmt.Errorf("table %s contains legacy NULL primary keys; repair them before opening with strict primary keys", info.Name)
			}
			changed = true
		}
		if err := primaryKeyColumns(info); err != nil {
			return err
		}
	}
	if changed {
		return s.saveSchema()
	}
	return nil
}

func suppliedColumn(columns []string, name string, defaultValues bool) bool {
	if defaultValues {
		return false
	}
	if len(columns) == 0 {
		return true
	}
	for _, column := range columns {
		if strings.EqualFold(column, name) {
			return true
		}
	}
	return false
}

var scalarExactArguments = map[string]int{"IFNULL": 2, "NULLIF": 2, "LENGTH": 1, "UPPER": 1, "LOWER": 1, "ABS": 1, "IIF": 3, "REPLACE": 3, "RANDOM": 0, "CAST": 2, "HEX": 1, "TYPEOF": 1, "QUOTE": 1, "INSTR": 2, "CHANGES": 0, "LAST_INSERT_ROWID": 0}

func validateScalarFunction(f *FuncCallExpr) error {
	if f.Name == fulltextFunction {
		return validateFulltextFunction(f)
	}
	name := strings.ToUpper(f.Name)
	n := len(f.Args)
	if exact, ok := scalarExactArguments[name]; ok {
		if n != exact {
			return fmt.Errorf("wrong number of arguments to %s", name)
		}
		if name == "CAST" {
			if literal, ok := f.Args[1].(*LiteralExpr); ok {
				switch strings.ToUpper(literal.Value.Text) {
				case "INT", "INTEGER", "BIGINT", "REAL", "FLOAT", "DOUBLE", "DOUBLE PRECISION", "TEXT", "BLOB":
				default:
					if _, _, ok := varyingCastType(literal.Value.Text); !ok {
						return fmt.Errorf("unsupported cast type: %s", literal.Value.Text)
					}
				}
			}
		}
		return nil
	}
	valid := false
	switch name {
	case "COALESCE", "MIN", "MAX":
		valid = n >= 2
	case "ROUND", "TRIM", "LTRIM", "RTRIM":
		valid = n >= 1 && n <= 2
	case "SUBSTR":
		valid = n == 2 || n == 3
	case "DATETIME", "DATE", "TIME", "JULIANDAY":
		valid = true
	case "STRFTIME":
		valid = n >= 1
	default:
		return fmt.Errorf("unknown function: %s", name)
	}
	if !valid {
		return fmt.Errorf("wrong number of arguments to %s", name)
	}
	return nil
}

func varyingCastType(name string) (ColumnType, int64, bool) {
	name = strings.ToUpper(name)
	for _, spec := range []struct {
		prefix string
		typ    ColumnType
	}{{"VARCHAR(", ColText}, {"VARBINARY(", ColBlob}} {
		if !strings.HasPrefix(name, spec.prefix) || !strings.HasSuffix(name, ")") {
			continue
		}
		length, err := strconv.ParseInt(name[len(spec.prefix):len(name)-1], 10, 64)
		if err == nil && length > 0 {
			return spec.typ, length, true
		}
	}
	return 0, 0, false
}

func castSQLValue(value Value, target string) (Value, error) {
	if value.Null {
		return NewNullValue(), nil
	}
	name := strings.ToUpper(target)
	if columnType, length, ok := varyingCastType(name); ok {
		underlying := "TEXT"
		if columnType == ColBlob {
			underlying = "BLOB"
		}
		converted, err := castSQLValue(value, underlying)
		if err != nil {
			return NewNullValue(), err
		}
		if err := validateColumnValue("CAST", ColumnDef{Name: underlying, ColType: columnType, Length: length}, converted); err != nil {
			return NewNullValue(), err
		}
		return converted, nil
	}
	switch name {
	case "INT", "INTEGER", "BIGINT", "REAL", "FLOAT", "DOUBLE", "DOUBLE PRECISION":
		v := value
		if v.Type == ColText {
			text := strings.TrimSpace(v.Text)
			if i, err := strconv.ParseInt(text, 10, 64); err == nil {
				v = NewIntValue(i)
			} else if f, err := strconv.ParseFloat(text, 64); err == nil {
				v = NewRealValue(f)
			} else {
				return NewNullValue(), fmt.Errorf("invalid input for %s: %q", name, text)
			}
		}
		if v.Type != ColInteger && v.Type != ColReal {
			return NewNullValue(), fmt.Errorf("invalid cast to %s", name)
		}
		if v.Type == ColReal && (math.IsNaN(v.Real) || math.IsInf(v.Real, 0)) {
			return NewNullValue(), fmt.Errorf("numeric value out of range")
		}
		if name == "INT" || name == "INTEGER" || name == "BIGINT" {
			if v.Type == ColInteger {
				return v, nil
			}
			if v.Real < math.MinInt64 || v.Real >= 9223372036854775808.0 {
				return NewNullValue(), fmt.Errorf("integer overflow")
			}
			return NewIntValue(int64(v.Real)), nil
		}
		return NewRealValue(toFloat(v)), nil
	case "TEXT":
		if value.Type == ColBlob {
			return NewTextValue(string(value.Blob)), nil
		}
		return NewTextValue(value.String()), nil
	case "BLOB":
		if value.Type == ColBlob {
			return value, nil
		}
		return NewBlobValue([]byte(value.String())), nil
	}
	return NewNullValue(), fmt.Errorf("unsupported cast type: %s", target)
}

func validateConstraintExpressions(info *TableInfo) error {
	check := func(expr Expression, defaults bool) error {
		if err := validateAggregateExpr(expr, false); err != nil {
			return err
		}
		return walkExpression(expr, func(e Expression) (bool, error) {
			switch a := e.(type) {
			case *ColumnRefExpr:
				if defaults {
					return true, fmt.Errorf("DEFAULT cannot reference column %s", a.Name)
				}
				found := false
				for _, col := range info.Columns {
					found = found || col.Name == a.Name
				}
				if !found {
					return true, fmt.Errorf("no such CHECK column: %s", a.Name)
				}
			case *SubqueryExpr, *ExistsExpr:
				return true, fmt.Errorf("subqueries are not allowed in constraints")
			}
			return false, nil
		})
	}
	for _, col := range info.Columns {
		if err := check(col.Default, true); err != nil {
			return err
		}
		if err := check(col.Check, false); err != nil {
			return err
		}
	}
	for _, expr := range info.TableCheck {
		if err := check(expr, false); err != nil {
			return err
		}
	}
	return nil
}

func (s *Schema) validateDMLExpressions(info *TableInfo, where Expression, sets []UpdateSet, returning []SelectColumn) error {
	columns := append([]SelectColumn(nil), returning...)
	for _, set := range sets {
		columns = append(columns, SelectColumn{Expr: set.Value})
	}
	for _, col := range columns {
		if err := validateAggregateExpr(col.Expr, false); err != nil {
			return err
		}
	}
	if err := validateAggregateExpr(where, false); err != nil {
		return err
	}
	return s.validateQuery(&SelectStmt{From: info.Name, Where: where, Columns: columns, Limit: -1}, info)
}
