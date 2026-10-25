package sql

import "strings"

func nativeFunctionRegistry() map[string]FunctionSpec {
	out := map[string]FunctionSpec{}
	core := func(s *Schema, f *FuncCallExpr, row map[string]Value) (Value, error) {
		return s.evalCoreFunction(f, row)
	}
	add := func(name string, min, max int, volatile bool, handler func(*Schema, *FuncCallExpr, map[string]Value) (Value, error)) {
		out[name] = FunctionSpec{Min: min, Max: max, Volatile: volatile, EvalExpr: handler, Metadata: func(args []ColumnDef) ColumnDef { return coreFunctionMetadata(name, args) }}
	}
	for name, n := range scalarExactArguments {
		add(name, n, n, name == "RANDOM" || name == "CHANGES" || name == "LAST_INSERT_ROWID", core)
	}
	for _, name := range []string{"COALESCE", "MIN", "MAX"} {
		add(name, 2, -1, false, core)
	}
	for _, name := range []string{"ROUND", "TRIM", "LTRIM", "RTRIM"} {
		add(name, 1, 2, false, core)
	}
	add("SUBSTR", 2, 3, false, core)
	for _, name := range []string{"DATETIME", "DATE", "TIME", "JULIANDAY"} {
		add(name, 0, -1, true, core)
	}
	add("STRFTIME", 1, -1, true, core)
	for name, limits := range jsonFunctionArity {
		name := name
		spec := FunctionSpec{Min: limits[0], Max: limits[1], Metadata: func(args []ColumnDef) ColumnDef { return coreFunctionMetadata(name, args) }, Eval: func(_ *Schema, a []Value) (Value, error) { return evaluateJSONFunction(name, a) }, ValidArgs: func(n int) bool { valid, _ := validJSONFunction(name, n); return valid }}
		out[name] = spec
	}
	specs := []struct {
		name     string
		min, max int
		volatile bool
	}{{"IF", 3, 3, false}, {"GREATEST", 2, -1, false}, {"LEAST", 2, -1, false}, {"CONCAT", 1, -1, false}, {"SUBSTRING", 2, 3, false}, {"SYSDATE", 0, 1, true}, {"NOW", 0, 1, true}, {"CURRENT_TIMESTAMP", 0, 1, true}, {"UNIX_TIMESTAMP", 0, 1, true}, {"GET_LOCK", 2, 2, true}, {"__MYSQL_CONVERT", 2, 2, false}, {"RELEASE_LOCK", 1, 1, true}, {"IS_FREE_LOCK", 1, 1, true}, {"CURRENT_USER", 0, 0, true}, {"DATABASE", 0, 0, true}, {"VERSION", 0, 0, true}, {"CONNECTION_ID", 0, 0, true}, {"RAND", 0, 1, true}, {"BENCHMARK", 2, 2, true}}
	for _, x := range specs {
		add(x.name, x.min, x.max, x.volatile, legacyMySQLFunction)
	}
	out[fulltextFunction] = FunctionSpec{Min: 1, Max: -1, Return: ColReal, EvalExpr: func(s *Schema, f *FuncCallExpr, row map[string]Value) (Value, error) { return s.evalFulltext(f, row) }}
	// Aggregate/window descriptors share the same metadata and volatility source;
	// their specialized operators remain responsible for evaluation.
	for _, name := range strings.Fields("COUNT SUM AVG TOTAL GROUP_CONCAT ROW_NUMBER RANK DENSE_RANK NTILE PERCENT_RANK CUME_DIST LAG LEAD FIRST_VALUE LAST_VALUE NTH_VALUE") {
		name := name
		out[name] = FunctionSpec{Min: 0, Max: -1, Metadata: func(args []ColumnDef) ColumnDef { return coreFunctionMetadata(name, args) }}
	}
	return out
}
func coreFunctionMetadata(name string, args []ColumnDef) ColumnDef {
	text := ColumnDef{ColType: ColText}
	integer := ColumnDef{ColType: ColInteger}
	real := ColumnDef{ColType: ColReal}
	common := text
	if len(args) > 0 {
		common = args[0]
		for _, a := range args[1:] {
			common = describeCommon(common, a)
		}
	}
	switch name {
	case "COUNT", "LENGTH", "OCTET_LENGTH", "CHAR_LENGTH", "INSTR", "CHANGES", "LAST_INSERT_ROWID", "ROW_NUMBER", "RANK", "DENSE_RANK", "NTILE", "CONNECTION_ID", "GET_LOCK", "RELEASE_LOCK", "IS_FREE_LOCK", "UNIX_TIMESTAMP", "RANDOM":
		return integer
	case "AVG":
		if common.ColType == ColDecimal || common.ColType == ColUnsigned {
			return ColumnDef{ColType: ColDecimal, Declared: "DECIMAL"}
		}
		return real
	case "TOTAL", "PERCENT_RANK", "CUME_DIST", fulltextFunction, "JULIANDAY", "RAND":
		return real
	case "MIN", "MAX", "ABS", "SUM", "COALESCE", "IFNULL", "NULLIF", "LAG", "LEAD", "FIRST_VALUE", "LAST_VALUE", "NTH_VALUE", "GREATEST", "LEAST":
		return common
	case "IF", "IIF":
		if len(args) == 3 {
			return describeCommon(args[1], args[2])
		}
	case "GROUP_CONCAT", "CONCAT", "SUBSTRING", "SUBSTR":
		for _, a := range args {
			if a.ColType == ColBlob {
				return ColumnDef{ColType: ColBlob}
			}
		}
		return text
	case "CAST":
		return ColumnDef{ColType: ColUnknown}
	}
	if strings.HasPrefix(name, "JSON_") || name == "JSON" || name == "JSONB" || strings.HasPrefix(name, "__JSON_") {
		switch name {
		case "JSON_UNQUOTE", "JSON_TYPE", "JSON_VALUE", "__JSON_GET_TEXT", "__JSON_PATH_TEXT":
			return text
		case "JSON_VALID", "JSON_LENGTH", "JSON_DEPTH", "JSON_CONTAINS", "JSON_CONTAINS_PATH", "JSON_OVERLAPS", "JSON_STORAGE_SIZE", "JSON_STORAGE_FREE":
			return integer
		}
		return ColumnDef{ColType: ColJSON, Declared: "JSON"}
	}
	return text
}
