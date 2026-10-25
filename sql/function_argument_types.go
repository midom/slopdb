package sql

// Function argument inference is declarative and shared with prepared metadata.
// Conversion policy stays on the resolved implementation: PG uses strict casts,
// whereas MySQL evaluators retain their documented numeric-prefix conversions.
func postgresFunctionArgumentTypes(name string) func([]ColumnDef) ([]ColumnDef, error) {
	var signature []string
	switch name {
	case "CHR":
		signature = []string{"INTEGER"}
	case "LEFT", "RIGHT", "REPEAT":
		signature = []string{"TEXT", "INTEGER"}
	case "LPAD", "RPAD":
		signature = []string{"TEXT", "INTEGER", "TEXT"}
	case "SPLIT_PART":
		signature = []string{"TEXT", "TEXT", "INTEGER"}
	case "STRPOS", "REPLACE", "TRANSLATE", "BTRIM", "LTRIM", "RTRIM", "REVERSE", "INITCAP", "UPPER", "LOWER":
		signature = []string{"TEXT", "TEXT", "TEXT"}
	case "LENGTH", "CHAR_LENGTH", "CHARACTER_LENGTH", "OCTET_LENGTH", "BIT_LENGTH", "ASCII", "MD5":
		signature = []string{"@string"}
	case "SUBSTR", "SUBSTRING":
		return func(args []ColumnDef) ([]ColumnDef, error) {
			types := functionUnknownColumns(len(args))
			if len(args) > 0 {
				types[0] = functionStringArgument(args[0])
			}
			if len(args) > 1 {
				target := "INTEGER"
				if args[1].ColType == ColText {
					target = "TEXT"
				}
				types[1] = functionArgumentColumn(target)
			}
			if len(args) > 2 {
				types[2] = functionArgumentColumn("INTEGER")
			}
			return types, nil
		}
	case "OVERLAY":
		signature = []string{"@string", "@string", "INTEGER", "INTEGER"}
	case "POSITION":
		signature = []string{"@string", "@string"}
	case "GET_BYTE":
		signature = []string{"BYTEA", "INTEGER"}
	case "GET_BIT":
		signature = []string{"BYTEA", "BIGINT"}
	case "SET_BYTE":
		signature = []string{"BYTEA", "INTEGER", "INTEGER"}
	case "SET_BIT":
		signature = []string{"BYTEA", "BIGINT", "INTEGER"}
	case "ENCODE":
		signature = []string{"BYTEA", "TEXT"}
	case "DECODE":
		signature = []string{"TEXT", "TEXT"}
	case "CONVERT_FROM":
		signature = []string{"BYTEA", "NAME"}
	case "CONVERT_TO":
		signature = []string{"TEXT", "NAME"}
	case "FORMAT":
		signature = []string{"TEXT"}
	case "CONCAT_WS":
		signature = []string{"TEXT"}
	case "TO_REGCLASS", "TO_REGNAMESPACE":
		signature = []string{"TEXT"}
	case "FORMAT_TYPE":
		signature = []string{"OID", "INTEGER"}
	case "PG_GET_VIEWDEF":
		signature = []string{"", "BOOLEAN"}
	case "PG_GET_CONSTRAINTDEF":
		signature = []string{"OID", "BOOLEAN"}
	case "PG_GET_INDEXDEF":
		signature = []string{"OID", "INTEGER", "BOOLEAN"}
	case "PG_GET_EXPR":
		signature = []string{"TEXT", "OID", "BOOLEAN"}
	case "PG_TABLE_IS_VISIBLE", "PG_GET_USERBYID":
		signature = []string{"OID"}
	case "PG_GET_SERIAL_SEQUENCE":
		signature = []string{"TEXT", "TEXT"}
	case "PG_ENCODING_TO_CHAR":
		signature = []string{"INTEGER"}
	case "CURRENT_SETTING":
		signature = []string{"TEXT", "BOOLEAN"}
	case "REGEXP_LIKE":
		signature = []string{"TEXT", "TEXT", "TEXT"}
	case "REGEXP_COUNT":
		signature = []string{"TEXT", "TEXT", "INTEGER", "TEXT"}
	case "REGEXP_INSTR":
		signature = []string{"TEXT", "TEXT", "INTEGER", "INTEGER", "INTEGER", "TEXT", "INTEGER"}
	case "REGEXP_SUBSTR":
		signature = []string{"TEXT", "TEXT", "INTEGER", "INTEGER", "TEXT", "INTEGER"}
	case "REGEXP_REPLACE":
		return func(args []ColumnDef) ([]ColumnDef, error) {
			types := functionUnknownColumns(len(args))
			for i := 0; i < len(types) && i < 3; i++ {
				types[i] = functionArgumentColumn("TEXT")
			}
			if len(args) > 3 {
				if args[3].ColType == ColText {
					types[3] = functionArgumentColumn("TEXT")
				} else {
					types[3] = functionArgumentColumn("INTEGER")
				}
			}
			if len(args) > 4 {
				types[4] = functionArgumentColumn("INTEGER")
			}
			if len(args) > 5 {
				types[5] = functionArgumentColumn("TEXT")
			}
			return types, nil
		}
	case "PG_SLEEP":
		signature = []string{"DOUBLE PRECISION"}
	case "CURRENT_TIMESTAMP", "CURRENT_TIME", "LOCALTIME", "LOCALTIMESTAMP":
		signature = []string{"INTEGER"}
	case "MAKE_DATE":
		signature = []string{"INTEGER", "INTEGER", "INTEGER"}
	case "MAKE_TIME":
		signature = []string{"INTEGER", "INTEGER", "DOUBLE PRECISION"}
	case "MAKE_TIMESTAMP":
		signature = []string{"INTEGER", "INTEGER", "INTEGER", "INTEGER", "INTEGER", "DOUBLE PRECISION"}
	case "TO_DATE":
		signature = []string{"TEXT", "TEXT"}
	case "TO_TIMESTAMP":
		return func(args []ColumnDef) ([]ColumnDef, error) {
			if len(args) == 1 {
				return []ColumnDef{functionArgumentColumn("DOUBLE PRECISION")}, nil
			}
			return []ColumnDef{functionArgumentColumn("TEXT"), functionArgumentColumn("TEXT")}, nil
		}
	case "TO_CHAR":
		signature = []string{"", "TEXT"}
	case "DATE_PART", "DATE_TRUNC":
		signature = []string{"TEXT", "", "TEXT"}
	case "JSON_ARRAY_LENGTH", "JSON_TYPEOF", "JSON_EXTRACT_PATH", "JSON_EXTRACT_PATH_TEXT", "JSON_STRIP_NULLS":
		signature = []string{"JSON"}
	case "JSONB_ARRAY_LENGTH", "JSONB_TYPEOF", "JSONB_EXTRACT_PATH", "JSONB_EXTRACT_PATH_TEXT", "JSONB_STRIP_NULLS", "JSONB_PRETTY":
		signature = []string{"JSONB"}
	case "JSONB_SET", "JSONB_INSERT":
		signature = []string{"JSONB", "TEXT", "JSONB", "BOOLEAN"}
	case "JSONB_SET_LAX":
		signature = []string{"JSONB", "TEXT", "JSONB", "BOOLEAN", "TEXT"}
	default:
		return nil
	}
	return func(args []ColumnDef) ([]ColumnDef, error) {
		types := functionUnknownColumns(len(args))
		for i, target := range signature {
			if i >= len(args) {
				break
			}
			if target == "" {
				continue
			}
			if target == "@string" {
				types[i] = functionStringArgument(args[i])
			} else {
				types[i] = functionArgumentColumn(target)
			}
		}
		return types, nil
	}
}
func functionUnknownColumns(n int) []ColumnDef {
	out := make([]ColumnDef, n)
	for i := range out {
		out[i].ColType = ColUnknown
	}
	return out
}
func functionArgumentColumn(name string) ColumnDef { return LogicalTypeFromDeclaration(name).Column() }
func functionStringArgument(arg ColumnDef) ColumnDef {
	if arg.ColType == ColBlob {
		return functionArgumentColumn("BYTEA")
	}
	return functionArgumentColumn("TEXT")
}
func postgresFunctionInteger(v Value, wide bool) (int64, error) {
	name := "INTEGER"
	if wide {
		name = "BIGINT"
	}
	result, err := CastPostgresValue(v, name)
	return result.Int, err
}

// Result coercion is protocol-neutral: declarations are translated into compact
// logical tags; neither a PostgreSQL OID nor MySQL packet code reaches values.
func functionDeclaredResult(v Value, column ColumnDef) Value {
	if t := column.LogicalType(); t.Known() {
		return v.WithSemanticType(t.Kind)
	}
	return v
}

// These common constraints are used by inference only. Runtime conversion
// remains the resolved descriptor's policy, so MySQL prefix affinity is intact.
func sharedFunctionArgumentTypes(name string) func([]ColumnDef) ([]ColumnDef, error) {
	switch name {
	case "IF", "IIF", "COALESCE", "IFNULL", "NULLIF", "GREATEST", "LEAST", "MIN", "MAX", "SUM", "AVG", "TOTAL", "ABS", "ROUND", "TRUNC", "TRUNCATE":
		return func(args []ColumnDef) ([]ColumnDef, error) {
			out := functionUnknownColumns(len(args))
			start := 0
			if name == "IF" || name == "IIF" {
				start = 1
				if len(out) > 0 {
					out[0] = functionArgumentColumn("BIGINT")
				}
			}
			end := len(args)
			if (name == "ROUND" || name == "TRUNC" || name == "TRUNCATE") && end > 1 {
				end = 1
				out[1] = functionArgumentColumn("INTEGER")
			}
			common := ColumnDef{ColType: ColUnknown}
			for i := start; i < end; i++ {
				if args[i].ColType == ColUnknown {
					continue
				}
				if common.ColType == ColUnknown {
					common = args[i]
				} else {
					common = describeCommon(common, args[i])
				}
			}
			if common.ColType == ColUnknown {
				switch name {
				case "SUM", "AVG", "TOTAL", "ABS", "ROUND", "TRUNC", "TRUNCATE":
					common = functionArgumentColumn("NUMERIC")
				}
			}
			for i := start; i < end; i++ {
				out[i] = common
			}
			return out, nil
		}
	case "LENGTH", "CHAR_LENGTH", "CHARACTER_LENGTH", "OCTET_LENGTH", "BIT_LENGTH", "UPPER", "LOWER", "ASCII", "TRIM", "LTRIM", "RTRIM", "BTRIM", "CONCAT", "CONCAT_WS", "REPLACE", "INSTR", "STRPOS":
		return func(args []ColumnDef) ([]ColumnDef, error) {
			out := functionUnknownColumns(len(args))
			for i := range out {
				out[i] = functionStringArgument(args[i])
			}
			return out, nil
		}
	case "CEIL", "CEILING", "FLOOR", "SIGN", "GCD", "LCM", "MOD", "DIV", "WIDTH_BUCKET", "SCALE", "MIN_SCALE", "TRIM_SCALE", "FACTORIAL":
		return func(args []ColumnDef) ([]ColumnDef, error) {
			out := functionUnknownColumns(len(args))
			for i := range out {
				out[i] = functionArgumentColumn("NUMERIC")
			}
			if name == "WIDTH_BUCKET" && len(out) == 4 {
				out[3] = functionArgumentColumn("INTEGER")
			}
			return out, nil
		}
	case "SQRT", "CBRT", "EXP", "LN", "LOG", "LOG10", "POWER", "POW", "SIN", "COS", "TAN", "ASIN", "ACOS", "ATAN", "ATAN2", "COT", "SINH", "COSH", "TANH", "ASINH", "ACOSH", "ATANH", "DEGREES", "RADIANS":
		return func(args []ColumnDef) ([]ColumnDef, error) {
			out := functionUnknownColumns(len(args))
			for i := range out {
				out[i] = functionArgumentColumn("DOUBLE PRECISION")
			}
			return out, nil
		}
	}
	return nil
}
