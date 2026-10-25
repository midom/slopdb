package sql

import "strings"

// IsLocalInspectionStatement admits no table/catalog reads. It is intentionally
// narrow: registering an arbitrary scalar function cannot bypass replication.
func IsLocalInspectionStatement(stmt Statement, dialects ...Dialect) bool {
	dialect := DialectNative
	if len(dialects) > 0 {
		dialect = dialects[0]
	}
	if local, ok := stmt.(interface{ LocalOnly() bool }); ok {
		return local.LocalOnly()
	}
	switch st := stmt.(type) {
	case *NamespaceStmt:
		return st.Operation == "use" || st.Operation == "search_path"
	case *ShowStmt:
		return st.Kind == "LSM STATUS" || st.Kind == "RAFT STATUS" || st.Kind == "GLOBAL VARIABLES" || st.Kind == "PROCESSLIST"
	case *SelectStmt:
		if activityInspectionSelect(st, dialect) {
			return true
		}
		if st.From != "" || st.FromExpr != nil || st.With != nil || st.Compound != nil || len(st.Joins) > 0 || st.Virtual != nil || len(st.GroupBy) > 0 || st.Having != nil || len(st.Windows) > 0 || st.Lock != nil {
			return false
		}
		observesState := false
		for _, c := range st.Columns {
			if c.Star || !localInspectionExpr(c.Expr) {
				return false
			}
			observesState = observesState || inspectionStateExpression(c.Expr)
		}
		if st.Where != nil && !localInspectionExpr(st.Where) {
			return false
		}
		observesState = observesState || inspectionStateExpression(st.Where)
		for _, o := range st.OrderBy {
			if !localInspectionExpr(o.Expr) {
				return false
			}
			observesState = observesState || inspectionStateExpression(o.Expr)
		}
		return observesState
	}
	return false
}

func inspectionStateExpression(expr Expression) bool {
	if expr == nil {
		return false
	}
	found := false
	_ = walkExpression(expr, func(e Expression) (bool, error) {
		if f, ok := e.(*FuncCallExpr); ok {
			switch strings.ToUpper(f.Name) {
			case "CURRENT_SETTING", "__SESSION_VARIABLE", "__SET_SESSION_VARIABLE", "__SYSTEM_VARIABLE":
				found = true
			}
		}
		return found, nil
	})
	return found
}
func localInspectionExpr(expr Expression) bool {
	switch e := expr.(type) {
	case *LiteralExpr, *ParameterExpr:
		return true
	case *UnaryExpr:
		return localInspectionExpr(e.Right)
	case *BinaryExpr:
		return localInspectionExpr(e.Left) && localInspectionExpr(e.Right)
	case *CaseExpr:
		if e.Expr != nil && !localInspectionExpr(e.Expr) {
			return false
		}
		for _, w := range e.Whens {
			if !localInspectionExpr(w.Cond) || !localInspectionExpr(w.Then) {
				return false
			}
		}
		return e.ElseExpr == nil || localInspectionExpr(e.ElseExpr)
	case *InListExpr:
		if !localInspectionExpr(e.Expr) {
			return false
		}
		for _, v := range e.List {
			if !localInspectionExpr(v) {
				return false
			}
		}
		return true
	case *BetweenExpr:
		return localInspectionExpr(e.Expr) && localInspectionExpr(e.Low) && localInspectionExpr(e.High)
	case *FuncCallExpr:
		if e.Window != nil || e.MySQLGroupConcat || len(e.GroupConcatOrder) > 0 || e.GroupConcatSeparator != nil {
			return false
		}
		switch strings.ToUpper(e.Name) {
		case "CURRENT_SETTING", "CAST", "COALESCE", "IFNULL", "NULLIF", "__SESSION_VARIABLE", "__SET_SESSION_VARIABLE", "__SYSTEM_VARIABLE":
		default:
			return false
		}
		for _, a := range e.Args {
			if !localInspectionExpr(a) {
				return false
			}
		}
		return true
	}
	return false
}

// Activity reads must contain only the built-in live relations. A user table,
// subquery, locking clause or extensible function cannot gain passive admission.
func activityInspectionSelect(st *SelectStmt, dialect Dialect) bool {
	if _, ok := ActivityRelationName(st.From, dialect); !ok {
		return false
	}
	if st.FromExpr != nil || st.With != nil || st.Compound != nil || st.Virtual != nil || len(st.Windows) > 0 || st.Lock != nil {
		return false
	}
	for _, j := range st.Joins {
		if _, ok := ActivityRelationName(j.Table, dialect); !ok {
			return false
		}
		if j.Subquery != nil || (j.On != nil && !activityInspectionExpr(j.On)) {
			return false
		}
	}
	for _, c := range st.Columns {
		if !c.Star && !activityInspectionExpr(c.Expr) {
			return false
		}
	}
	if st.Where != nil && !activityInspectionExpr(st.Where) {
		return false
	}
	for _, e := range st.GroupBy {
		if !activityInspectionExpr(e) {
			return false
		}
	}
	if st.Having != nil && !activityInspectionExpr(st.Having) {
		return false
	}
	for _, o := range st.OrderBy {
		if !activityInspectionExpr(o.Expr) {
			return false
		}
	}
	return true
}
func activityInspectionExpr(expr Expression) bool {
	valid := true
	_ = walkExpression(expr, func(e Expression) (bool, error) {
		switch v := e.(type) {
		case *LiteralExpr, *ParameterExpr, *ColumnRefExpr, *UnaryExpr, *BinaryExpr, *CaseExpr, *InListExpr, *BetweenExpr:
		case *FuncCallExpr:
			if v.Window != nil || v.MySQLGroupConcat || len(v.GroupConcatOrder) > 0 || v.GroupConcatSeparator != nil {
				valid = false
				return true, nil
			}
			switch strings.ToUpper(v.Name) {
			case "CAST", "COALESCE", "IFNULL", "NULLIF", "COUNT", "SUM", "AVG", "MIN", "MAX", "LENGTH", "CHAR_LENGTH", "CHARACTER_LENGTH", "LOWER", "UPPER", "CONCAT", "CONNECTION_ID", "PG_BACKEND_PID":
			default:
				valid = false
			}
		default:
			valid = false
		}
		return !valid, nil
	})
	return valid
}
