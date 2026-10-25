package sql

import "strings"

// IsLocalInspectionStatement admits no table/catalog reads. It is intentionally
// narrow: registering an arbitrary scalar function cannot bypass replication.
func IsLocalInspectionStatement(stmt Statement) bool {
	if local, ok := stmt.(interface{ LocalOnly() bool }); ok {
		return local.LocalOnly()
	}
	switch st := stmt.(type) {
	case *NamespaceStmt:
		return st.Operation == "use" || st.Operation == "search_path"
	case *ShowStmt:
		return st.Kind == "LSM STATUS" || st.Kind == "RAFT STATUS" || st.Kind == "GLOBAL VARIABLES"
	case *SelectStmt:
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
