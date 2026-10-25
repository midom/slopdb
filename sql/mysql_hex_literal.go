package sql

func mysqlNumericMetadataFunction(name string) bool {
	switch name {
	case "ABS", "ROUND", "TRUNCATE", "CEIL", "CEILING", "FLOOR", "MOD":
		return true
	}
	return false
}

func mysqlNumericHexExpression(expression Expression) bool {
	switch e := expression.(type) {
	case *LiteralExpr:
		return !e.BoundParameter && e.Value.HexLiteral
	case *SubqueryExpr:
		if e.Stmt.From == "" && len(e.Stmt.Joins) == 0 && len(e.Stmt.Columns) == 1 {
			return mysqlNumericHexExpression(e.Stmt.Columns[0].Expr)
		}
	}
	return false
}
