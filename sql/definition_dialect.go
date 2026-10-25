package sql

// DefinitionDialect records expression semantics independently of the invoking
// session. Zero preserves legacy catalog behavior; new definitions use dialect+1.
// Physical values, parsing of canonical stored SQL and transaction policies do
// not depend on this optional metadata.
type DefinitionDialect uint8

func definitionDialect(d Dialect) DefinitionDialect { return DefinitionDialect(d) + 1 }
func (d DefinitionDialect) valid() bool             { return d <= DefinitionDialect(DialectPostgreSQL)+1 }

// ExpressionDialect selects function, operator and cast semantics for the
// expression currently being evaluated. Dialect remains the session policy for
// isolation, locking, temporary-table lifecycle and protocol behavior.
func (s *Schema) ExpressionDialect() Dialect {
	if s.expressionDialect != 0 {
		return Dialect(s.expressionDialect - 1)
	}
	return s.Dialect()
}
func (s *Schema) expressionMySQL() bool    { return s.ExpressionDialect() == DialectMySQL }
func (s *Schema) expressionPostgres() bool { return s.ExpressionDialect() == DialectPostgreSQL }
func (s *Schema) enterDefinitionDialect(d DefinitionDialect) func() {
	previous := s.expressionDialect
	if d != 0 {
		s.expressionDialect = d
	}
	return func() { s.expressionDialect = previous }
}

func (s *Schema) stampColumnDefinitions(col *ColumnDef) {
	if col.Default != nil && col.DefaultDialect == 0 {
		col.DefaultDialect = definitionDialect(s.ExpressionDialect())
	}
	if col.Check != nil && col.CheckDialect == 0 {
		col.CheckDialect = definitionDialect(s.ExpressionDialect())
	}
}
func (s *Schema) evalDefinitionExpression(expr Expression, d DefinitionDialect, row map[string]Value) (Value, error) {
	defer s.enterDefinitionDialect(d)()
	return s.evalExpr(expr, row)
}
func (t *TableInfo) tableCheckDialect(index int) DefinitionDialect {
	if index < len(t.TableCheckDialects) {
		return t.TableCheckDialects[index]
	}
	return 0
}
func (t *TableInfo) padTableCheckDialects() {
	for len(t.TableCheckDialects) < len(t.TableCheck) {
		t.TableCheckDialects = append(t.TableCheckDialects, 0)
	}
}
