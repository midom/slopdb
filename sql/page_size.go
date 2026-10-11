package sql

import "fmt"

func (s *Schema) pragmaPageSize(stmt *PragmaStmt) (*ResultSet, error) {
	expression := stmt.Value
	if len(stmt.Args) > 0 {
		if len(stmt.Args) != 1 || expression != nil {
			return nil, fmt.Errorf("PRAGMA page_size accepts one integer value")
		}
		expression = stmt.Args[0]
	}
	if expression == nil {
		return s.singleValueResult("page_size", NewIntValue(s.lsm.PageSize()))
	}
	if s.InTransaction() {
		return nil, fmt.Errorf("PRAGMA page_size cannot change inside a transaction")
	}
	literal, ok := tupleLiteral(expression)
	if !ok || literal.Value.Null || literal.Value.Type != ColInteger {
		return nil, fmt.Errorf("PRAGMA page_size requires an integer value")
	}
	if err := s.checkInterrupt(); err != nil {
		return nil, err
	}
	if err := s.lsm.SetPageSize(literal.Value.Int); err != nil {
		return nil, err
	}
	return &ResultSet{}, nil
}
