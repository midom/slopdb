package sql

import "fmt"

// Dialect selects session SQL behavior. Persistent catalog types and physical
// storage are shared; a session always has exactly one SQL dialect.
type Dialect uint8

const (
	DialectNative Dialect = iota
	DialectMySQL
	DialectPostgreSQL
)

func (d Dialect) String() string {
	switch d {
	case DialectNative:
		return "native"
	case DialectMySQL:
		return "mysql"
	case DialectPostgreSQL:
		return "postgresql"
	}
	return fmt.Sprintf("dialect(%d)", uint8(d))
}
func (d Dialect) Valid() bool      { return d <= DialectPostgreSQL }
func (s *Schema) Dialect() Dialect { return s.dialect }

// SetDialect changes private session semantics and invalidates execution recipes.
// It cannot change the interpretation of an active transaction.
func (s *Schema) SetDialect(d Dialect) error {
	if !d.Valid() {
		return fmt.Errorf("unsupported SQL dialect %d", d)
	}
	if s.dialect == d {
		return nil
	}
	if s.inTx {
		return fmt.Errorf("SQL dialect cannot change during a transaction")
	}
	s.dialect = d
	s.dialectEpoch++
	s.preparedPlan = nil
	return nil
}
func (s *Schema) MySQLMode() bool    { return s.dialect == DialectMySQL }
func (s *Schema) PostgresMode() bool { return s.dialect == DialectPostgreSQL }

// Compatibility setters select one dialect; disabling an inactive dialect
// leaves the current selection unchanged. New callers can use SetDialect to
// observe validation errors, including attempts during an active transaction.
func (s *Schema) SetMySQLMode(enabled bool) {
	if enabled {
		_ = s.SetDialect(DialectMySQL)
	} else if s.MySQLMode() {
		_ = s.SetDialect(DialectNative)
	}
}
func (s *Schema) SetPostgresMode(enabled bool) {
	if enabled {
		_ = s.SetDialect(DialectPostgreSQL)
	} else if s.PostgresMode() {
		_ = s.SetDialect(DialectNative)
	}
}

// ParseOptions derives connection syntax without mutating the physical database
// option. Native callers retain the existing Open-time input preference.
func (d Dialect) ParseOptions(base ParseOptions) ParseOptions {
	switch d {
	case DialectMySQL:
		base.MySQLQuoting = true
	case DialectPostgreSQL:
		base.MySQLQuoting = false
	}
	return base
}
