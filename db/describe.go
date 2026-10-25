package db

import (
	"context"
	"fmt"
	"slopdb/sql"
)

// DescribePrepared inspects a statement's current result metadata. It never
// executes user SQL, evaluates defaults/functions, or obtains row locks.
func (s *Session) DescribePrepared(ctx context.Context, stmt *Stmt) ([]sql.ColumnDef, error) {
	d, err := s.DescribePreparedTypes(ctx, stmt, sql.DescribeOptions{})
	return d.Results, err
}

// DescribePreparedTypes supplies one shared, protocol-neutral metadata result.
// Parameter types are constraints; no placeholder values or SQL are executed.
func (s *Session) DescribePreparedTypes(ctx context.Context, stmt *Stmt, options sql.DescribeOptions) (sql.PreparedDescription, error) {
	if stmt == nil || stmt.db != s.db {
		return sql.PreparedDescription{}, fmt.Errorf("statement belongs to another database")
	}
	if stmt.closed.Load() {
		return sql.PreparedDescription{}, ErrStmtClosed
	}
	var result sql.PreparedDescription
	err := s.readCatalog(ctx, func(schema *sql.Schema) error {
		resolved, err := schema.ResolveNamespaces(stmt.query.MetadataStatement())
		if err != nil {
			return err
		}
		result, err = schema.DescribePrepared(resolved, options)
		return err
	})
	return result, err
}
