package sql

import (
	"context"
	"strings"

	"slopdb/storage"
)

// countRange runs synchronously and pins one snapshot for its full traversal.
func (s *Schema) countRange(ctx context.Context, lower, upper []byte) (int64, error) {
	snapshot := s.statementSnapshot
	if !s.statementActive {
		snapshot = s.lsm.SnapshotForReader(s.snapshotReader)
		defer s.lsm.ReleaseSnapshotForReader(s.snapshotReader, snapshot)
	}
	return s.lsm.CountRangeAtContext(ctx, snapshot, lower, upper, s.snapshotReader)
}

// The common final projection still applies LIMIT/OFFSET and ownership.
func (s *Schema) simpleCount(stmt *SelectStmt, info *TableInfo, cols []selectExprCol) ([]map[string]Value, bool, error) {
	// Profiling uses the ordinary scan, whose Key/Value visits record actual
	// rows and bytes. A key-only count intentionally does not access either.
	if s.profile != nil {
		return nil, false, nil
	}
	if stmt.Where != nil || len(stmt.Joins) != 0 || len(stmt.GroupBy) != 0 || stmt.Having != nil || len(stmt.OrderBy) != 0 || len(cols) == 0 {
		return nil, false, nil
	}
	for _, c := range cols {
		f, ok := c.expr.(*FuncCallExpr)
		if !ok || !strings.EqualFold(f.Name, "COUNT") || f.Distinct || len(f.Args) != 1 {
			return nil, false, nil
		}
		switch a := f.Args[0].(type) {
		case *ColumnRefExpr:
			if a.Name != "*" {
				return nil, false, nil
			}
		case *LiteralExpr:
			if a.Value.Null {
				return nil, false, nil
			}
		default:
			return nil, false, nil
		}
	}

	lower := []byte(info.storagePrefix(info.Name))
	upper := prefixSuccessor(lower)
	var n int64
	var err error
	if (!s.inTx || len(s.txLatest) == 0) && !s.isTemporaryTable(info.Name) {
		ctx := s.ctx
		if ctx == nil {
			ctx = context.Background()
		}
		n, err = s.countRange(ctx, lower, upper)
	} else {
		it := s.newKeyRangeReader(lower, upper, false, "Scan")
		defer it.Close()
		for it.Valid() {
			if n&255 == 0 {
				if e := s.checkInterrupt(); e != nil {
					return nil, true, e
				}
			}
			n++
			it.Next()
		}
		err = storage.IteratorError(it)
	}
	if err != nil {
		return nil, true, err
	}

	row := make(map[string]Value, len(cols))
	for _, c := range cols {
		// Numeric literals can require dialect-specific DECIMAL validation.
		// Preserve its lazy error on nonempty input without evaluating it per row.
		if literal, ok := c.expr.(*FuncCallExpr).Args[0].(*LiteralExpr); ok && n > 0 && literal.Value.NumericText != "" {
			if _, err := s.evalExpr(literal, nil); err != nil {
				return nil, true, err
			}
		}
		row[c.name] = NewIntValue(n)
	}
	return []map[string]Value{row}, true, nil
}
