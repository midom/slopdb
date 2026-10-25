package sql

import (
	"context"
	"fmt"
)

// IsReplicaReadStatement conservatively admits queries and local transaction
// control, rejecting mutations, setting PRAGMAs and locks in nested queries.
func IsReplicaReadStatement(statement Statement) bool {
	var with *WithClause
	var expressions []Expression
	switch s := statement.(type) {
	case *ReplicationPositionStmt, *ReadBarrierStmt:
		return true
	case *BeginStmt, *SetIsolationStmt, *SetConstraintsStmt, *RollbackStmt, *SavepointStmt, *ReleaseStmt:
		return true
	case *NamespaceStmt:
		return s.Operation == "use" || s.Operation == "search_path"
	case *CommitStmt:
		return s.Durability != "durable"
	case *ShowStmt:
		expressions = append(expressions, s.Where)
	case *PragmaStmt:
		if s.Value != nil {
			return false
		}
		switch s.Name {
		case "table_info":
			return len(s.Args) == 1
		case "table_list", "database_list", "user_version", "page_size", "collation", "compile_options", "foreign_keys":
			return len(s.Args) == 0
		default:
			return false
		}
	case *ExplainStmt:
		return IsReplicaReadStatement(s.Stmt)
	case *SelectStmt:
		if s.Lock != nil || s.Compound != nil && !IsReplicaReadStatement(s.Compound) {
			return false
		}
		with = s.With
		expressions = append(expressions, s.FromExpr, s.Where, s.Having)
		for _, c := range s.Columns {
			expressions = append(expressions, c.Expr)
		}
		expressions = append(expressions, s.GroupBy...)
		for _, o := range s.OrderBy {
			expressions = append(expressions, o.Expr)
		}
		for _, j := range s.Joins {
			if j.Subquery != nil && !IsReplicaReadStatement(j.Subquery) {
				return false
			}
			expressions = append(expressions, j.On)
		}
		if s.Virtual != nil {
			expressions = append(expressions, s.Virtual.Args...)
		}
		for _, w := range s.Windows {
			expressions = append(expressions, windowExpressions(w.Spec)...)
		}
	case *UnionStmt:
		if !IsReplicaReadStatement(s.Left) || !IsReplicaReadStatement(s.Right) {
			return false
		}
		with = s.With
		for _, o := range s.OrderBy {
			expressions = append(expressions, o.Expr)
		}
	default:
		return false
	}
	if with != nil {
		for _, entry := range with.Entries {
			if !IsReplicaReadStatement(entry.Query) {
				return false
			}
		}
	}
	readOnly := true
	for _, expression := range expressions {
		_ = walkExpression(expression, func(e Expression) (bool, error) {
			switch q := e.(type) {
			case *SubqueryExpr:
				readOnly = readOnly && IsReplicaReadStatement(q.Stmt)
				return true, nil
			case *ExistsExpr:
				readOnly = readOnly && IsReplicaReadStatement(q.Stmt)
				return true, nil
			}
			return false, nil
		})
	}
	return readOnly
}

// SetStatementSnapshotHook installs the replication owner's short snapshot
// capture hook. Install before exposure, or with the session gate and database
// execution view held.
func (s *Schema) SetStatementSnapshotHook(hook func(context.Context, *Schema) (bool, error)) {
	s.statementSnapshotHook = hook
}

// CaptureReplicaStatement adopts an immutable, physically loaded catalog and
// pins data while the owner excludes FSM application. Subsequent reads run
// without blocking replication. Transactions retain READ COMMITTED semantics.
func (s *Schema) CaptureReplicaStatement(catalog *Schema, version uint64) error {
	return s.captureReplicaStatement(catalog, version, nil)
}

func (s *Schema) HasReplicaReadSnapshot() bool { return s.replicaReadOnly }

func (s *Schema) CaptureReplicaStatementAt(catalog *Schema, version, snapshot uint64) error {
	return s.captureReplicaStatement(catalog, version, &snapshot)
}

func (s *Schema) captureReplicaStatement(catalog *Schema, version uint64, snapshot *uint64) error {
	if s.lsm != catalog.lsm || s.txHasWrites || s.txHasDDL || len(s.txWrites) != 0 {
		return fmt.Errorf("cannot adopt replica read snapshot with private writes or different storage")
	}
	s.EndStatement()
	catalog.commitMu.Lock()
	defer catalog.commitMu.Unlock()
	latest := catalog.captureState()
	if s.inTx {
		// Replica transactions contain only reads/control. Rebase every saved
		// catalog so rollback to a savepoint cannot resurrect dropped objects.
		s.txState = latest
		for i := range s.savepoints {
			s.savepoints[i].state = cloneState(latest)
		}
	}
	s.restoreState(latest)
	s.readCatalogVersion, s.readStateVersion, s.readVersion = version, version, version
	s.replicaReadOnly = true
	if snapshot == nil {
		s.statementSnapshot = s.lsm.SnapshotForReader(s.snapshotReader)
	} else {
		if err := s.lsm.RetainSnapshotForReader(s.snapshotReader, *snapshot); err != nil {
			return err
		}
		s.statementSnapshot = *snapshot
	}
	s.statementActive = true
	return nil
}
