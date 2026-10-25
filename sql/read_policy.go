package sql

// ReadPolicy describes which reads share a REPEATABLE READ snapshot. Storage
// snapshots and row locks remain common to every dialect.
type ReadPolicy struct {
	SnapshotConstants    bool
	SnapshotMutations    bool
	ConflictOnChangedRow bool
}

func (d Dialect) ReadPolicy() ReadPolicy {
	if d == DialectPostgreSQL {
		return ReadPolicy{SnapshotConstants: true, SnapshotMutations: true, ConflictOnChangedRow: true}
	}
	return ReadPolicy{}
}

func snapshotControl(statement Statement) bool {
	switch statement.(type) {
	case *BeginStmt, *CommitStmt, *RollbackStmt, *SavepointStmt, *ReleaseStmt, *SetIsolationStmt, *SetConstraintsStmt:
		return true
	}
	return false
}
