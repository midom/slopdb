package sql

// TemporaryDDLPolicy is session semantics above the same WAL-less LSM ranges.
// It does not select another storage engine or duplicate row execution.
type TemporaryDDLPolicy struct {
	TransactionalDefinitions bool
	CommitActions            bool
}

func (d Dialect) TemporaryDDLPolicy() TemporaryDDLPolicy {
	if d == DialectPostgreSQL {
		return TemporaryDDLPolicy{TransactionalDefinitions: true, CommitActions: true}
	}
	return TemporaryDDLPolicy{}
}
func (s *Schema) transactionalTemporarySnapshot() *temporaryCatalog {
	if !s.Dialect().TemporaryDDLPolicy().TransactionalDefinitions {
		return nil
	}
	return s.snapshotTemporaryDefinitions()
}
func (s *Schema) restoreTransactionalTemporary(snapshot *temporaryCatalog) {
	if s.Dialect().TemporaryDDLPolicy().TransactionalDefinitions {
		current := s.temporary
		restored := cloneTemporaryDefinitions(snapshot)
		// Automatic/hidden IDs are reservations, not transactional row contents.
		if restored != nil && current != nil {
			for name, info := range restored.tables {
				if live := current.tables[name]; live != nil && live.ID == info.ID {
					if current.autoinc[name] > restored.autoinc[name] {
						restored.autoinc[name] = current.autoinc[name]
					}
				}
			}
			for key, n := range current.nextRow {
				if n > restored.nextRow[key] {
					restored.nextRow[key] = n
				}
			}
		}
		s.temporary = restored
	}
}
func (s *Schema) hasPendingTemporaryCleanup() bool {
	return s.temporary != nil && len(s.temporary.pendingCleanup) > 0
}
func (s *Schema) finishTemporaryCommit() {
	if s.temporary != nil {
		s.temporary.pendingCleanup = nil
	}
}
