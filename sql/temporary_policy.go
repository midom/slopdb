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

// TemporaryCommitAction applies when the owning transaction commits successfully.
// The unspecified action preserves rows, matching PostgreSQL's default.
type TemporaryCommitAction uint8

const (
	TemporaryCommitUnspecified TemporaryCommitAction = iota
	TemporaryCommitPreserveRows
	TemporaryCommitDeleteRows
	TemporaryCommitDrop
)

func (s *Schema) prepareTemporaryCommit() error {
	if !s.Dialect().TemporaryDDLPolicy().CommitActions || s.temporary == nil {
		return nil
	}
	t := s.temporary
	// Copy names because DROP modifies the private definitions during iteration.
	var drops []string
	for name, table := range t.tables {
		switch t.actions[table.ID] {
		case TemporaryCommitDrop:
			drops = append(drops, name)
		case TemporaryCommitDeleteRows:
			if t.pendingCleanup == nil {
				t.pendingCleanup = map[uint64]bool{}
			}
			t.pendingCleanup[table.ID] = true
			for _, idx := range t.indexes {
				if idx.Table == name {
					t.pendingCleanup[idx.ID] = true
				}
			}
		}
	}
	for _, name := range drops {
		if err := s.dropTemporaryTable(&DropTableStmt{Table: name}); err != nil {
			return err
		}
	}
	return nil
}
