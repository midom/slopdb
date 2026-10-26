package sql

// The execution owns its input scratch; private transaction writes are only
// borrowed while the session gate is held and are never reused as read buffers.
func (s *Schema) readPreparedMutationInto(key []byte, p *preparedExecutionPlan) ([]byte, bool, error) {
	if s.profile != nil || s.temporaryKey(key) || s.HasReplicaReadSnapshot() {
		return s.schemaGet(key)
	}
	if index, ok := s.txLatest[string(key)]; ok {
		w := s.txWrites[index]
		return w.value, !w.del, nil
	}
	if !s.statementActive {
		return s.schemaGet(key)
	}
	data, found, err := s.lsm.GetAtForReaderInto(key, s.statementSnapshot, s.snapshotReader, p.pointValueScratch)
	p.pointValueScratch = data
	return data, found, err
}

func (s *Schema) prepareReusablePointUpdate(stmt *UpdateStmt) *PointUpdate {
	if stmt.prepared == nil {
		return s.PreparePointUpdate(stmt)
	}
	return s.PreparePointUpdateInto(stmt, &stmt.prepared.pointUpdateRun)
}
