package db

import (
	"context"
	"slopdb/sql"
)

// SchemaNames lists all persisted virtual namespaces in this physical database.
func (d *Database) SchemaNames(ctx context.Context) ([]string, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := d.execution.RLockContext(ctx); err != nil {
		return nil, err
	}
	defer d.execution.RUnlock()
	if d.closed.Load() {
		return nil, ErrSessionClosed
	}
	return d.schema.CommittedSchemaNames(), nil
}

// RegisterRootSchemaAlias lets a wire protocol expose the existing main catalog
// under its configured database/public name without copying data or allocating
// another storage engine. It is private to this session.
func (s *Session) RegisterRootSchemaAlias(ctx context.Context, name string) error {
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	if err := s.gate.LockContext(ctx); err != nil {
		return err
	}
	defer s.gate.Unlock()
	if err := s.db.execution.RLockContextForReader(ctx, s.executionReader); err != nil {
		return err
	}
	defer s.db.execution.RUnlockForReader(s.executionReader)
	if err := s.registerActive(cancel); err != nil {
		return err
	}
	defer s.clearActive()
	s.schema.RegisterRootSchemaAlias(name)
	return nil
}

// NamespaceSettings returns the session's current default and resolution path.
func (s *Session) NamespaceSettings(ctx context.Context) (string, []string, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	if err := s.gate.LockContext(ctx); err != nil {
		return "", nil, err
	}
	defer s.gate.Unlock()
	if err := s.db.execution.RLockContextForReader(ctx, s.executionReader); err != nil {
		return "", nil, err
	}
	defer s.db.execution.RUnlockForReader(s.executionReader)
	if err := s.registerActive(cancel); err != nil {
		return "", nil, err
	}
	defer s.clearActive()
	return s.schema.CurrentSchema(), s.schema.SessionSearchPath(), nil
}

var _ sql.Statement = (*sql.NamespaceStmt)(nil)
