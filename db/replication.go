package db

import (
	"context"
	"errors"
	"fmt"

	"slopdb/sql"
	"slopdb/storage"
)

// ErrDurabilityUnconfirmed means SQL committed but stable-quorum confirmation
// failed. Retrying the transaction can duplicate its effects.
var ErrDurabilityUnconfirmed = errors.New("transaction committed asynchronously; durable confirmation failed")

// ReplicationGuard admits SQL only while a node can provide leader consistency.
// Install before exposing the database to clients. Storage access below is for
// a replication owner; applications must continue to use SQL APIs.
type ReplicationGuard struct {
	Check func(context.Context) error
	// CheckControl may admit transaction-control statements that access only
	// session state. Storage hooks must still obtain consensus for every write
	// before a commit can succeed. Nil retains Check for every statement.
	CheckControl func(context.Context) error
	Durable      func(context.Context) error
}

func (d *Database) SetReplicationGuard(g *ReplicationGuard) { d.replication.Store(g) }
func (d *Database) checkReplication(ctx context.Context) error {
	if g := d.replication.Load(); g != nil {
		return g.Check(ctx)
	}
	return nil
}

func (d *Database) checkStatementReplication(ctx context.Context, statements []sql.Statement) error {
	g := d.replication.Load()
	if g == nil {
		return nil
	}
	if g.CheckControl != nil {
		local := len(statements) > 0
		for _, statement := range statements {
			switch statement.(type) {
			case *sql.BeginStmt, *sql.CommitStmt, *sql.RollbackStmt, *sql.SavepointStmt, *sql.ReleaseStmt:
			default:
				local = false
			}
		}
		if local {
			return g.CheckControl(ctx)
		}
	}
	return g.Check(ctx)
}

// ReplicationStorage is valid until ReplaceReplicaStorage or Close. The owner
// must serialize FSM application, snapshots and replacement.
func (d *Database) ReplicationStorage() *storage.LSMTree { return d.currentStorage.Load() }

// WithReplicaStopped cancels active SQL, drains execution and closes sessions.
// The owner must fence new SQL before calling this function. Default-session
// locking precedes the execution writer, matching the normal SQL lock order.
func (d *Database) WithReplicaStopped(fn func() error) error {
	d.mu.Lock()
	if d.defaultCancel != nil {
		d.defaultCancel()
	}
	for s := range d.sessions {
		s.cancelActive()
	}
	d.mu.Unlock()
	d.defaultGate.Lock()
	defer d.defaultGate.Unlock()
	d.execution.Lock()
	defer d.execution.Unlock()
	if d.closed.Load() {
		return fmt.Errorf("database closed")
	}
	d.mu.Lock()
	for s := range d.sessions {
		s.closeLocked()
	}
	_ = d.schema.Rollback()
	d.locks.release(d.schema)
	d.mu.Unlock()
	d.generation.Add(1)
	return fn()
}

// ReloadReplicaCatalog discards SQL state after a leadership transition. It
// performs no catalog repairs or local logical writes.
func (d *Database) ReloadReplicaCatalog() error {
	return d.WithReplicaStopped(func() error { return d.reloadReplicaCatalog() })
}
func (d *Database) reloadReplicaCatalog() error {
	options := *d.opt
	options.Replica = true
	schema, err := sql.NewSchemaWithOptions(d.lsm, int(d.lsm.PageSize()), &options)
	if err != nil {
		return err
	}
	d.schema = schema.NewSession()
	d.locks = newLockManager()
	d.installLocks(d.schema)
	d.executionReader = d.locks.state(d.schema).id
	return nil
}

// ReplaceReplicaStorage adopts a fully verified, durable snapshot generation.
// publish atomically switches the on-disk generation pointer. The owner retires
// the previous storage after outstanding snapshot readers release it. On failure the
// owner must fence/shut down this database; it must not resume SQL.
func (d *Database) ReplaceReplicaStorage(next *storage.LSMTree, directory string, publish func() error) error {
	return d.WithReplicaStopped(func() error {
		if err := publish(); err != nil {
			return err
		}
		d.lsm, d.dir = next, directory
		d.currentStorage.Store(next)
		return d.reloadReplicaCatalog()
	})
}

// CleanupReplicaIndexes reclaims abandoned staging only on the Raft leader.
// Every tombstone and cleanup marker deletion passes through its commit hook.
func (d *Database) CleanupReplicaIndexes() error {
	return d.WithReplicaStopped(func() error { return d.schema.CleanupIndexGarbage() })
}
