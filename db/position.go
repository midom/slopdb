package db

import (
	"context"
	"fmt"
	"time"

	"slopdb/sql"
)

type minimumPositionKey struct{}

func MinimumPositionFromContext(ctx context.Context) string {
	position, _ := ctx.Value(minimumPositionKey{}).(string)
	return position
}

// WithMinimumPosition applies a causal lower bound to Exec and streaming Query
// calls (including prepared calls). Normal consistency admission still applies.
func WithMinimumPosition(ctx context.Context, position string) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, minimumPositionKey{}, position)
}

func (d *Database) waitMinimumPosition(ctx context.Context) error {
	if position, _ := ctx.Value(minimumPositionKey{}).(string); position != "" {
		return d.WaitPosition(ctx, position)
	}
	return nil
}

func (d *Database) AckedPosition(ctx context.Context) (string, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if d.closed.Load() {
		return "", fmt.Errorf("database closed")
	}
	if g := d.replication.Load(); g != nil && g.AckedPosition != nil {
		return g.AckedPosition(ctx)
	}
	return "", fmt.Errorf("replication positions require a replicated database")
}

func (d *Database) WaitPosition(ctx context.Context, position string) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if d.closed.Load() {
		return fmt.Errorf("database closed")
	}
	if g := d.replication.Load(); g != nil && g.WaitPosition != nil {
		return g.WaitPosition(ctx, position)
	}
	return fmt.Errorf("replication positions require a replicated database")
}

// Called after publication, while the SQL session gate remains held. The
// replication owner publishes its immutable position before resolving commits.
func (d *Database) recordCommitPosition(schema *sql.Schema, result *ExecResult) {
	if g := d.replication.Load(); g != nil && g.Position != nil {
		result.CommitPosition = g.Position()
		if result.CommitPosition != "" {
			schema.SetLastCommitPosition(result.CommitPosition)
		}
	}
}

func (d *Database) commitPositionResult(schema *sql.Schema, stmt sql.Statement, result *ExecResult) *ExecResult {
	if g := d.replication.Load(); g == nil || g.Position == nil || schema.InTransaction() {
		return result
	}
	switch s := stmt.(type) {
	case *sql.SelectStmt, *sql.UnionStmt, *sql.ShowStmt, *sql.ExplainStmt, *sql.BeginStmt, *sql.RollbackStmt, *sql.SavepointStmt:
		return result
	case *sql.PragmaStmt:
		if sql.IsReplicaReadStatement(s) {
			return result
		}
	}
	if result == nil {
		result = &ExecResult{}
	}
	d.recordCommitPosition(schema, result)
	return result
}

func (d *Database) execPosition(ctx context.Context, schema *sql.Schema, stmt *sql.ReplicationPositionStmt) (*ExecResult, error) {
	if stmt.Operation == "wait" {
		value, ok := stmt.Position.(*sql.LiteralExpr)
		if !ok || value.Value.Null || value.Value.Type != sql.ColText || value.Value.Text == "" {
			return nil, fmt.Errorf("WAIT FOR POSITION requires a nonempty text token")
		}
		if stmt.Timeout != nil {
			literal, ok := stmt.Timeout.(*sql.LiteralExpr)
			if !ok || literal.Value.Null || literal.Value.Type != sql.ColText {
				return nil, fmt.Errorf("position wait TIMEOUT requires a text duration")
			}
			duration, err := time.ParseDuration(literal.Value.Text)
			if err != nil || duration < 0 {
				return nil, fmt.Errorf("invalid position wait timeout")
			}
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, duration)
			defer cancel()
		}
		if err := d.WaitPosition(ctx, value.Value.Text); err != nil {
			return nil, err
		}
		return &ExecResult{}, nil
	}
	var position string
	var err error
	if stmt.Operation == "last" {
		position = schema.LastCommitPosition()
	} else {
		position, err = d.AckedPosition(ctx)
	}
	if err != nil {
		return nil, err
	}
	value := sql.NewTextValue(position)
	if position == "" {
		value = sql.NewNullValue()
	}
	return &ExecResult{Columns: stmt.ResultColumns(), Rows: []map[string]sql.Value{{"Position": value}}}, nil
}

// CommitWithPosition commits using the cluster's configured durability mode.
// The returned position covers this commit; it is a visibility token, not a
// durability certificate. Use COMMIT DURABLE when stable persistence is needed.
func (tx *Tx) CommitWithPosition(ctx context.Context) (string, error) {
	result, err := tx.ExecContext(ctx, "COMMIT")
	if err != nil {
		return "", err
	}
	return result.CommitPosition, nil
}
