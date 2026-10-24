package db

import (
	"context"
	"fmt"

	"slopdb/sql"
)

type ReadBarrier string

const (
	ReadBarrierDefault   ReadBarrier = ""
	ReadBarrierPrevious  ReadBarrier = "previous"
	ReadBarrierNext      ReadBarrier = "next"
	ReadBarrierCommitted ReadBarrier = "committed"
)

type readBarrierKey struct{}
type readSnapshotKey struct{}
type reuseReadSnapshotKey struct{}

func ValidReadBarrier(mode ReadBarrier) bool {
	return mode == ReadBarrierDefault || mode == ReadBarrierPrevious || mode == ReadBarrierNext || mode == ReadBarrierCommitted
}
func WithReadBarrier(ctx context.Context, mode ReadBarrier) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, readBarrierKey{}, mode)
}
func ReadBarrierFromContext(ctx context.Context) ReadBarrier {
	mode, _ := ctx.Value(readBarrierKey{}).(ReadBarrier)
	return mode
}
func IsReadStatementSnapshot(ctx context.Context) bool {
	read, _ := ctx.Value(readSnapshotKey{}).(bool)
	return read
}
func withSessionReadBarrier(ctx context.Context, schema *sql.Schema) context.Context {
	if _, exists := ctx.Value(readBarrierKey{}).(ReadBarrier); !exists && schema.ReadBarrier() != "" {
		return WithReadBarrier(ctx, ReadBarrier(schema.ReadBarrier()))
	}
	return ctx
}
func readStatementContext(ctx context.Context, schema *sql.Schema, statement sql.Statement) context.Context {
	ctx = withSessionReadBarrier(ctx, schema)
	read := false
	switch statement.(type) {
	case *sql.SelectStmt, *sql.UnionStmt, *sql.ShowStmt:
		read = true
	case *sql.ExplainStmt, *sql.PragmaStmt:
		read = sql.IsReplicaReadStatement(statement)
	}
	return context.WithValue(ctx, readSnapshotKey{}, read)
}
func hasReadBarrierSetting(statements []sql.Statement) bool {
	for _, stmt := range statements {
		if _, ok := stmt.(*sql.ReadBarrierStmt); ok {
			return true
		}
	}
	return false
}
func (d *Database) setReadBarrier(schema *sql.Schema, setting *sql.ReadBarrierStmt) error {
	if setting.Mode == "previous" || setting.Mode == "next" {
		if g := d.replication.Load(); g == nil || g.ReadSnapshot == nil {
			return fmt.Errorf("durable read barriers require replication")
		}
	}
	schema.SetReadBarrier(setting.Mode)
	return nil
}
