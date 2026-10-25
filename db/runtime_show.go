package db

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"slopdb/sql"
)

type localInspectionKey struct{}

func (d *Database) inspectionContext(ctx context.Context, stmt sql.Statement) context.Context {
	if g := d.replication.Load(); g != nil && g.CheckInspection != nil && sql.IsLocalInspectionStatement(stmt) {
		return context.WithValue(ctx, localInspectionKey{}, true)
	}
	return ctx
}
func (d *Database) runtimeShow(ctx context.Context, show *sql.ShowStmt) (*ExecResult, error) {
	if show.Where != nil {
		return nil, fmt.Errorf("runtime status supports LIKE filtering only")
	}
	var values map[string]string
	if show.Kind == "RAFT STATUS" {
		v, e := d.RaftState(ctx)
		if e != nil {
			return nil, e
		}
		values = v.Variables()
	} else {
		v, e := d.ServerState(ctx)
		if e != nil {
			return nil, e
		}
		values = v.Variables()
	}
	names := make([]string, 0, len(values))
	for name := range values {
		names = append(names, name)
	}
	sort.Strings(names)
	r := &ExecResult{Columns: []sql.ColumnDef{{Name: "Variable_name", ColType: sql.ColText}, {Name: "Value", ColType: sql.ColText}}}
	for _, n := range names {
		if show.Like != nil && !sql.SystemVariableLike(n, *show.Like) {
			continue
		}
		r.Rows = append(r.Rows, map[string]sql.Value{"Variable_name": sql.NewTextValue(n), "Value": sql.NewTextValue(values[n])})
	}
	return r, nil
}

// RuntimeVariable returns a neutral live value without parsing SQL.
func (d *Database) RuntimeVariable(ctx context.Context, name string) (string, bool, error) {
	s, e := d.ServerState(ctx)
	if e != nil {
		return "", false, e
	}
	name = strings.ToLower(strings.ReplaceAll(name, ".", "_"))
	v, ok := s.Variables()[name]
	return v, ok, nil
}
