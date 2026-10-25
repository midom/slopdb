package sql

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"slopdb/storage"
)

// Provider answers deliberately change between calls. A self-join therefore
// has a stable answer only if both aliases use one observation for the query.
func TestActivityMetadataIsPassiveAndAliasesShareSnapshot(t *testing.T) {
	tree, e := storage.NewLSMTree(t.TempDir(), nil)
	if e != nil {
		t.Fatal(e)
	}
	defer tree.Close()
	s, e := NewSchema(tree, 4096)
	if e != nil {
		t.Fatal(e)
	}
	var calls atomic.Int64
	fail := errors.New("metadata must not observe live sessions")
	s.SetActivitySnapshot(func(context.Context) ([]SessionActivity, error) { calls.Add(1); return nil, fail })
	for _, tc := range []struct {
		dialect Dialect
		query   string
		first   TypeID
	}{
		{DialectMySQL, "SELECT id,user,db,info FROM information_schema.processlist", TypeUint64},
		{DialectPostgreSQL, "SELECT pid,datname,query_start,state FROM pg_catalog.pg_stat_activity", TypeInt32},
	} {
		if e = s.SetDialect(tc.dialect); e != nil {
			t.Fatal(e)
		}
		parsed, e := ParseWithOptions(tc.query, s.Dialect().ParseOptions(ParseOptions{}))
		if e != nil {
			t.Fatal(e)
		}
		columns, e := s.Describe(parsed[0])
		if e != nil {
			t.Fatalf("Describe ran provider or failed: %v", e)
		}
		if len(columns) != 4 || columns[0].LogicalType().Kind != tc.first {
			t.Fatalf("static metadata %s: %+v", tc.query, columns)
		}
	}
	if calls.Load() != 0 {
		t.Fatal("metadata collected runtime activity", calls.Load())
	}
	now := time.Date(2026, 10, 25, 7, 0, 0, 0, time.UTC)
	s.SetActivitySnapshot(func(context.Context) ([]SessionActivity, error) {
		n := calls.Add(1)
		return []SessionActivity{
			{SessionID: 1, ID: uint64(1000 + n), Dialect: DialectMySQL, User: "alice", Database: "app", StartedAt: now, QueryStart: now, StateChange: now, State: ActivityActive, Query: "SELECT 1"},
			{SessionID: 2, ID: uint64(2000 + n), Dialect: DialectPostgreSQL, User: "bob", Database: "app", StartedAt: now, StateChange: now, State: ActivityIdle},
		}, nil
	})
	for _, dialect := range []Dialect{DialectMySQL, DialectPostgreSQL} {
		s.SetDialect(dialect)
		table, column := "information_schema.processlist", "id"
		if dialect == DialectPostgreSQL {
			table, column = "pg_catalog.pg_stat_activity", "pid"
		}
		parsed, e := ParseWithOptions("SELECT a."+column+" AS a,b."+column+" AS b FROM "+table+" a JOIN "+table+" b ON a."+column+"=b."+column, s.Dialect().ParseOptions(ParseOptions{}))
		if e != nil {
			t.Fatal(e)
		}
		for repeat := 0; repeat < 2; repeat++ {
			before := calls.Load()
			r, e := s.Select(parsed[0].(*SelectStmt))
			if e != nil || len(r.Rows) != 2 {
				t.Fatalf("inconsistent activity aliases: %v %v", r, e)
			}
			if calls.Load() != before+1 {
				t.Fatalf("one query observed activity %d times", calls.Load()-before)
			}
		}
	}
}
