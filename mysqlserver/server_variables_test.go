package mysqlserver

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"slopdb/db"
)

func TestLiveGlobalVariablesAndRaftInspection(t *testing.T) {
	s := startTestServer(t, "tcp", Config{})
	var follower atomic.Bool
	follower.Store(true)
	var checks atomic.Int64
	denied := errors.New("not the writable leader")
	s.database.SetReplicationGuard(&db.ReplicationGuard{
		Check: func(context.Context) error {
			checks.Add(1)
			if follower.Load() {
				return denied
			}
			return nil
		},
		CheckSession:    func(context.Context) error { return denied },
		CheckInspection: func(ctx context.Context) error { return ctx.Err() },
		State: func(ctx context.Context) (db.RaftState, error) {
			f := follower.Load()
			role := "Leader"
			if f {
				role = "Follower"
			}
			return db.RaftState{Enabled: true, Role: role, Ready: !f, Writable: !f, NodeID: "node-b", ClusterID: "cluster", LeaderID: "node-a", Term: 7, AppliedIndex: 19, CommitIndex: 21, DurableIndex: 18}, ctx.Err()
		},
	})
	pool := s.open(t, "slopdb", "", "slopdb")
	pool.SetMaxOpenConns(1)
	ctx := context.Background()
	c, e := pool.Conn(ctx)
	if e != nil {
		t.Fatal(e)
	}
	defer c.Close()
	for _, query := range []string{"SET autocommit=0", "SET @observed=(SELECT @@read_only)"} {
		if _, e = c.ExecContext(ctx, query); e != nil {
			t.Fatal(query, e)
		}
	}
	var observed int64
	if e = c.QueryRowContext(ctx, "SELECT @observed").Scan(&observed); e != nil || observed != 1 {
		t.Fatal(observed, e)
	}
	prepared, e := c.PrepareContext(ctx, "SET @observed=?")
	if e != nil {
		t.Fatal(e)
	}
	if _, e = prepared.ExecContext(ctx, 2); e != nil {
		t.Fatal(e)
	}
	prepared.Close()
	if e = c.QueryRowContext(ctx, "SELECT @observed").Scan(&observed); e != nil || observed != 2 {
		t.Fatal(observed, e)
	}
	var readOnly, super, raft, term, log int64
	var role, engine string
	q := "SELECT @@read_only,@@super_read_only,@@global.slopdb_raft_enabled,@@global.slopdb_raft_term,@@global.slopdb_raft_role,@@global.slopdb_storage_engine,@@log_bin"
	if e = c.QueryRowContext(ctx, q).Scan(&readOnly, &super, &raft, &term, &role, &engine, &log); e != nil {
		t.Fatal(e)
	}
	if readOnly != 1 || super != 1 || raft != 1 || term != 7 || role != "Follower" || engine != "LSM" || log != 0 {
		t.Fatalf("live variables %d %d %d %d %s %s %d", readOnly, super, raft, term, role, engine, log)
	}
	rows, e := c.QueryContext(ctx, "SHOW RAFT STATUS")
	if e != nil {
		t.Fatal(e)
	}
	status := map[string]string{}
	for rows.Next() {
		var n, v string
		if e = rows.Scan(&n, &v); e != nil {
			t.Fatal(e)
		}
		status[n] = v
	}
	if e = rows.Err(); e != nil {
		t.Fatal(e)
	}
	rows.Close()
	if status["role"] != "Follower" || status["node_id"] != "node-b" || status["term"] != "7" || status["writable"] != "OFF" {
		t.Fatalf("raft status %+v", status)
	}
	rows, e = c.QueryContext(ctx, "SHOW GLOBAL VARIABLES")
	if e != nil {
		t.Fatal(e)
	}
	globals := map[string]string{}
	for rows.Next() {
		var n, v string
		if e = rows.Scan(&n, &v); e != nil {
			t.Fatal(e)
		}
		globals[n] = v
	}
	if e = rows.Err(); e != nil {
		t.Fatal(e)
	}
	rows.Close()
	for _, n := range []string{"read_only", "super_read_only", "slopdb_raft_role", "slopdb_raft_commit_index", "slopdb_page_size", "slopdb_wal_enabled", "slopdb_query_memory_bytes", "slopdb_sstable_bytes", "slopdb_connections", "version_compile_os"} {
		if _, ok := globals[n]; !ok {
			t.Fatalf("missing global %s", n)
		}
	}
	if globals["read_only"] != "ON" || globals["slopdb_raft_commit_index"] != "21" {
		t.Fatalf("global snapshot %+v", globals)
	}
	if checks.Load() != 0 {
		t.Fatalf("inspection performed %d write/quorum admission checks", checks.Load())
	}
	if s.database.ReplicationStorage().ChangeLogEnabled() {
		t.Fatal("state inspection activated CDC")
	}
	if _, e = c.ExecContext(ctx, "CREATE TABLE forbidden(id INTEGER)"); e == nil {
		t.Fatal("follower inspection session bypassed write admission")
	}
	follower.Store(false)
	if e = c.QueryRowContext(ctx, "SELECT @@read_only,@@slopdb_raft_role").Scan(&readOnly, &role); e != nil || readOnly != 0 || role != "Leader" {
		t.Fatalf("role switch %d %s %v", readOnly, role, e)
	}
}
func TestStandaloneStateInspectionAndExplicitCDCActivation(t *testing.T) {
	s := startTestServer(t, "tcp", Config{})
	c := s.open(t, "slopdb", "", "slopdb")
	var readonly, enabled, log int64
	var role, uuid string
	if e := c.QueryRow("SELECT @@read_only,@@slopdb_raft_enabled,@@slopdb_raft_role,@@log_bin,@@server_uuid").Scan(&readonly, &enabled, &role, &log, &uuid); e != nil {
		t.Fatal(e)
	}
	if readonly != 0 || enabled != 0 || role != "Standalone" || log != 0 || uuid != "" {
		t.Fatalf("standalone state %d %d %s %d %s", readonly, enabled, role, log, uuid)
	}
	if _, e := s.database.EnableChangefeed(context.Background()); e != nil {
		t.Fatal(e)
	}
	if e := c.QueryRow("SELECT @@log_bin,@@server_uuid").Scan(&log, &uuid); e != nil || log != 1 || len(uuid) != 36 {
		t.Fatalf("active CDC %d %s %v", log, uuid, e)
	}
}
