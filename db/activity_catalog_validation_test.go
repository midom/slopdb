package db

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"unicode/utf8"

	"slopdb/sql"
)

func activityValidationNumber(t *testing.T, v sql.Value) uint64 {
	t.Helper()
	if v.Null {
		t.Fatal("unexpected NULL activity identifier")
	}
	if v.Type == sql.ColUnsigned {
		return v.Uint
	}
	if v.Type == sql.ColInteger && v.Int >= 0 {
		return uint64(v.Int)
	}
	t.Fatalf("invalid activity identifier: %+v", v)
	return 0
}
func activityValidationContext(id uint32, dialect sql.Dialect) context.Context {
	return sql.WithSessionContext(context.Background(), sql.SessionContext{Dialect: dialect, BackendID: uint64(id), Function: func(_ context.Context, name string, _ []sql.Value) (sql.Value, bool, error) {
		if name == "CONNECTION_ID" {
			return sql.NewUnsignedValue(uint64(id)), true, nil
		}
		return sql.Value{}, false, nil
	}})
}

func TestActivityCatalogViewsAndFullProcesslistObserveIndependentSession(t *testing.T) {
	d := projectionDatabase(t)
	busy := activityValidationSession(t, d, sql.DialectMySQL)
	id, e := d.ReserveBackendID()
	if e != nil {
		t.Fatal(e)
	}
	busy.SetActivityIdentity(sql.ActivityIdentity{BackendID: id, Dialect: sql.DialectMySQL, User: "activity-user", Database: "activity-db", ApplicationName: "activity-test", Host: "127.0.0.1:40502", ClientAddress: "127.0.0.1", ClientPort: 40502})
	query := "SELECT SLEEP(60) /*" + strings.Repeat("🪐", 180) + "*/"
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { _, e := busy.ExecContext(ctx, query); done <- e }()
	activityValidationWait(t, d, busy.SessionID(), func(a sql.SessionActivity) bool { return a.State == sql.ActivityActive && a.Query == query })
	for _, dialect := range []sql.Dialect{sql.DialectMySQL, sql.DialectPostgreSQL} {
		observer := activityValidationSession(t, d, dialect)
		q := fmt.Sprintf("SELECT id,user,host,db,command,state,info FROM information_schema.processlist WHERE id=%d", id)
		if dialect == sql.DialectPostgreSQL {
			q = fmt.Sprintf("SELECT pid,usename,datname,application_name,client_addr,client_port,state,query FROM pg_catalog.pg_stat_activity WHERE pid=%d", id)
		}
		for _, prepared := range []bool{false, true} {
			var r *ExecResult
			var e error
			if prepared {
				p, err := observer.Prepare(q)
				if err != nil {
					t.Fatal(err)
				}
				r, e = p.Exec()
				p.Close()
			} else {
				r, e = observer.Exec(q)
			}
			if e != nil || len(r.Rows) != 1 {
				t.Fatalf("%s prepared=%v: %v %v", q, prepared, r, e)
			}
			row := r.Rows[0]
			if dialect == sql.DialectMySQL {
				if activityValidationNumber(t, row["id"]) != uint64(id) || row["user"].Text != "activity-user" || row["db"].Text != "activity-db" || row["host"].Text != "127.0.0.1:40502" || row["command"].Text != "Query" || row["info"].Text != query {
					t.Fatalf("MySQL process projection %+v", row)
				}
			} else {
				if activityValidationNumber(t, row["pid"]) != uint64(id) || row["usename"].Text != "activity-user" || row["datname"].Text != "activity-db" || row["application_name"].Text != "activity-test" || row["client_addr"].Text != "127.0.0.1" || row["client_port"].Int != 40502 || row["state"].Text != "active" || row["query"].Text != query {
					t.Fatalf("PG activity projection %+v", row)
				}
			}
		}
	}
	mysql := activityValidationSession(t, d, sql.DialectMySQL)
	for _, full := range []bool{false, true} {
		q := "SHOW PROCESSLIST"
		if full {
			q = "SHOW FULL PROCESSLIST"
		}
		r, e := mysql.Exec(q)
		if e != nil {
			t.Fatal(e)
		}
		found := false
		for _, row := range r.Rows {
			if activityValidationNumber(t, row["Id"]) != uint64(id) {
				continue
			}
			found = true
			info := row["Info"].Text
			if full {
				if info != query {
					t.Fatal("FULL truncated activity source", info)
				}
			} else if utf8.RuneCountInString(info) != 100 || !utf8.ValidString(info) || !strings.HasPrefix(query, info) {
				t.Fatal("short process list did not truncate at100 Unicode code points", info)
			}
		}
		if !found {
			t.Fatal("SHOW omitted independent active backend")
		}
	}
	cancel()
	activityValidationFinish(t, done, true)
}

func TestActivityCatalogFollowerInspectionRejectsDataDependencies(t *testing.T) {
	d := projectionDatabase(t)
	objectExec(t, d, "CREATE TABLE activity_data(id INTEGER PRIMARY KEY);INSERT INTO activity_data VALUES(1);CREATE TABLE pg_stat_activity(id INTEGER);INSERT INTO pg_stat_activity VALUES(7)")
	unavailable := errors.New("data SQL requires normal admission")
	var dataChecks, snapshots atomic.Int64
	d.SetReplicationGuard(&ReplicationGuard{
		Check:           func(context.Context) error { dataChecks.Add(1); return unavailable },
		CheckInspection: func(context.Context) error { return nil },
		ReadSnapshot: func(context.Context, *sql.Schema) (bool, error) {
			snapshots.Add(1)
			return false, errors.New("activity must not request a data snapshot")
		},
	})
	for _, dialect := range []sql.Dialect{sql.DialectMySQL, sql.DialectPostgreSQL} {
		observer, e := d.NewInspectionSession(context.Background())
		if e != nil {
			t.Fatal(e)
		}
		t.Cleanup(func() { _ = observer.Close() })
		if e = observer.SetDialect(dialect); e != nil {
			t.Fatal(e)
		}
		id, e := d.ReserveBackendID()
		if e != nil {
			t.Fatal(e)
		}
		observer.SetActivityIdentity(sql.ActivityIdentity{BackendID: id, Dialect: dialect})
		ctx := activityValidationContext(id, dialect)
		table, key, self, text := "information_schema.processlist", "id", "CONNECTION_ID()", "info"
		if dialect == sql.DialectPostgreSQL {
			table, key, self, text = "pg_catalog.pg_stat_activity", "pid", "PG_BACKEND_PID()", "query"
		}
		q := "SELECT " + key + "," + text + " FROM " + table + " WHERE " + key + "=" + self
		r, e := observer.ExecContext(ctx, q)
		if e != nil || len(r.Rows) != 1 || activityValidationNumber(t, r.Rows[0][key]) != uint64(id) || r.Rows[0][text].Text != q {
			t.Fatalf("follower self activity %s: %v %v", q, r, e)
		}
		q = "SELECT COUNT(*) AS n FROM " + table + " a JOIN " + table + " b ON a." + key + "=b." + key
		if r, e = observer.ExecContext(ctx, q); e != nil || len(r.Rows) != 1 || r.Rows[0]["n"].Int < 1 {
			t.Fatalf("follower aliases %v %v", r, e)
		}
		if dataChecks.Load() != 0 || snapshots.Load() != 0 {
			t.Fatal("passive activity invoked data admission/snapshot", dataChecks.Load(), snapshots.Load())
		}
		for _, q := range []string{
			"SELECT * FROM main.activity_data",
			"SELECT a." + key + " FROM " + table + " a JOIN main.activity_data b ON a." + key + "=b.id",
			"SELECT (SELECT id FROM main.activity_data LIMIT 1) AS value FROM " + table,
			"WITH q AS (SELECT id FROM main.activity_data) SELECT * FROM " + table,
			"SELECT * FROM " + table + " FOR UPDATE",
			"SELECT SLEEP(0) FROM " + table,
		} {
			if _, e = observer.ExecContext(ctx, q); !errors.Is(e, unavailable) {
				t.Fatalf("follower data dependency bypassed admission: %s => %v", q, e)
			}
		}
		dataChecks.Store(0)
	}
	native, e := d.NewInspectionSession(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	defer native.Close()
	if _, e = native.Exec("SELECT * FROM pg_stat_activity"); !errors.Is(e, unavailable) {
		t.Fatal("native user relation mistaken for activity view", e)
	}
	if snapshots.Load() != 0 {
		t.Fatal("rejected activity shape invoked data snapshot", snapshots.Load())
	}
}
