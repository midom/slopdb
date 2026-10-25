package postgresserver

import (
	"context"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgproto3"
	"slopdb/sql"
	"strconv"
	"testing"
	"time"
)

func wireActivity(t *testing.T, s *testServer, id uint32) sql.SessionActivity {
	t.Helper()
	records, err := s.database.Activity(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, record := range records {
		if record.ID == uint64(id) {
			return record
		}
	}
	t.Fatalf("backend %d absent from activity: %+v", id, records)
	return sql.SessionActivity{}
}
func TestPostgresActivityIdentitySettingsAndFailedTransaction(t *testing.T) {
	for _, network := range []string{"tcp", "unix"} {
		t.Run(network, func(t *testing.T) {
			s := startTestServer(t, network, Config{})
			c := connectRawApplication(t, s, "initial-application")
			initial := wireActivity(t, s, c.id)
			if c.id == 0 || initial.Dialect != sql.DialectPostgreSQL || initial.User != "slopdb" || initial.Database != "slopdb" || initial.ApplicationName != "initial-application" || initial.State != sql.ActivityIdle {
				t.Fatal(initial)
			}
			if network == "tcp" && (initial.Host != c.conn.LocalAddr().String() || initial.ClientAddress != "127.0.0.1" || initial.ClientPort <= 0) {
				t.Fatal(initial)
			}
			if network == "unix" && (initial.Host != "localhost" || initial.ClientAddress != "" || initial.ClientPort != -1) {
				t.Fatal(initial)
			}
			c.send(t, &pgproto3.Query{String: "SELECT pg_backend_pid()"})
			receive[*pgproto3.RowDescription](t, c)
			row := receive[*pgproto3.DataRow](t, c)
			if len(row.Values) != 1 || string(row.Values[0]) != strconv.FormatUint(uint64(c.id), 10) {
				t.Fatal("PID differs from advertised backend key", row, c.id)
			}
			receive[*pgproto3.CommandComplete](t, c)
			receive[*pgproto3.ReadyForQuery](t, c)

			c.query(t, "SET application_name='changed-application'")
			if record := wireActivity(t, s, c.id); record.ApplicationName != "changed-application" {
				t.Fatal(record)
			}
			c.query(t, "SET search_path TO public")
			if record := wireActivity(t, s, c.id); record.Database != "slopdb" {
				t.Fatal("search path replaced database identity", record)
			}
			c.query(t, "BEGIN")
			inTx := wireActivity(t, s, c.id)
			if inTx.State != sql.ActivityIdleInTransaction || inTx.TransactionStart.IsZero() {
				t.Fatal(inTx)
			}
			c.send(t, &pgproto3.Query{String: "SELECT 1/0"})
			if failure := receive[*pgproto3.ErrorResponse](t, c); failure.Code != "22012" {
				t.Fatal(failure)
			}
			if ready := receive[*pgproto3.ReadyForQuery](t, c); ready.TxStatus != 'E' {
				t.Fatal(ready)
			}
			failed := wireActivity(t, s, c.id)
			if failed.State != sql.ActivityIdleInTransactionAborted || !failed.TransactionStart.Equal(inTx.TransactionStart) {
				t.Fatal(inTx, failed)
			}
			c.query(t, "ROLLBACK")
			if record := wireActivity(t, s, c.id); record.State != sql.ActivityIdle || !record.TransactionStart.IsZero() {
				t.Fatal(record)
			}
			c.query(t, "DISCARD ALL")
			if record := wireActivity(t, s, c.id); record.ApplicationName != "" || record.ID != initial.ID {
				t.Fatal(record)
			}
			c.send(t, &pgproto3.Terminate{})
			c.conn.Close()
			deadline := time.Now().Add(2 * time.Second)
			for {
				records, err := s.database.Activity(context.Background())
				if err != nil {
					t.Fatal(err)
				}
				found := false
				for _, record := range records {
					found = found || record.ID == uint64(c.id)
				}
				if !found {
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("disconnected backend retained activity", records)
				}
				time.Sleep(5 * time.Millisecond)
			}
		})
	}
}

func TestPostgresPreparedActivityRetainsOriginalSource(t *testing.T) {
	s := startTestServer(t, "tcp", Config{})
	connection, err := pgx.Connect(context.Background(), s.dsn("slopdb", "", "slopdb"))
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close(context.Background())
	id := connection.PgConn().PID()
	const query = "SELECT pg_sleep($1), $2::text AS private_value"
	const secret = "bound-secret-must-not-be-retained"
	done := make(chan error, 1)
	go func() { _, e := connection.Exec(context.Background(), query, 0.25, secret); done <- e }()
	deadline := time.Now().Add(2 * time.Second)
	for {
		record := wireActivity(t, s, id)
		if record.State == sql.ActivityActive && record.Query == query {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("original prepared source not visible while running", record)
		}
		time.Sleep(5 * time.Millisecond)
	}
	if err = <-done; err != nil {
		t.Fatal(err)
	}
	if record := wireActivity(t, s, id); record.Query != query || record.State != sql.ActivityIdle {
		t.Fatal("implicit commit overwrote original SQL", record)
	}
	var self int
	if err = connection.QueryRow(context.Background(), "SELECT COUNT(*) FROM pg_stat_activity WHERE pid=pg_backend_pid() AND usename='slopdb'").Scan(&self); err != nil || self != 1 {
		t.Fatal("PID differs from pg_stat_activity identity", self, err)
	}
}
