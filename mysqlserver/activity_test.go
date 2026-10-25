package mysqlserver

import (
	"context"
	"slopdb/sql"
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
func TestMySQLActivityIdentityUseResetAndDisconnect(t *testing.T) {
	for _, network := range []string{"tcp", "unix"} {
		t.Run(network, func(t *testing.T) {
			s := startTestServer(t, network, Config{})
			c := connectRaw(t, s)
			initial := wireActivity(t, s, c.id)
			if c.id == 0 || initial.Dialect != sql.DialectMySQL || initial.User != "slopdb" || initial.Database != "" || initial.State != sql.ActivityIdle {
				t.Fatal(initial)
			}
			if network == "tcp" && (initial.Host != c.conn.LocalAddr().String() || initial.ClientAddress != "127.0.0.1" || initial.ClientPort <= 0) {
				t.Fatal(initial)
			}
			if network == "unix" && (initial.Host != "localhost" || initial.ClientAddress != "" || initial.ClientPort != -1) {
				t.Fatal(initial)
			}
			expectOK(t, c.command(t, append([]byte{2}, "slopdb"...)), 2)
			if record := wireActivity(t, s, c.id); record.Database != "slopdb" {
				t.Fatal(record)
			}
			expectOK(t, c.command(t, append([]byte{3}, "CREATE DATABASE selected_activity"...)), 2)
			expectOK(t, c.command(t, append([]byte{3}, "USE selected_activity"...)), 2)
			if record := wireActivity(t, s, c.id); record.Database != "selected_activity" {
				t.Fatal(record)
			}
			expectOK(t, c.command(t, append([]byte{3}, "BEGIN"...)), 3)
			if record := wireActivity(t, s, c.id); record.State != sql.ActivityIdleInTransaction {
				t.Fatal(record)
			}
			expectOK(t, c.command(t, []byte{31}), 2)
			reset := wireActivity(t, s, c.id)
			if !reset.StartedAt.Equal(initial.StartedAt) || reset.ID != initial.ID || reset.SessionID == initial.SessionID || reset.Database != "" || reset.User != initial.User || reset.State != sql.ActivityIdle {
				t.Fatal(initial, reset)
			}
			c.send(t, []byte{1})
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

func TestMySQLPreparedActivityRetainsPlaceholderSource(t *testing.T) {
	s := startTestServer(t, "tcp", Config{})
	pool := s.open(t, "slopdb", "", "slopdb")
	connection, err := pool.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	var id uint32
	if err = connection.QueryRowContext(context.Background(), "SELECT CONNECTION_ID()").Scan(&id); err != nil {
		t.Fatal(err)
	}
	const query = "SELECT SLEEP(?), ? AS private_value"
	const secret = "bound-secret-must-not-be-retained"
	done := make(chan error, 1)
	go func() { _, e := connection.ExecContext(context.Background(), query, 0.25, secret); done <- e }()
	deadline := time.Now().Add(2 * time.Second)
	for {
		record := wireActivity(t, s, id)
		if record.State == sql.ActivityActive && record.Query == query {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("prepared source not visible while running", record)
		}
		time.Sleep(5 * time.Millisecond)
	}
	if err = <-done; err != nil {
		t.Fatal(err)
	}
	if record := wireActivity(t, s, id); record.Query != query || record.State != sql.ActivityIdle {
		t.Fatal(record)
	}
	var self int
	if err = connection.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM information_schema.processlist WHERE id=CONNECTION_ID() AND user='slopdb'").Scan(&self); err != nil || self != 1 {
		t.Fatal("CONNECTION_ID differs from processlist identity", self, err)
	}
}
