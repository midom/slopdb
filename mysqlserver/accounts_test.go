package mysqlserver

import (
	"context"
	"encoding/json"
	"net"
	"testing"

	"slopdb/db"
	"slopdb/storage"
)

func accountServer(t *testing.T, directory string) *testServer {
	t.Helper()
	options := storage.DefaultOptions()
	options.DisableWALSync = true
	options.MySQLQuoting = true
	database, err := db.Open(directory, options)
	if err != nil {
		t.Fatal(err)
	}
	server, err := New(database, Config{Username: "owner", Password: "owner-password", Database: "compat"})
	if err != nil {
		database.Close()
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		server.Close()
		database.Close()
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	t.Cleanup(func() {
		if err := server.Close(); err != nil {
			t.Error(err)
		}
		if err := database.Close(); err != nil {
			t.Error(err)
		}
		if err := <-done; err != nil {
			t.Error(err)
		}
	})
	return &testServer{server: server, database: database, address: listener.Addr().String(), network: "tcp"}
}

func TestMySQLAuthenticationWithoutPermissionEnforcement(t *testing.T) {
	s := accountServer(t, t.TempDir())
	owner := s.open(t, "owner", "owner-password", "compat")
	mustExec(t, owner, "CREATE USER 'app'@'%' IDENTIFIED BY 'password'")
	app := s.open(t, "app", "password", "compat")
	mustExec(t, app, "CREATE TABLE records(id INT PRIMARY KEY,v TEXT)")
	mustExec(t, app, "INSERT INTO records VALUES(1,'value')")
	mustExec(t, app, "UPDATE records SET v='updated'")
	var value string
	if err := app.QueryRow("SELECT v FROM records").Scan(&value); err != nil || value != "updated" {
		t.Fatal(value, err)
	}
	mustExec(t, app, "CREATE DATABASE other")
	mustExec(t, app, "CREATE TABLE other.records(id INT)")
	mustExec(t, app, "DROP TABLE records")
	bad := s.open(t, "app", "wrong", "compat")
	if err := bad.Ping(); err == nil {
		t.Fatal("incorrect password accepted")
	}
}

func TestMySQLAccountAndDatabasePersistence(t *testing.T) {
	directory := t.TempDir()
	s := accountServer(t, directory)
	owner := s.open(t, "owner", "owner-password", "compat")
	mustExec(t, owner, "CREATE USER 'app'@'%' IDENTIFIED BY 'app-password'")
	mustExec(t, owner, "CREATE DATABASE compat_two")
	mustExec(t, owner, "GRANT ALL PRIVILEGES ON compat_two.* TO 'app'@'%'")
	app := s.open(t, "app", "app-password", "compat_two")
	mustExec(t, app, "CREATE TABLE payloads(id INTEGER PRIMARY KEY,v TEXT)")
	mustExec(t, app, "INSERT INTO payloads VALUES(1,'durable')")
	if _, err := owner.Exec("SELECT * FROM payloads"); err == nil {
		t.Fatal("additional database aliased default tables")
	}
	if _, err := app.Exec("USE compat"); err != nil {
		t.Fatal(err)
	}
	app.Close()
	owner.Close()
	if err := s.server.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.database.Close(); err != nil {
		t.Fatal(err)
	}
	reopened := accountServer(t, directory)
	app = reopened.open(t, "app", "app-password", "compat_two")
	var value string
	if err := app.QueryRow("SELECT v FROM payloads WHERE id=1").Scan(&value); err != nil || value != "durable" {
		t.Fatal(value, err)
	}
	owner = reopened.open(t, "owner", "owner-password", "compat")
	mustExec(t, owner, "ALTER USER 'app'@'%' IDENTIFIED BY 'changed-password'")
	bad := reopened.open(t, "app", "app-password", "compat_two")
	if err := bad.Ping(); err == nil {
		t.Fatal("old password accepted")
	}
	good := reopened.open(t, "app", "changed-password", "compat_two")
	if err := good.Ping(); err != nil {
		t.Fatal(err)
	}
	mustExec(t, owner, "DROP USER 'app'@'%'")
	if _, err := good.Exec("SELECT * FROM payloads"); err != nil {
		t.Fatal(err)
	}
	removed := reopened.open(t, "app", "changed-password", "compat_two")
	if err := removed.Ping(); err == nil {
		t.Fatal("dropped account reauthenticated")
	}
}

func TestMySQLUseDoesNotAbandonTransaction(t *testing.T) {
	s := accountServer(t, t.TempDir())
	owner := s.open(t, "owner", "owner-password", "compat")
	mustExec(t, owner, "CREATE DATABASE extra")
	c, err := owner.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if _, err = c.ExecContext(context.Background(), "BEGIN"); err != nil {
		t.Fatal(err)
	}
	if _, err = c.ExecContext(context.Background(), "USE extra"); err != nil {
		t.Fatal(err)
	}
	if _, err = c.ExecContext(context.Background(), "CREATE TABLE temporary_ddl(id INT)"); err != nil {
		t.Fatal(err)
	}
	if _, err = c.ExecContext(context.Background(), "ROLLBACK"); err != nil {
		t.Fatal(err)
	}
	if _, err = c.ExecContext(context.Background(), "USE extra"); err != nil {
		t.Fatal(err)
	}
}

func TestMySQLTemporaryTablesWithoutGrants(t *testing.T) {
	s := accountServer(t, t.TempDir())
	owner := s.open(t, "owner", "owner-password", "compat")
	mustExec(t, owner, "CREATE TABLE source(id INT PRIMARY KEY,v TEXT)")
	mustExec(t, owner, "INSERT INTO source VALUES(1,'source')")
	mustExec(t, owner, "CREATE USER 'app'@'%' IDENTIFIED BY 'password'")
	pool := s.open(t, "app", "password", "compat")
	connection, err := pool.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	for _, q := range []string{"CREATE TEMPORARY TABLE scratch LIKE source", "INSERT INTO scratch SELECT * FROM source", "UPDATE scratch SET v='private'", "CREATE INDEX private_v ON scratch(v)", "DROP INDEX private_v ON scratch"} {
		if _, err = connection.ExecContext(context.Background(), q); err != nil {
			t.Fatal(q, err)
		}
	}
	var value string
	if err = owner.QueryRow("SELECT v FROM source").Scan(&value); err != nil || value != "source" {
		t.Fatal(value, err)
	}
}

func TestMySQLAccountsRefreshAppliedMetadata(t *testing.T) {
	s := accountServer(t, t.TempDir())
	owner := s.open(t, "owner", "owner-password", "compat")
	mustExec(t, owner, "CREATE TABLE guarded(id INT PRIMARY KEY)")
	mustExec(t, owner, "INSERT INTO guarded VALUES(1)")
	mustExec(t, owner, "CREATE USER 'reader'@'localhost' IDENTIFIED BY 'password'")
	mustExec(t, owner, "GRANT SELECT ON compat.* TO 'reader'@'localhost'")
	reader := s.open(t, "reader", "password", "compat")
	var id int
	if err := reader.QueryRow("SELECT id FROM guarded").Scan(&id); err != nil {
		t.Fatal(err)
	}
	// Simulate a metadata record applied through replication by another leader.
	data, err := s.database.MySQLServerState()
	if err != nil {
		t.Fatal(err)
	}
	var state administrativeState
	if err = json.Unmarshal(data, &state); err != nil {
		t.Fatal(err)
	}
	key := accountKey("reader", "localhost")
	account := state.Accounts[key]
	account.PasswordHash = passwordHash("changed")
	account.Grants = map[string]map[string]bool{}
	state.Accounts[key] = account
	data, err = json.Marshal(state)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.database.SetMySQLServerState(context.Background(), data); err != nil {
		t.Fatal(err)
	}
	if err = reader.QueryRow("SELECT id FROM guarded").Scan(&id); err != nil {
		t.Fatal(err)
	}
	stale := s.open(t, "reader", "password", "")
	if err = stale.Ping(); err == nil {
		t.Fatal("applied password ignored")
	}
	changed := s.open(t, "reader", "changed", "")
	if err = changed.Ping(); err != nil {
		t.Fatal(err)
	}
}
