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
	server, err := New(database, Config{Username: "owner", Password: "owner-password", Database: "wiki"})
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

func TestMySQLAccountsEnforceAndRevokePrivileges(t *testing.T) {
	s := accountServer(t, t.TempDir())
	owner := s.open(t, "owner", "owner-password", "wiki")
	mustExec(t, owner, "CREATE TABLE records(id INTEGER PRIMARY KEY,v TEXT)")
	mustExec(t, owner, "CREATE USER 'writer'@'localhost' IDENTIFIED BY 'writer-password'")
	mustExec(t, owner, "GRANT SELECT, INSERT, UPDATE ON wiki.* TO 'writer'@'localhost'")
	writer := s.open(t, "writer", "writer-password", "wiki")
	mustExec(t, writer, "INSERT INTO records VALUES(1,'value')")
	var value string
	if err := writer.QueryRow("SELECT v FROM records WHERE id=1").Scan(&value); err != nil || value != "value" {
		t.Fatal(value, err)
	}
	if _, err := writer.Exec("DROP TABLE records"); err == nil {
		t.Fatal("DROP without privilege accepted")
	}
	if _, err := writer.Exec("CREATE USER 'intruder'@'%' IDENTIFIED BY 'bad'"); err == nil {
		t.Fatal("unprivileged account creation accepted")
	}
	mustExec(t, owner, "REVOKE INSERT ON wiki.* FROM 'writer'@'localhost'")
	if _, err := writer.Exec("INSERT INTO records VALUES(2,'blocked')"); err == nil {
		t.Fatal("revoked INSERT still accepted")
	}
	if _, err := writer.Exec("SELECT @@autocommit FROM records; DELETE FROM records"); err == nil {
		t.Fatal("multi-query privilege bypass")
	}
	mustExec(t, owner, "CREATE USER 'visitor'@'localhost' IDENTIFIED BY 'visitor-password'")
	visitor := s.open(t, "visitor", "visitor-password", "")
	if _, err := visitor.Exec("SELECT * FROM records"); err == nil {
		t.Fatal("USAGE account read table without grant")
	}
	if _, err := visitor.Exec("SELECT @@autocommit FROM records"); err == nil {
		t.Fatal("variable rewrite bypassed SELECT privilege")
	}
	var one int
	if err := visitor.QueryRow("SELECT 1").Scan(&one); err != nil || one != 1 {
		t.Fatal(one, err)
	}
}

func TestMySQLAccountAndDatabasePersistence(t *testing.T) {
	directory := t.TempDir()
	s := accountServer(t, directory)
	owner := s.open(t, "owner", "owner-password", "wiki")
	mustExec(t, owner, "CREATE USER 'app'@'%' IDENTIFIED BY 'app-password'")
	mustExec(t, owner, "CREATE DATABASE wiki_two")
	mustExec(t, owner, "GRANT ALL PRIVILEGES ON wiki_two.* TO 'app'@'%'")
	app := s.open(t, "app", "app-password", "wiki_two")
	mustExec(t, app, "CREATE TABLE payloads(id INTEGER PRIMARY KEY,v TEXT)")
	mustExec(t, app, "INSERT INTO payloads VALUES(1,'durable')")
	if _, err := owner.Exec("SELECT * FROM payloads"); err == nil {
		t.Fatal("additional database aliased default tables")
	}
	if _, err := app.Exec("USE wiki"); err == nil {
		t.Fatal("USE without database grant accepted")
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
	app = reopened.open(t, "app", "app-password", "wiki_two")
	var value string
	if err := app.QueryRow("SELECT v FROM payloads WHERE id=1").Scan(&value); err != nil || value != "durable" {
		t.Fatal(value, err)
	}
	owner = reopened.open(t, "owner", "owner-password", "wiki")
	mustExec(t, owner, "ALTER USER 'app'@'%' IDENTIFIED BY 'changed-password'")
	bad := reopened.open(t, "app", "app-password", "wiki_two")
	if err := bad.Ping(); err == nil {
		t.Fatal("old password accepted")
	}
	good := reopened.open(t, "app", "changed-password", "wiki_two")
	if err := good.Ping(); err != nil {
		t.Fatal(err)
	}
	mustExec(t, owner, "DROP USER 'app'@'%'")
	if _, err := good.Exec("SELECT * FROM payloads"); err == nil {
		t.Fatal("dropped account retained access")
	}
}

func TestMySQLUseDoesNotAbandonTransaction(t *testing.T) {
	s := accountServer(t, t.TempDir())
	owner := s.open(t, "owner", "owner-password", "wiki")
	mustExec(t, owner, "CREATE DATABASE extra")
	c, err := owner.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if _, err = c.ExecContext(context.Background(), "BEGIN"); err != nil {
		t.Fatal(err)
	}
	if _, err = c.ExecContext(context.Background(), "USE extra"); err == nil {
		t.Fatal("database switch abandoned an active transaction")
	}
	if _, err = c.ExecContext(context.Background(), "ROLLBACK"); err != nil {
		t.Fatal(err)
	}
	if _, err = c.ExecContext(context.Background(), "USE extra"); err != nil {
		t.Fatal(err)
	}
}

func TestMySQLInsertConflictPrivilegeChecks(t *testing.T) {
	s := accountServer(t, t.TempDir())
	owner := s.open(t, "owner", "owner-password", "wiki")
	mustExec(t, owner, "CREATE TABLE guarded(id INT PRIMARY KEY,v TEXT)")
	mustExec(t, owner, "INSERT INTO guarded VALUES(1,'protected')")
	mustExec(t, owner, "CREATE USER 'inserter'@'localhost' IDENTIFIED BY 'password'")
	mustExec(t, owner, "GRANT INSERT,SELECT ON wiki.* TO 'inserter'@'localhost'")
	inserter := s.open(t, "inserter", "password", "wiki")
	for _, query := range []string{"REPLACE INTO guarded VALUES(1,'bad')", "INSERT INTO guarded VALUES(1,'bad') ON DUPLICATE KEY UPDATE v='bad'"} {
		if _, err := inserter.Exec(query); err == nil {
			t.Fatal("mutation bypassed required privilege", query)
		}
	}
	var value string
	if err := owner.QueryRow("SELECT v FROM guarded WHERE id=1").Scan(&value); err != nil || value != "protected" {
		t.Fatal(value, err)
	}
	mustExec(t, owner, "GRANT UPDATE,DELETE ON wiki.* TO 'inserter'@'localhost'")
	mustExec(t, inserter, "REPLACE INTO guarded VALUES(1,'allowed')")
	mustExec(t, inserter, "INSERT INTO guarded VALUES(1,'value') ON DUPLICATE KEY UPDATE v='updated'")
}

func TestMySQLTemporaryAndLikePrivileges(t *testing.T) {
	s := accountServer(t, t.TempDir())
	owner := s.open(t, "owner", "owner-password", "wiki")
	mustExec(t, owner, "CREATE TABLE source(id INT PRIMARY KEY)")
	mustExec(t, owner, "CREATE USER 'temporary'@'localhost' IDENTIFIED BY 'password'")
	mustExec(t, owner, "GRANT CREATE TEMPORARY TABLES ON wiki.* TO 'temporary'@'localhost'")
	user := s.open(t, "temporary", "password", "wiki")
	connection, err := user.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	if _, err = connection.ExecContext(context.Background(), "CREATE TEMPORARY TABLE scratch(id INT)"); err != nil {
		t.Fatal(err)
	}
	if _, err = connection.ExecContext(context.Background(), "CREATE TABLE durable(id INT)"); err == nil {
		t.Fatal("temporary privilege allowed persistent creation")
	}
	if _, err = connection.ExecContext(context.Background(), "CREATE TEMPORARY TABLE copy LIKE source"); err == nil {
		t.Fatal("LIKE read source without SELECT")
	}
	mustExec(t, owner, "GRANT SELECT ON wiki.* TO 'temporary'@'localhost'")
	if _, err = connection.ExecContext(context.Background(), "CREATE TEMPORARY TABLE copy LIKE source"); err != nil {
		t.Fatal(err)
	}
}

func TestMySQLAccountsRefreshAppliedMetadata(t *testing.T) {
	s := accountServer(t, t.TempDir())
	owner := s.open(t, "owner", "owner-password", "wiki")
	mustExec(t, owner, "CREATE TABLE guarded(id INT PRIMARY KEY)")
	mustExec(t, owner, "INSERT INTO guarded VALUES(1)")
	mustExec(t, owner, "CREATE USER 'reader'@'localhost' IDENTIFIED BY 'password'")
	mustExec(t, owner, "GRANT SELECT ON wiki.* TO 'reader'@'localhost'")
	reader := s.open(t, "reader", "password", "wiki")
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
	if err = reader.QueryRow("SELECT id FROM guarded").Scan(&id); err == nil {
		t.Fatal("applied revoke ignored on existing connection")
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

func TestMySQLTemporaryOwnerPrivilegesAndPersistentSources(t *testing.T) {
	s := accountServer(t, t.TempDir())
	owner := s.open(t, "owner", "owner-password", "wiki")
	mustExec(t, owner, "CREATE TABLE scratch(id INT PRIMARY KEY,v INT)")
	mustExec(t, owner, "INSERT INTO scratch VALUES(1,700)")
	mustExec(t, owner, "CREATE TABLE protected(id INT PRIMARY KEY,secret INT)")
	mustExec(t, owner, "INSERT INTO protected VALUES(1,99)")
	mustExec(t, owner, "CREATE USER 'temporary'@'localhost' IDENTIFIED BY 'password'")
	mustExec(t, owner, "GRANT CREATE TEMPORARY TABLES ON wiki.* TO 'temporary'@'localhost'")
	pool := s.open(t, "temporary", "password", "wiki")
	ctx := context.Background()
	connection, err := pool.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	exec := func(query string) {
		t.Helper()
		if _, err := connection.ExecContext(ctx, query); err != nil {
			t.Fatal(query, err)
		}
	}
	deny := func(query string) {
		t.Helper()
		if _, err := connection.ExecContext(ctx, query); err == nil {
			t.Fatal("persistent privilege bypass", query)
		}
	}
	exec("CREATE TEMPORARY TABLE scratch(id INT PRIMARY KEY,v INT)")
	exec("INSERT INTO scratch VALUES(1,1)")
	exec("UPDATE scratch SET v=2 WHERE id=1")
	exec("REPLACE INTO scratch VALUES(1,3)")
	exec("INSERT INTO scratch VALUES(1,4) ON DUPLICATE KEY UPDATE v=4")
	exec("ALTER TABLE scratch ADD COLUMN extra INT DEFAULT 0")
	exec("CREATE INDEX private_v ON scratch(v)")
	exec("DROP INDEX private_v ON scratch")
	statement, err := connection.PrepareContext(ctx, "UPDATE scratch SET v=? WHERE id=1")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = statement.ExecContext(ctx, 5); err != nil {
		t.Fatal(err)
	}
	statement.Close()
	var value int
	if err = connection.QueryRowContext(ctx, "SELECT v FROM scratch").Scan(&value); err != nil || value != 5 {
		t.Fatal(value, err)
	}
	deny("SELECT secret FROM protected")
	deny("INSERT INTO scratch(id,v) SELECT id,secret FROM protected")
	deny("UPDATE scratch t JOIN protected p ON t.id=p.id SET t.v=p.secret")
	deny("DROP TABLE protected")
	mustExec(t, owner, "GRANT SELECT ON wiki.* TO 'temporary'@'localhost'")
	exec("UPDATE scratch t JOIN protected p ON t.id=p.id SET t.v=p.secret")
	exec("UPDATE scratch t JOIN protected p ON t.id=p.id SET v=p.secret")
	deny("UPDATE scratch t JOIN protected p ON t.id=p.id SET p.secret=0")
	deny("UPDATE scratch t JOIN protected p ON t.id=p.id SET secret=0")
	deny("DELETE p FROM scratch t JOIN protected p ON t.id=p.id")
	exec("DELETE t FROM scratch t JOIN protected p ON t.id=p.id")
	exec("DROP TABLE scratch")
	// The private exemption ends with DROP; its permanent shadow is protected.
	deny("UPDATE scratch SET v=0")
	deny("DROP TABLE scratch")
	if err = owner.QueryRow("SELECT v FROM scratch").Scan(&value); err != nil || value != 700 {
		t.Fatal(value, err)
	}
	if err = owner.QueryRow("SELECT secret FROM protected").Scan(&value); err != nil || value != 99 {
		t.Fatal(value, err)
	}
}
