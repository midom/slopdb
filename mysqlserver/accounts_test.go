package mysqlserver

import (
	"context"
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
