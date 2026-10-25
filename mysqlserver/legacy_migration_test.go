package mysqlserver

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"path/filepath"
	"testing"

	"slopdb/db"
)

func TestMySQLServerMigratesLegacyPhysicalDatabases(t *testing.T) {
	dir := t.TempDir()
	hash := sha256.Sum256([]byte("archive"))
	source, err := db.Open(filepath.Join(dir, "mysql-databases", hex.EncodeToString(hash[:])), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = source.Exec("CREATE TABLE records(id INTEGER PRIMARY KEY,v TEXT);INSERT INTO records VALUES(1,'legacy')"); err != nil {
		t.Fatal(err)
	}
	if err = source.Close(); err != nil {
		t.Fatal(err)
	}
	parent, err := db.Open(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(administrativeState{Version: 1, Databases: []string{"wiki", "archive"}, Accounts: map[string]mysqlAccount{}})
	if err != nil {
		t.Fatal(err)
	}
	if err = parent.SetMySQLServerState(context.Background(), data); err != nil {
		t.Fatal(err)
	}
	if err = parent.Close(); err != nil {
		t.Fatal(err)
	}
	s := accountServer(t, dir)
	client := s.open(t, "owner", "owner-password", "archive")
	var value string
	if err = client.QueryRow("SELECT v FROM records WHERE id=1").Scan(&value); err != nil || value != "legacy" {
		t.Fatal(value, err)
	}
	mustExec(t, client, "CREATE DATABASE second")
	mustExec(t, client, "CREATE TABLE second.records(id INTEGER PRIMARY KEY,v TEXT)")
	mustExec(t, client, "INSERT INTO second.records VALUES(1,'virtual')")
	if err = client.QueryRow("SELECT b.v FROM archive.records a JOIN second.records b ON a.id=b.id").Scan(&value); err != nil || value != "virtual" {
		t.Fatal(value, err)
	}
}
