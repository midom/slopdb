package mysqlserver

import (
	"context"
	gosql "database/sql"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

// This optional interoperability check starts its own isolated reference server;
// the ordinary suite requires no external SQL server or daemon installation.
func TestBinlogIndependentMySQLReplicaApply(t *testing.T) {
	if os.Getenv("SLOPDB_TEST_MYSQL_REPLICA") != "1" {
		t.Skip("set SLOPDB_TEST_MYSQL_REPLICA=1 for the isolated MySQL replica check")
	}
	daemon, err := exec.LookPath("mysqld")
	if err != nil {
		t.Fatal(err)
	}
	directory := t.TempDir()
	data := filepath.Join(directory, "data")
	socket := filepath.Join(directory, "mysql.sock")
	log := filepath.Join(directory, "mysql.log")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	initialized := exec.CommandContext(ctx, daemon, "--no-defaults", "--initialize-insecure", "--datadir="+data, "--log-error="+log)
	if output, err := initialized.CombinedOutput(); err != nil {
		diagnostic, _ := os.ReadFile(log)
		t.Fatalf("initialize reference MySQL: %v\n%s\n%s", err, output, diagnostic)
	}
	command := exec.Command(daemon, "--no-defaults", "--datadir="+data, "--socket="+socket, "--skip-networking", "--pid-file="+filepath.Join(directory, "mysql.pid"), "--log-error="+log, "--server-id=222", "--gtid-mode=ON", "--enforce-gtid-consistency=ON", "--log-bin="+filepath.Join(directory, "binlog"), "--relay-log="+filepath.Join(directory, "relay"), "--skip-replica-start", "--performance-schema=OFF", "--mysqlx=OFF", "--innodb-buffer-pool-size=16M")
	if err = command.Start(); err != nil {
		t.Fatal(err)
	}
	exited := make(chan error, 1)
	go func() { exited <- command.Wait() }()
	t.Cleanup(func() {
		// Distribution AppArmor profiles can prohibit even same-user signals
		// to mysqld. SQL shutdown is both graceful and permitted by that profile.
		shutdown, openErr := gosql.Open("mysql", "root@unix("+socket+")/?timeout=1s&readTimeout=3s&writeTimeout=3s")
		if openErr == nil {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			_, _ = shutdown.ExecContext(ctx, "SHUTDOWN")
			cancel()
			_ = shutdown.Close()
		}
		_ = command.Process.Signal(syscall.SIGTERM)
		select {
		case <-exited:
		case <-time.After(10 * time.Second):
			if err := command.Process.Kill(); err != nil {
				t.Errorf("reference shutdown: %v", err)
				return
			}
			select {
			case <-exited:
			case <-time.After(5 * time.Second):
				t.Error("reference daemon did not exit")
			}
		}
	})
	target, err := gosql.Open("mysql", "root@unix("+socket+")/?timeout=1s&readTimeout=3s&writeTimeout=3s")
	if err != nil {
		t.Fatal(err)
	}
	defer target.Close()
	ready := time.Now().Add(20 * time.Second)
	for {
		if err = target.Ping(); err == nil {
			break
		}
		if time.Now().After(ready) {
			diagnostic, _ := os.ReadFile(log)
			t.Fatalf("reference startup: %v\n%s", err, diagnostic)
		}
		time.Sleep(25 * time.Millisecond)
	}
	source := startTestServer(t, "tcp", Config{})
	pool := source.open(t, "slopdb", "", "slopdb")
	if _, err = source.server.binlogSnapshot(context.Background()); err != nil {
		t.Fatal(err)
	}
	mustExec(t, target, "CREATE DATABASE main")
	_, port, err := net.SplitHostPort(source.address)
	if err != nil {
		t.Fatal(err)
	}
	portNumber, _ := strconv.Atoi(port)
	mustExec(t, target, fmt.Sprintf("CHANGE REPLICATION SOURCE TO SOURCE_HOST='127.0.0.1', SOURCE_PORT=%d, SOURCE_USER='slopdb', SOURCE_PASSWORD='', SOURCE_AUTO_POSITION=1", portNumber))
	mustExec(t, target, "START REPLICA")
	mustExec(t, pool, "CREATE TABLE replicated(u BIGINT UNSIGNED PRIMARY KEY,d DECIMAL(30,5),j JSON)")
	mustExec(t, pool, `INSERT INTO replicated VALUES(18446744073709551615,12345678901234567890.12345,'{"u":18446744073709551615,"f":1.2500,"x":"hello"}')`)
	deadline := time.Now().Add(10 * time.Second)
	var u, d, j string
	for {
		err = target.QueryRow("SELECT u,d,j FROM main.replicated").Scan(&u, &d, &j)
		if err == nil && u == "18446744073709551615" && d == "12345678901234567890.12345" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("replica did not apply exact values: %q %q %q %v\n%s", u, d, j, err, replicaDiagnostics(target))
		}
		time.Sleep(25 * time.Millisecond)
	}
	if !strings.Contains(j, "18446744073709551615") || !strings.Contains(j, "hello") {
		t.Fatalf("replica JSON changed: %s", j)
	}
	waitReplicaScalar(t, target, "SELECT JSON_EXTRACT(j,'$.f') FROM main.replicated", "1.2500")
	mustExec(t, pool, "UPDATE replicated SET d=42.00100 WHERE u=18446744073709551615")
	deadline = time.Now().Add(5 * time.Second)
	for {
		err = target.QueryRow("SELECT d FROM main.replicated").Scan(&d)
		if err == nil && d == "42.00100" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("replica update", d, err, replicaDiagnostics(target))
		}
		time.Sleep(25 * time.Millisecond)
	}
	mustExec(t, pool, "CREATE TABLE parent_stream(id BIGINT PRIMARY KEY)")
	mustExec(t, pool, "CREATE TABLE child_stream(id BIGINT PRIMARY KEY,parent BIGINT,CONSTRAINT parent_stream_fk FOREIGN KEY(parent) REFERENCES parent_stream(id) ON UPDATE CASCADE ON DELETE CASCADE)")
	mustExec(t, pool, "INSERT INTO parent_stream VALUES(1)")
	mustExec(t, pool, "INSERT INTO child_stream VALUES(10,1)")
	mustExec(t, pool, "UPDATE parent_stream SET id=2 WHERE id=1")
	waitReplicaScalar(t, target, "SELECT parent FROM main.child_stream WHERE id=10", "2")
	mustExec(t, pool, "DELETE FROM parent_stream WHERE id=2")
	waitReplicaScalar(t, target, "SELECT COUNT(*) FROM main.child_stream", "0")
	// Both physical PK movement and its explicit child changes must apply once.
	// Restarting the receiver exercises source-SID filtering alongside the
	// replica's unrelated local GTIDs from CREATE DATABASE above.
	mustExec(t, target, "STOP REPLICA")
	mustExec(t, pool, "INSERT INTO parent_stream VALUES(3)")
	mustExec(t, pool, "INSERT INTO child_stream VALUES(30,3)")
	mustExec(t, target, "START REPLICA")
	waitReplicaScalar(t, target, "SELECT parent FROM main.child_stream WHERE id=30", "3")
	mustExec(t, pool, "CREATE TABLE long_defaults_stream(id BIGINT PRIMARY KEY,content TEXT DEFAULT 'empty')")
	mustExec(t, pool, "CREATE INDEX long_prefix_stream ON long_defaults_stream(content(8))")
	large := strings.Repeat("unicode 🪐 ", 8000)
	mustExec(t, pool, "INSERT INTO long_defaults_stream VALUES(1,?)", large)
	mustExec(t, pool, "INSERT INTO long_defaults_stream(id) VALUES(2)")
	waitReplicaScalar(t, target, "SELECT content FROM main.long_defaults_stream WHERE id=1", large)
	waitReplicaScalar(t, target, "SELECT content FROM main.long_defaults_stream WHERE id=2", "empty")
	waitReplicaScalar(t, target, "SELECT SUB_PART FROM information_schema.statistics WHERE TABLE_SCHEMA='main' AND TABLE_NAME='long_defaults_stream' AND INDEX_NAME='long_prefix_stream'", "8")
	mustExec(t, target, "STOP REPLICA")
}
func waitReplicaScalar(t *testing.T, database *gosql.DB, query, want string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		var got string
		err := database.QueryRow(query).Scan(&got)
		if err == nil && got == want {
			return
		}
		if time.Now().After(deadline) {
			if len(got) > 100 {
				got = got[:100] + "..."
			}
			t.Fatalf("replica scalar %s: %q (want length%d), %v\n%s", query, got, len(want), err, replicaDiagnostics(database))
		}
		time.Sleep(25 * time.Millisecond)
	}
}
func replicaDiagnostics(database *gosql.DB) string {
	rows, err := database.Query("SHOW REPLICA STATUS")
	if err != nil {
		return err.Error()
	}
	defer rows.Close()
	columns, _ := rows.Columns()
	var out strings.Builder
	if rows.Next() {
		values := make([]any, len(columns))
		data := make([][]byte, len(columns))
		for i := range values {
			values[i] = &data[i]
		}
		if err = rows.Scan(values...); err != nil {
			return err.Error()
		}
		for i, name := range columns {
			if strings.Contains(name, "Error") || strings.Contains(name, "Running") {
				fmt.Fprintf(&out, "%s=%s\n", name, data[i])
			}
		}
	}
	return out.String()
}
