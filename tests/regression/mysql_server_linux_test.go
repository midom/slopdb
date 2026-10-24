//go:build linux

package regression

import (
	"bufio"
	"context"
	"crypto/tls"
	"crypto/x509"
	gosql "database/sql"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	driver "github.com/go-sql-driver/mysql"
	"slopdb/internal/testcert"
)

func TestCLIMySQLTCPAndUnixPersistence(t *testing.T) {
	for _, mutual := range []bool{false, true} {
		name := "plaintext"
		if mutual {
			name = "mutual_TLS"
		}
		t.Run(name, func(t *testing.T) { testCLIMySQLTCPAndUnixPersistence(t, mutual) })
	}
}
func testCLIMySQLTCPAndUnixPersistence(t *testing.T, mutual bool) {
	env := newRegressionEnv(t)
	dir := filepath.Join(t.TempDir(), "db")
	socket := filepath.Join(t.TempDir(), "mysql.sock")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	args := []string{"--mysql-tcp=127.0.0.1:0", "--mysql-socket=" + socket, "--mysql-password-env=SLOPDB_TEST_MYSQL_PASSWORD", "--no-wal"}
	var clientTLS *tls.Config
	if mutual {
		serverCA, clientCA := testcert.NewCA(t, "server CA"), testcert.NewCA(t, "client CA")
		now := time.Now()
		serverCert := serverCA.Issue(t, "server", x509.ExtKeyUsageServerAuth, now.Add(-time.Hour), now.Add(time.Hour))
		clientCert := clientCA.Issue(t, "client", x509.ExtKeyUsageClientAuth, now.Add(-time.Hour), now.Add(time.Hour))
		cert, key := testcert.WriteKeyPair(t, t.TempDir(), "server", serverCert)
		ca := clientCA.Write(t, t.TempDir(), "client-ca.pem")
		args = append(args, "--mysql-tls-cert="+cert, "--mysql-tls-key="+key, "--mysql-tls-ca="+ca)
		clientTLS = &tls.Config{RootCAs: serverCA.Pool(), ServerName: "localhost", Certificates: []tls.Certificate{clientCert}, MinVersion: tls.VersionTLS12}
	}
	command := exec.CommandContext(ctx, env.cli(), append(args, dir)...)
	command.Env = append(os.Environ(), "SLOPDB_TEST_MYSQL_PASSWORD=secret")
	stderr, err := command.StderrPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	finished := false
	t.Cleanup(func() {
		if !finished {
			_ = command.Process.Kill()
			_ = command.Wait()
		}
	})
	addresses := make(chan string, 2)
	logs := make(chan string, 1)
	go func() {
		var output strings.Builder
		scanner := bufio.NewScanner(stderr)
		for scanner.Scan() {
			line := scanner.Text()
			fmt.Fprintln(&output, line)
			fields := strings.Fields(line)
			if len(fields) > 4 && strings.HasPrefix(line, "MySQL listening on tcp ") {
				addresses <- fields[4]
			}
			if strings.HasPrefix(line, "MySQL listening on unix ") {
				addresses <- socket
			}
		}
		if err := scanner.Err(); err != nil {
			fmt.Fprintln(&output, err)
		}
		logs <- output.String()
	}()
	var address string
	for i := 0; i < 2; i++ {
		select {
		case a := <-addresses:
			if a != socket {
				address = a
			}
		case <-ctx.Done():
			t.Fatal("server did not start")
		}
	}
	if mode, err := os.Stat(socket); err != nil || mode.Mode().Perm() != 0600 {
		t.Fatalf("socket permissions %v %v", mode, err)
	}
	open := func(network, address string) *gosql.DB {
		t.Helper()
		config := driver.NewConfig()
		config.User = "slopdb"
		config.Passwd = "secret"
		config.DBName = "slopdb"
		config.Net = network
		config.Addr = address
		config.Timeout = 3 * time.Second
		config.ReadTimeout = 5 * time.Second
		config.TLS = clientTLS
		connector, err := driver.NewConnector(config)
		if err != nil {
			t.Fatal(err)
		}
		client := gosql.OpenDB(connector)
		t.Cleanup(func() { _ = client.Close() })
		return client
	}
	tcp := open("tcp", address)
	unix := open("unix", socket)
	for _, q := range []string{`CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)`, `INSERT INTO t VALUES(1,"MySQL string")`} {
		if _, err := tcp.Exec(q); err != nil {
			t.Fatal(err)
		}
	}
	var value string
	if err := unix.QueryRow("SELECT v FROM t WHERE id=?", 1).Scan(&value); err != nil || value != "MySQL string" {
		t.Fatalf("shared TCP/Unix database %q %v", value, err)
	}
	tx, err := unix.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec("INSERT INTO t VALUES(2,'uncommitted')"); err != nil {
		t.Fatal(err)
	}
	if err := command.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	err = command.Wait()
	finished = true
	output := <-logs
	if err != nil || strings.Contains(output, "DATA RACE") {
		t.Fatalf("server shutdown %v: %s", err, output)
	}
	_ = tx.Rollback()
	if _, err := os.Stat(socket); !os.IsNotExist(err) {
		t.Fatalf("socket not removed: %v", err)
	}
	database, err := env.openDatabase(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	result, err := database.Exec("SELECT id,v FROM t ORDER BY id")
	if err != nil || len(result.Rows) != 1 || result.Rows[0]["id"].Int != 1 || result.Rows[0]["v"].Text != "MySQL string" {
		t.Fatalf("clean shutdown persistence: %v %v", result, err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
}
func TestCLIMySQLStartupFailurePreservesSocketPath(t *testing.T) {
	env := newRegressionEnv(t)
	socket := filepath.Join(t.TempDir(), "existing")
	if err := os.WriteFile(socket, []byte("keep"), 0600); err != nil {
		t.Fatal(err)
	}
	command := exec.Command(env.cli(), "--mysql-tcp=127.0.0.1:0", "--mysql-socket="+socket, filepath.Join(t.TempDir(), "db"))
	if output, err := command.CombinedOutput(); err == nil || !strings.Contains(string(output), "MySQL server:") {
		t.Fatalf("startup should fail: %v %s", err, output)
	}
	if data, err := os.ReadFile(socket); err != nil || string(data) != "keep" {
		t.Fatalf("existing path changed %q %v", data, err)
	}
}

func TestCLIMySQLTLSInvalidConfiguration(t *testing.T) {
	env := newRegressionEnv(t)
	for _, args := range [][]string{
		{"--mysql-tcp=127.0.0.1:0", "--mysql-require-tls"},
		{"--mysql-tcp=127.0.0.1:0", "--mysql-tls-ca=missing.pem"},
		{"--mysql-tcp=127.0.0.1:0", "--mysql-tls-cert=missing.pem"},
		{"--mysql-tls-cert=missing.pem", "--mysql-tls-key=missing.key"},
	} {
		path := filepath.Join(t.TempDir(), "database")
		command := exec.Command(env.cli(), append(args, path)...)
		output, err := command.CombinedOutput()
		if err == nil || !strings.Contains(string(output), "MySQL TLS") {
			t.Fatalf("invalid TLS flags accepted: %v %s", err, output)
		}
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("invalid TLS startup created a database: %v", err)
		}
	}
}
