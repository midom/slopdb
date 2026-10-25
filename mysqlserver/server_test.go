package mysqlserver

import (
	"bytes"
	"context"
	gosql "database/sql"
	"errors"
	"fmt"
	"net"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	driver "github.com/go-sql-driver/mysql"
	"slopdb/db"
	"slopdb/storage"
)

type testServer struct {
	server   *Server
	database *db.Database
	address  string
	network  string
}

func startTestServer(t *testing.T, network string, config Config) *testServer {
	t.Helper()
	opt := storage.DefaultOptions()
	opt.DisableWALSync = true
	opt.MySQLQuoting = true
	database, err := db.Open(t.TempDir(), opt)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := database.Close(); err != nil {
			t.Error(err)
		}
	})
	server, err := New(database, config)
	if err != nil {
		t.Fatal(err)
	}
	address := "127.0.0.1:0"
	if network == "unix" {
		if runtime.GOOS == "windows" {
			t.Skip("Unix sockets")
		}
		address = filepath.Join(t.TempDir(), "mysql.sock")
	}
	listener, err := net.Listen(network, address)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	t.Cleanup(func() {
		if err := server.Close(); err != nil {
			t.Error(err)
		}
		if err := <-done; err != nil && !errors.Is(err, net.ErrClosed) {
			t.Error(err)
		}
	})
	return &testServer{server, database, listener.Addr().String(), network}
}
func (s *testServer) open(t *testing.T, user, password, database string) *gosql.DB {
	t.Helper()
	config := driver.NewConfig()
	config.User = user
	config.Passwd = password
	config.DBName = database
	config.Net = s.network
	config.Addr = s.address
	config.Timeout = 3 * time.Second
	config.ReadTimeout = 5 * time.Second
	config.WriteTimeout = 5 * time.Second
	client, err := gosql.Open("mysql", config.FormatDSN())
	if err != nil {
		t.Fatal(err)
	}
	client.SetMaxOpenConns(8)
	t.Cleanup(func() { _ = client.Close() })
	return client
}
func mustExec(t *testing.T, c interface {
	Exec(string, ...any) (gosql.Result, error)
}, q string, args ...any) gosql.Result {
	t.Helper()
	result, err := c.Exec(q, args...)
	if err != nil {
		t.Fatalf("%s: %v", q, err)
	}
	return result
}
func TestDriverTCPAndUnix(t *testing.T) {
	for _, network := range []string{"tcp", "unix"} {
		t.Run(network, func(t *testing.T) {
			s := startTestServer(t, network, Config{Username: "tester", Password: "secret"})
			client := s.open(t, "tester", "secret", "slopdb")
			if err := client.Ping(); err != nil {
				t.Fatal(err)
			}
			mustExec(t, client, `CREATE TABLE items (id INTEGER PRIMARY KEY AUTOINCREMENT, label TEXT, data BLOB, amount REAL, optional TEXT)`)
			result := mustExec(t, client, `INSERT INTO items(label,data,amount,optional) VALUES(?,CAST(? AS BLOB),?,?)`, "héllo\x00world", []byte{0, 0xff, 1}, 1.2345678901234567, nil)
			if id, err := result.LastInsertId(); err != nil || id != 1 {
				t.Fatalf("last ID %d %v", id, err)
			}
			if n, err := result.RowsAffected(); err != nil || n != 1 {
				t.Fatalf("affected %d %v", n, err)
			}
			stmt, err := client.Prepare(`SELECT id,label,data,amount,optional FROM items WHERE id=?`)
			if err != nil {
				t.Fatal(err)
			}
			defer stmt.Close()
			for i := 0; i < 3; i++ {
				var id int64
				var label string
				var blob []byte
				var amount float64
				var optional gosql.NullString
				if err := stmt.QueryRow(1).Scan(&id, &label, &blob, &amount, &optional); err != nil {
					t.Fatal(err)
				}
				if id != 1 || label != "héllo\x00world" || !bytes.Equal(blob, []byte{0, 0xff, 1}) || amount != 1.2345678901234567 || optional.Valid {
					t.Fatalf("bad typed row %d %q %v %v %v", id, label, blob, amount, optional)
				}
			}
			var label string
			if err := client.QueryRow(`SELECT label FROM items WHERE id=1`).Scan(&label); err != nil || label != "héllo\x00world" {
				t.Fatalf("text result: %q %v", label, err)
			}
			mustExec(t, client, `INSERT INTO items(label,amount) VALUES("double quoted", 0.0)`)
			var name string
			if err := client.QueryRow(`SELECT DATABASE()`).Scan(&name); err != nil || name != "slopdb" {
				t.Fatalf("database %q %v", name, err)
			}
			if err := client.QueryRow(`SHOW DATABASES`).Scan(&name); err != nil || name != "slopdb" {
				t.Fatalf("SHOW DATABASES %q %v", name, err)
			}
			mustExec(t, client, "USE `slopdb`")
			if _, err := client.Exec(`INSERT INTO items(id) VALUES(1)`); err == nil {
				t.Fatal("duplicate key accepted")
			}
			if _, err := client.Exec(`not valid SQL`); err == nil {
				t.Fatal("invalid SQL accepted")
			}
			if err := client.Ping(); err != nil {
				t.Fatalf("connection lost after SQL error: %v", err)
			}
		})
	}
}
func TestAuthenticationAndDatabase(t *testing.T) {
	for _, password := range []string{"", "secret"} {
		t.Run(fmt.Sprintf("password=%t", password != ""), func(t *testing.T) {
			s := startTestServer(t, "tcp", Config{Username: "alice", Password: password, Database: "app"})
			for _, test := range []struct {
				user, password, database string
				code                     uint16
			}{
				{"alice", password, "app", 0}, {"alice", password, "", 0}, {"other", password, "app", 1045}, {"alice", "wrong", "app", 1045}, {"alice", password, "missing", 1049},
			} {
				client := s.open(t, test.user, test.password, test.database)
				err := client.Ping()
				if test.code == 0 {
					if err != nil {
						t.Fatal(err)
					}
					continue
				}
				var mysqlErr *driver.MySQLError
				if !errors.As(err, &mysqlErr) || mysqlErr.Number != test.code {
					t.Fatalf("login %q/%q: %v", test.user, test.database, err)
				}
			}
		})
	}
}
func TestIndependentTransactionsAndAutocommit(t *testing.T) {
	s := startTestServer(t, "tcp", Config{})
	client := s.open(t, "slopdb", "", "slopdb")
	mustExec(t, client, `CREATE TABLE accounts(id INTEGER PRIMARY KEY, balance INTEGER)`)
	mustExec(t, client, `INSERT INTO accounts VALUES(1,100)`)
	tx, err := client.BeginTx(context.Background(), &gosql.TxOptions{Isolation: gosql.LevelReadCommitted})
	if err != nil {
		t.Fatal(err)
	}
	mustExec(t, tx, `UPDATE accounts SET balance=? WHERE id=?`, 200, 1)
	var balance int
	if err := client.QueryRow(`SELECT balance FROM accounts WHERE id=1`).Scan(&balance); err != nil || balance != 100 {
		t.Fatalf("dirty read %d %v", balance, err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := client.QueryRow(`SELECT balance FROM accounts WHERE id=1`).Scan(&balance); err != nil || balance != 200 {
		t.Fatalf("commit %d %v", balance, err)
	}
	conn, err := client.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := conn.ExecContext(context.Background(), `SET autocommit=0`); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.ExecContext(context.Background(), `UPDATE accounts SET balance=300 WHERE id=1`); err != nil {
		t.Fatal(err)
	}
	if err := client.QueryRow(`SELECT balance FROM accounts WHERE id=1`).Scan(&balance); err != nil || balance != 200 {
		t.Fatalf("autocommit private write %d %v", balance, err)
	}
	if _, err := conn.ExecContext(context.Background(), `ROLLBACK`); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.ExecContext(context.Background(), `UPDATE accounts SET balance=400 WHERE id=1`); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.ExecContext(context.Background(), `SET SESSION autocommit=ON`); err != nil {
		t.Fatal(err)
	}
	if err := client.QueryRow(`SELECT balance FROM accounts WHERE id=1`).Scan(&balance); err != nil || balance != 400 {
		t.Fatalf("enable autocommit commits %d %v", balance, err)
	}
	if _, err := conn.ExecContext(context.Background(), `SET sql_mode='ANSI'`); err == nil {
		t.Fatal("unsupported SET silently accepted")
	}
	// A rejected multi-statement command must execute none of its SQL.
	if _, err := client.Exec(`UPDATE accounts SET balance=999; SELECT 1`); err == nil {
		t.Fatal("multiple statements accepted")
	}
	if err := client.QueryRow(`SELECT balance FROM accounts WHERE id=1`).Scan(&balance); err != nil || balance != 400 {
		t.Fatalf("multi statement side effect %d %v", balance, err)
	}
}
func TestConcurrentClientsAndShutdown(t *testing.T) {
	s := startTestServer(t, "tcp", Config{})
	client := s.open(t, "slopdb", "", "slopdb")
	mustExec(t, client, `CREATE TABLE updates(id INTEGER PRIMARY KEY, n INTEGER)`)
	for i := 0; i < 8; i++ {
		mustExec(t, client, `INSERT INTO updates VALUES(?,0)`, i)
	}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for n := 0; n < 10; n++ {
				if _, err := client.Exec(`UPDATE updates SET n=n+1 WHERE id=?`, id); err != nil {
					t.Error(err)
					return
				}
			}
		}(i)
	}
	wg.Wait()
	var sum int
	if err := client.QueryRow(`SELECT SUM(n) FROM updates`).Scan(&sum); err != nil || sum != 80 {
		t.Fatalf("total %d %v", sum, err)
	}
	tx, err := client.Begin()
	if err != nil {
		t.Fatal(err)
	}
	mustExec(t, tx, `UPDATE updates SET n=100 WHERE id=0`)
	waitingTx, err := client.Begin()
	if err != nil {
		t.Fatal(err)
	}
	waiting := make(chan error, 1)
	go func() { _, err := waitingTx.Exec(`UPDATE updates SET n=200 WHERE id=0`); waiting <- err }()
	deadline := time.Now().Add(3 * time.Second)
	for len(s.database.LockWaits()) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("client did not register lock wait")
		}
		time.Sleep(time.Millisecond)
	}
	// An incomplete handshake must also be interrupted by shutdown.
	idle, err := net.Dial("tcp", s.address)
	if err != nil {
		t.Fatal(err)
	}
	defer idle.Close()
	closed := make(chan struct{})
	go func() { _ = s.server.Close(); close(closed) }()
	select {
	case <-closed:
	case <-time.After(3 * time.Second):
		t.Fatal("shutdown blocked")
	}
	select {
	case <-waiting:
		// A statement may finish as another session releases its locks, but
		// both explicit transactions must still be rolled back by shutdown.
	case <-time.After(3 * time.Second):
		t.Fatal("blocked SQL did not stop")
	}
	_ = tx.Rollback()
	_ = waitingTx.Rollback()
	result, err := s.database.Exec(`SELECT n FROM updates WHERE id=0`)
	if err != nil || result.Rows[0]["n"].Int != 10 {
		t.Fatalf("shutdown did not roll back: %v %v", result, err)
	}
}

func TestDriverBoundaryResultsAndLongStrings(t *testing.T) {
	s := startTestServer(t, "tcp", Config{})
	client := s.open(t, "slopdb", "", "slopdb")
	var minimum, maximum int64
	var real float64
	if err := client.QueryRow(`SELECT CAST('-9223372036854775808' AS INTEGER) AS a, 9223372036854775807 AS b, 1.2345678901234567 AS c`).Scan(&minimum, &maximum, &real); err != nil || minimum != -9223372036854775808 || maximum != 9223372036854775807 || real != 1.2345678901234567 {
		t.Fatalf("text numeric boundaries: %d %d %.17g %v", minimum, maximum, real, err)
	}
	var nil1, nil9 any
	var value int64
	if err := client.QueryRow(`SELECT ? AS a,1 AS b,2 AS c,3 AS d,4 AS e,5 AS f,6 AS g,7 AS h,? AS i,? AS j`, nil, nil, int64(9223372036854775807)).Scan(&nil1, new(int), new(int), new(int), new(int), new(int), new(int), new(int), &nil9, &value); err != nil || nil1 != nil || nil9 != nil || value != 9223372036854775807 {
		t.Fatalf("binary NULL bitmap: %v %v %d %v", nil1, nil9, value, err)
	}
	rows, err := client.Query(`SELECT 9223372036854775807 AS v UNION ALL SELECT 0.5 AS v`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var got []string
	for rows.Next() {
		var v string
		if err := rows.Scan(&v); err != nil {
			t.Fatal(err)
		}
		got = append(got, v)
	}
	if err := rows.Err(); err != nil || len(got) != 2 || got[0] != "9223372036854775807" || got[1] != "0.5" {
		t.Fatalf("mixed expression types %v %v", got, err)
	}
	var unsigned uint64
	if err := client.QueryRow(`SELECT ?`, uint64(18446744073709551615)).Scan(&unsigned); err != nil || unsigned != uint64(18446744073709551615) {
		t.Fatalf("full-range unsigned parameter: %d %v", unsigned, err)
	}
	// Force a standard driver to use COM_STMT_SEND_LONG_DATA for string values.
	config := driver.NewConfig()
	config.User = "slopdb"
	config.Net = s.network
	config.Addr = s.address
	config.MaxAllowedPacket = 2048
	config.ReadTimeout = 5 * time.Second
	longClient, err := gosql.Open("mysql", config.FormatDSN())
	if err != nil {
		t.Fatal(err)
	}
	defer longClient.Close()
	mustExec(t, longClient, `CREATE TABLE long_values(v TEXT)`)
	text := string(bytes.Repeat([]byte("a\x00é"), 3000))
	mustExec(t, longClient, `INSERT INTO long_values VALUES(?)`, text)
	var stored string
	if err := client.QueryRow(`SELECT v FROM long_values`).Scan(&stored); err != nil || stored != text {
		t.Fatalf("long text length %d: %v", len(stored), err)
	}
}

func TestTextTriggerAndRejectedControlBatch(t *testing.T) {
	s := startTestServer(t, "tcp", Config{})
	client := s.open(t, "slopdb", "", "slopdb")
	mustExec(t, client, `CREATE TABLE source(id INTEGER PRIMARY KEY)`)
	mustExec(t, client, `CREATE TABLE audit(id INTEGER)`)
	mustExec(t, client, `CREATE TRIGGER record AFTER INSERT ON source BEGIN INSERT INTO audit VALUES(NEW.id); INSERT INTO audit VALUES(NEW.id+1); END;`)
	mustExec(t, client, `INSERT INTO source VALUES(41)`)
	var n int
	if err := client.QueryRow(`SELECT SUM(id) FROM audit`).Scan(&n); err != nil || n != 83 {
		t.Fatalf("trigger body %d %v", n, err)
	}
	for _, q := range []string{`SET autocommit=0; INSERT INTO source VALUES(2)`, `USE missing; INSERT INTO source VALUES(2)`, `SHOW DATABASES; INSERT INTO source VALUES(2)`} {
		if _, err := client.Exec(q); err == nil {
			t.Fatalf("accepted command batch: %s", q)
		}
	}
	if err := client.QueryRow(`SELECT @@autocommit`).Scan(&n); err != nil || n != 1 {
		t.Fatalf("failed control batch had effects: %d %v", n, err)
	}
}
