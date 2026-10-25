package driver

import (
	"bytes"
	"context"
	stdsql "database/sql"
	sqldriver "database/sql/driver"
	"errors"
	"io"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"slopdb/db"
	nativesql "slopdb/sql"
	"slopdb/storage"
)

func testPool(t *testing.T) (*db.Database, *stdsql.DB) {
	t.Helper()
	options := storage.DefaultOptions()
	options.DisableWALSync = true
	database, e := db.Open(t.TempDir(), options)
	if e != nil {
		t.Fatal(e)
	}
	connector, e := NewConnector(database)
	if e != nil {
		t.Fatal(e)
	}
	pool := stdsql.OpenDB(connector)
	pool.SetMaxOpenConns(4)
	t.Cleanup(func() {
		if e := pool.Close(); e != nil {
			t.Error(e)
		}
		if e := database.Close(); e != nil {
			t.Error(e)
		}
	})
	return database, pool
}
func execute(t *testing.T, pool interface {
	Exec(string, ...any) (stdsql.Result, error)
}, query string, args ...any) stdsql.Result {
	t.Helper()
	r, e := pool.Exec(query, args...)
	if e != nil {
		t.Fatalf("%s: %v", query, e)
	}
	return r
}

type argumentValuer struct {
	value sqldriver.Value
	err   error
}

func (v argumentValuer) Value() (sqldriver.Value, error) { return v.value, v.err }

func TestDriverExactTypesPreparedAndMetadata(t *testing.T) {
	database, pool := testPool(t)
	execute(t, pool, `CREATE TABLE typed(id INTEGER PRIMARY KEY AUTOINCREMENT,i INTEGER,u BIGINT UNSIGNED,d DECIMAL(38,20),b BLOB,s VARCHAR(6),j JSON,n TEXT)`)
	decimal, e := nativesql.NewDecimalValue("123456789012345678.12345678901234567890")
	if e != nil {
		t.Fatal(e)
	}
	json, e := nativesql.NewJSONValue(`{"n":18446744073709551615}`)
	if e != nil {
		t.Fatal(e)
	}
	stmt, e := pool.Prepare(`INSERT INTO typed(i,u,d,b,s,j) VALUES(?,?,?,?,?,?)`)
	if e != nil {
		t.Fatal(e)
	}
	defer stmt.Close()
	result, e := stmt.Exec(int64(math.MinInt64), uint64(math.MaxUint64), decimal, []byte{0, 255, 1}, argumentValuer{value: "héllo"}, json)
	if e != nil {
		t.Fatal(e)
	}
	if id, e := result.LastInsertId(); e != nil || id != 1 {
		t.Fatal(id, e)
	}
	if n, e := result.RowsAffected(); e != nil || n != 1 {
		t.Fatal(n, e)
	}
	read, e := pool.Prepare(`SELECT id,i,u,d,b,s,j,n FROM typed WHERE id=?`)
	if e != nil {
		t.Fatal(e)
	}
	defer read.Close()
	for attempt := 0; attempt < 3; attempt++ {
		rows, e := read.Query(1)
		if e != nil {
			t.Fatal(e)
		}
		types, e := rows.ColumnTypes()
		if e != nil {
			t.Fatal(e)
		}
		if types[3].DatabaseTypeName() != "DECIMAL" {
			t.Fatal(types[3].DatabaseTypeName())
		}
		if p, s, ok := types[3].DecimalSize(); !ok || p != 38 || s != 20 {
			t.Fatal(p, s, ok)
		}
		if l, ok := types[5].Length(); !ok || l != 6 {
			t.Fatal(l, ok)
		}
		if nullable, ok := types[0].Nullable(); !ok || nullable {
			t.Fatal(nullable, ok)
		}
		if nullable, ok := types[7].Nullable(); !ok || !nullable {
			t.Fatal(nullable, ok)
		}
		if types[1].ScanType() != reflect.TypeOf(int64(0)) || types[3].ScanType() != reflect.TypeOf("") {
			t.Fatal(types)
		}
		if !rows.Next() {
			t.Fatal(rows.Err())
		}
		var id, i int64
		var u, d, s, j string
		var b []byte
		var n stdsql.NullString
		if e = rows.Scan(&id, &i, &u, &d, &b, &s, &j, &n); e != nil {
			t.Fatal(e)
		}
		if id != 1 || i != math.MinInt64 || u != "18446744073709551615" || d != decimal.Text || !bytes.Equal(b, []byte{0, 255, 1}) || s != "héllo" || j != json.Text || n.Valid {
			t.Fatal(id, i, u, d, b, s, j, n)
		}
		b[0] = 99
		if rows.Next() {
			t.Fatal("extra row")
		}
		if e = rows.Err(); e != nil {
			t.Fatal(e)
		}
		if e = rows.Close(); e != nil {
			t.Fatal(e)
		}
	}
	if e = pool.Close(); e != nil {
		t.Fatal(e)
	}
	if _, e = database.Exec(`SELECT COUNT(*) FROM typed`); e != nil {
		t.Fatal("external engine closed with pool", e)
	}
}

func TestDriverTransactionsReturningAndUnsupportedOptions(t *testing.T) {
	_, pool := testPool(t)
	pool.SetMaxOpenConns(1)
	execute(t, pool, `CREATE TABLE items(id INTEGER PRIMARY KEY AUTOINCREMENT,v INTEGER)`)
	tx, e := pool.BeginTx(context.Background(), &stdsql.TxOptions{Isolation: stdsql.LevelRepeatableRead})
	if e != nil {
		t.Fatal(e)
	}
	var id int64
	if e = tx.QueryRow(`INSERT INTO items(v) VALUES(?) RETURNING id`, 17).Scan(&id); e != nil || id != 1 {
		t.Fatal(id, e)
	}
	update, e := tx.Prepare(`UPDATE items SET v=v+? WHERE id=? RETURNING v`)
	if e != nil {
		t.Fatal(e)
	}
	var v int64
	if e = update.QueryRow(3, 1).Scan(&v); e != nil || v != 20 {
		t.Fatal(v, e)
	}
	update.Close()
	if e = tx.Commit(); e != nil {
		t.Fatal(e)
	}
	tx, e = pool.Begin()
	if e != nil {
		t.Fatal(e)
	}
	execute(t, tx, `DELETE FROM items`)
	if e = tx.Rollback(); e != nil {
		t.Fatal(e)
	}
	if e = pool.QueryRow(`SELECT v FROM items`).Scan(&v); e != nil || v != 20 {
		t.Fatal(v, e)
	}
	for _, options := range []stdsql.TxOptions{{ReadOnly: true}, {Isolation: stdsql.LevelSerializable}, {Isolation: stdsql.LevelReadUncommitted}, {Isolation: stdsql.LevelSnapshot}} {
		if tx, e = pool.BeginTx(context.Background(), &options); e == nil {
			tx.Rollback()
			t.Fatal("accepted unsupported options", options)
		}
	}
	if _, e = pool.Exec(`INSERT INTO items(v) VALUES(?)`, stdsql.Named("v", 1)); e == nil {
		t.Fatal("accepted named parameter")
	}
	valuerErr := errors.New("valuer failure")
	if _, e = pool.Exec(`INSERT INTO items(v) VALUES(?)`, argumentValuer{err: valuerErr}); e == nil || !strings.Contains(e.Error(), valuerErr.Error()) {
		t.Fatal(e)
	}
	if e = pool.QueryRow(`SELECT COUNT(*) FROM items`).Scan(&v); e != nil || v != 1 {
		t.Fatal(v, e)
	}
}

func TestDriverPathPoolsShareEngineAndPersist(t *testing.T) {
	parent := t.TempDir()
	directory := filepath.Join(parent, "real", "db")
	if e := os.MkdirAll(filepath.Dir(directory), 0755); e != nil {
		t.Fatal(e)
	}
	alias := filepath.Join(parent, "alias")
	if e := os.Symlink(filepath.Dir(directory), alias); e != nil {
		t.Skip(e)
	}
	first, e := stdsql.Open("slopdb", directory)
	if e != nil {
		t.Fatal(e)
	}
	defer first.Close()
	second, e := stdsql.Open("slopdb", filepath.Join(alias, "db"))
	if e != nil {
		t.Fatal(e)
	}
	defer second.Close()
	execute(t, first, `CREATE TABLE persistent(id INTEGER PRIMARY KEY,v TEXT); INSERT INTO persistent VALUES(1,'one')`)
	var v string
	if e = second.QueryRow(`SELECT v FROM persistent WHERE id=?`, 1).Scan(&v); e != nil || v != "one" {
		t.Fatal(v, e)
	}
	if e = first.Close(); e != nil {
		t.Fatal(e)
	}
	execute(t, second, `INSERT INTO persistent VALUES(2,'two')`)
	if e = second.Close(); e != nil {
		t.Fatal(e)
	}
	reopened, e := db.Open(directory, nil)
	if e != nil {
		t.Fatal(e)
	}
	defer reopened.Close()
	r, e := reopened.Exec(`SELECT COUNT(*) AS n FROM persistent`)
	if e != nil || r.Rows[0]["n"].Int != 2 {
		t.Fatal(r, e)
	}
}

func TestDriverRowsHoldConnectionAndCancel(t *testing.T) {
	database, pool := testPool(t)
	execute(t, pool, `CREATE TABLE streamed(id INTEGER PRIMARY KEY,b BLOB); INSERT INTO streamed VALUES(1,X'0102'),(2,X'0304')`)
	connection, e := pool.Conn(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	defer connection.Close()
	ctx, cancel := context.WithCancel(context.Background())
	rows, e := connection.QueryContext(ctx, `SELECT id,b FROM streamed`)
	if e != nil {
		t.Fatal(e)
	}
	if !rows.Next() {
		t.Fatal(rows.Err())
	}
	var id int64
	var blob []byte
	if e = rows.Scan(&id, &blob); e != nil {
		t.Fatal(e)
	}
	blocked, stop := context.WithTimeout(context.Background(), time.Second)
	defer stop()
	if _, e = connection.ExecContext(blocked, `INSERT INTO streamed VALUES(3,X'05')`); !errors.Is(e, errRowsActive) {
		t.Fatal("live rows did not reject concurrent connection use", e)
	}
	cancel()
	_ = rows.Close()
	if !bytes.Equal(blob, []byte{1, 2}) {
		t.Fatal("retained BLOB changed")
	}
	if e = connection.PingContext(blocked); e != nil {
		t.Fatal("canceled stream retained session gate", e)
	}
	if _, e = database.Exec(`SELECT COUNT(*) FROM streamed`); e != nil {
		t.Fatal(e)
	}
}

func TestDriverRawCloseWithActiveRowsAndPoolReset(t *testing.T) {
	database, _ := testPool(t)
	connector, _ := NewConnector(database)
	raw, e := connector.Connect(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	c := raw.(*conn)
	if _, e = c.ExecContext(context.Background(), `CREATE TABLE reset_values(id INTEGER PRIMARY KEY,v INTEGER); INSERT INTO reset_values VALUES(1,17)`, nil); e != nil {
		t.Fatal(e)
	}
	if e = c.ResetSession(context.Background()); e != nil {
		t.Fatal(e)
	}
	query, e := c.PrepareContext(context.Background(), `SELECT v FROM reset_values WHERE id=?`)
	if e != nil {
		t.Fatal(e)
	}
	defer query.Close()
	if e = c.session.SetMySQLMode(true); e != nil {
		t.Fatal(e)
	}
	assigned, e := c.QueryContext(context.Background(), `SELECT @x:=7`, nil)
	if e != nil {
		t.Fatal(e)
	}
	values := make([]sqldriver.Value, 1)
	if e = assigned.Next(values); e != nil {
		t.Fatal(e)
	}
	assigned.Close()
	if !c.dirty {
		t.Fatal("SELECT assignment did not mark session dirty")
	}
	if e = c.ResetSession(context.Background()); e != nil {
		t.Fatal(e)
	}
	if c.session.Dialect() != nativesql.DialectNative {
		t.Fatal("dialect leaked")
	}
	if _, e = c.ExecContext(context.Background(), `CREATE SCHEMA tenant; USE tenant; CREATE TEMPORARY TABLE private(id INTEGER); BEGIN; UPDATE main.reset_values SET v=99 WHERE id=1`, nil); e != nil {
		t.Fatal(e)
	}
	if e = c.ResetSession(context.Background()); e != nil {
		t.Fatal(e)
	}
	read, e := query.(sqldriver.StmtQueryContext).QueryContext(context.Background(), []sqldriver.NamedValue{{Ordinal: 1, Value: int64(1)}})
	if e != nil {
		t.Fatal(e)
	}
	if e = read.Next(values); e != nil || values[0] != int64(17) {
		t.Fatal(values, e)
	}
	read.Close()
	if _, e = c.QueryContext(context.Background(), `SELECT * FROM tenant.private`, nil); e == nil {
		t.Fatal("temporary table leaked")
	}
	active, e := c.QueryContext(context.Background(), `SELECT * FROM reset_values`, nil)
	if e != nil {
		t.Fatal(e)
	}
	done := make(chan error, 1)
	go func() { done <- c.Close() }()
	select {
	case e = <-done:
		if e != nil {
			t.Fatal(e)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("connection close deadlocked with rows")
	}
	if e = active.Next(make([]sqldriver.Value, 2)); e != io.EOF {
		t.Fatal(e)
	}
	if _, e = database.Exec(`SELECT 1`); e != nil {
		t.Fatal("caller engine closed", e)
	}
}

func TestDriverConcurrentSessionsAndPreparedStatement(t *testing.T) {
	_, pool := testPool(t)
	pool.SetMaxOpenConns(8)
	execute(t, pool, `CREATE TABLE concurrent_rows(id INTEGER PRIMARY KEY,v INTEGER); INSERT INTO concurrent_rows VALUES(1,17),(2,34)`)
	statement, e := pool.Prepare(`SELECT v FROM concurrent_rows WHERE id=?`)
	if e != nil {
		t.Fatal(e)
	}
	defer statement.Close()
	var workers sync.WaitGroup
	failures := make(chan error, 8)
	for worker := 0; worker < 8; worker++ {
		workers.Add(1)
		go func(worker int) {
			defer workers.Done()
			for call := 0; call < 20; call++ {
				id := int64((worker+call)%2 + 1)
				var value int64
				if e := statement.QueryRow(id).Scan(&value); e != nil {
					failures <- e
					return
				}
				if value != 17*id {
					failures <- errors.New("incorrect concurrent row")
					return
				}
			}
		}(worker)
	}
	workers.Wait()
	close(failures)
	for e := range failures {
		t.Error(e)
	}
}
