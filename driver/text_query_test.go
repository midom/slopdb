package driver

import (
	"context"
	stdsql "database/sql"
	sqldriver "database/sql/driver"
	"io"
	"testing"

	"slopdb/db"
	"slopdb/storage"
)

func BenchmarkDriverTextQuery(b *testing.B) {
	options := storage.DefaultOptions()
	options.DisableWALSync = true
	native, e := db.Open(b.TempDir(), options)
	if e != nil {
		b.Fatal(e)
	}
	defer native.Close()
	if _, e = native.Exec(`CREATE TABLE text_point(id INTEGER PRIMARY KEY,v INTEGER); INSERT INTO text_point VALUES(1,17)`); e != nil {
		b.Fatal(e)
	}
	connector, e := NewConnector(native)
	if e != nil {
		b.Fatal(e)
	}
	pool := stdsql.OpenDB(connector)
	defer pool.Close()
	pool.SetMaxOpenConns(1)
	query := func() {
		var id, value int64
		if e := pool.QueryRow(`SELECT id,v FROM text_point WHERE id=1`).Scan(&id, &value); e != nil || id != 1 || value != 17 {
			b.Fatalf("incorrect row %d %d %v", id, value, e)
		}
	}
	query()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		query()
	}
	b.StopTimer()
}

func TestDriverTextQueriesAvoidPreparedStatements(t *testing.T) {
	native, _ := testPool(t)
	connector, _ := NewConnector(native)
	raw, e := connector.Connect(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	connection := raw.(*conn)
	defer connection.Close()
	if _, e = connection.ExecContext(context.Background(), `CREATE TABLE text_point(id INTEGER PRIMARY KEY,v INTEGER); INSERT INTO text_point VALUES(1,17)`, nil); e != nil {
		t.Fatal(e)
	}
	if e = connection.ResetSession(context.Background()); e != nil {
		t.Fatal(e)
	}
	for _, query := range []string{`SELECT id,v FROM text_point WHERE id=1`, "-- comment\nselect id,v FROM text_point WHERE id=1", `SELECT id,v FROM text_point UNION ALL SELECT 2,34`, `WITH input AS (SELECT id,v FROM text_point) SELECT id,v FROM input`} {
		result, e := connection.QueryContext(context.Background(), query, nil)
		if e != nil {
			t.Fatal(query, e)
		}
		if len(connection.statements) != 0 {
			result.Close()
			t.Fatalf("plain query allocated native prepared statement: %s", query)
		}
		values := make([]sqldriver.Value, 2)
		if e = result.Next(values); e != nil || values[0] != int64(1) || values[1] != int64(17) {
			t.Fatal(query, values, e)
		}
		for e == nil {
			e = result.Next(values)
		}
		if e != io.EOF {
			t.Fatal(e)
		}
		result.Close()
	}
}

func TestDriverTextQueryAllocationOverhead(t *testing.T) {
	native, _ := testPool(t)
	connector, _ := NewConnector(native)
	raw, e := connector.Connect(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	connection := raw.(*conn)
	defer connection.Close()
	if _, e = connection.ExecContext(context.Background(), `CREATE TABLE text_alloc(id INTEGER PRIMARY KEY,v INTEGER); INSERT INTO text_alloc VALUES(1,1700)`, nil); e != nil {
		t.Fatal(e)
	}
	if e = connection.ResetSession(context.Background()); e != nil {
		t.Fatal(e)
	}
	query := `SELECT id,v FROM text_alloc WHERE id=1`
	nativeAllocs := testing.AllocsPerRun(50, func() {
		result, e := connection.session.QueryContext(context.Background(), query)
		if e != nil {
			t.Fatal(e)
		}
		if len(result.Columns()) != 2 || !result.Next() {
			t.Fatal(result.Err())
		}
		values, e := result.Values()
		if e != nil || values[1].Int != 1700 {
			t.Fatal(values, e)
		}
		result.Close()
	})
	destination := make([]sqldriver.Value, 2)
	driverAllocs := testing.AllocsPerRun(50, func() {
		result, e := connection.QueryContext(context.Background(), query, nil)
		if e != nil {
			t.Fatal(e)
		}
		if e = result.Next(destination); e != nil || destination[1] != int64(1700) {
			t.Fatal(destination, e)
		}
		result.Close()
	})
	// The adapter needs result bookkeeping and interface values, not another SQL
	// parser, PreparedQuery counter shards, metadata AST clone or sync.Pool.
	if driverAllocs > nativeAllocs+16 {
		t.Fatalf("text adapter added %.0f allocations (native %.0f driver %.0f)", driverAllocs-nativeAllocs, nativeAllocs, driverAllocs)
	}
}

func TestDriverTextReturningAndSingleStatementValidation(t *testing.T) {
	_, pool := testPool(t)
	execute(t, pool, `CREATE TABLE text_once(id INTEGER PRIMARY KEY AUTOINCREMENT,v INTEGER)`)
	var id int64
	if e := pool.QueryRow(`INSERT INTO text_once(v) VALUES(17) RETURNING id`).Scan(&id); e != nil || id != 1 {
		t.Fatal(id, e)
	}
	if e := pool.QueryRow(`WITH input AS (SELECT 34 AS v) INSERT INTO text_once(v) SELECT v FROM input RETURNING id`).Scan(&id); e != nil || id != 2 {
		t.Fatal(id, e)
	}
	for _, query := range []string{`SELECT 1; INSERT INTO text_once(v) VALUES(99)`, `INSERT INTO text_once(v) VALUES(99) RETURNING id; DELETE FROM text_once`, `WITH input AS (SELECT 51 AS v) INSERT INTO text_once(v) SELECT v FROM input RETURNING id; DELETE FROM text_once`} {
		result, e := pool.Query(query)
		if e == nil {
			result.Close()
			t.Fatalf("accepted multiple query statements: %s", query)
		}
	}
	var count int64
	if e := pool.QueryRow(`SELECT COUNT(*) FROM text_once`).Scan(&count); e != nil || count != 2 {
		t.Fatal("query classification executed a mutation twice or before validation", count, e)
	}
}
