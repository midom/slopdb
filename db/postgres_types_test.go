package db_test

import (
	"path/filepath"
	"testing"

	"slopdb/db"
)

func TestPostgresDeclaredTypePersistenceAndAssignments(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "db")
	for pass := 0; pass < 2; pass++ {
		database, e := db.Open(dir, nil)
		if e != nil {
			t.Fatal(e)
		}
		session, e := database.NewSession()
		if e != nil {
			t.Fatal(e)
		}
		if e = session.SetPostgresMode(true); e != nil {
			t.Fatal(e)
		}
		exec := func(q string) *db.ExecResult {
			t.Helper()
			r, e := session.Exec(q)
			if e != nil {
				t.Fatalf("%s: %v", q, e)
			}
			return r
		}
		if pass == 0 {
			exec(`CREATE TABLE pg_values(id SERIAL PRIMARY KEY,b BOOLEAN,n NUMERIC,t TIMESTAMP(3),d DATE,u UUID)`)
			exec(`INSERT INTO pg_values(b,n,t,d,u) VALUES(TRUE,12345678901234567890.123456789,'2026-10-25 12:34:56.123789',DATE '2026-10-25','550e8400-e29b-41d4-a716-446655440000')`)
			exec(`ALTER TABLE pg_values ADD COLUMN small SMALLINT DEFAULT 32767`)
			exec(`ALTER TABLE pg_values ALTER COLUMN d TYPE TIMESTAMP`)
		}
		result := exec(`SELECT id,b,n,t,d,u,small FROM pg_values`)
		row := result.Rows[0]
		if row["id"].Int != 1 || row["b"].Int != 1 || row["n"].Text != "12345678901234567890.123456789" || row["t"].Text != "2026-10-25 12:34:56.124" || row["d"].Text != "2026-10-25 00:00:00" || len(row["u"].Blob) != 16 || row["small"].Int != 32767 {
			t.Fatalf("pass %d: %+v", pass, row)
		}
		for _, name := range []string{"id", "b", "n", "t", "d", "u", "small"} {
			if row[name].PGType == "" {
				t.Errorf("lost type of %s", name)
			}
		}
		for _, q := range []string{`UPDATE pg_values SET id='1.5'`, `UPDATE pg_values SET small=32768`, `INSERT INTO pg_values(id) VALUES(NULL)`, `ALTER TABLE pg_values ALTER COLUMN small TYPE UUID`} {
			if _, e = session.Exec(q); e == nil {
				t.Fatalf("accepted %s", q)
			}
		}
		// PostgreSQL integer casts round numeric inputs; string inputs stay strict.
		r := exec(`SELECT 1.5::integer AS a, 2.5::integer AS b, (-2.5)::integer AS c, 2.5::float8::integer AS d`)
		if r.Rows[0]["a"].Int != 2 || r.Rows[0]["b"].Int != 3 || r.Rows[0]["c"].Int != -3 || r.Rows[0]["d"].Int != 2 {
			t.Fatal(r.Rows)
		}
		if e = session.Close(); e != nil {
			t.Fatal(e)
		}
		if e = database.Close(); e != nil {
			t.Fatal(e)
		}
	}
}

func TestPostgresTypeModeIsolation(t *testing.T) {
	database, e := db.Open(t.TempDir(), nil)
	if e != nil {
		t.Fatal(e)
	}
	defer database.Close()
	if _, e = database.Exec(`CREATE TABLE native_values(i INTEGER, n DECIMAL); INSERT INTO native_values VALUES(9223372036854775807,12.6)`); e != nil {
		t.Fatal(e)
	}
	r, e := database.Exec(`SELECT i,n,CAST(2.5 AS INTEGER) AS c FROM native_values`)
	if e != nil {
		t.Fatal(e)
	}
	if r.Rows[0]["i"].Int != 9223372036854775807 || r.Rows[0]["n"].Text != "13" || r.Rows[0]["c"].Int != 2 {
		t.Fatal(r.Rows)
	}
	pg, e := database.NewSession()
	if e != nil {
		t.Fatal(e)
	}
	defer pg.Close()
	if e = pg.SetPostgresMode(true); e != nil {
		t.Fatal(e)
	}
	if _, e = pg.Exec(`CREATE TABLE pg_ints(i INTEGER)`); e != nil {
		t.Fatal(e)
	}
	if _, e = pg.Exec(`INSERT INTO pg_ints VALUES(2147483648)`); e == nil {
		t.Fatal("PostgreSQL INTEGER accepted 64-bit value")
	}
	r, e = pg.Exec(`SELECT 'AbC' ~* '^a' AS a, 'AbC' !~ 'a' AS b, 'AbC' !~* '^z' AS c, 'AbC' OPERATOR(pg_catalog.~*) '^a' AS d`)
	if e != nil {
		t.Fatal(e)
	}
	for _, name := range []string{"a", "b", "c", "d"} {
		if r.Rows[0][name].Int != 1 {
			t.Fatal(r.Rows)
		}
	}
}
