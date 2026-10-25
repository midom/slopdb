package db

import (
	"context"
	"slopdb/sql"
	"slopdb/storage"
	"testing"
)

func TestStoredViewAndTriggerDialect(t *testing.T) {
	path := t.TempDir()
	for pass := 0; pass < 2; pass++ {
		opt := storage.DefaultOptions()
		opt.DisableWALSync = true
		d, err := Open(path, opt)
		if err != nil {
			t.Fatal(err)
		}
		session := func(dialect sql.Dialect) *Session {
			t.Helper()
			s, err := d.NewSession()
			if err != nil {
				t.Fatal(err)
			}
			if err = s.SetDialect(dialect); err != nil {
				t.Fatal(err)
			}
			return s
		}
		mysql, pg := session(sql.DialectMySQL), session(sql.DialectPostgreSQL)
		exec := func(s *Session, q string) {
			t.Helper()
			if _, err := s.Exec(q); err != nil {
				t.Fatal(q, err)
			}
		}
		scalar := func(s *Session, q string, want int64) {
			t.Helper()
			r, err := s.Exec(q)
			if err != nil {
				t.Fatal(q, err)
			}
			if len(r.Rows) != 1 || r.Rows[0][r.Columns[0].Name].Int != want {
				t.Fatal(q, r.Rows, want)
			}
		}
		if pass == 0 {
			exec(pg, "CREATE VIEW pg_view AS SELECT LENGTH('世界') AS n, TRUE AS b")
			exec(mysql, "CREATE VIEW mysql_view AS SELECT LENGTH('世界') AS n")
			exec(mysql, "CREATE VIEW nested_view AS SELECT n+LENGTH('世界') AS n FROM pg_view")
			exec(pg, "CREATE VIEW pg_invalid AS SELECT CAST('invalid' AS INTEGER) AS n")
			exec(pg, "CREATE TABLE trigger_source(id bigint)")
			exec(pg, "CREATE TABLE bad_source(id bigint)")
			exec(pg, "CREATE TRIGGER bad_log AFTER INSERT ON bad_source BEGIN SELECT CAST('invalid' AS INTEGER); END")
			exec(pg, "CREATE TABLE trigger_log(n bigint)")
			exec(pg, "CREATE TRIGGER pg_log AFTER INSERT ON trigger_source BEGIN INSERT INTO trigger_log VALUES(LENGTH('世界')); END")
			exec(mysql, "CREATE TRIGGER mysql_log AFTER INSERT ON trigger_source BEGIN INSERT INTO trigger_log VALUES(LENGTH('世界')); END")
		}
		scalar(mysql, "SELECT n FROM pg_view", 2)
		scalar(pg, "SELECT n FROM mysql_view", 6)
		scalar(mysql, "SELECT n FROM nested_view", 8)
		scalar(pg, "SELECT n FROM nested_view", 8)
		scalar(mysql, "SELECT n+LENGTH('世界') AS v FROM pg_view", 8)
		scalar(pg, "SELECT n+LENGTH('世界') AS v FROM mysql_view", 8)
		p, err := mysql.Prepare("SELECT b FROM pg_view")
		if err != nil {
			t.Fatal(err)
		}
		desc, err := mysql.DescribePreparedTypes(context.Background(), p, sql.DescribeOptions{})
		if err != nil {
			t.Fatal(err)
		}
		p.Close()
		if desc.Results[0].LogicalType().Kind != sql.TypeBoolean {
			t.Fatal(desc)
		}
		if _, err = mysql.Exec("SELECT * FROM pg_invalid"); err == nil {
			t.Fatal("expected stored cast error")
		}
		scalar(mysql, "SELECT LENGTH('世界')", 6)
		scalar(pg, "SELECT LENGTH('世界')", 2)
		if _, err = mysql.Exec("INSERT INTO bad_source VALUES(1)"); err == nil {
			t.Fatal("expected trigger error")
		}
		scalar(mysql, "SELECT LENGTH('世界')", 6)
		exec(mysql, "INSERT INTO trigger_source VALUES(1)")
		exec(pg, "INSERT INTO trigger_source VALUES(2)")
		scalar(pg, "SELECT SUM(n) FROM trigger_log", int64((pass+1)*16))
		// Definition scoping must leave active transaction dialect policies intact.
		exec(mysql, "BEGIN")
		scalar(mysql, "SELECT n FROM pg_view", 2)
		if mysql.schema.Dialect() != sql.DialectMySQL {
			t.Fatal("definition changed session dialect")
		}
		exec(mysql, "ROLLBACK")
		exec(pg, "BEGIN")
		exec(pg, "ALTER VIEW mysql_view AS SELECT LENGTH('世界') AS n")
		scalar(pg, "SELECT n FROM mysql_view", 2)
		exec(pg, "ROLLBACK")
		scalar(mysql, "SELECT n FROM mysql_view", 6)
		mysql.Close()
		pg.Close()
		if err = d.Close(); err != nil {
			t.Fatal(err)
		}
	}
}
