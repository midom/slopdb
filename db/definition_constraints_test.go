package db

import (
	"context"
	"fmt"
	"io"
	"slopdb/sql"
	"slopdb/storage"
	"testing"
)

func TestStoredDefaultAndCheckDialects(t *testing.T) {
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
		exec := func(s *Session, q string) *ExecResult {
			t.Helper()
			r, err := s.Exec(q)
			if err != nil {
				t.Fatal(q, err)
			}
			return r
		}
		if pass == 0 {
			exec(pg, "CREATE TABLE pg_defaults(id bigint PRIMARY KEY, n bigint DEFAULT LENGTH('世界'), v TEXT DEFAULT '世界' CHECK(LENGTH(v)=2), CONSTRAINT pg_table_check CHECK(LENGTH('世界')=2))")
			exec(mysql, "CREATE TABLE mysql_defaults(id bigint PRIMARY KEY, n bigint DEFAULT LENGTH('世界'), v TEXT DEFAULT '世界' CHECK(LENGTH(v)=6), CONSTRAINT mysql_table_check CHECK(LENGTH('世界')=6))")
			exec(mysql, "CREATE TABLE copied_pg LIKE pg_defaults")
			exec(mysql, "CREATE TABLE copied_mysql LIKE mysql_defaults")
			exec(pg, "ALTER TABLE mysql_defaults ADD CONSTRAINT added_pg_check CHECK(LENGTH('世界')=2)")
			exec(mysql, "ALTER TABLE mysql_defaults ADD CONSTRAINT added_mysql_check CHECK(LENGTH('世界')=6)")
			exec(mysql, "ALTER TABLE mysql_defaults DROP CONSTRAINT mysql_table_check")
			exec(pg, "CREATE TABLE same_checks(id bigint, CONSTRAINT pg_same CHECK(LENGTH('世界')=2))")
			exec(mysql, "ALTER TABLE same_checks ADD CONSTRAINT mysql_same CHECK(LENGTH('世界')=2)")
			exec(mysql, "ALTER TABLE same_checks DROP CONSTRAINT mysql_same")
			exec(pg, "CREATE TABLE fk_parent(id bigint PRIMARY KEY)")
			exec(pg, "CREATE TABLE fk_child(id bigint PRIMARY KEY,pid bigint DEFAULT LENGTH('世界'),FOREIGN KEY(pid) REFERENCES fk_parent(id) ON DELETE SET DEFAULT)")
			exec(pg, "INSERT INTO fk_parent VALUES(2)")
			exec(pg, "INSERT INTO fk_child VALUES(1,2)")
		}
		exec(mysql, fmt.Sprintf("INSERT INTO same_checks VALUES(%d)", pass+1))
		for _, tc := range []struct {
			table  string
			caller *Session
			want   int64
		}{{"pg_defaults", mysql, 2}, {"mysql_defaults", pg, 6}, {"copied_pg", mysql, 2}, {"copied_mysql", pg, 6}} {
			p, err := tc.caller.Prepare("INSERT INTO " + tc.table + "(id) VALUES(?)")
			if err != nil {
				t.Fatal(err)
			}
			if _, err = p.Exec(pass + 1); err != nil {
				t.Fatal(tc.table, err)
			}
			p.Close()
			r := exec(tc.caller, fmt.Sprintf("SELECT n FROM %s WHERE id=%d", tc.table, pass+1))
			if r.Rows[0]["n"].Int != tc.want {
				t.Fatal(tc.table, r.Rows, tc.want)
			}
			exec(tc.caller, fmt.Sprintf("UPDATE %s SET v='世界' WHERE id=%d", tc.table, pass+1))
			if _, err = tc.caller.Exec(fmt.Sprintf("UPDATE %s SET v='x' WHERE id=%d", tc.table, pass+1)); err == nil {
				t.Fatal("accepted invalid column CHECK", tc.table)
			}
		}
		exec(mysql, "BEGIN")
		exec(mysql, "ALTER TABLE pg_defaults ALTER COLUMN n SET DEFAULT LENGTH('世界')")
		exec(mysql, "ROLLBACK")
		r := exec(mysql, "INSERT INTO pg_defaults(id) VALUES(100) RETURNING n")
		if r.Rows[0]["n"].Int != 2 {
			t.Fatal(r.Rows)
		}
		exec(mysql, "DELETE FROM pg_defaults WHERE id=100")
		exec(mysql, "ALTER TABLE pg_defaults ALTER COLUMN n SET DEFAULT LENGTH('世界')")
		r = exec(mysql, "INSERT INTO pg_defaults(id) VALUES(100) RETURNING n")
		if r.Rows[0]["n"].Int != 6 {
			t.Fatal(r.Rows)
		}
		exec(mysql, "DELETE FROM pg_defaults WHERE id=100")
		exec(pg, "ALTER TABLE pg_defaults ALTER COLUMN n SET DEFAULT LENGTH('世界')")
		if pass == 0 {
			exec(mysql, "ALTER TABLE pg_defaults ADD extra bigint DEFAULT LENGTH('世界') CHECK(extra=6)")
		}
		emitted := false
		if _, err = pg.BulkInsert(context.Background(), "pg_defaults", []string{"id"}, func(context.Context) ([]any, error) {
			if emitted {
				return nil, io.EOF
			}
			emitted = true
			return []any{int64(pass + 20)}, nil
		}); err != nil {
			t.Fatal(err)
		}
		exec(pg, "ALTER TABLE mysql_defaults MODIFY n bigint DEFAULT LENGTH('世界')")
		r = exec(mysql, "INSERT INTO mysql_defaults(id) VALUES(100) RETURNING n")
		if r.Rows[0]["n"].Int != 2 {
			t.Fatal(r.Rows)
		}
		exec(mysql, "DELETE FROM mysql_defaults WHERE id=100")
		exec(mysql, "ALTER TABLE mysql_defaults MODIFY n bigint DEFAULT LENGTH('世界')")
		exec(pg, "INSERT INTO fk_parent VALUES(6)")
		exec(mysql, "UPDATE fk_child SET pid=6 WHERE id=1")
		exec(mysql, "DELETE FROM fk_parent WHERE id=6")
		r = exec(mysql, "SELECT pid FROM fk_child WHERE id=1")
		if r.Rows[0]["pid"].Int != 2 {
			t.Fatal("FK SET DEFAULT used invoking dialect", r.Rows)
		}
		mysql.Close()
		pg.Close()
		if err = d.Close(); err != nil {
			t.Fatal(err)
		}
	}
}
