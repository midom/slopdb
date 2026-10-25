package db

import (
	"context"
	"slopdb/sql"
	"slopdb/storage"
	"testing"
)

func TestStoredDialectCatalogAndNamespaceResolution(t *testing.T) {
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
			exec(pg, "CREATE TABLE root_marker(id bigint)")
			exec(mysql, "CREATE DATABASE public")
			exec(mysql, "CREATE TABLE public.mysql_marker(id bigint)")
			exec(pg, "CREATE VIEW pg_catalog_view AS SELECT COUNT(*) AS n FROM pg_catalog.pg_class WHERE relname='root_marker'")
			exec(pg, "CREATE VIEW pg_schema_view AS SELECT COUNT(*) AS n FROM information_schema.tables WHERE table_schema='public' AND table_name='root_marker'")
			exec(mysql, "CREATE VIEW mysql_schema_view AS SELECT COUNT(*) AS n FROM information_schema.tables WHERE table_schema='public' AND table_name='mysql_marker'")
			exec(pg, "CREATE VIEW pg_lookup_view AS SELECT to_regclass('public.root_marker') IS NOT NULL AS n")
			exec(pg, "CREATE VIEW identity_view AS SELECT CURRENT_DATABASE() AS d, CURRENT_USER AS u")
			exec(mysql, "CREATE VIEW mysql_root_catalog AS SELECT COUNT(*) AS n FROM information_schema.tables WHERE table_schema='invoking_db' AND table_name='root_marker'")
		}
		for _, tc := range []struct {
			s *Session
			q string
		}{{mysql, "SELECT n FROM pg_catalog_view"}, {mysql, "SELECT n FROM pg_schema_view"}, {pg, "SELECT n FROM mysql_schema_view"}, {mysql, "SELECT n FROM pg_lookup_view"}} {
			r := exec(tc.s, tc.q)
			if len(r.Rows) != 1 || r.Rows[0]["n"].Int != 1 {
				t.Fatal(tc.q, r.Rows)
			}
			p, err := tc.s.Prepare(tc.q)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = tc.s.DescribePreparedTypes(context.Background(), p, sql.DescribeOptions{}); err != nil {
				t.Fatal(tc.q, err)
			}
			p.Close()
		}
		ctx := sql.WithMySQLSession(context.Background(), sql.MySQLSessionOptions{Database: "invoking_db", RootDatabase: "invoking_db", User: "invoking_user"})
		r, err := mysql.ExecContext(ctx, "SELECT * FROM identity_view")
		if err != nil {
			t.Fatal(err)
		}
		if r.Rows[0]["d"].Text != "invoking_db" || r.Rows[0]["u"].Text != "invoking_user" {
			t.Fatal("definition replaced invoking identity", r.Rows)
		}
		pgctx := sql.WithPostgresSession(context.Background(), sql.PostgresSessionOptions{Database: "invoking_db", User: "pg_user"})
		r, err = pg.ExecContext(pgctx, "SELECT n FROM mysql_root_catalog")
		if err != nil || r.Rows[0]["n"].Int != 1 {
			t.Fatal("MySQL catalog root lost invoking PG database identity", r, err)
		}
		if mysql.Dialect() != sql.DialectMySQL || pg.Dialect() != sql.DialectPostgreSQL {
			t.Fatal("definition changed session dialect")
		}
		mysql.Close()
		pg.Close()
		if err = d.Close(); err != nil {
			t.Fatal(err)
		}
	}
}
