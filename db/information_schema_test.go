package db

import (
	"context"
	"strings"
	"testing"

	"slopdb/sql"
)

func TestSharedInformationSchemaFactsAndVisibility(t *testing.T) {
	for _, dialect := range []sql.Dialect{sql.DialectNative, sql.DialectMySQL, sql.DialectPostgreSQL} {
		t.Run(dialect.String(), func(t *testing.T) {
			path := t.TempDir()
			d, e := Open(path, nil)
			if e != nil {
				t.Fatal(e)
			}
			t.Cleanup(func() { d.Close() })
			s, e := d.NewSession()
			if e != nil {
				t.Fatal(e)
			}
			defer s.Close()
			if e = s.SetDialect(dialect); e != nil {
				t.Fatal(e)
			}
			ctx := context.Background()
			root := "main"
			switch dialect {
			case sql.DialectMySQL:
				ctx = sql.WithMySQLSession(ctx, sql.MySQLSessionOptions{Database: "wiki", RootDatabase: "wiki"})
				root = "wiki"
			case sql.DialectPostgreSQL:
				ctx = sql.WithPostgresSession(ctx, sql.PostgresSessionOptions{Database: "wiki"})
				root = "public"
			}
			exec := func(q string) *ExecResult {
				t.Helper()
				r, err := s.ExecContext(ctx, q)
				if err != nil {
					t.Fatalf("%s: %v", q, err)
				}
				return r
			}
			exec("CREATE TABLE parent(id BIGINT PRIMARY KEY, code VARCHAR(20) UNIQUE)")
			exec("CREATE TABLE child(id BIGINT PRIMARY KEY, parent_id BIGINT, amount DECIMAL(9,2) DEFAULT 1.25, stamp TEXT DEFAULT CURRENT_TIMESTAMP, CONSTRAINT child_parent FOREIGN KEY(parent_id) REFERENCES parent(id), CONSTRAINT amount_positive CHECK(amount>0))")
			exec("CREATE INDEX child_parent_index ON child(parent_id)")
			exec("CREATE VIEW child_names AS SELECT id,stamp FROM child")
			q := "SELECT column_name,ordinal_position,data_type,numeric_precision,numeric_scale,slopdb_default_expression FROM information_schema.columns WHERE table_schema='" + root + "' AND table_name='child' ORDER BY ordinal_position"
			r := exec(q)
			if len(r.Rows) != 4 || r.Columns[1].ColType != sql.ColInteger {
				t.Fatalf("column facts: %+v", r)
			}
			if r.Rows[2]["numeric_precision"].Int != 9 || r.Rows[2]["numeric_scale"].Int != 2 || !strings.Contains(strings.ToUpper(r.Rows[3]["slopdb_default_expression"].Text), "CURRENT_TIMESTAMP") {
				t.Fatal(r.Rows)
			}
			r = exec("SELECT constraint_type FROM information_schema.table_constraints WHERE table_schema='" + root + "' AND table_name='child' ORDER BY constraint_type")
			if len(r.Rows) != 3 {
				t.Fatalf("constraints %+v", r)
			}
			r = exec("SELECT referenced_table_schema,referenced_table_name,referenced_column_name FROM information_schema.key_column_usage WHERE constraint_name='child_parent'")
			if len(r.Rows) != 1 || r.Rows[0]["referenced_table_schema"].Text != root || r.Rows[0]["referenced_table_name"].Text != "parent" {
				t.Fatal(r)
			}
			r = exec("SELECT check_clause FROM information_schema.check_constraints WHERE constraint_name='amount_positive'")
			if len(r.Rows) != 1 || r.Rows[0]["check_clause"].Null {
				t.Fatal(r)
			}
			r = exec("SELECT column_name FROM information_schema.columns WHERE table_schema='" + root + "' AND table_name='child_names' ORDER BY ordinal_position")
			if len(r.Rows) != 2 {
				t.Fatal(r)
			}
			exec("BEGIN")
			exec("CREATE TABLE pending(id INTEGER)")
			r = exec("SELECT table_name FROM information_schema.tables WHERE table_schema='" + root + "' AND table_name='pending'")
			if len(r.Rows) != 1 {
				t.Fatal(r)
			}
			exec("ROLLBACK")
			r = exec("SELECT table_name FROM information_schema.tables WHERE table_name='pending'")
			if len(r.Rows) != 0 {
				t.Fatal("rolled back DDL remains", r)
			}
			exec("CREATE TEMPORARY TABLE local_only(id INTEGER)")
			r = exec("SELECT slopdb_temporary FROM information_schema.tables WHERE table_name='local_only'")
			if len(r.Rows) != 1 || r.Rows[0]["slopdb_temporary"].Text != "YES" {
				t.Fatal(r)
			}
			for _, name := range []string{"statistics", "table_indexes", "referential_constraints", "constraint_column_usage", "triggers", "character_sets", "collations", "collation_character_set_applicability", "engines", "routines", "parameters"} {
				exec("SELECT * FROM information_schema." + name + " LIMIT 1")
			}
			s.Close()
			if e = d.Close(); e != nil {
				t.Fatal(e)
			}
			d, e = Open(path, nil)
			if e != nil {
				t.Fatal(e)
			}
			r, e = d.Exec("SELECT table_name FROM information_schema.tables WHERE table_name='child'")
			if e != nil || len(r.Rows) != 1 {
				t.Fatalf("reopen %+v %v", r, e)
			}
			r, e = d.Exec("SELECT table_name FROM information_schema.tables WHERE table_name='local_only'")
			if e != nil || len(r.Rows) != 0 {
				t.Fatalf("temporary reopen %+v %v", r, e)
			}
		})
	}
}
