package db

import (
	"context"
	"reflect"
	"slopdb/sql"
	"testing"
)

func TestQualifiedStarAcrossDialects(t *testing.T) {
	for _, dialect := range []sql.Dialect{sql.DialectNative, sql.DialectMySQL, sql.DialectPostgreSQL} {
		t.Run(dialect.String(), func(t *testing.T) {
			d := projectionDatabase(t)
			s, err := d.NewSession()
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			if err = s.SetDialect(dialect); err != nil {
				t.Fatal(err)
			}
			for _, q := range []string{"CREATE TABLE source_a(id bigint PRIMARY KEY, name TEXT)", "CREATE TABLE source_b(id bigint PRIMARY KEY, value TEXT)", "INSERT INTO source_a VALUES(1,'source_a')", "INSERT INTO source_b VALUES(2,'source_b')", "CREATE VIEW starred AS SELECT a.* FROM source_a a"} {
				if _, err = s.Exec(q); err != nil {
					t.Fatal(q, err)
				}
			}
			for _, tc := range []struct {
				q      string
				labels []string
				values []string
			}{
				{"SELECT a.*,b.* FROM source_a a JOIN source_b b ON 1=1", []string{"id", "name", "id", "value"}, []string{"1", "source_a", "2", "source_b"}},
				{"SELECT b.*,a.name AS label,a.* FROM source_a a JOIN source_b b ON 1=1", []string{"id", "value", "label", "id", "name"}, []string{"2", "source_b", "source_a", "1", "source_a"}},
				{"SELECT source_a.* FROM source_a", []string{"id", "name"}, []string{"1", "source_a"}},
				{"SELECT q.* FROM (SELECT id,name FROM source_a) q", []string{"id", "name"}, []string{"1", "source_a"}},
				{"WITH q AS (SELECT a.* FROM source_a a) SELECT q.* FROM q", []string{"id", "name"}, []string{"1", "source_a"}},
				{"WITH q AS (SELECT a.* FROM source_a a JOIN source_b b ON 1=1) SELECT q.* FROM q", []string{"id", "name"}, []string{"1", "source_a"}},
				{"SELECT v.* FROM starred v", []string{"id", "name"}, []string{"1", "source_a"}},
			} {
				r, err := s.Exec(tc.q)
				if err != nil {
					t.Fatal(tc.q, err)
				}
				labels := []string{}
				values := []string{}
				for _, c := range r.Columns {
					labels = append(labels, c.ResultName())
					values = append(values, r.Rows[0][c.Name].String())
				}
				if !reflect.DeepEqual(labels, tc.labels) || !reflect.DeepEqual(values, tc.values) {
					t.Fatal(tc.q, labels, values)
				}
				p, err := s.Prepare(tc.q)
				if err != nil {
					t.Fatal(err)
				}
				desc, err := s.DescribePreparedTypes(context.Background(), p, sql.DescribeOptions{})
				p.Close()
				if err != nil {
					t.Fatal(tc.q, err)
				}
				labels = nil
				for _, c := range desc.Results {
					labels = append(labels, c.ResultName())
				}
				if !reflect.DeepEqual(labels, tc.labels) {
					t.Fatal(tc.q, labels, tc.labels)
				}
			}

			for _, q := range []string{"INSERT INTO source_a VALUES(3,'returning') RETURNING source_a.*", "UPDATE source_a SET name='changed' WHERE id=3 RETURNING main.source_a.*", "DELETE FROM source_a WHERE id=3 RETURNING source_a.*"} {
				p, err := s.Prepare(q)
				if err != nil {
					t.Fatal(q, err)
				}
				desc, err := s.DescribePreparedTypes(context.Background(), p, sql.DescribeOptions{})
				p.Close()
				if err != nil || len(desc.Results) != 2 {
					t.Fatal(q, desc, err)
				}
				r, err := s.Exec(q)
				if err != nil || len(r.Rows) != 1 || len(r.Columns) != 2 || r.Rows[0][r.Columns[0].Name].Int != 3 {
					t.Fatal(q, r, err)
				}
			}
			if _, err = s.Exec("INSERT INTO source_a VALUES(4,'bad') RETURNING missing.*"); err == nil {
				t.Fatal("accepted invalid RETURNING star")
			}
			r, err := s.Exec("SELECT COUNT(*) AS n FROM source_a WHERE id=4")
			if err != nil || r.Rows[0]["n"].Int != 0 {
				t.Fatal("invalid RETURNING did not roll back", r, err)
			}
			for _, q := range []string{"SELECT missing.* FROM source_a", "SELECT source_a.*+1 FROM source_a", "SELECT source_a..* FROM source_a", "SELECT source_a.* AS bad FROM source_a", "SELECT a.b.c.* FROM source_a"} {
				if _, err = s.Exec(q); err == nil {
					t.Fatal("accepted invalid star", q)
				}
			}
		})
	}
}
