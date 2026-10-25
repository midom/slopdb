package mysqlserver

import (
	gosql "database/sql"
	"reflect"
	"testing"
)

func TestQualifiedColumnLabelsInWordPressTermQueries(t *testing.T) {
	s := startTestServer(t, "tcp", Config{})
	c := s.open(t, "slopdb", "", "slopdb")
	for _, q := range []string{
		"CREATE TABLE wp_terms(term_id BIGINT PRIMARY KEY,name TEXT,slug TEXT)",
		"CREATE TABLE wp_term_taxonomy(term_taxonomy_id BIGINT PRIMARY KEY,term_id BIGINT,taxonomy TEXT)",
		"CREATE TABLE dotted_label(`a.b` INTEGER)",
		"INSERT INTO dotted_label VALUES(1)",
		"INSERT INTO wp_terms VALUES(3,'SlopDB Category','slopdb-category')",
		"INSERT INTO wp_term_taxonomy VALUES(7,3,'category')",
	} {
		if _, e := c.Exec(q); e != nil {
			t.Fatal(e)
		}
	}
	for _, q := range []string{"SELECT `a.b` FROM dotted_label", "SELECT d.`a.b` FROM dotted_label d"} {
		rows, e := c.Query(q)
		if e != nil {
			t.Fatal(e)
		}
		cols, e := rows.Columns()
		if e != nil || !reflect.DeepEqual(cols, []string{"a.b"}) {
			t.Fatalf("quoted label %s: %v %v", q, cols, e)
		}
		rows.Close()
	}
	for _, q := range []string{
		"SELECT t.term_id FROM wp_terms t WHERE t.slug='slopdb-category'",
		"SELECT t.term_id FROM wp_terms t WHERE t.term_id=3",
		"SELECT t.term_id FROM wp_terms AS t INNER JOIN wp_term_taxonomy AS tt ON t.term_id=tt.term_id WHERE tt.taxonomy IN ('category') AND t.slug IN ('slopdb-category') ORDER BY t.name ASC",
	} {
		for _, prepared := range []bool{false, true} {
			var rows *gosql.Rows
			var e error
			if prepared {
				p, err := c.Prepare(q)
				if err != nil {
					t.Fatal(err)
				}
				defer p.Close()
				rows, e = p.Query()
			} else {
				rows, e = c.Query(q)
			}
			if e != nil {
				t.Fatal(q, e)
			}
			cols, e := rows.Columns()
			if e != nil || !reflect.DeepEqual(cols, []string{"term_id"}) {
				t.Fatalf("%s %v %v", q, cols, e)
			}
			var value int64
			if !rows.Next() {
				t.Fatal("missing term", rows.Err())
			}
			if e = rows.Scan(&value); e != nil || value != 3 {
				t.Fatal(value, e)
			}
			rows.Close()
		}
	}
	for _, q := range []string{"SELECT t.term_id,tt.term_id FROM wp_terms t JOIN wp_term_taxonomy tt ON t.term_id=tt.term_id", "SELECT * FROM wp_terms t JOIN wp_term_taxonomy tt ON t.term_id=tt.term_id"} {
		rows, e := c.Query(q)
		if e != nil {
			t.Fatal(e)
		}
		cols, e := rows.Columns()
		if e != nil {
			t.Fatal(e)
		}
		for _, label := range cols {
			if label == "t.term_id" || label == "tt.term_id" {
				t.Fatal("private qualifier leaked", cols)
			}
		}
		rows.Close()
	}
}
