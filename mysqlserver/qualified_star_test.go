package mysqlserver

import (
	"reflect"
	"testing"
)

func TestMySQLQualifiedStars(t *testing.T) {
	s := startTestServer(t, "tcp", Config{})
	pool := s.open(t, "slopdb", "", "slopdb")
	for _, q := range []string{
		"CREATE TABLE terms (term_id bigint PRIMARY KEY, name VARCHAR(50), slug VARCHAR(50))",
		"CREATE TABLE term_groups(term_taxonomy_id bigint PRIMARY KEY, term_id bigint, taxonomy VARCHAR(50), count bigint)",
		"INSERT INTO terms VALUES(3,'Uncategorized','uncategorized')",
		"INSERT INTO term_groups VALUES(7,3,'category',1)",
		"CREATE TABLE mixed_case_columns(ID bigint, Title TEXT)",
		"INSERT INTO mixed_case_columns VALUES(1,'title')",
	} {
		if _, err := pool.Exec(q); err != nil {
			t.Fatal(q, err)
		}
	}
	upper, err := pool.Query("SELECT t.* FROM mixed_case_columns t")
	if err != nil {
		t.Fatal(err)
	}
	upperCols, err := upper.Columns()
	upper.Close()
	if err != nil || !reflect.DeepEqual(upperCols, []string{"ID", "Title"}) {
		t.Fatal(upperCols, err)
	}
	if rows, err := pool.Query("SELECT missing.* FROM mixed_case_columns t"); err == nil {
		rows.Close()
		t.Fatal("accepted unknown streaming qualifier")
	}
	query := "SELECT t.*, tt.* FROM terms AS t INNER JOIN term_groups AS tt ON t.term_id = tt.term_id WHERE t.term_id = 3"
	want := []string{"term_id", "name", "slug", "term_taxonomy_id", "term_id", "taxonomy", "count"}
	for _, prepared := range []bool{false, true} {
		if prepared {
			p, err := pool.Prepare(query)
			if err != nil {
				t.Fatal(err)
			}
			rows, err := p.Query()
			if err != nil {
				t.Fatal(err)
			}
			cols, err := rows.Columns()
			rows.Close()
			p.Close()
			if err != nil || !reflect.DeepEqual(cols, want) {
				t.Fatal(cols, err)
			}
			continue
		}
		rows, err := pool.Query(query)
		if err != nil {
			t.Fatal(err)
		}
		cols, err := rows.Columns()
		if err != nil || !reflect.DeepEqual(cols, want) {
			t.Fatal(cols, err)
		}
		if !rows.Next() {
			t.Fatal("missing joined row", rows.Err())
		}
		var id, taxID, duplicateID, count int64
		var name, slug, taxonomy string
		if err = rows.Scan(&id, &name, &slug, &taxID, &duplicateID, &taxonomy, &count); err != nil {
			t.Fatal(err)
		}
		rows.Close()
		if id != 3 || duplicateID != 3 || taxID != 7 || count != 1 || taxonomy != "category" {
			t.Fatal(id, duplicateID, taxID, count, taxonomy)
		}
	}
}
