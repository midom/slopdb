package db

import (
	"slopdb/sql"
	"testing"
)

func TestMySQLFunctionNamesAsColumns(t *testing.T) {
	d := projectionDatabase(t)
	s, e := d.NewSession()
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	if e = s.SetDialect(sql.DialectMySQL); e != nil {
		t.Fatal(e)
	}
	// A function name is also a valid unquoted column identifier.
	statements := []string{`CREATE TABLE term_groups (
 term_taxonomy_id bigint(20) unsigned NOT NULL auto_increment,
 term_id bigint(20) unsigned NOT NULL default '0',
 taxonomy varchar(32) NOT NULL default '', description longtext NOT NULL,
 parent bigint(20) unsigned NOT NULL default '0', count bigint(20) NOT NULL default '0',
 PRIMARY KEY(term_taxonomy_id), UNIQUE KEY term_id_taxonomy(term_id,taxonomy), KEY taxonomy(taxonomy))`,
		`INSERT INTO term_groups(term_id,taxonomy,description,count) VALUES(1,'category','',2)`,
		`UPDATE term_groups SET count=count+1 WHERE term_id=1`,
		`CREATE TABLE function_names(count INTEGER,sum INTEGER,avg INTEGER,min INTEGER,max INTEGER)`,
		`INSERT INTO function_names(count,sum,avg,min,max) VALUES(1,2,3,4,5)`,
		`UPDATE function_names SET count=count+1,sum=sum+1,avg=avg+1,min=min+1,max=max+1`,
	}
	for _, q := range statements {
		if _, e = s.Exec(q); e != nil {
			t.Fatalf("%s: %v", q, e)
		}
	}
	r, e := s.Exec("SELECT count,COUNT(*) AS n,SUM(count) AS total FROM term_groups GROUP BY count")
	if e != nil {
		t.Fatal(e)
	}
	if len(r.Rows) != 1 || r.Rows[0]["count"].Int != 3 || r.Rows[0]["n"].Int != 1 || r.Rows[0]["total"].Int != 3 {
		t.Fatal(r.Rows)
	}
	r, e = s.Exec("SELECT count,sum,avg,min,max FROM function_names")
	if e != nil {
		t.Fatal(e)
	}
	for i, n := range []string{"count", "sum", "avg", "min", "max"} {
		if r.Rows[0][n].Int != int64(i+2) {
			t.Fatal(r.Rows)
		}
	}
}
