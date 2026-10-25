package db

import (
	"context"
	"slopdb/sql"
	"slopdb/storage"
	"testing"
)

func TestMySQLColumnSpellingReopen(t *testing.T) {
	path := t.TempDir()
	for pass := 0; pass < 2; pass++ {
		opt := storage.DefaultOptions()
		opt.DisableWALSync = true
		d, err := Open(path, opt)
		if err != nil {
			t.Fatal(err)
		}
		s, err := d.NewSession()
		if err != nil {
			t.Fatal(err)
		}
		if err = s.SetDialect(sql.DialectMySQL); err != nil {
			t.Fatal(err)
		}
		if pass == 0 {
			for _, q := range []string{"CREATE TABLE spelling(ID bigint PRIMARY KEY, Body TEXT)", "INSERT INTO spelling VALUES(1,'body')", "ALTER TABLE spelling CHANGE Body NewBody TEXT"} {
				if _, err = s.Exec(q); err != nil {
					t.Fatal(q, err)
				}
			}
		}
		r, err := s.Exec("SELECT * FROM spelling")
		if err != nil {
			t.Fatal(err)
		}
		if r.Columns[0].ResultName() != "ID" || r.Columns[1].ResultName() != "NewBody" || r.Rows[0]["id"].Int != 1 {
			t.Fatal(r.Columns, r.Rows)
		}
		p, err := s.Prepare("SELECT * FROM spelling WHERE ID=?")
		if err != nil {
			t.Fatal(err)
		}
		desc, err := s.DescribePreparedTypes(context.Background(), p, sql.DescribeOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if desc.Results[0].ResultName() != "ID" {
			t.Fatal(desc)
		}
		p.Close()
		s.Close()
		if err = d.Close(); err != nil {
			t.Fatal(err)
		}
	}
}
