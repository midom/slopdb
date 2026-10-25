package db

import (
	"context"
	"slopdb/sql"
	"testing"
)

func TestSharedPreparedDescriptionAcrossDialects(t *testing.T) {
	d := projectionDatabase(t)
	for _, dialect := range []sql.Dialect{sql.DialectNative, sql.DialectMySQL, sql.DialectPostgreSQL} {
		t.Run(dialect.String(), func(t *testing.T) {
			s, e := d.NewSession()
			if e != nil {
				t.Fatal(e)
			}
			defer s.Close()
			if e = s.SetDialect(dialect); e != nil {
				t.Fatal(e)
			}
			table := "types_" + dialect.String()
			if _, e = s.Exec("CREATE TABLE " + table + "(id INTEGER PRIMARY KEY,label VARCHAR(20),amount DECIMAL(12,3))"); e != nil {
				t.Fatal(e)
			}
			check := func(q string, opts sql.DescribeOptions) sql.PreparedDescription {
				t.Helper()
				p, e := s.Prepare(q)
				if e != nil {
					t.Fatal(e)
				}
				defer p.Close()
				desc, e := s.DescribePreparedTypes(context.Background(), p, opts)
				if e != nil {
					t.Fatalf("%s: %v", q, e)
				}
				return desc
			}
			wantID := sql.TypeInt64
			if dialect != sql.DialectNative {
				wantID = sql.TypeInt32
			}
			got := check("INSERT INTO "+table+"(id,label,amount) VALUES(?,?,?) RETURNING id,amount", sql.DescribeOptions{})
			if len(got.Parameters) != 3 || got.Parameters[0].Kind != wantID || got.Parameters[1].Kind != sql.TypeVarchar || got.Parameters[2].Kind != sql.TypeDecimal || len(got.Results) != 2 {
				t.Fatalf("insert %+v", got)
			}
			got = check("WITH c AS(SELECT id,label FROM "+table+") SELECT ? AS x,id AS x FROM c WHERE id=? LIMIT ?", sql.DescribeOptions{})
			if len(got.Parameters) != 3 || got.Parameters[0].Known() || got.Parameters[1].Kind != wantID || got.Parameters[2].Kind != sql.TypeInt64 || got.Results[0].Name == got.Results[1].Name || got.Results[1].ResultName() != "x" {
				t.Fatalf("select %+v", got)
			}
			got = check("SELECT ? AS value FROM "+table+" WHERE id=?", sql.DescribeOptions{ParameterPositions: []int{0, 0}})
			if len(got.Parameters) != 1 || got.Parameters[0].Kind != wantID || got.Results[0].LogicalType().Kind != wantID {
				t.Fatalf("repeated %+v", got)
			}
			got = check("UPDATE "+table+" SET amount=? WHERE id IN (?,?) RETURNING label", sql.DescribeOptions{})
			if len(got.Parameters) != 3 || got.Parameters[0].Kind != sql.TypeDecimal || got.Parameters[1].Kind != wantID || got.Parameters[2].Kind != wantID {
				t.Fatalf("update %+v", got)
			}
			got = check("DELETE FROM "+table+" WHERE id BETWEEN ? AND ? RETURNING id", sql.DescribeOptions{})
			if len(got.Parameters) != 2 || got.Parameters[0].Kind != wantID || got.Parameters[1].Kind != wantID {
				t.Fatalf("delete %+v", got)
			}
			if r, e := s.Exec("SELECT count(*) AS n FROM " + table); e != nil || r.Rows[0]["n"].Int != 0 {
				t.Fatal("description ran DML", r, e)
			}
		})
	}
}
