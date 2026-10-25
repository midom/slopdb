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

func TestSharedPreparedFunctionAndCastConstraints(t *testing.T) {
	d := projectionDatabase(t)
	s, e := d.NewSession()
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	if e = s.SetDialect(sql.DialectPostgreSQL); e != nil {
		t.Fatal(e)
	}
	cases := []struct {
		q      string
		params []sql.TypeID
		result sql.TypeID
	}{
		{"WITH c AS(SELECT ? AS v) SELECT v FROM c WHERE v=CAST(? AS BIGINT)", []sql.TypeID{sql.TypeInt64, sql.TypeInt64}, sql.TypeInt64},
		{"SELECT ? AND NOT ?", []sql.TypeID{sql.TypeBoolean, sql.TypeBoolean}, sql.TypeBoolean},
		{"SELECT ? LIKE ?", []sql.TypeID{sql.TypeText, sql.TypeText}, sql.TypeBoolean},
		{"SELECT left(?,?)", []sql.TypeID{sql.TypeText, sql.TypeInt32}, sql.TypeText},
		{"SELECT encode(?,?)", []sql.TypeID{sql.TypeBinary, sql.TypeText}, sql.TypeText},
		{"SELECT get_byte(?,?)", []sql.TypeID{sql.TypeBinary, sql.TypeInt32}, sql.TypeInt32},
		{"SELECT CAST(CAST(? AS BIGINT) AS TEXT)", []sql.TypeID{sql.TypeInt64}, sql.TypeText},
		{"SELECT CAST(? AS TIMESTAMP WITH TIME ZONE)", []sql.TypeID{sql.TypeTimestampTZ}, sql.TypeTimestampTZ},
	}
	for _, tc := range cases {
		p, e := s.Prepare(tc.q)
		if e != nil {
			t.Fatal(e)
		}
		desc, e := s.DescribePreparedTypes(context.Background(), p, sql.DescribeOptions{})
		p.Close()
		if e != nil {
			t.Fatalf("%s: %v", tc.q, e)
		}
		if len(desc.Parameters) != len(tc.params) {
			t.Fatal(desc)
		}
		for i, want := range tc.params {
			if desc.Parameters[i].Kind != want {
				t.Fatalf("%s parameter%d=%+v want%v", tc.q, i, desc.Parameters[i], want)
			}
		}
		if len(desc.Results) != 1 || desc.Results[0].LogicalType().Kind != tc.result {
			t.Fatalf("%s results %+v", tc.q, desc.Results)
		}
	}
	p, e := s.Prepare("SELECT CAST(? AS BIGINT)")
	if e != nil {
		t.Fatal(e)
	}
	defer p.Close()
	desc, e := s.DescribePreparedTypes(context.Background(), p, sql.DescribeOptions{ParameterTypes: []sql.LogicalType{sql.LogicalTypeFromDeclaration("TEXT")}})
	if e != nil || desc.Parameters[0].Kind != sql.TypeText {
		t.Fatalf("supplied input type overwritten %+v %v", desc, e)
	}
}

func TestSharedDescriptionQualifiedParameterCloning(t *testing.T) {
	d := projectionDatabase(t)
	s, e := d.NewSession()
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	if e = s.SetDialect(sql.DialectPostgreSQL); e != nil {
		t.Fatal(e)
	}
	if _, e = s.Exec("CREATE SCHEMA tenant; CREATE TABLE tenant.items(id BIGINT PRIMARY KEY)"); e != nil {
		t.Fatal(e)
	}
	p, e := s.Prepare("SELECT ? AS output FROM tenant.items WHERE id=?")
	if e != nil {
		t.Fatal(e)
	}
	defer p.Close()
	desc, e := s.DescribePreparedTypes(context.Background(), p, sql.DescribeOptions{ParameterPositions: []int{0, 0}})
	if e != nil {
		t.Fatal(e)
	}
	if len(desc.Parameters) != 1 || desc.Parameters[0].Kind != sql.TypeInt64 || desc.Results[0].LogicalType().Kind != sql.TypeInt64 {
		t.Fatalf("%+v", desc)
	}
}

func TestSharedDescriptionRejectsInvalidParameterMaps(t *testing.T) {
	d := projectionDatabase(t)
	s, e := d.NewSession()
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	p, e := s.Prepare("SELECT ?")
	if e != nil {
		t.Fatal(e)
	}
	defer p.Close()
	for _, opts := range []sql.DescribeOptions{{ParameterPositions: []int{-1}}, {ParameterPositions: []int{int(^uint(0) >> 1)}}, {ParameterPositions: []int{0, 0}}, {ParameterTypes: []sql.LogicalType{{}, {}}}} {
		if _, e = s.DescribePreparedTypes(context.Background(), p, opts); e == nil {
			t.Fatalf("accepted %+v", opts)
		}
	}
}

func TestPreparedBindingsPreserveNeutralTypeIdentity(t *testing.T) {
	d := projectionDatabase(t)
	s, e := d.NewSession()
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	if e = s.SetDialect(sql.DialectPostgreSQL); e != nil {
		t.Fatal(e)
	}
	p, e := s.Prepare("SELECT ? AS v")
	if e != nil {
		t.Fatal(e)
	}
	defer p.Close()
	decimal, e := sql.NewDecimalValue("123.4500")
	if e != nil {
		t.Fatal(e)
	}
	document, e := sql.NewJSONValue(`{"n":123.45}`)
	if e != nil {
		t.Fatal(e)
	}
	for _, v := range []sql.Value{decimal.WithSemanticType(sql.TypeDecimal), document.WithSemanticType(sql.TypeJSONB), sql.NewNullValue().WithSemanticType(sql.TypeUUID)} {
		r, e := p.Exec(v)
		if e != nil {
			t.Fatal(e)
		}
		if r.Rows[0]["v"].LogicalType().Kind != v.LogicalType().Kind || r.Columns[0].LogicalType().Kind != v.LogicalType().Kind {
			t.Fatalf("lost identity %+v => %+v %+v", v, r.Columns, r.Rows)
		}
	}
	r, e := s.Exec("SELECT CASE WHEN TRUE THEN NULL ELSE TRUE END AS b")
	if e != nil || r.Columns[0].LogicalType().Kind != sql.TypeBoolean {
		t.Fatal(r, e)
	}
}
