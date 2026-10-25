package db

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"slopdb/sql"
	"slopdb/storage"
)

func projectionDatabase(t *testing.T) *Database {
	t.Helper()
	opt := storage.DefaultOptions()
	opt.DisableWALSync = true
	d, e := Open(t.TempDir(), opt)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { _ = d.Close() })
	return d
}
func assertProjection(t *testing.T, r *ExecResult, labels []string, want [][]int64) {
	t.Helper()
	if len(r.Columns) != len(labels) || len(r.Rows) != len(want) {
		t.Fatalf("columns/rows %v %+v", r.Columns, r.Rows)
	}
	keys := map[string]bool{}
	for i, col := range r.Columns {
		if col.ResultName() != labels[i] || keys[col.Name] {
			t.Fatalf("column%d %+v", i, col)
		}
		keys[col.Name] = true
	}
	for i, row := range r.Rows {
		for j, col := range r.Columns {
			v, ok := row[col.Name]
			if !ok || v.Null || v.Int != want[i][j] {
				t.Fatalf("row%d col%d %q got %v want%d; row=%+v", i, j, col.Name, v, want[i][j], row)
			}
		}
	}
}
func TestDuplicateResultProjectionSlots(t *testing.T) {
	d := projectionDatabase(t)
	if _, e := d.Exec("CREATE TABLE p(id INTEGER PRIMARY KEY,a INTEGER,b INTEGER);INSERT INTO p VALUES(1,10,20),(2,11,21);CREATE INDEX p_a ON p(a)"); e != nil {
		t.Fatal(e)
	}
	cases := []struct {
		q      string
		labels []string
		want   [][]int64
	}{
		{"SELECT 1 AS x,2 AS x,3 AS x", []string{"x", "x", "x"}, [][]int64{{1, 2, 3}}},
		{"SELECT a AS x,b AS x FROM p ORDER BY id", []string{"x", "x"}, [][]int64{{10, 20}, {11, 21}}},
		{"SELECT a,a FROM p WHERE id=1", []string{"a", "a"}, [][]int64{{10, 10}}},
		{"SELECT a AS x,b AS x FROM p WHERE a>=10 ORDER BY a LIMIT 1", []string{"x", "x"}, [][]int64{{10, 20}}},
		{"SELECT a AS x,b AS x FROM p ORDER BY 2 DESC LIMIT 1", []string{"x", "x"}, [][]int64{{11, 21}}},
		{"SELECT count(*) AS x,sum(a) AS x FROM p", []string{"x", "x"}, [][]int64{{2, 21}}},
		{"SELECT id AS x,sum(a) AS x FROM p GROUP BY id ORDER BY id", []string{"x", "x"}, [][]int64{{1, 10}, {2, 11}}},
		{"SELECT a AS x,b AS x FROM p UNION ALL SELECT b AS x,a AS x FROM p", []string{"x", "x"}, [][]int64{{10, 20}, {11, 21}, {20, 10}, {21, 11}}},
		{"SELECT 1 AS x,2 AS x UNION SELECT 1 AS x,3 AS x ORDER BY 2", []string{"x", "x"}, [][]int64{{1, 2}, {1, 3}}},
		{"SELECT *,a+1 AS a FROM p WHERE id=1", []string{"id", "a", "b", "a"}, [][]int64{{1, 10, 20, 11}}},
		{"SELECT a+1 AS a,* FROM p WHERE id=1", []string{"a", "id", "a", "b"}, [][]int64{{11, 1, 10, 20}}},
		{"SELECT *,* FROM p WHERE id=1", []string{"id", "a", "b", "id", "a", "b"}, [][]int64{{1, 10, 20, 1, 10, 20}}},
		{"SELECT * FROM (SELECT 1 AS x,2 AS x) q", []string{"x", "x"}, [][]int64{{1, 2}}},
		{"SELECT *,x+1 AS x FROM (SELECT 1 AS x,2 AS x) q", []string{"x", "x", "x"}, [][]int64{{1, 2, 2}}},
		{"SELECT p.a AS x,q.b AS x FROM p JOIN p q ON p.id=q.id WHERE p.id=1", []string{"x", "x"}, [][]int64{{10, 20}}},
		{"SELECT a AS x,row_number() OVER(ORDER BY id) AS x FROM p ORDER BY id", []string{"x", "x"}, [][]int64{{10, 1}, {11, 2}}},
	}
	for _, tc := range cases {
		t.Run(tc.q, func(t *testing.T) {
			r, e := d.Exec(tc.q)
			if e != nil {
				t.Fatal(e)
			}
			assertProjection(t, r, tc.labels, tc.want)
			p, e := d.Prepare(tc.q)
			if e != nil {
				t.Fatal(e)
			}
			defer p.Close()
			r, e = p.Exec()
			if e != nil {
				t.Fatal(e)
			}
			assertProjection(t, r, tc.labels, tc.want)
		})
	}
	for _, query := range []string{"SELECT 1 AS x,2 AS x FROM sqlite_master WHERE name='p'", "SELECT 1 AS x,2 AS x FROM sqlite_stat1 WHERE tbl='p'"} {
		if _, e := d.Exec("ANALYZE p"); e != nil {
			t.Fatal(e)
		}
		r, e := d.Exec(query)
		if e != nil {
			t.Fatal(e)
		}
		assertProjection(t, r, []string{"x", "x"}, [][]int64{{1, 2}})
	}
	for _, q := range []string{"UPDATE p SET b=b+1 WHERE id=1 RETURNING a AS x,b AS x", "INSERT INTO p VALUES(3,30,40) RETURNING a AS x,b AS x", "DELETE FROM p WHERE id=3 RETURNING a AS x,b AS x"} {
		r, e := d.Exec(q)
		if e != nil {
			t.Fatal(e)
		}
		want := [][]int64{{30, 40}}
		if strings.HasPrefix(q, "UPDATE") {
			want = [][]int64{{10, 21}}
		}
		assertProjection(t, r, []string{"x", "x"}, want)
	}
}
func TestDuplicateStreamingProjectionAndDescribe(t *testing.T) {
	d := projectionDatabase(t)
	if _, e := d.Exec("CREATE TABLE p(id INTEGER PRIMARY KEY,a INTEGER,b INTEGER);INSERT INTO p VALUES(1,10,20),(2,11,21)"); e != nil {
		t.Fatal(e)
	}
	for _, query := range []string{"SELECT a AS x,b AS x FROM p ORDER BY id", "SELECT *,a+1 AS a FROM p ORDER BY id"} {
		rows, e := d.Query(query)
		if e != nil {
			t.Fatal(e)
		}
		if rows.Materialized() {
			t.Fatal("simple duplicate projection materialized")
		}
		columns := rows.Columns()
		i := 0
		for rows.Next() {
			values, e := rows.Values()
			if e != nil {
				t.Fatal(e)
			}
			expected := []int64{10 + int64(i), 20 + int64(i)}
			if len(columns) == 4 {
				expected = []int64{1 + int64(i), 10 + int64(i), 20 + int64(i), 11 + int64(i)}
			}
			for j, v := range values {
				if v.Int != expected[j] {
					t.Fatalf("%s row%d values%v", query, i, values)
				}
			}
			i++
		}
		if e = rows.Err(); e != nil {
			t.Fatal(e)
		}
		if e = rows.Close(); e != nil {
			t.Fatal(e)
		}
		if i != 2 {
			t.Fatal(i)
		}
		statement, e := d.Prepare(query)
		if e != nil {
			t.Fatal(e)
		}
		defer statement.Close()
		session, e := d.NewSession()
		if e != nil {
			t.Fatal(e)
		}
		meta, e := session.DescribePrepared(context.Background(), statement)
		_ = session.Close()
		if e != nil {
			t.Fatal(e)
		}
		for j, col := range meta {
			if col.Name != columns[j].Name || col.ResultName() != columns[j].ResultName() {
				t.Fatalf("describe mismatch %+v %+v", meta, columns)
			}
		}
	}
	col := sql.ColumnDef{Name: "internal", DisplayName: "visible"}
	encoded, e := json.Marshal(col)
	if e != nil || strings.Contains(string(encoded), "visible") {
		t.Fatalf("displayname persisted: %s %v", encoded, e)
	}
}
func TestDuplicateVolatileProjectionsAreIndependent(t *testing.T) {
	d := projectionDatabase(t)
	s, e := d.NewSession()
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	if e = s.SetPostgresMode(true); e != nil {
		t.Fatal(e)
	}
	var next int64
	ctx := sql.WithPostgresSession(context.Background(), sql.PostgresSessionOptions{Function: func(_ context.Context, name string, args []sql.Value) (sql.Value, bool, error) {
		if name == "CURRENT_SETTING" {
			next++
			return sql.NewIntValue(next), true, nil
		}
		return sql.Value{}, false, nil
	}})
	r, e := s.ExecContext(ctx, "SELECT current_setting('projection.counter'),current_setting('projection.counter'),current_setting('projection.counter')")
	if e != nil {
		t.Fatal(e)
	}
	assertProjection(t, r, []string{"CURRENT_SETTING", "CURRENT_SETTING", "CURRENT_SETTING"}, [][]int64{{1, 2, 3}})
	if !reflect.DeepEqual(next, int64(3)) {
		t.Fatal(next)
	}
}
