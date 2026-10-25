package db

import (
	"context"
	"fmt"
	"testing"

	"slopdb/sql"
)

func TestPreparedPointPredicateBindingsAndResiduals(t *testing.T) {
	d := namespaceTestDatabase(t)
	s := isolationSession(t, d)
	isolationExec(t, s, "CREATE TABLE point_predicate(id INTEGER PRIMARY KEY,v INTEGER);INSERT INTO point_predicate VALUES(1,10),(2,20)")
	cases := []struct {
		where string
		args  []any
		want  int64
	}{
		{"id=?", []any{int64(1)}, 10}, {"id=?", []any{int64(2)}, 20}, {"id=?", []any{nil}, 0}, {"id=?", []any{"1"}, 10}, {"id=?", []any{float64(1)}, 10}, {"id=?", []any{uint64(1)}, 10}, {"id=?", []any{true}, 10},
		{"id=? AND id=?", []any{int64(1), int64(1)}, 10}, {"id=? AND id=?", []any{int64(1), int64(2)}, 0}, {"id=? AND id=?", []any{nil, int64(1)}, 0},
		{"id=? AND v=?", []any{int64(1), int64(10)}, 10}, {"id=? AND v=?", []any{int64(1), int64(20)}, 0},
		{"point_predicate.id=?", []any{int64(1)}, 10}, {"id=BINARY ?", []any{int64(1)}, 10},
	}
	decimal, e := sql.NewDecimalValue("1.0")
	if e != nil {
		t.Fatal(e)
	}
	cases = append(cases, struct {
		where string
		args  []any
		want  int64
	}{"id=?", []any{decimal}, 10})
	for _, tc := range cases {
		t.Run(fmt.Sprint(tc.where, tc.args), func(t *testing.T) {
			statement, e := s.Prepare("SELECT v FROM point_predicate WHERE " + tc.where)
			if e != nil {
				t.Fatal(e)
			}
			defer statement.Close()
			var result ExecResult
			for n := 0; n < 3; n++ {
				r, err := s.ExecPreparedInto(&result, statement, tc.args...)
				if err != nil {
					t.Fatal(err)
				}
				if tc.want == 0 {
					if len(r.Rows) != 0 {
						t.Fatal(r)
					}
				} else if len(r.Rows) != 1 || r.Rows[0]["v"].Int != tc.want {
					t.Fatal(r)
				}
			}
		})
	}
	statement, e := s.Prepare("SELECT v FROM point_predicate WHERE id=?")
	if e != nil {
		t.Fatal(e)
	}
	defer statement.Close()
	for _, tc := range []struct {
		arg  any
		want int64
	}{{int64(1), 10}, {nil, 0}, {"2", 20}, {int64(2), 20}, {nil, 0}, {int64(1), 10}} {
		r, err := statement.Exec(tc.arg)
		if err != nil {
			t.Fatal(err)
		}
		if tc.want == 0 {
			if len(r.Rows) != 0 {
				t.Fatal(r)
			}
		} else if len(r.Rows) != 1 || r.Rows[0]["v"].Int != tc.want {
			t.Fatal(r)
		}
	}
	for _, query := range []string{"SELECT v FROM point_predicate WHERE id=? AND 1/0=0", "SELECT v FROM point_predicate WHERE id=? AND missing=1"} {
		p, err := s.Prepare(query)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = p.Exec(int64(1)); err == nil {
			t.Fatalf("skipped invalid residual %s", query)
		}
		p.Close()
	}
}
func TestPreparedPointPredicateCompositeAndCatalogChanges(t *testing.T) {
	d := namespaceTestDatabase(t)
	s := isolationSession(t, d)
	isolationExec(t, s, "CREATE TABLE composite_predicate(a INTEGER,b INTEGER,v INTEGER,PRIMARY KEY(a,b));INSERT INTO composite_predicate VALUES(1,2,12),(2,1,21)")
	p, e := s.Prepare("SELECT v FROM composite_predicate WHERE a=? AND ?=b")
	if e != nil {
		t.Fatal(e)
	}
	defer p.Close()
	for _, tc := range []struct {
		a, b any
		want int64
	}{{int64(1), int64(2), 12}, {nil, int64(2), 0}, {int64(2), int64(1), 21}, {int64(1), "2", 12}} {
		r, err := p.Exec(tc.a, tc.b)
		if err != nil {
			t.Fatal(err)
		}
		if tc.want == 0 {
			if len(r.Rows) != 0 {
				t.Fatal(r)
			}
		} else if len(r.Rows) != 1 || r.Rows[0]["v"].Int != tc.want {
			t.Fatal(r)
		}
	}
	isolationExec(t, s, "CREATE TABLE changing_predicate(id INTEGER PRIMARY KEY,v INTEGER);INSERT INTO changing_predicate VALUES(1,10)")
	changing, e := s.Prepare("SELECT v FROM changing_predicate WHERE id=?")
	if e != nil {
		t.Fatal(e)
	}
	defer changing.Close()
	if r, e := changing.Exec(int64(1)); e != nil || len(r.Rows) != 1 {
		t.Fatal(r, e)
	}
	isolationExec(t, s, "DROP TABLE changing_predicate;CREATE TABLE changing_predicate(id TEXT PRIMARY KEY,v INTEGER);INSERT INTO changing_predicate VALUES('One',11)")
	if r, e := changing.Exec("One"); e != nil || len(r.Rows) != 1 || r.Rows[0]["v"].Int != 11 {
		t.Fatal(r, e)
	}
	if e = d.SetCollation("NFC_CI"); e != nil {
		t.Fatal(e)
	}
	if r, e := changing.Exec("one"); e != nil || len(r.Rows) != 1 || r.Rows[0]["v"].Int != 11 {
		t.Fatal(r, e)
	}
	isolationExec(t, s, "DROP TABLE changing_predicate;CREATE TABLE changing_predicate(id INTEGER PRIMARY KEY,v INTEGER);INSERT INTO changing_predicate VALUES(1,20)")
	if r, e := changing.Exec(int64(1)); e != nil || len(r.Rows) != 1 || r.Rows[0]["v"].Int != 20 {
		t.Fatal(r, e)
	}
}
func TestPreparedPointPredicateKeepsVolatileResidual(t *testing.T) {
	d := namespaceTestDatabase(t)
	s := isolationSession(t, d)
	if e := s.SetDialect(sql.DialectPostgreSQL); e != nil {
		t.Fatal(e)
	}
	isolationExec(t, s, "CREATE TABLE volatile_predicate(id BIGINT PRIMARY KEY,v INTEGER);INSERT INTO volatile_predicate VALUES(1,10)")
	statement, e := s.Prepare("SELECT v FROM volatile_predicate WHERE id=? AND current_setting('predicate.test')='allow'")
	if e != nil {
		t.Fatal(e)
	}
	defer statement.Close()
	calls := 0
	ctx := sql.WithPostgresSession(context.Background(), sql.PostgresSessionOptions{Function: func(_ context.Context, name string, args []sql.Value) (sql.Value, bool, error) {
		if name != "CURRENT_SETTING" {
			return sql.Value{}, false, nil
		}
		calls++
		value := "allow"
		if calls%2 == 0 {
			value = "deny"
		}
		return sql.NewTextValue(value), true, nil
	}})
	for i := 1; i <= 4; i++ {
		r, e := statement.ExecContext(ctx, int64(1))
		if e != nil {
			t.Fatal(e)
		}
		want := i % 2
		if len(r.Rows) != want || calls != i {
			t.Fatalf("call%d result%+v callback%d", i, r, calls)
		}
	}
}
