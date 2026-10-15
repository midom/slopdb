package sql

import "testing"

func TestPreparedPointProjectionRecipeReuse(t *testing.T) {
	s, _ := statisticsSchema(t, t.TempDir())
	statisticsSQL(t, s, "CREATE TABLE t(id BIGINT PRIMARY KEY,v BIGINT NOT NULL,payload BLOB NOT NULL); INSERT INTO t VALUES(1,17,X'0102'),(2,34,X'0304')")
	q, err := Prepare("SELECT id,v,payload FROM t WHERE id=?")
	if err != nil {
		t.Fatal(err)
	}
	b, err := q.Bind([]Value{NewIntValue(1)})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Release()
	stmt := b.statements[0].(*SelectStmt)
	var recipe *ColumnDef
	for _, id := range []int64{1, 2, 1} {
		b.literals[0].Value = NewIntValue(id)
		if err = s.StartStatement(); err != nil {
			t.Fatal(err)
		}
		r, e := s.Select(stmt)
		s.EndStatement()
		if e != nil {
			t.Fatal(e)
		}
		if len(r.Rows) != 1 || r.Rows[0]["v"].Int != id*17 {
			t.Fatal("wrong point answer")
		}
		if stmt.prepared == nil || len(stmt.prepared.pointProjection) != 3 {
			t.Fatalf("point projection was not compiled: prepared=%+v, statement=%+v, pk=%+v", stmt.prepared, stmt, s.tables["t"].PKColumns)
		}
		if recipe == nil {
			recipe = &stmt.prepared.pointProjection[0]
		} else if recipe != &stmt.prepared.pointProjection[0] {
			t.Fatal("projection recipe rebuilt without metadata changes")
		}
		r.Columns[0].Name = "caller-owned"
	}
	if got := q.PlanStats(); got.Builds != 1 || got.Hits != 2 {
		t.Fatalf("point plan reuse: %+v", got)
	}
}
