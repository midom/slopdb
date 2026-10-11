package sql

import (
	"fmt"
	"testing"

	"slopdb/storage"
)

func TestPreparedPlanReuseAndParameterBounds(t *testing.T) {
	s, _ := statisticsSchema(t, t.TempDir())
	statisticsSQL(t, s, "CREATE TABLE t(id INTEGER PRIMARY KEY,k INTEGER,v INTEGER);CREATE INDEX tk ON t(k);INSERT INTO t VALUES(1,10,100),(2,20,200),(3,30,300);ANALYZE")
	q, err := Prepare("SELECT SUM(v+?) AS total FROM t WHERE id BETWEEN ? AND ?")
	if err != nil {
		t.Fatal(err)
	}
	b, err := q.Bind([]Value{NewIntValue(1), NewIntValue(1), NewIntValue(2)})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Release()
	stmt := b.statements[0].(*SelectStmt)
	var compiled *typedAggregate
	for i, bounds := range [][2]int64{{1, 2}, {2, 3}, {3, 3}, {4, 9}} {
		b.literals[0].Value = NewIntValue(int64(i + 1))
		b.literals[1].Value = NewIntValue(bounds[0])
		b.literals[2].Value = NewIntValue(bounds[1])
		if err := s.StartStatement(); err != nil {
			t.Fatal(err)
		}
		rs, err := s.Select(stmt)
		s.EndStatement()
		if err != nil {
			t.Fatal(err)
		}
		var want int64
		count := int64(0)
		for id := bounds[0]; id <= bounds[1]; id++ {
			if id <= 3 {
				want += id*100 + int64(i+1)
				count++
			}
		}
		got := rs.Rows[0]["total"]
		if count == 0 {
			if !got.Null {
				t.Fatalf("expected NULL, got %v", got)
			}
		} else if got.Int != want {
			t.Fatalf("bounds %v: got %v want %d", bounds, got, want)
		}
		if i == 0 {
			compiled = stmt.prepared.aggregate
			if compiled == nil {
				t.Fatal("typed aggregate was not compiled")
			}
		} else if compiled != stmt.prepared.aggregate {
			t.Fatal("typed instructions rebuilt without metadata changes")
		}
	}
	if got := q.PlanStats(); got.Builds != 1 || got.Hits != 3 {
		t.Fatalf("plan reuse: %+v", got)
	}
	// ANALYZE must invalidate even when schema epoch and catalog pointers do not change.
	statisticsSQL(t, s, "ANALYZE")
	if _, err := s.Select(stmt); err != nil {
		t.Fatal(err)
	}
	if stmt.prepared.aggregate == compiled || q.PlanStats().Builds != 2 {
		t.Fatal("ANALYZE did not invalidate typed/physical hints")
	}
}

func TestPreparedPointUpdateBindingsReuse(t *testing.T) {
	root, _ := statisticsSchema(t, t.TempDir())
	statisticsSQL(t, root, "CREATE TABLE t(id INTEGER PRIMARY KEY,k INTEGER,v INTEGER);CREATE INDEX tk ON t(k)")
	s := root.NewSession()
	q, err := Prepare("UPDATE t SET k=k+?,v=v+? WHERE id=?")
	if err != nil {
		t.Fatal(err)
	}
	b, err := q.Bind([]Value{NewIntValue(1), NewIntValue(2), NewNullValue()})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Release()
	stmt := b.statements[0].(*UpdateStmt)
	if p := s.PreparePointUpdate(stmt); p != nil {
		t.Fatal("NULL must not produce a key")
	}
	compiled := stmt.prepared.pointUpdate
	if compiled == nil {
		t.Fatal("NULL first execution prevented structural compilation")
	}
	for _, id := range []int64{1, 2, 999} {
		b.literals[2].Value = NewIntValue(id)
		p := s.PreparePointUpdate(stmt)
		if p == nil || p.schema != s || p.stmt != stmt || len(p.key) == 0 {
			t.Fatal("missing private bound execution")
		}
		if stmt.prepared.pointUpdate != compiled {
			t.Fatal("update target/index bindings rebuilt")
		}
		want, _ := s.uncachedPrimaryKeyLookup(stmt.Where, "t", s.tables["t"])
		if string(p.key) != string(want) {
			t.Fatalf("stale key for %d", id)
		}
	}
	if got := q.PlanStats(); got.Builds != 1 || got.Hits != 3 {
		t.Fatalf("update reuse: %+v", got)
	}
	// Covering/affected index bindings must disappear after a catalog refresh.
	if err := root.DropIndex(&DropIndexStmt{Index: "tk"}); err != nil {
		t.Fatal(err)
	}
	// Direct-schema DDL does not publish epochs; import its immutable catalog explicitly.
	s.restoreState(root.captureState())
	if p := s.PreparePointUpdate(stmt); p == nil || len(p.indexes) != 0 {
		t.Fatal("dropped index retained in update plan")
	}
}

func BenchmarkPreparedPlanningReuse(b *testing.B) {
	o := storage.DefaultOptions()
	o.DisableWALSync = true
	lsm, err := storage.NewLSMTree(b.TempDir(), o)
	if err != nil {
		b.Fatal(err)
	}
	defer lsm.Close()
	root, err := NewSchema(lsm, 4096)
	if err != nil {
		b.Fatal(err)
	}
	statements, err := Parse("CREATE TABLE t(id INTEGER PRIMARY KEY,k INTEGER,v INTEGER);CREATE INDEX tk ON t(k)")
	if err != nil {
		b.Fatal(err)
	}
	for _, statement := range statements {
		switch st := statement.(type) {
		case *CreateTableStmt:
			err = root.CreateTable(st)
		case *CreateIndexStmt:
			err = root.CreateIndex(st)
		}
		if err != nil {
			b.Fatal(err)
		}
	}
	for _, kind := range []string{"point-read", "point-update", "range"} {
		for _, cached := range []bool{true, false} {
			b.Run(fmt.Sprintf("%s/cached=%v", kind, cached), func(b *testing.B) {
				text := "SELECT v FROM t WHERE id=?"
				if kind == "point-update" {
					text = "UPDATE t SET k=k+? WHERE id=?"
				}
				if kind == "range" {
					text = "SELECT v FROM t WHERE k BETWEEN ? AND ?"
				}
				q, err := Prepare(text)
				if err != nil {
					b.Fatal(err)
				}
				args := []Value{NewIntValue(1)}
				if kind != "point-read" {
					args = append(args, NewIntValue(2))
				}
				bound, err := q.Bind(args)
				if err != nil {
					b.Fatal(err)
				}
				defer bound.Release()
				session := root.NewSession()
				info := session.tables["t"]
				b.ReportAllocs()
				b.ResetTimer()
				for n := 0; n < b.N; n++ {
					switch stmt := bound.statements[0].(type) {
					case *UpdateStmt:
						if cached {
							_ = session.PreparePointUpdate(stmt)
						} else {
							_ = session.preparePointUpdateUncached(stmt)
						}
					case *SelectStmt:
						if cached {
							previous := session.usePreparedPlan(stmt.prepared, info)
							if err := session.validateQuery(stmt, info); err != nil {
								b.Fatal(err)
							}
							if kind == "range" {
								_ = session.tuplePlan(stmt, info)
							} else {
								_, _ = session.primaryKeyLookup(stmt.Where, "t", info)
							}
							session.restorePreparedPlan(previous)
						} else {
							if err := session.validateQueryUncached(stmt, info); err != nil {
								b.Fatal(err)
							}
							if kind == "range" {
								copy := *stmt
								copy.prepared = nil
								_ = session.tuplePlan(&copy, info)
							} else {
								_, _ = session.uncachedPrimaryKeyLookup(stmt.Where, "t", info)
							}
						}
					}
				}
			})
		}
	}
}

func TestPreparedPlanParameterSensitiveIndexCost(t *testing.T) {
	s, _ := statisticsSchema(t, t.TempDir())
	statisticsSQL(t, s, "CREATE TABLE skew(id INTEGER PRIMARY KEY,k INTEGER,payload TEXT);CREATE INDEX skew_k ON skew(k)")
	rows := ""
	for id := 1; id <= 1000; id++ {
		if id > 1 {
			rows += ","
		}
		k := 0
		if id > 900 {
			k = id
		}
		rows += fmt.Sprintf("(%d,%d,'wide payload')", id, k)
	}
	statisticsSQL(t, s, "INSERT INTO skew VALUES"+rows+";ANALYZE")
	q, err := Prepare("SELECT payload FROM skew WHERE k=?")
	if err != nil {
		t.Fatal(err)
	}
	b, err := q.Bind([]Value{NewIntValue(999)})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Release()
	stmt := b.statements[0].(*SelectStmt)
	for _, tc := range []struct {
		k     int64
		index bool
	}{{999, true}, {0, false}, {998, true}} {
		b.literals[0].Value = NewIntValue(tc.k)
		previous := s.usePreparedPlan(stmt.prepared, s.tables["skew"])
		plan := s.tuplePlan(stmt, s.tables["skew"])
		s.restorePreparedPlan(previous)
		if (plan != nil && plan.index != nil) != tc.index {
			t.Fatalf("k=%d stale parameter cost choice: %+v", tc.k, plan)
		}
	}
	if got := q.PlanStats(); got.Builds != 1 || got.Hits != 2 {
		t.Fatalf("parameter-sensitive plan reused structure: %+v", got)
	}
}
