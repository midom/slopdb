package sql

import (
	"strings"
	"testing"
)

func TestLiteralPointRecipeAndScratchLifetime(t *testing.T) {
	s, _ := statisticsSchema(t, t.TempDir())
	statisticsSQL(t, s, "CREATE TABLE literal_points(id BIGINT PRIMARY KEY,v BIGINT,b BLOB);INSERT INTO literal_points VALUES(1,10,X'0102'),(2,20,X'"+strings.Repeat("5a", 128<<10)+"')")
	var column *ColumnDef
	for _, query := range []string{
		"SELECT v,b FROM literal_points WHERE id=1",
		"SELECT v,b FROM literal_points WHERE id=2",
		"SELECT v,b FROM literal_points WHERE 1=id",
	} {
		r := statisticsSQL(t, s, query)
		if len(r.Rows) != 1 {
			t.Fatal(r)
		}
		p := s.directPointPlan
		if p == nil || len(p.pointProjection) != 2 || p.where != nil || p.equalities[0].literal != nil {
			t.Fatal("literal plan retained the parsed query or failed to compile metadata")
		}
		if column == nil {
			column = &p.pointProjection[0]
		} else if column != &p.pointProjection[0] {
			t.Fatal("unchanged projection was rebuilt")
		}
		if cap(p.pointValueScratch) > 64<<10 {
			t.Fatal("one-off large row retained unbounded storage scratch")
		}
	}
	// Changing physical types must invalidate the cached projection/decode plan.
	statisticsSQL(t, s, "ALTER TABLE literal_points MODIFY COLUMN v TEXT")
	r := statisticsSQL(t, s, "SELECT v,b FROM literal_points WHERE id=1")
	if r.Columns[0].ColType != ColText || r.Rows[0]["v"].Text != "10" {
		t.Fatal("type change retained stale metadata or row decoding", r)
	}
}
