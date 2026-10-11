package regression

import (
	"fmt"
	"strings"
	"testing"
)

func TestProfileBoundedIndexedProbeTrace(t *testing.T) {
	d := cteDatabase(t)
	cteExec(t, d, "CREATE TABLE c(id INTEGER PRIMARY KEY)")
	cteExec(t, d, "CREATE TABLE o(id INTEGER PRIMARY KEY,cid INTEGER,payload TEXT)")
	cteExec(t, d, "CREATE INDEX oc ON o(cid)")
	var left, right []string
	for i := 1; i <= 5000; i++ {
		if i <= 200 {
			left = append(left, fmt.Sprint("(", i, ")"))
		}
		right = append(right, fmt.Sprint("(", i, ",", i, ",'x')"))
	}
	cteExec(t, d, "INSERT INTO c VALUES "+strings.Join(left, ","))
	cteExec(t, d, "INSERT INTO o VALUES "+strings.Join(right, ","))
	cteExec(t, d, "ANALYZE")
	r := cteExec(t, d, "EXPLAIN ANALYZE SELECT c.id,o.payload FROM c LEFT JOIN o ON o.cid=c.id")
	p := r.Profile
	if p == nil {
		t.Fatal("missing profile")
	}
	if len(p.Operators) > 20 {
		t.Fatalf("unbounded indexed-probe trace: %d", len(p.Operators))
	}
	var probes, lookups int64
	for _, op := range p.Operators {
		if op.Operator == "RangeScan" && op.Relation == "o.oc" {
			probes += op.Invocations
		}
		lookups += op.PointLookups
	}
	if probes != 200 || p.IndexEntriesExamined != 200 || (p.RowsExamined < 400 || p.RowsExamined > 401) || (lookups != p.PointLookups || lookups != 200) || p.Operators[0].RowsReturned != 200 {
		t.Fatalf("rows=%d indices=%d output=%d probes=%d lookups=%d", p.RowsExamined, p.IndexEntriesExamined, p.Operators[0].RowsReturned, probes, lookups)
	}
}
