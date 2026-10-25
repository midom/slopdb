package cli

import (
	"slopdb/db"
	"slopdb/sql"
	"strings"
	"testing"
)

func TestDuplicateResultLabelsInOutput(t *testing.T) {
	r := &db.ExecResult{Columns: []sql.ColumnDef{{Name: "x", ColType: sql.ColInteger}, {Name: "\x00result:1", DisplayName: "x", ColType: sql.ColInteger}}, Rows: []map[string]sql.Value{{"x": sql.NewIntValue(1), "\x00result:1": sql.NewIntValue(2)}}}
	if got := formatResultCSV(r); got != "\"x\",\"x\"\n\"1\",\"2\"\n" {
		t.Fatal(got)
	}
	if got := formatResultTabs(r); got != "x\tx\n1\t2\n" {
		t.Fatal(got)
	}
	for _, got := range []string{formatResultTable(r), db.FormatResult(r)} {
		if strings.Contains(got, "\x00") || !strings.Contains(got, "1 | 2") {
			t.Fatalf("output %q", got)
		}
	}
}
