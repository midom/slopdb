package sql

import "testing"

func TestLocalInspectionRequiresStateAndExcludesDataOperators(t *testing.T) {
	for _, tc := range []struct {
		q    string
		want bool
	}{
		{"SELECT 1", false}, {"SELECT 1+2", false}, {"SELECT @@read_only", true}, {"SELECT COALESCE(@@read_only,0),1", true},
		{"SELECT current_setting('slopdb.raft.role')", true}, {"SET @a=(SELECT @@read_only)", true},
		{"SHOW LSM STATUS", true}, {"SHOW RAFT STATUS", true}, {"SELECT @@read_only FROM private_data", false},
		{"SELECT @@read_only GROUP BY (SELECT id FROM private_data)", false},
		{"SELECT @@read_only HAVING (SELECT id FROM private_data)=1", false},
		{"SELECT @@read_only FOR UPDATE", false},
		{"SELECT COALESCE(@@read_only,0) OVER (ORDER BY (SELECT id FROM private_data))", false},
	} {
		stmts, e := ParseWithOptions(tc.q, ParseOptions{MySQLQuoting: true})
		if e != nil {
			t.Fatal(tc.q, e)
		}
		if got := IsLocalInspectionStatement(stmts[0]); got != tc.want {
			t.Errorf("%s inspection=%v", tc.q, got)
		}
	}
}
