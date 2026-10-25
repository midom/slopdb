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

func TestActivityInspectionRequiresOnlyLiveRelations(t *testing.T) {
	for _, tc := range []struct {
		query   string
		dialect Dialect
		want    bool
	}{
		{"SHOW PROCESSLIST", DialectMySQL, true},
		{"SHOW FULL PROCESSLIST", DialectMySQL, true},
		{"SELECT * FROM information_schema.processlist WHERE id=CONNECTION_ID()", DialectMySQL, true},
		{"SELECT COUNT(*) FROM information_schema.processlist", DialectMySQL, true},
		{"SELECT p.id,q.info FROM information_schema.processlist p JOIN information_schema.processlist q ON p.id=q.id", DialectMySQL, true},
		{"SELECT * FROM pg_stat_activity WHERE pid=pg_backend_pid()", DialectPostgreSQL, true},
		{"SELECT * FROM pg_catalog.pg_stat_activity", DialectPostgreSQL, true},
		{"SELECT * FROM pg_stat_activity", DialectNative, false},
		{"SELECT * FROM pg_catalog.pg_stat_activity", DialectMySQL, false},
		{"SELECT * FROM information_schema.processlist p JOIN private_data d ON p.id=d.id", DialectMySQL, false},
		{"SELECT (SELECT id FROM private_data) FROM information_schema.processlist", DialectMySQL, false},
		{"SELECT * FROM information_schema.processlist WHERE id IN (SELECT id FROM private_data)", DialectMySQL, false},
		{"SELECT * FROM information_schema.processlist FOR UPDATE", DialectMySQL, false},
		{"SELECT SLEEP(1) FROM information_schema.processlist", DialectMySQL, false},
		{"SELECT GET_LOCK('x',1) FROM information_schema.processlist", DialectMySQL, false},
		{"WITH t AS (SELECT * FROM private_data) SELECT * FROM information_schema.processlist", DialectMySQL, false},
	} {
		statements, err := ParseWithOptions(tc.query, tc.dialect.ParseOptions(ParseOptions{}))
		if err != nil {
			t.Fatal(tc.query, err)
		}
		if got := IsLocalInspectionStatement(statements[0], tc.dialect); got != tc.want {
			t.Errorf("%s (%s): got %v want %v", tc.query, tc.dialect, got, tc.want)
		}
	}
}
