package sql

import "testing"

func TestReplicaReadStatementClassification(t *testing.T) {
	for _, test := range []struct {
		query string
		read  bool
	}{
		{"SELECT * FROM t", true},
		{"SELECT (SELECT x FROM t)", true},
		{"SELECT * FROM (SELECT x FROM t) AS q", true},
		{"WITH q AS (SELECT * FROM t) SELECT * FROM q UNION SELECT * FROM t", true},
		{"EXPLAIN ANALYZE SELECT * FROM t", true},
		{"SHOW TABLES", true},
		{"PRAGMA table_info('t')", true},
		{"PRAGMA page_size", true},
		{"BEGIN", true}, {"COMMIT ASYNC", true}, {"SAVEPOINT s", true},
		{"SELECT * FROM t FOR UPDATE", false},
		{"SELECT * FROM t FOR SHARE", false},
		{"SELECT (SELECT x FROM t FOR UPDATE)", false},
		{"SELECT 1 WHERE EXISTS(SELECT * FROM t FOR UPDATE)", false},
		{"SELECT * FROM (SELECT * FROM t FOR UPDATE) AS q", false},
		{"SELECT * FROM t JOIN (SELECT * FROM t FOR UPDATE) AS q ON t.x=q.x", false},
		{"WITH q AS (SELECT * FROM t FOR UPDATE) SELECT * FROM q", false},
		{"SELECT SUM(x) OVER (ORDER BY (SELECT x FROM t FOR UPDATE)) FROM t", false},
		{"INSERT INTO t VALUES(1)", false},
		{"WITH q AS (SELECT 1) DELETE FROM t", false},
		{"EXPLAIN ANALYZE DELETE FROM t", false},
		{"PRAGMA page_size=8192", false},
		{"PRAGMA collation('NFC')", false},
		{"PRAGMA future_setting", false},
		{"COMMIT DURABLE", false},
	} {
		t.Run(test.query, func(t *testing.T) {
			statements, err := Parse(test.query)
			if err != nil {
				t.Fatal(err)
			}
			if got := IsReplicaReadStatement(statements[0]); got != test.read {
				t.Fatalf("read classification=%t want=%t", got, test.read)
			}
		})
	}
}
