package regression

import (
	"strings"
	"testing"
)

func TestExplainIndexPublicationPolicy(t *testing.T) {
	env := newRegressionEnv(t)
	d, err := env.openDatabase(env.path("index_publication_plans"), nil)
	if err != nil {
		t.Fatal(err)
	}
	requireExec(t, d, "CREATE TABLE t(id INTEGER PRIMARY KEY, value TEXT)")
	requireExec(t, d, "CREATE INDEX existing ON t(value)")
	for _, query := range []string{
		"CREATE INDEX planned ON t(value)",
		"DROP INDEX existing",
		"ALTER TABLE t ADD INDEX planned(value)",
		"ALTER TABLE t DROP INDEX existing",
	} {
		result := requireExec(t, d, "EXPLAIN "+query+" KILL TRANSACTIONS AFTER '250ms'")
		if len(result.Rows) != 1 || !strings.Contains(result.Rows[0]["locking"].Text, "abort blocking transactions after 250ms") || !strings.Contains(result.Rows[0]["detail"].Text, "rolled back") {
			t.Fatalf("%s: missing abort policy in plan: %v", query, result.Rows)
		}
	}
	if d.GetIndexSQL("planned") != "" || d.GetIndexSQL("existing") == "" {
		t.Fatal("publication explanation changed indexes")
	}
	requireExec(t, d, "BEGIN")
	for _, query := range []string{
		"EXPLAIN CREATE INDEX planned ON t(value) KILL TRANSACTIONS AFTER '0s'",
		"EXPLAIN DROP INDEX existing KILL TRANSACTIONS AFTER '0s'",
	} {
		if _, err := d.Exec(query); err == nil {
			t.Fatalf("transactional explanation accepted cancellation: %s", query)
		}
	}
	requireExec(t, d, "ROLLBACK")
}
