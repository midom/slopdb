package sql

import "testing"

func TestReplicationPositionStatements(t *testing.T) {
	for query, operation := range map[string]string{
		"SHOW REPLICATION POSITION": "current", "SHOW LAST COMMIT POSITION;": "last",
		"WAIT FOR POSITION 'token'": "wait", "wait for position 'token' timeout '2s';": "wait",
	} {
		statements, err := Parse(query)
		if err != nil || len(statements) != 1 {
			t.Fatalf("%s: %v", query, err)
		}
		stmt, ok := statements[0].(*ReplicationPositionStmt)
		if !ok || stmt.Operation != operation {
			t.Fatalf("%s: %#v", query, statements)
		}
	}
	query, err := Prepare("WAIT FOR POSITION ? TIMEOUT ?")
	if err != nil {
		t.Fatal(err)
	}
	bound, err := query.Bind([]Value{NewTextValue("a"), NewTextValue("1s")})
	if err != nil {
		t.Fatal(err)
	}
	stmt := bound.Statements()[0].(*ReplicationPositionStmt)
	if stmt.Position.(*LiteralExpr).Value.Text != "a" || stmt.Timeout.(*LiteralExpr).Value.Text != "1s" {
		t.Fatal("position arguments not bound")
	}
	bound.Release()
	for _, query := range []string{"WAIT POSITION 'x'", "WAIT FOR 'x'", "WAIT FOR POSITION", "WAIT FOR POSITION 'x' TIMEOUT", "SHOW REPLICATION", "SHOW LAST POSITION", "SHOW LAST COMMIT", "SHOW FULL REPLICATION POSITION"} {
		if _, err := Parse(query); err == nil {
			t.Fatalf("accepted malformed %q", query)
		}
	}
	// Contextual command words remain available as identifiers.
	if _, err := Parse("CREATE TABLE positions(wait TEXT, position TEXT, replication TEXT)"); err != nil {
		t.Fatal(err)
	}
}
