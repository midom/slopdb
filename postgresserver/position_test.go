package postgresserver

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgproto3"
	"slopdb/db"
)

func TestCommitPositionResponses(t *testing.T) {
	s := startTestServer(t, "tcp", Config{})
	s.database.SetReplicationGuard(&db.ReplicationGuard{Check: func(context.Context) error { return nil }, Position: func() string { return "test-position" }})
	c := connectRaw(t, s)
	check := func(messages []pgproto3.BackendMessage, want bool) {
		t.Helper()
		found := false
		for _, message := range messages {
			if status, ok := message.(*pgproto3.ParameterStatus); ok && status.Name == "slopdb_commit_position" {
				if status.Value != "test-position" {
					t.Fatal(status)
				}
				found = true
			}
			if failure, ok := message.(*pgproto3.ErrorResponse); ok {
				t.Fatal(failure)
			}
		}
		if found != want {
			t.Fatalf("commit position present=%t want=%t: %+v", found, want, messages)
		}
	}
	check(c.query(t, "CREATE TABLE items(id INTEGER PRIMARY KEY)"), true)
	check(c.query(t, "BEGIN"), false)
	check(c.query(t, "INSERT INTO items VALUES(1)"), false)
	check(c.query(t, "COMMIT"), true)
	check(c.query(t, "BEGIN"), false)
	check(c.query(t, "ROLLBACK"), false)
	check(c.query(t, "INSERT INTO items VALUES(2)"), true)
	// Extended protocol acknowledges its implicit commit at Sync, after the
	// Execute completion but before ReadyForQuery.
	c.send(t, &pgproto3.Parse{Query: "INSERT INTO items VALUES(3)"}, &pgproto3.Bind{}, &pgproto3.Execute{}, &pgproto3.Sync{})
	var messages []pgproto3.BackendMessage
	for {
		message, err := c.frontend.Receive()
		if err != nil {
			t.Fatal(err)
		}
		messages = append(messages, message)
		if _, ready := message.(*pgproto3.ReadyForQuery); ready {
			break
		}
	}
	check(messages, true)
	for i, message := range messages {
		if _, status := message.(*pgproto3.ParameterStatus); status {
			if i < 1 {
				t.Fatal("position preceded execution")
			}
			if _, complete := messages[i-1].(*pgproto3.CommandComplete); !complete {
				t.Fatal("position did not follow implicit commit")
			}
		}
	}
}
