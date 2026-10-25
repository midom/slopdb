package postgresserver

import (
	"context"
	"fmt"
	"sync/atomic"
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

func TestPreparedPositionFramesAndParameterTypes(t *testing.T) {
	s := startTestServer(t, "tcp", Config{})
	var positionCalls, waitCalls atomic.Int64
	s.database.SetReplicationGuard(&db.ReplicationGuard{
		Check:         func(context.Context) error { return nil },
		AckedPosition: func(context.Context) (string, error) { positionCalls.Add(1); return "current-position", nil },
		WaitPosition: func(_ context.Context, position string) error {
			waitCalls.Add(1)
			if position != "visible-position" {
				return fmt.Errorf("unexpected position %q", position)
			}
			return nil
		},
	})
	c := connectRaw(t, s)
	for _, tc := range []struct {
		name, query string
		params      int
		value       string
	}{{"current", "SHOW REPLICATION POSITION", 0, "current-position"}, {"last", "SHOW LAST COMMIT POSITION", 0, ""}, {"wait", "WAIT FOR POSITION $1 TIMEOUT $2", 2, ""}} {
		beforePosition, beforeWait := positionCalls.Load(), waitCalls.Load()
		c.send(t, &pgproto3.Parse{Name: tc.name, Query: tc.query}, &pgproto3.Describe{ObjectType: 'S', Name: tc.name}, &pgproto3.Flush{})
		receive[*pgproto3.ParseComplete](t, c)
		parameters := receive[*pgproto3.ParameterDescription](t, c)
		if len(parameters.ParameterOIDs) != tc.params {
			t.Fatal(tc.name, parameters)
		}
		for _, oid := range parameters.ParameterOIDs {
			if oid != 25 {
				t.Fatal(tc.name, "parameter must be TEXT", parameters)
			}
		}
		if tc.params == 0 {
			fields := receive[*pgproto3.RowDescription](t, c)
			if len(fields.Fields) != 1 || string(fields.Fields[0].Name) != "Position" || fields.Fields[0].DataTypeOID != 25 {
				t.Fatal(tc.name, fields)
			}
		} else {
			receive[*pgproto3.NoData](t, c)
		}
		if positionCalls.Load() != beforePosition || waitCalls.Load() != beforeWait {
			t.Fatal("Parse/Describe executed a position command")
		}
		bind := &pgproto3.Bind{PreparedStatement: tc.name}
		if tc.params != 0 {
			bind.Parameters = [][]byte{[]byte("visible-position"), []byte("1s")}
		}
		c.send(t, bind, &pgproto3.Describe{ObjectType: 'P'}, &pgproto3.Execute{}, &pgproto3.Sync{})
		receive[*pgproto3.BindComplete](t, c)
		if tc.params == 0 {
			fields := receive[*pgproto3.RowDescription](t, c)
			if len(fields.Fields) != 1 || fields.Fields[0].DataTypeOID != 25 {
				t.Fatal(tc.name, fields)
			}
			row := receive[*pgproto3.DataRow](t, c)
			if len(row.Values) != 1 || tc.name == "last" && row.Values[0] != nil || tc.name != "last" && string(row.Values[0]) != tc.value {
				t.Fatal(tc.name, row)
			}
		} else {
			receive[*pgproto3.NoData](t, c)
		}
		receive[*pgproto3.CommandComplete](t, c)
		receive[*pgproto3.ReadyForQuery](t, c)
	}
	if positionCalls.Load() != 1 || waitCalls.Load() != 1 {
		t.Fatal(positionCalls.Load(), waitCalls.Load())
	}
}
