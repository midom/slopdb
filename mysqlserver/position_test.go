package mysqlserver

import (
	"context"
	"fmt"
	"testing"

	"slopdb/db"
)

func TestCommitPositionOKResponses(t *testing.T) {
	for _, tracking := range []bool{false, true} {
		t.Run(fmt.Sprint(tracking), func(t *testing.T) {
			s := startTestServer(t, "tcp", Config{})
			s.database.SetReplicationGuard(&db.ReplicationGuard{Check: func(context.Context) error { return nil }, Position: func() string { return "test-position" }})
			flags := uint32(0)
			if tracking {
				flags = clientSessionTrack
			}
			c := connectRawWithFlags(t, s, flags)
			command := func(query string) []byte { return c.command(t, append([]byte{3}, query...)) }
			command("CREATE TABLE items(id INTEGER PRIMARY KEY)")
			command("BEGIN")
			p := command("INSERT INTO items VALUES(1)")
			if len(p) < 7 || p[0] != 0 {
				t.Fatalf("insert: %x", p)
			}
			p = command("COMMIT")
			if len(p) < 7 || p[0] != 0 {
				t.Fatalf("commit: %x", p)
			}
			d := decoder{data: p[1:]}
			d.length()
			d.length()
			status := d.u16()
			d.u16()
			if tracking {
				if status&(1<<14) == 0 {
					t.Fatal("session-state changed flag missing")
				}
				if string(d.str()) != "slopdb_commit_position=test-position" {
					t.Fatal("commit info missing")
				}
				state := decoder{data: d.str()}
				if state.u8() != 0 {
					t.Fatal("wrong session state type")
				}
				variables := decoder{data: state.str()}
				if string(variables.str()) != "slopdb_commit_position" || string(variables.str()) != "test-position" || variables.err != nil || len(variables.data) != 0 || state.err != nil || len(state.data) != 0 || d.err != nil || len(d.data) != 0 {
					t.Fatalf("malformed state: %x", p)
				}
			} else if string(d.data) != "slopdb_commit_position=test-position" {
				t.Fatalf("plain OK commit info missing: %x", p)
			}
			command("BEGIN")
			p = command("ROLLBACK")
			if len(p) > 8 {
				t.Fatalf("rollback emitted commit metadata: %x", p)
			}
			command("BEGIN")
			p = command("INSERT INTO items VALUES(1)")
			if p[0] != 0xff {
				t.Fatal("expected duplicate insert rejection")
			}
			command("ROLLBACK")
		})
	}
}
