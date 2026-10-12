package cli

import (
	"strings"
	"testing"
	"time"

	"slopdb/db"
	"slopdb/sql"
)

func TestFormatLockWaits(t *testing.T) {
	if got := formatLockWaits(nil); got != "No active lock waits.\n" {
		t.Fatalf("empty lock diagnostic: %q", got)
	}
	got := formatLockWaits([]db.LockWaitInfo{{
		SessionID: 42, BlockingSessionIDs: []uint64{1, 7},
		ResourceKind: "unique", Resource: "name\nwith\ttabs", Exclusive: true,
		Values: []sql.Value{sql.NewTextValue("line\nwith\ttabs")}, WaitDuration: 125 * time.Millisecond,
	}})
	for _, want := range []string{"Session", "Blockers", "42", "1,7", "unique", "exclusive", "125ms", `"name\nwith\ttabs"`, `"line\nwith\ttabs"`} {
		if !strings.Contains(got, want) {
			t.Fatalf("missing %q in %s", want, got)
		}
	}
	if strings.Count(got, "\n") != 2 {
		t.Fatalf("resource or values broke diagnostic rows: %q", got)
	}
}
