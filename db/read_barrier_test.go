package db

import (
	"context"
	"testing"
)

func TestDurableReadBarrierRequiresReplication(t *testing.T) {
	database, err := Open(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	for _, mode := range []ReadBarrier{ReadBarrierPrevious, ReadBarrierNext, ReadBarrier("invalid")} {
		if _, err = database.ExecWithOptions("SELECT 1", ExecOptions{ReadBarrier: mode}); err == nil {
			t.Fatalf("standalone ignored %s", mode)
		}
		if _, err = database.QueryContext(WithReadBarrier(context.Background(), mode), "SELECT 1"); err == nil {
			t.Fatalf("stream ignored %s", mode)
		}
	}
	for _, query := range []string{"SET READ BARRIER PREVIOUS", "SET READ BARRIER NEXT"} {
		if _, err = database.Exec(query); err == nil {
			t.Fatalf("standalone accepted %q", query)
		}
	}
}
