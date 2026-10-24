package db

import (
	"context"
	"testing"
)

func TestStandaloneRejectsReplicationPositionRequirements(t *testing.T) {
	database, err := Open(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	for _, query := range []string{"SHOW REPLICATION POSITION", "WAIT FOR POSITION 'token'", "WAIT FOR POSITION NULL", "WAIT FOR POSITION 1", "WAIT FOR POSITION 'token' TIMEOUT '-1s'", "WAIT FOR POSITION 'token' TIMEOUT 2"} {
		if _, err := database.Exec(query); err == nil {
			t.Fatalf("standalone accepted %q", query)
		}
	}
	if _, err := database.ExecWithOptions("SELECT 1", ExecOptions{MinimumPosition: "token"}); err == nil {
		t.Fatal("standalone ignored minimum position")
	}
	if _, err := database.QueryContext(WithMinimumPosition(context.Background(), "token"), "SELECT 1"); err == nil {
		t.Fatal("standalone stream ignored minimum position")
	}
	result, err := database.Exec("SHOW LAST COMMIT POSITION")
	if err != nil || !result.Rows[0]["Position"].Null {
		t.Fatalf("unexpected standalone commit position: %+v %v", result, err)
	}
}
