package db

import (
	"context"
	"slopdb/sql"
	"strings"
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

func TestPreparedPositionDescriptionIsStaticAcrossDialects(t *testing.T) {
	database, err := Open(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	for _, dialect := range []sql.Dialect{sql.DialectNative, sql.DialectMySQL, sql.DialectPostgreSQL} {
		session, err := database.NewSession()
		if err != nil {
			t.Fatal(err)
		}
		if err = session.SetDialect(dialect); err != nil {
			t.Fatal(err)
		}
		for _, query := range []string{"SHOW REPLICATION POSITION", "SHOW LAST COMMIT POSITION", "WAIT FOR POSITION ? TIMEOUT ?"} {
			statement, err := session.Prepare(query)
			if err != nil {
				t.Fatal(query, err)
			}
			desc, err := session.DescribePreparedTypes(context.Background(), statement, sql.DescribeOptions{})
			statement.Close()
			if err != nil {
				t.Fatal(query, err)
			}
			if strings.HasPrefix(query, "WAIT") {
				if desc.Kind != "WAIT" || len(desc.Results) != 0 || len(desc.Parameters) != 2 || desc.Parameters[0].Kind != sql.TypeText || desc.Parameters[1].Kind != sql.TypeText {
					t.Fatal(dialect, query, desc)
				}
			} else if desc.Kind != "POSITION" || len(desc.Parameters) != 0 || len(desc.Results) != 1 || desc.Results[0].Name != "Position" || desc.Results[0].LogicalType().Kind != sql.TypeText {
				t.Fatal(dialect, query, desc)
			}
		}
		session.Close()
	}
	// No replication owner exists: successful descriptions above cannot have
	// queried positions or waited for a token as part of preparation.
}
