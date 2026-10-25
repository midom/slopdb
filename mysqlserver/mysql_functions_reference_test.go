package mysqlserver

import (
	gosql "database/sql"
	"encoding/json"
	"math"
	"os"
	"strconv"
	"testing"
)

// Expected answers originate only from the independent Oracle MySQL fixture.
// Both protocols execute the same expressions against an isolated native LSM.
func TestMySQLFunctionReferenceAnswers(t *testing.T) {
	var fixture struct {
		Source map[string]string
		Cases  []struct {
			Expression string
			Kind       string
			Value      *string
			Error      bool
			ErrorCode  int `json:"error_code"`
		}
	}
	data, err := os.ReadFile("testdata/mysql-functions-mysql8.json")
	if err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal(data, &fixture); err != nil {
		t.Fatal(err)
	}
	if fixture.Source["engine"] != "Oracle MySQL" || fixture.Source["time_zone"] != "+00:00" || fixture.Source["collation"] != "utf8mb4_bin" || len(fixture.Cases) < 200 {
		t.Fatal("missing independent function reference coverage")
	}
	server := startTestServer(t, "tcp", Config{})
	client := server.open(t, "slopdb", "", "slopdb")
	for _, reference := range fixture.Cases {
		reference := reference
		t.Run(reference.Expression, func(t *testing.T) {
			query := "SELECT " + reference.Expression + " AS answer"
			check := func(mode string, row *gosql.Row) {
				t.Helper()
				var got gosql.NullString
				err := row.Scan(&got)
				if reference.Error {
					if err == nil {
						t.Errorf("%s accepted invalid MySQL expression; source error %d", mode, reference.ErrorCode)
					}
					return
				}
				if err != nil {
					t.Fatalf("%s: %v", mode, err)
				}
				if reference.Value == nil {
					if got.Valid {
						t.Errorf("%s got %q, want NULL", mode, got.String)
					}
					return
				}
				if !got.Valid {
					t.Fatalf("%s got NULL, want %q", mode, *reference.Value)
				}
				if reference.Kind == "DOUBLE" {
					actual, err := strconv.ParseFloat(got.String, 64)
					expected, refErr := strconv.ParseFloat(*reference.Value, 64)
					if err != nil || refErr != nil || math.IsNaN(actual) || math.Abs(actual-expected) > 1e-12*math.Max(1, math.Abs(expected)) {
						t.Errorf("%s got %q, want %q", mode, got.String, *reference.Value)
					}
					return
				}
				if got.String != *reference.Value {
					t.Errorf("%s got %q, want %q (%s)", mode, got.String, *reference.Value, reference.Kind)
				}
			}
			check("text", client.QueryRow(query))
			prepared, err := client.Prepare(query)
			if err != nil {
				if reference.Error {
					return
				}
				t.Fatalf("prepare: %v", err)
			}
			defer prepared.Close()
			check("prepared", prepared.QueryRow())
		})
	}
}
