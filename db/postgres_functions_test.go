package db_test

import (
	"encoding/hex"
	"encoding/json"
	"os"
	"slopdb/db"
	"slopdb/sql"
	"testing"
)

func TestPostgresFunctionReference(t *testing.T) {
	data, err := os.ReadFile("testdata/postgres_functions.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixture struct {
		Source string
		Cases  []struct {
			SQL   string
			Value *string
			Error bool
		}
	}
	if err = json.Unmarshal(data, &fixture); err != nil {
		t.Fatal(err)
	}
	d, err := db.Open(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	s, err := d.NewSession()
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	s.SetPostgresMode(true)
	for _, tc := range fixture.Cases {
		t.Run(tc.SQL, func(t *testing.T) {
			r, err := s.Exec("SELECT " + tc.SQL + " AS v")
			if tc.Error {
				if err == nil {
					t.Fatal("expected PostgreSQL error")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			v := r.Rows[0]["v"]
			if tc.Value == nil {
				if !v.Null {
					t.Fatalf("got %v, want NULL", v)
				}
				return
			}
			got := v.String()
			if v.Type == sql.ColBlob {
				got = "\\x" + hex.EncodeToString(v.Blob)
			}
			if v.Null || got != *tc.Value {
				t.Fatalf("got %q (type %v), want %q from %s", got, v.Type, *tc.Value, fixture.Source)
			}
		})
	}
}
