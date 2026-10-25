package db_test

import (
	"encoding/json"
	"os"
	"reflect"
	"slopdb/db"
	"strings"
	"testing"
)

// Answers come from the independent MySQL8 server, never from SlopDB.
func TestMySQLExtraFunctionReference(t *testing.T) {
	var fixture struct {
		Source string
		Cases  []struct {
			SQL         string
			Value       *string
			Error, JSON bool
		}
	}
	data, e := os.ReadFile("testdata/mysql_extra_functions.json")
	if e != nil {
		t.Fatal(e)
	}
	if e = json.Unmarshal(data, &fixture); e != nil {
		t.Fatal(e)
	}
	d, e := db.Open(t.TempDir(), nil)
	if e != nil {
		t.Fatal(e)
	}
	defer d.Close()
	s, e := d.NewSession()
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	s.SetMySQLMode(true)
	for _, tc := range fixture.Cases {
		for _, prepared := range []bool{false, true} {
			label := tc.SQL
			if prepared {
				label += "/prepared"
			}
			t.Run(label, func(t *testing.T) {
				q := "SELECT " + tc.SQL + " AS v"
				var r *db.ExecResult
				var err error
				if prepared {
					p, e := s.Prepare(q)
					err = e
					if err == nil {
						r, err = p.Exec()
						p.Close()
					}
				} else {
					r, err = s.Exec(q)
				}
				if tc.Error {
					if err == nil {
						t.Fatal("expected reference error")
					}
					return
				}
				if err != nil {
					t.Fatal(err)
				}
				v := r.Rows[0]["v"]
				if tc.Value == nil {
					if !v.Null {
						t.Fatal("expected NULL", v)
					}
					return
				}
				if v.Null {
					t.Fatal("unexpected NULL")
				}
				if tc.JSON {
					decode := func(text string) any {
						t.Helper()
						decoder := json.NewDecoder(strings.NewReader(text))
						decoder.UseNumber()
						var x any
						if e := decoder.Decode(&x); e != nil {
							t.Fatal(e)
						}
						return x
					}
					if !reflect.DeepEqual(decode(v.String()), decode(*tc.Value)) {
						t.Fatalf("got %s, want %s from %s", v.String(), *tc.Value, fixture.Source)
					}
				} else if v.String() != *tc.Value {
					t.Fatalf("got %q want %q from %s", v.String(), *tc.Value, fixture.Source)
				}
			})
		}
	}
}
