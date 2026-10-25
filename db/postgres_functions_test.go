package db_test

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math"
	"os"
	"reflect"
	"slopdb/db"
	"slopdb/sql"
	"strconv"
	"strings"
	"testing"
	"time"
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
			JSON  bool
			Float bool
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
		for _, prepared := range []bool{false, true} {
			label := tc.SQL
			if prepared {
				label += "/prepared"
			}
			t.Run(label, func(t *testing.T) {
				var r *db.ExecResult
				var err error
				query := "SELECT " + tc.SQL + " AS v"
				if prepared {
					p, e := s.Prepare(query)
					err = e
					if err == nil {
						r, err = p.Exec()
						_ = p.Close()
					}
				} else {
					r, err = s.Exec(query)
				}
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
				if tc.JSON && !v.Null {
					var a, b any
					decA := json.NewDecoder(strings.NewReader(got))
					decA.UseNumber()
					decB := json.NewDecoder(strings.NewReader(*tc.Value))
					decB.UseNumber()
					if decA.Decode(&a) != nil || decB.Decode(&b) != nil || !reflect.DeepEqual(a, b) {
						t.Fatalf("JSON %s, want %s", got, *tc.Value)
					}
					return
				}
				if tc.Float && !v.Null {
					a, e1 := strconv.ParseFloat(got, 64)
					b, e2 := strconv.ParseFloat(*tc.Value, 64)
					if e1 != nil || e2 != nil || math.Abs(a-b) > 1e-12*math.Max(1, math.Abs(b)) {
						t.Fatalf("float %s, want %s", got, *tc.Value)
					}
					return
				}
				if v.Null || got != *tc.Value {
					t.Fatalf("got %q (type %v), want %q from %s", got, v.Type, *tc.Value, fixture.Source)
				}
			})
		}
	}
}

func TestPostgresClockSnapshotsAndCancellation(t *testing.T) {
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
	exec := func(q string) *db.ExecResult {
		t.Helper()
		r, e := s.Exec(q)
		if e != nil {
			t.Fatal(e)
		}
		return r
	}
	exec("BEGIN")
	r := exec("SELECT now() AS n, transaction_timestamp() AS t, statement_timestamp() AS s, clock_timestamp() AS c")
	initial := r.Rows[0]["n"].Text
	if initial == "" || r.Rows[0]["t"].Text != initial {
		t.Fatal(r.Rows)
	}
	exec("SELECT pg_sleep(0.001)")
	r = exec("SELECT now() AS n,statement_timestamp() AS s")
	if r.Rows[0]["n"].Text != initial || r.Rows[0]["s"].Text <= initial {
		t.Fatal(r.Rows)
	}
	exec("ROLLBACK")
	r = exec("SELECT now() AS n,statement_timestamp() AS s")
	if r.Rows[0]["n"].Text == initial || r.Rows[0]["n"].Text != r.Rows[0]["s"].Text {
		t.Fatal(r.Rows)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if _, e := s.ExecContext(ctx, "SELECT pg_sleep(10)"); !errors.Is(e, context.DeadlineExceeded) {
		t.Fatal(e)
	}
	exec("SELECT 1")
}

func TestPostgresBareSessionKeywordsPreserveNativeColumns(t *testing.T) {
	d, e := db.Open(t.TempDir(), nil)
	if e != nil {
		t.Fatal(e)
	}
	defer d.Close()
	for _, mysql := range []bool{false, true} {
		s, e := d.NewSession()
		if e != nil {
			t.Fatal(e)
		}
		s.SetMySQLMode(mysql)
		if _, e = s.Exec("CREATE TABLE IF NOT EXISTS bare_keywords(id INTEGER PRIMARY KEY,user TEXT,current_schema TEXT)"); e != nil {
			t.Fatal(e)
		}
		if _, e = s.Exec("REPLACE INTO bare_keywords VALUES(1,'alice','local')"); e != nil {
			t.Fatal(e)
		}
		p, e := s.Prepare("SELECT user AS u,current_schema AS s FROM bare_keywords WHERE id=?")
		if e != nil {
			t.Fatal(e)
		}
		r, e := p.Exec(int64(1))
		if e != nil || r.Rows[0]["u"].Text != "alice" || r.Rows[0]["s"].Text != "local" {
			t.Fatal(mysql, r, e)
		}
		p.Close()
		s.Close()
	}
	s, e := d.NewSession()
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	s.SetPostgresMode(true)
	ctx := sql.WithPostgresSession(context.Background(), sql.PostgresSessionOptions{Database: "demo", User: "alice"})
	r, e := s.ExecContext(ctx, "SELECT current_user AS u,current_database AS d,current_schema AS s")
	if e != nil || r.Rows[0]["u"].Text != "alice" || r.Rows[0]["d"].Text != "demo" || r.Rows[0]["s"].Text != "public" {
		t.Fatal(r, e)
	}
}

func TestPostgresFunctionArityOnEmptyInputs(t *testing.T) {
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
	s.SetPostgresMode(true)
	if _, e = s.Exec("CREATE TABLE arity_input(id INTEGER PRIMARY KEY)"); e != nil {
		t.Fatal(e)
	}
	for _, expr := range []string{"CONCAT_WS(',')", "JSON_BUILD_OBJECT('a')", "REGEXP_SUBSTR('a','a',1,1,'',1,1)"} {
		if _, e = s.Exec("SELECT " + expr + " FROM arity_input"); e == nil {
			t.Fatal("accepted invalid PostgreSQL arity", expr)
		}
	}
}
