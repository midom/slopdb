package db

import (
	"context"
	"slopdb/sql"
	"testing"
)

func TestActivityScopeReusesRegistrationWithoutAllocating(t *testing.T) {
	d, err := Open(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = d.Close() })
	ctx := context.Background()
	const query = "SELECT id,v,payload FROM small_rows WHERE id=?"
	if n := testing.AllocsPerRun(1000, func() { d.beginActivity(d.schema, ctx, query).end() }); n != 0 {
		t.Fatalf("activity scope allocated %g objects per query", n)
	}
	records, err := d.Activity(ctx)
	if err != nil || len(records) != 1 || records[0].Query != query || records[0].State != "idle" {
		t.Fatalf("scope state: %+v, %v", records, err)
	}
	var missing *activityRecord
	missing.end()
}

func TestActivityNativeFactsInvalidateProtocolTransactionReports(t *testing.T) {
	d, err := Open(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = d.Close() })
	s, err := d.NewSession()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	execute := func(query string) {
		t.Helper()
		if _, err := s.Exec(query); err != nil {
			t.Fatal(err)
		}
	}
	current := func() sql.SessionActivity {
		t.Helper()
		records, err := d.Activity(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		for _, record := range records {
			if record.SessionID == s.SessionID() {
				return record
			}
		}
		t.Fatal("session missing")
		return sql.SessionActivity{}
	}
	execute("BEGIN")
	execute("SELECT 1")
	first := current()
	if first.State != sql.ActivityIdleInTransaction || first.TransactionStart.IsZero() {
		t.Fatal(first)
	}
	s.SetActivityTransactionState(false, false)
	execute("SELECT 1")
	if record := current(); record.State != sql.ActivityIdleInTransaction || record.TransactionStart.IsZero() {
		t.Fatal("native facts did not supersede protocol report", record)
	}
	execute("ROLLBACK")
	execute("SELECT 1")
	s.SetActivityTransactionState(true, true)
	if record := current(); record.State != sql.ActivityIdleInTransactionAborted {
		t.Fatal(record)
	}
	execute("SELECT 1")
	if record := current(); record.State != sql.ActivityIdle || !record.TransactionStart.IsZero() {
		t.Fatal("stale protocol report survived native execution", record)
	}
}
