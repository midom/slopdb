package db

import (
	"context"
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
