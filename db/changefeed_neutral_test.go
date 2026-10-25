package db

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
)

func TestNeutralChangefeedAndLegacyCDCCompatibility(t *testing.T) {
	path := t.TempDir()
	d, err := Open(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	ctx := context.Background()
	if _, err = d.Exec("CREATE TABLE changes(id INTEGER PRIMARY KEY,n DECIMAL(12,3),j JSON);CREATE UNIQUE INDEX changes_n ON changes(n)"); err != nil {
		t.Fatal(err)
	}
	metadata, err := d.EnableChangefeed(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(metadata.Tables) != 1 || metadata.Tables[0].Kind != "table" {
		t.Fatalf("neutral catalog %+v", metadata)
	}
	data, err := json.Marshal(metadata)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), "CreateSQL") {
		t.Fatal("neutral metadata carries dialect-rendered SQL")
	}
	legacy, err := d.CDCMetadata(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(legacy.Tables) != 1 || !strings.Contains(legacy.Tables[0].CreateSQL, "CREATE TABLE") {
		t.Fatalf("legacy DDL lost: %+v", legacy)
	}
	r, err := d.NewChangeReader(ctx, 0)
	if err != nil {
		t.Fatal(err)
	}
	old, err := d.NewCDCReader(ctx, 0)
	if err != nil {
		t.Fatal(err)
	}
	m := r.Metadata()
	m.Tables[0].Columns[0].Name = "mutated"
	m.Tables[0].Definition.Columns[0].Name = "mutated"
	m.Tables[0].Indexes[0].Column = "mutated"
	again := r.Metadata()
	if again.Tables[0].Columns[0].Name != "id" || again.Tables[0].Definition.Columns[0].Name != "id" || again.Tables[0].Indexes[0].Column == "mutated" {
		t.Fatal("neutral metadata aliases caller state")
	}
	if _, err = d.Exec(`INSERT INTO changes VALUES(1,12.345,'{"a":true}')`); err != nil {
		t.Fatal(err)
	}
	changes, err := r.Read(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	compat, err := old.Read(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(changes) != 1 || len(compat) != 1 || changes[0].Sequence != compat[0].Sequence || len(changes[0].Changes) != 1 || len(compat[0].Changes) != 1 {
		t.Fatalf("change readers diverged: %+v %+v", changes, compat)
	}
	row := changes[0].Changes[0]
	if row.Kind != "insert" || len(row.Before) != 0 || len(row.After) != 3 || row.After[1].Text != "12.345" {
		t.Fatalf("neutral row images: %+v", row)
	}
	if changes[0].Tables[0].ID != compat[0].Tables[0].ID || compat[0].Tables[0].CreateSQL == "" {
		t.Fatal("legacy row table conversion lost metadata")
	}
	if err = d.Close(); err != nil {
		t.Fatal(err)
	}
	d, err = Open(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	resumed, err := d.NewChangeReader(ctx, r.Position())
	if err != nil {
		t.Fatal(err)
	}
	rest, err := resumed.Read(ctx, 10)
	if err != nil || len(rest) != 0 {
		t.Fatalf("neutral durable resume: %+v %v", rest, err)
	}
}
