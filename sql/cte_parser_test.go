package sql

import (
	"strings"
	"testing"
)

func TestCTEFormatRoundTrip(t *testing.T) {
	for _, query := range []string{
		"WITH q(n) AS MATERIALIZED (SELECT 1 UNION ALL SELECT 2), r(v) AS (SELECT n+1 FROM q) SELECT v FROM r ORDER BY v DESC LIMIT 9223372036854775807 OFFSET 4294967297",
		"WITH RECURSIVE q(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM q WHERE n<4) SELECT n FROM q",
		"WITH q(n) AS (SELECT 1) SELECT n FROM q UNION ALL SELECT n+1 FROM q ORDER BY n DESC LIMIT 8 OFFSET 2",
		"SELECT * FROM (WITH q(n) AS (SELECT 1 UNION ALL SELECT 2) SELECT n FROM q) a",
		"SELECT * FROM (SELECT 1 AS n UNION ALL SELECT 2 ORDER BY n DESC LIMIT 2) a",
	} {
		parsed, err := Parse(query)
		if err != nil {
			t.Fatal(err)
		}
		formatted := formatCTEQuery(parsed[0])
		again, err := Parse(formatted)
		if err != nil {
			t.Fatalf("%s: %v", formatted, err)
		}
		if second := formatCTEQuery(again[0]); formatted != second {
			t.Fatalf("unstable format:\n%s\n%s", formatted, second)
		}
	}
}
func TestCTERejectsNonQueryContexts(t *testing.T) {
	for _, query := range []string{
		"WITH q AS (WITH x AS (SELECT 1) INSERT INTO dst SELECT * FROM x) SELECT * FROM q",
		"SELECT (WITH x AS (SELECT 1) DELETE FROM dst)",
		"CREATE VIEW v AS WITH x AS (SELECT 1) UPDATE dst SET n=1",
		"WITH x AS (SELECT 1) WITH y AS (SELECT 2) SELECT * FROM y",
	} {
		if _, err := Parse(query); err == nil {
			t.Fatalf("accepted non-query %s", query)
		}
	}
	prepared, err := Prepare("CREATE VIEW v AS WITH q(n) AS (SELECT ?) SELECT n FROM q")
	if err == nil {
		t.Fatalf("schema parameters accepted: %+v", prepared)
	}
	if !strings.Contains(err.Error(), "schema definitions") {
		t.Fatal(err)
	}
}
