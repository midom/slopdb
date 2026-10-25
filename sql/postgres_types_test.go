package sql

import "testing"

func TestPostgresTypeConversions(t *testing.T) {
	cases := []struct {
		input, target string
		oid           uint32
		text          string
	}{
		{"yes", "BOOLEAN", 16, "1"}, {"2147483647", "INTEGER", 23, "2147483647"}, {"32767", "SMALLINT", 21, "32767"},
		{"550e8400-e29b-41d4-a716-446655440000", "UUID", 2950, ""}, {"2026-10-25", "DATE", 1082, "2026-10-25"},
		{"2026-10-25 10:20:30.123456+02", "TIMESTAMPTZ", 1184, "2026-10-25 08:20:30.123456+00"},
	}
	for _, c := range cases {
		t.Run(c.target, func(t *testing.T) {
			v, e := CastPostgresValue(NewTextValue(c.input), c.target)
			if e != nil {
				t.Fatal(e)
			}
			if PostgresValueOID(v) != c.oid {
				t.Fatalf("OID=%d", PostgresValueOID(v))
			}
			if c.text != "" && v.String() != c.text {
				t.Fatalf("got %s", v.String())
			}
		})
	}
	for _, c := range [][2]string{{"2147483648", "INTEGER"}, {"32768", "SMALLINT"}, {"maybe", "BOOLEAN"}, {"not-a-uuid", "UUID"}, {"2026-02-30", "DATE"}} {
		if _, e := CastPostgresValue(NewTextValue(c[0]), c[1]); e == nil {
			t.Errorf("accepted %q as %s", c[0], c[1])
		}
	}
}

func TestPostgresParserExtensions(t *testing.T) {
	for _, query := range []string{
		`SELECT OVERLAY('abcdef' PLACING 'xy' FROM 2 FOR 3)`,
		`SELECT CURRENT_DATABASE, CURRENT_SCHEMA, CURRENT_USER, SESSION_USER, USER`,
		`SELECT pg_catalog.lower('ABC'), 'a' ~* 'A', 'a' !~ 'z', 'a' !~* 'Z'`,
		`SELECT DATE '2026-10-25', TIMESTAMP '2026-10-25 12:00:00', INTERVAL '1 day'`,
		`SELECT ('5'::int2)::pg_catalog.int4::text, CAST('12:34:56' AS TIME(0) WITH TIME ZONE)`,
	} {
		if _, err := Parse(query); err != nil {
			t.Errorf("%s: %v", query, err)
		}
	}
	for _, query := range []string{`SELECT E'\u0000'`, `SELECT E'\xFF'`, `SELECT $tag$unterminated`, `SELECT 1::`, `SELECT OVERLAY('x' PLACING 'y' FROM)`} {
		if _, err := Parse(query); err == nil {
			t.Errorf("accepted %s", query)
		}
	}
}
