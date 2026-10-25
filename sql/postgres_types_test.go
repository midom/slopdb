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

func TestPostgresIntervalExactComponents(t *testing.T) {
	for _, c := range []struct {
		text string
		m, d int32
		u    int64
	}{
		{"1.5 months 2.5 days", 1, 17, 43200000000},
		{"1 ms 2 us", 0, 0, 1002},
		{"0.000001 seconds", 0, 0, 1},
		{"0.0000015 seconds", 0, 0, 1},
		{"0.0000025 seconds", 0, 0, 2},
		{"00:00:00.0000015", 0, 0, 2},
		{"0.1 year", 1, 0, 0},
		{"2562047788:00:54.775807", 0, 0, 9223372036854775807},
		{"-2562047788:00:54.775808", 0, 0, -9223372036854775808},
		{"1 month 2 days 03:04:05.123456 ago", -1, -2, -11045123456},
	} {
		m, d, u, e := ParsePostgresInterval(c.text)
		if e != nil || m != c.m || d != c.d || u != c.u {
			t.Errorf("%s => %d %d %d %v", c.text, m, d, u, e)
		}
		if e == nil {
			round := FormatPostgresInterval(m, d, u)
			m2, d2, u2, e := ParsePostgresInterval(round)
			if e != nil || m2 != m || d2 != d || u2 != u {
				t.Errorf("roundtrip %s: %d %d %d %v", round, m2, d2, u2, e)
			}
		}
	}
	for _, bad := range []string{"bad:01:02", "1 / 2 day", "1 nanosecond", "2147483648 months", "2562047788:00:54.775808"} {
		if _, _, _, e := ParsePostgresInterval(bad); e == nil {
			t.Errorf("accepted %s", bad)
		}
	}
}

func TestPostgresBinaryAndCharacterCasts(t *testing.T) {
	for _, input := range []string{`\x00ff5c`, `\000\377\\`} {
		v, e := CastPostgresValue(NewTextValue(input), "BYTEA")
		if e != nil || len(v.Blob) != 3 || v.Blob[0] != 0 || v.Blob[1] != 255 || v.Blob[2] != '\\' {
			t.Fatal(v, e)
		}
		text, e := CastPostgresValue(v, "TEXT")
		if e != nil || text.Text != `\x00ff5c` {
			t.Fatal(text, e)
		}
	}
	for _, input := range []string{`\x0`, `\xG0`, `\xA B`, `\777`, `\q`} {
		if _, e := ParsePostgresBytea(input); e == nil {
			t.Errorf("accepted %s", input)
		}
	}
	v, e := CastPostgresValue(NewTextValue("abc☺"), "VARCHAR(3)")
	if e != nil || v.Text != "abc" {
		t.Fatal(v, e)
	}
	v, e = CastPostgresValue(NewTextValue("abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghij☺"), "NAME")
	if e != nil || len(v.Text) > 63 {
		t.Fatal(v, e)
	}
	for _, bad := range []string{"-550e8400e29b41d4a716446655440000", "550e8400e29b41d4a716446655440000-", "550e8-400e29b41d4a716446655440000", "{550e8400-e29b-41d4-a716-446655440000"} {
		if _, e := CastPostgresValue(NewTextValue(bad), "UUID"); e == nil {
			t.Errorf("accepted UUID %s", bad)
		}
	}
}
