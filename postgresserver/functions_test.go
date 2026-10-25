package postgresserver

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

func TestPostgresFunctionTypedMetadata(t *testing.T) {
	s := startTestServer(t, "tcp", Config{})
	ctx := context.Background()
	c, e := pgx.Connect(ctx, s.dsn("slopdb", "", "slopdb"))
	if e != nil {
		t.Fatal(e)
	}
	defer c.Close(ctx)
	for _, mode := range []pgx.QueryExecMode{pgx.QueryExecModeCacheStatement, pgx.QueryExecModeSimpleProtocol} {
		rows, e := c.Query(ctx, `SELECT jsonb_build_object('n',42) AS doc,json_typeof('null') AS kind,date_trunc('day',CAST('2024-02-29 12:34:56' AS TIMESTAMP)) AS ts,now() AS tz,make_date(2024,2,29) AS d,make_time(12,34,56) AS tm,char_length('é') AS n,scale(CAST('1.20' AS NUMERIC)) AS scale`, mode)
		if e != nil {
			t.Fatal(e)
		}
		want := []uint32{oidJSONB, oidText, oidTimestamp, oidTimestamptz, oidDate, oidTime, oidInt4, oidInt8}
		for i, f := range rows.FieldDescriptions() {
			if f.DataTypeOID != want[i] {
				t.Fatalf("mode %v field %s OID %d want %d", mode, f.Name, f.DataTypeOID, want[i])
			}
		}
		if !rows.Next() {
			t.Fatal(rows.Err())
		}
		values, e := rows.Values()
		if e != nil {
			t.Fatal(e)
		}
		rows.Close()
		if values[1] != "null" || !values[2].(time.Time).Equal(time.Date(2024, 2, 29, 0, 0, 0, 0, time.UTC)) || values[6] != int32(1) {
			t.Fatal(values)
		}
		if !values[3].(time.Time).After(time.Now().Add(-time.Minute)) {
			t.Fatal(values[3])
		}
	}
	var padded string
	if e = c.QueryRow(ctx, `SELECT lpad($1,5,'x')`, "é").Scan(&padded); e != nil || padded != "xxxxé" {
		t.Fatal(padded, e)
	}
}

func TestPostgresPredicateTypesInBothProtocols(t *testing.T) {
	s := startTestServer(t, "tcp", Config{})
	ctx := context.Background()
	c, e := pgx.Connect(ctx, s.dsn("slopdb", "", "slopdb"))
	if e != nil {
		t.Fatal(e)
	}
	defer c.Close(ctx)
	if _, e = c.Exec(ctx, `CREATE TABLE predicate_input(id INTEGER PRIMARY KEY,v TEXT);INSERT INTO predicate_input VALUES(1,'Abc')`); e != nil {
		t.Fatal(e)
	}
	for _, mode := range []pgx.QueryExecMode{pgx.QueryExecModeCacheStatement, pgx.QueryExecModeSimpleProtocol} {
		for _, from := range []string{"", " FROM predicate_input", " FROM predicate_input WHERE 0"} {
			rows, e := c.Query(ctx, `SELECT 'Abc' ILIKE 'a%' AS a,NULL IS DISTINCT FROM NULL AS b,1 IN (1,2) AS c,1 BETWEEN 0 AND 2 AS d,NOT false AS e,EXISTS(SELECT 1) AS f,NULL=1 AS g`+from, mode)
			if e != nil {
				t.Fatal(e)
			}
			for _, f := range rows.FieldDescriptions() {
				if f.DataTypeOID != oidBool {
					t.Fatalf("mode %v from %q field %s OID %d", mode, from, f.Name, f.DataTypeOID)
				}
			}
			if strings.Contains(from, "WHERE 0") {
				if rows.Next() || rows.Err() != nil {
					t.Fatal(rows.Err())
				}
				rows.Close()
				continue
			}
			if !rows.Next() {
				t.Fatal(rows.Err())
			}
			values, e := rows.Values()
			if e != nil {
				t.Fatal(e)
			}
			rows.Close()
			want := []any{true, false, true, true, true, true, nil}
			for i, v := range values {
				if v != want[i] {
					t.Fatalf("mode %v values %#v", mode, values)
				}
			}
		}
	}
}
