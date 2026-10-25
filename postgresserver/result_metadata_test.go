package postgresserver

import (
	"context"
	"github.com/jackc/pgx/v5"
	"testing"
)

func TestSharedResultFrameNullsUnionsAndNumericPromotion(t *testing.T) {
	s := startTestServer(t, "tcp", Config{})
	ctx := context.Background()
	c, e := pgx.Connect(ctx, s.dsn("slopdb", "", "slopdb"))
	if e != nil {
		t.Fatal(e)
	}
	defer c.Close(ctx)
	if _, e = c.Exec(ctx, "CREATE TABLE frames(id BIGINT PRIMARY KEY,b BOOLEAN,r DOUBLE PRECISION);INSERT INTO frames VALUES(1,NULL,1.25)"); e != nil {
		t.Fatal(e)
	}
	for _, mode := range []pgx.QueryExecMode{pgx.QueryExecModeCacheStatement, pgx.QueryExecModeSimpleProtocol} {
		for _, tc := range []struct {
			q   string
			oid uint32
		}{
			{"SELECT CASE WHEN TRUE THEN NULL ELSE TRUE END", oidBool},
			{"SELECT CASE WHEN TRUE THEN NULL ELSE TRUE END FROM frames", oidBool},
			{"SELECT CASE WHEN TRUE THEN 1 ELSE 1.5::double precision END", oidFloat8},
			{"SELECT 1 AS v UNION ALL SELECT 'text' AS v", oidText},
			{"UPDATE frames SET r=r RETURNING CASE WHEN TRUE THEN NULL ELSE TRUE END", oidBool},
		} {
			rows, e := c.Query(ctx, tc.q, mode)
			if e != nil {
				t.Fatal(e)
			}
			if len(rows.FieldDescriptions()) != 1 || rows.FieldDescriptions()[0].DataTypeOID != tc.oid {
				t.Fatalf("mode%v %s fields%+v", mode, tc.q, rows.FieldDescriptions())
			}
			for rows.Next() {
				if _, e = rows.Values(); e != nil {
					t.Fatalf("%s: %v", tc.q, e)
				}
			}
			if rows.Err() != nil {
				t.Fatal(rows.Err())
			}
			rows.Close()
		}
	}
}
