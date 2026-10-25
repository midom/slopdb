package db

import (
	"context"
	"testing"

	"slopdb/sql"
)

func TestCatalogProjectionsShareNativeFacts(t *testing.T) {
	d, _ := bulkDatabase(t)
	s, e := d.NewSession()
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	if e = s.SetDialect(sql.DialectPostgreSQL); e != nil {
		t.Fatal(e)
	}
	ctx := sql.WithPostgresSession(context.Background(), sql.PostgresSessionOptions{Database: "catalog"})
	exec := func(q string) *ExecResult {
		t.Helper()
		r, e := s.ExecContext(ctx, q)
		if e != nil {
			t.Fatalf("%s: %v", q, e)
		}
		return r
	}
	exec("CREATE SCHEMA tenant;CREATE TABLE tenant.fact(id BIGINT PRIMARY KEY,label VARCHAR(10) DEFAULT 'x',CONSTRAINT label_valid CHECK(label<>''));CREATE INDEX fact_label ON tenant.fact(label);CREATE TRIGGER tenant.fact_insert AFTER INSERT ON tenant.fact BEGIN SELECT 1; END")
	r := exec("SELECT slopdb_table_id FROM information_schema.tables WHERE table_schema='tenant' AND table_name='fact'")
	id := r.Rows[0]["slopdb_table_id"].Uint
	r = exec("SELECT oid,relnatts,relchecks,relhastriggers FROM pg_class WHERE relname='fact'")
	if len(r.Rows) != 1 || r.Rows[0]["oid"].Int != int64(id)+16384 || r.Rows[0]["relnatts"].Int != 2 || r.Rows[0]["relchecks"].Int != 1 || r.Rows[0]["relhastriggers"].Int != 1 {
		t.Fatalf("class facts: %+v", r)
	}
	r = exec("SELECT a.attname,c.column_name FROM pg_attribute a JOIN pg_class r ON a.attrelid=r.oid JOIN information_schema.columns c ON c.ordinal_position=a.attnum WHERE r.relname='fact' AND c.table_schema='tenant' AND c.table_name='fact' ORDER BY a.attnum")
	if len(r.Rows) != 2 {
		t.Fatal(r)
	}
	for _, row := range r.Rows {
		if row["attname"].Text != row["column_name"].Text {
			t.Fatal(row)
		}
	}
	r = exec("SELECT pg_get_expr(d.adbin,d.adrelid) AS expression FROM pg_attrdef d JOIN pg_class r ON r.oid=d.adrelid WHERE r.relname='fact'")
	if len(r.Rows) != 1 || r.Rows[0]["expression"].Text != "'x'" {
		t.Fatal(r)
	}
	r = exec("SELECT i.slopdb_index_id AS native_id,c.oid AS oid FROM information_schema.statistics i JOIN pg_class c ON i.index_name=c.relname WHERE i.table_schema='tenant' AND i.table_name='fact' AND i.index_name='fact_label'")
	if len(r.Rows) != 1 || int64(r.Rows[0]["native_id"].Uint)+16384 != r.Rows[0]["oid"].Int {
		t.Fatal(r)
	}
	exec("CREATE SCHEMA other;CREATE TABLE other.referrer(id BIGINT REFERENCES tenant.fact(id))")
	r = exec("SELECT table_schema,table_name,column_name FROM information_schema.constraint_column_usage WHERE constraint_schema='other'")
	if len(r.Rows) != 1 || r.Rows[0]["table_schema"].Text != "tenant" || r.Rows[0]["table_name"].Text != "fact" {
		t.Fatalf("cross-schema constraint projection: %+v", r)
	}
	r = exec("SELECT tableowner FROM pg_tables WHERE schemaname='tenant' AND tablename='fact'")
	if len(r.Rows) != 1 || !r.Rows[0]["tableowner"].Null {
		t.Fatal("unknown ownership must remain NULL", r)
	}
}
