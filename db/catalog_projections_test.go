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

func TestCatalogDeclaredNamesAndEffectiveCollations(t *testing.T) {
	d, _ := bulkDatabase(t)
	s, e := d.NewSession()
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	if e = s.SetDialect(sql.DialectMySQL); e != nil {
		t.Fatal(e)
	}
	ctx := sql.WithMySQLSession(context.Background(), sql.MySQLSessionOptions{Database: "compat", RootDatabase: "compat"})
	exec := func(q string) *ExecResult {
		t.Helper()
		r, e := s.ExecContext(ctx, q)
		if e != nil {
			t.Fatalf("%s: %v", q, e)
		}
		return r
	}
	exec("CREATE TABLE parent_names(ID BIGINT PRIMARY KEY);CREATE TABLE named(ID BIGINT PRIMARY KEY,ParentID BIGINT,Word VARCHAR(20),Exact VARCHAR(20) COLLATE utf8mb4_bin,KEY WordIndex(Word(7)),CONSTRAINT parent_fk FOREIGN KEY(ParentID) REFERENCES parent_names(ID)) COLLATE utf8mb4_unicode_520_ci")
	r := exec("SELECT column_name,collation_name FROM information_schema.columns WHERE table_schema='compat' AND table_name='named' ORDER BY ordinal_position")
	if len(r.Rows) != 4 || r.Rows[0]["column_name"].Text != "ID" || r.Rows[2]["collation_name"].Text != "utf8mb4_unicode_520_ci" || r.Rows[3]["collation_name"].Text != "utf8mb4_bin" {
		t.Fatalf("column display: %+v", r)
	}
	r = exec("SHOW FULL COLUMNS FROM named")
	if r.Rows[0]["Field"].Text != "ID" || r.Rows[2]["Collation"].Text != "utf8mb4_unicode_520_ci" || !r.Rows[2]["Privileges"].Null {
		t.Fatal(r)
	}
	r = exec("SELECT column_name,sub_part FROM information_schema.statistics WHERE table_name='named' AND index_name='wordindex'")
	if len(r.Rows) != 1 || r.Rows[0]["column_name"].Text != "Word" || r.Rows[0]["sub_part"].Int != 7 {
		t.Fatal(r)
	}
	r = exec("SELECT column_name,referenced_column_name FROM information_schema.key_column_usage WHERE constraint_name='parent_fk'")
	if len(r.Rows) != 1 || r.Rows[0]["column_name"].Text != "ParentID" || r.Rows[0]["referenced_column_name"].Text != "ID" {
		t.Fatal(r)
	}
	r = exec("SELECT table_collation FROM information_schema.tables WHERE table_name='named'")
	if len(r.Rows) != 1 || r.Rows[0]["table_collation"].Text != "utf8mb4_unicode_520_ci" {
		t.Fatal(r)
	}
	r = exec("SELECT collation_name FROM information_schema.collations WHERE collation_name='utf8mb4_unicode_520_ci'")
	if len(r.Rows) != 1 {
		t.Fatal(r)
	}
}

func TestSharedCatalogNamespacePolicy(t *testing.T) {
	d, _ := bulkDatabase(t)
	my, e := d.NewSession()
	if e != nil {
		t.Fatal(e)
	}
	defer my.Close()
	if e = my.SetDialect(sql.DialectMySQL); e != nil {
		t.Fatal(e)
	}
	ctx := sql.WithMySQLSession(context.Background(), sql.MySQLSessionOptions{Database: "compat", RootDatabase: "compat"})
	if _, e = my.ExecContext(ctx, "CREATE DATABASE public;CREATE TABLE public.shadow(id BIGINT);CREATE TABLE root_visible(id BIGINT)"); e != nil {
		t.Fatal(e)
	}
	r, e := my.ExecContext(ctx, "SELECT schema_name FROM information_schema.schemata WHERE schema_name IN ('compat','public') ORDER BY schema_name")
	if e != nil || len(r.Rows) != 2 {
		t.Fatal(r, e)
	}
	r, e = my.ExecContext(ctx, "SELECT table_schema FROM information_schema.tables WHERE table_name='shadow'")
	if e != nil || len(r.Rows) != 1 || r.Rows[0]["table_schema"].Text != "public" {
		t.Fatal(r, e)
	}
	pg, e := d.NewSession()
	if e != nil {
		t.Fatal(e)
	}
	defer pg.Close()
	if e = pg.SetDialect(sql.DialectPostgreSQL); e != nil {
		t.Fatal(e)
	}
	r, e = pg.Exec("SELECT nspname FROM pg_namespace WHERE nspname='public'")
	if e != nil || len(r.Rows) != 1 {
		t.Fatal("duplicate PostgreSQL namespace", r, e)
	}
	for _, q := range []string{"SELECT table_name FROM information_schema.tables WHERE table_name='shadow'", "SELECT relname FROM pg_class WHERE relname='shadow'"} {
		r, e = pg.Exec(q)
		if e != nil || len(r.Rows) != 0 {
			t.Fatal("unreachable shadow leaked", q, r, e)
		}
	}
	r, e = pg.Exec("SELECT table_name FROM information_schema.tables WHERE table_schema='public' AND table_name='root_visible'")
	if e != nil || len(r.Rows) != 1 {
		t.Fatal(r, e)
	}
	if e = my.RegisterRootSchemaAlias(ctx, "public"); e != nil {
		t.Fatal(e)
	}
	r, e = my.ExecContext(ctx, "SELECT table_name FROM information_schema.tables WHERE table_name='shadow'")
	if e != nil || len(r.Rows) != 0 {
		t.Fatal("explicit root alias leaked physical shadow", r, e)
	}
}

func TestCatalogSelfDescriptionAndShowFrames(t *testing.T) {
	for _, dialect := range []sql.Dialect{sql.DialectMySQL, sql.DialectPostgreSQL} {
		t.Run(dialect.String(), func(t *testing.T) {
			d, _ := bulkDatabase(t)
			s, e := d.NewSession()
			if e != nil {
				t.Fatal(e)
			}
			defer s.Close()
			if e = s.SetDialect(dialect); e != nil {
				t.Fatal(e)
			}
			exec := func(q string) *ExecResult {
				t.Helper()
				r, e := s.Exec(q)
				if e != nil {
					t.Fatalf("%s: %v", q, e)
				}
				return r
			}
			r := exec("SELECT table_name,table_type FROM information_schema.tables WHERE table_schema='information_schema' AND table_name='columns'")
			if len(r.Rows) != 1 || r.Rows[0]["table_type"].Text != "SYSTEM VIEW" {
				t.Fatal(r)
			}
			r = exec("SELECT column_name,ordinal_position FROM information_schema.columns WHERE table_schema='information_schema' AND table_name='columns' ORDER BY ordinal_position")
			if len(r.Rows) < 30 || r.Rows[0]["column_name"].Text != "table_catalog" || r.Rows[0]["ordinal_position"].Int != 1 {
				t.Fatal("missing static self description")
			}
			r = exec("SHOW TABLES FROM information_schema LIKE 'columns'")
			if len(r.Rows) != 1 {
				t.Fatal(r)
			}
			r = exec("SHOW COLUMNS FROM information_schema.columns")
			if len(r.Rows) < 30 || r.Rows[4]["Type"].Text != "BIGINT" {
				t.Fatal("wrong system column descriptions")
			}
			exec("CREATE TABLE described(id BIGINT PRIMARY KEY, value INTEGER DEFAULT (1/0));CREATE VIEW described_view AS SELECT id FROM described;CREATE TRIGGER described_trigger AFTER INSERT ON described BEGIN SELECT 1; END")
			for _, test := range []struct{ show, query, key string }{
				{"SHOW CREATE TABLE described", "SELECT slopdb_create_sql FROM information_schema.tables WHERE table_name='described'", "Create table"},
				{"SHOW CREATE VIEW described_view", "SELECT slopdb_create_sql FROM information_schema.views WHERE table_name='described_view'", "Create view"},
				{"SHOW CREATE TRIGGER described_trigger", "SELECT slopdb_create_sql FROM information_schema.triggers WHERE trigger_name='described_trigger'", "SQL Original Statement"},
			} {
				show := exec(test.show)
				catalog := exec(test.query)
				if len(catalog.Rows) != 1 || show.Rows[0][test.key].Text != catalog.Rows[0]["slopdb_create_sql"].Text {
					t.Fatalf("definition projections differ %s: show=%q catalog=%q count=%d", test.show, show.Rows[0][test.key].Text, catalog.Rows[0]["slopdb_create_sql"].Text, len(catalog.Rows))
				}
			}
			exec("INSERT INTO described(id,value) VALUES(1,1),(2,2)")
			status := exec("SHOW TABLE STATUS LIKE 'described'")
			info := exec("SELECT table_rows,data_length,index_length,auto_increment FROM information_schema.tables WHERE table_name='described' AND table_schema='" + map[sql.Dialect]string{sql.DialectMySQL: "main", sql.DialectPostgreSQL: "public"}[dialect] + "'")
			if len(status.Rows) != 1 || len(info.Rows) != 1 || status.Rows[0]["Rows"].Int != info.Rows[0]["table_rows"].Int || status.Rows[0]["Data_length"].Int != info.Rows[0]["data_length"].Int || status.Rows[0]["Index_length"].Int != info.Rows[0]["index_length"].Int {
				t.Fatal("status projections differ", status.Rows, info.Rows)
			}
			for _, q := range []string{"SHOW FULL COLUMNS FROM described", "SHOW FULL TABLES", "SHOW INDEX FROM described", "SHOW TABLE STATUS LIKE 'described'", "SHOW CREATE TABLE described", "SHOW CREATE VIEW described_view", "SHOW CREATE TRIGGER described_trigger", "SHOW TRIGGERS"} {
				parsed, e := s.Prepare(q)
				if e != nil {
					t.Fatal(q, e)
				}
				description, e := s.DescribePreparedTypes(context.Background(), parsed, sql.DescribeOptions{})
				if e != nil {
					t.Fatal(q, e)
				}
				actual := exec(q)
				if len(description.Results) != len(actual.Columns) {
					t.Fatalf("%s descriptor count %d != %d", q, len(description.Results), len(actual.Columns))
				}
				for i, c := range description.Results {
					if c.Name != actual.Columns[i].Name || c.ColType != actual.Columns[i].ColType {
						t.Fatalf("%s static column %d %+v != %+v", q, i, c, actual.Columns[i])
					}
				}
				parsed.Close()
			}
			if dialect == sql.DialectPostgreSQL {
				r = exec("SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='information_schema' AND c.relname='columns'")
				if len(r.Rows) != 1 {
					t.Fatal("system view absent from PG catalog", r)
				}
			}
		})
	}
}
