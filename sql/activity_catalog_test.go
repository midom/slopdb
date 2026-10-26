package sql

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"
	"unicode/utf8"
)

func TestSessionActivityProjectionFieldsAndTypes(t *testing.T) {
	now := time.Date(2030, 1, 2, 3, 4, 5, 123456000, time.FixedZone("peer", 2*3600))
	source := []SessionActivity{
		{SessionID: 20, ID: 200, Dialect: DialectMySQL, User: "compat", Database: "tenant", ApplicationName: "client", ClientAddress: "::1", ClientPort: 33061, StartedAt: now.Add(-time.Hour), QueryStart: now.Add(-7500 * time.Millisecond), StateChange: now.Add(-time.Second), State: ActivityActive, Query: "SELECT 1", Waiting: true, WaitKind: "row", WaitResource: "opaque-row-resource"},
		{SessionID: 10, ID: 201, Dialect: DialectPostgreSQL, User: "app", Database: "physical", Host: "localhost", ClientPort: -1, StartedAt: now.Add(-time.Hour), TransactionStart: now.Add(-5 * time.Minute), QueryStart: now.Add(-10 * time.Second), StateChange: now.Add(-2 * time.Second), State: ActivityIdleInTransactionAborted, Query: "SELECT 1/0"},
		{SessionID: 30, ID: 1 << 31, Dialect: DialectNative, State: ActivityIdle, StateChange: now.Add(time.Second)},
	}
	original := append([]SessionActivity(nil), source...)
	process, e := ProjectSessionActivity("information_schema.processlist", source, now)
	if e != nil {
		t.Fatal(e)
	}
	if len(process.Rows) != 3 || process.Columns[0].LogicalType().Kind != TypeUint64 {
		t.Fatal("processlist must preserve uint64 identifiers")
	}
	idle, active := process.Rows[0], process.Rows[1]
	if idle["id"].Uint != 201 || idle["command"].Text != "Sleep" || !idle["info"].Null || idle["time"].Int != 2 || idle["state"].Text != "idle in transaction (aborted)" {
		t.Fatal("wrong idle transaction projection", idle)
	}
	if active["id"].Uint != 200 || active["slopdb_session_id"].Uint != 20 || active["slopdb_protocol"].Text != "mysql" || active["host"].Text != "[::1]:33061" || active["db"].Text != "tenant" || active["time"].Int != 7 || active["state"].Text != "Waiting for row lock" || active["info"].Text != "SELECT 1" {
		t.Fatal("wrong active projection", active)
	}
	if process.Rows[2]["time"].Int != 0 || !process.Rows[2]["db"].Null || !process.Rows[2]["user"].Null {
		t.Fatal("unknown facts or future state time invented")
	}
	pg, e := ProjectSessionActivity("pg_stat_activity", source, now)
	if e != nil {
		t.Fatal(e)
	}
	descriptors := map[string]ColumnDef{}
	for _, c := range pg.Columns {
		descriptors[c.Name] = c
	}
	if descriptors["pid"].LogicalType().Kind != TypeInt32 || descriptors["client_port"].LogicalType().Kind != TypeInt32 || descriptors["query_start"].LogicalType().Kind != TypeTimestampTZ || descriptors["client_addr"].LogicalType().Kind != TypeText {
		t.Fatal("incorrect PG activity descriptor", descriptors)
	}
	idle, active = pg.Rows[0], pg.Rows[1]
	if idle["pid"].Int != 201 || idle["state"].Text != "idle in transaction (aborted)" || idle["query"].Text != "SELECT 1/0" || idle["client_port"].Int != -1 || !idle["client_addr"].Null || !idle["client_hostname"].Null || idle["xact_start"].Text != source[1].TransactionStart.UTC().Format(time.RFC3339Nano) {
		t.Fatal("wrong PG idle projection", idle)
	}
	if active["client_addr"].Text != "::1" || active["client_port"].Int != 33061 || active["wait_event_type"].Text != "Lock" || active["wait_event"].Text != "row" || active["slopdb_wait_resource"].Text != "opaque-row-resource" || active["query_start"].LogicalType().Kind != TypeTimestampTZ {
		t.Fatal("wrong PG wait projection", active)
	}
	for _, field := range []string{"datid", "usesysid", "leader_pid", "backend_xid", "backend_xmin", "query_id", "client_hostname"} {
		if !active[field].Null {
			t.Fatal("unavailable field must remain NULL", field)
		}
	}
	if !pg.Rows[2]["pid"].Null || pg.Rows[2]["slopdb_backend_id"].Uint != 1<<31 || !pg.Rows[2]["query_start"].Null {
		t.Fatal("native ID overflow or missing timestamp was fabricated")
	}
	if !reflect.DeepEqual(source, original) {
		t.Fatal("projection mutated caller snapshot")
	}
}

func TestSessionActivityProjectionBoundsAndShowMetadata(t *testing.T) {
	now := time.Now()
	query := strings.Repeat("😀", 101) + " SELECT ?"
	source := []SessionActivity{{SessionID: 1, ID: 7, State: ActivityActive, Query: query}}
	short, full := ProcesslistResult(source, false, now), ProcesslistResult(source, true, now)
	if utf8.RuneCountInString(short.Rows[0]["Info"].Text) != 100 || full.Rows[0]["Info"].Text != query {
		t.Fatal("SHOW character truncation changed full retained text")
	}
	if len(short.Columns) != 8 || short.Columns[0].Name != "Id" || short.Columns[0].LogicalType().Kind != TypeUint64 || short.Columns[5].LogicalType().Kind != TypeInt64 {
		t.Fatal("wrong SHOW PROCESSLIST descriptor")
	}
	source[0].Query = strings.Repeat("a", ActivityQueryLimit-1) + "😀tail"
	full = ProcesslistResult(source, true, now)
	text := full.Rows[0]["Info"].Text
	if len(text) != ActivityQueryLimit-1 || !utf8.ValidString(text) {
		t.Fatal("FULL exceeded byte bound or split a Unicode code point")
	}
	pg, e := ProjectSessionActivity("pg_catalog.pg_stat_activity", source, now)
	if e != nil || pg.Rows[0]["query"].Text != text {
		t.Fatal("PG query bound differs", e)
	}
	s, _ := statisticsSchema(t, t.TempDir())
	for _, query := range []string{"SHOW PROCESSLIST", "SHOW FULL PROCESSLIST"} {
		statements, e := Parse(query)
		if e != nil {
			t.Fatal(e)
		}
		columns, e := s.Describe(statements[0])
		if e != nil || !reflect.DeepEqual(columns, short.Columns) {
			t.Fatal("SHOW static metadata differs", query, e)
		}
	}
	for _, query := range []string{"SHOW PROCESSLIST FROM main", "SHOW PROCESSLIST LIKE '%'", "SHOW FULL PROCESSLIST WHERE Id=7"} {
		if _, e := Parse(query); e == nil {
			t.Fatal("unsupported filter accepted", query)
		}
	}
	if _, e := s.ExecuteShow(&ShowStmt{Kind: "PROCESSLIST", Database: "main"}); e == nil {
		t.Fatal("unsupported AST filter ignored")
	}
}

func TestSessionActivityCatalogSelfDescription(t *testing.T) {
	s, _ := statisticsSchema(t, t.TempDir())
	if e := s.SetDialect(DialectPostgreSQL); e != nil {
		t.Fatal(e)
	}
	s.SetActivitySnapshot(func(_ context.Context) ([]SessionActivity, error) {
		t.Fatal("catalog self description called live activity provider")
		return nil, nil
	})
	for _, table := range []struct{ schema, name string }{{"information_schema", "processlist"}, {"pg_catalog", "pg_stat_activity"}} {
		r, e := s.informationSchemaTable("tables", catalogFilter{Schema: table.schema, Table: table.name})
		if e != nil || len(r.Rows) != 1 || r.Rows[0]["table_type"].Text != "SYSTEM VIEW" {
			t.Fatal("missing virtual table metadata", table, e)
		}
		columns, e := s.informationSchemaTable("columns", catalogFilter{Schema: table.schema, Table: table.name})
		if e != nil || len(columns.Rows) < 8 {
			t.Fatal("missing virtual column metadata", table, e)
		}
		if table.name == "pg_stat_activity" {
			found := false
			for _, row := range columns.Rows {
				if row["column_name"].Text == "pid" {
					found = true
					if row["data_type"].Text != "integer" || row["numeric_precision"].Int != 32 {
						t.Fatal("PG PID self description lost int4 precision", row)
					}
				}
			}
			if !found {
				t.Fatal("missing PID")
			}
		}
	}
	classes, e := s.PostgresCatalogTable("pg_class")
	if e != nil {
		t.Fatal(e)
	}
	found := map[string]bool{}
	for _, row := range classes.Rows {
		if row["relname"].Text == "pg_stat_activity" || row["relname"].Text == "processlist" {
			found[row["relname"].Text] = true
			if row["relkind"].Text != "v" {
				t.Fatal("activity relation is not a virtual catalog view")
			}
		}
	}
	if len(found) != 2 {
		t.Fatal("activity relation absent from pg_class", found)
	}
	for _, dialect := range []Dialect{DialectNative, DialectMySQL, DialectPostgreSQL} {
		for _, name := range []string{"information_schema.processlist", "pg_stat_activity", "pg_catalog.pg_stat_activity"} {
			_, known := ActivityRelationName(name, dialect)
			want := name == "information_schema.processlist" || dialect == DialectPostgreSQL
			if known != want {
				t.Fatal("unsafe activity relation recognition", name, dialect)
			}
		}
	}
}
