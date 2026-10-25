package regression

import (
	"context"
	"os"
	"strings"
	"testing"

	"slopdb/db"
	"slopdb/sql"
	"slopdb/storage"
)

// Exercise the complete upstream core schema, then the query shapes whose
// incorrect results blocked editing, search, user groups and undelete in a real
// installation. The expected values follow directly from these fixed inserts.
func TestMediaWikiCoreSchemaAndWorkflows(t *testing.T) {
	e := newRegressionEnv(t)
	options := storage.DefaultOptions()
	options.DisableWALSync = true
	d, err := e.openDatabase(e.path("wiki"), options)
	if err != nil {
		t.Fatal(err)
	}
	var session *db.Session
	openSession := func() {
		t.Helper()
		session, err = d.NewSession()
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = session.Close() })
		if err = session.SetMySQLMode(true); err != nil {
			t.Fatal(err)
		}
	}
	openSession()
	ctx := sql.WithMySQLSession(context.Background(), sql.MySQLSessionOptions{SQLMode: "", GroupConcatMaxLen: 262144})
	exec := func(query string) *db.ExecResult {
		t.Helper()
		r, err := session.ExecContext(ctx, query)
		if err != nil {
			t.Fatalf("%.200s: %v", query, err)
		}
		return r
	}
	schema, err := os.ReadFile("testdata/mediawiki/schema-1.46.2.sql")
	if err != nil {
		t.Fatal(err)
	}
	exec(string(schema))
	if rows := exec("SHOW TABLES").Rows; len(rows) != 64 {
		t.Fatalf("core table count = %d, want 64", len(rows))
	}
	exec("INSERT INTO page(page_namespace,page_title,page_random,page_touched,page_latest,page_len) VALUES(0,'Compatibility_probe',0.5,'20261025000000',2,42)")
	exec("INSERT INTO revision(rev_id,rev_page,rev_comment_id,rev_actor,rev_timestamp) VALUES(1,1,1,1,'20261025000000'),(2,1,1,1,'20261025000001')")
	exec("INSERT INTO user_groups(ug_user,ug_group) VALUES(1,'sysop'),(1,'bureaucrat'),(1,'interface-admin')")
	exec("INSERT INTO searchindex(si_page,si_title,si_text) VALUES(1,'Compatibility probe','A quasar and nebula compatibility test')")
	check := func() {
		t.Helper()
		rows := exec("SELECT page_id,page_title FROM page WHERE page_namespace=0 AND page_title LIKE 'Compatibility%' ESCAPE '`'").Rows
		if len(rows) != 1 || rows[0]["page_id"].Int != 1 || string(rows[0]["page_title"].Blob) != "Compatibility_probe" {
			t.Fatalf("prefix results: %+v", rows)
		}
		rows = exec("SELECT GROUP_CONCAT(ug_group ORDER BY ug_group SEPARATOR '|') AS group_names FROM user_groups WHERE ug_user=1").Rows
		if len(rows) != 1 || string(rows[0]["group_names"].Blob) != "bureaucrat|interface-admin|sysop" {
			t.Fatalf("group results: %+v", rows)
		}
		rows = exec("SELECT page_id FROM page INNER JOIN searchindex ON page_id=si_page WHERE MATCH(si_text) AGAINST('+quasar' IN BOOLEAN MODE) AND page_namespace=0 ORDER BY MATCH(si_text) AGAINST('+quasar' IN BOOLEAN MODE) DESC").Rows
		if len(rows) != 1 || rows[0]["page_id"].Int != 1 {
			t.Fatalf("search results: %+v", rows)
		}
		for _, row := range exec("SHOW TABLE STATUS").Rows {
			if row["Engine"].Text != "LSM" {
				t.Fatalf("table engine: %+v", row)
			}
		}
		if ddl := exec("SHOW CREATE TABLE searchindex").Rows[0]["Create table"].Text; !strings.Contains(ddl, "FULLTEXT") || !strings.Contains(ddl, "ENGINE=LSM") {
			t.Fatalf("search metadata: %s", ddl)
		}
	}
	check()
	// The application archives revisions, deletes quoted IDs and later restores
	// those exact primary keys. Zero-row deletes previously surfaced only at restore.
	exec("BEGIN")
	if r := exec("DELETE FROM revision WHERE rev_id IN ('1','2')"); r.Changes != 2 {
		t.Fatalf("archive delete affected %d, want 2", r.Changes)
	}
	exec("INSERT INTO revision(rev_id,rev_page,rev_comment_id,rev_actor,rev_timestamp) VALUES(1,1,1,1,'20261025000000'),(2,1,1,1,'20261025000001')")
	exec("COMMIT")
	if err = session.Close(); err != nil {
		t.Fatal(err)
	}
	if err = d.Close(); err != nil {
		t.Fatal(err)
	}
	d, err = e.openDatabase(e.path("wiki"), options)
	if err != nil {
		t.Fatal(err)
	}
	openSession()
	check()
	if n := exec("SELECT COUNT(*) AS n FROM revision").Rows[0]["n"].Int; n != 2 {
		t.Fatalf("reopened revision count: %d", n)
	}
	if _, err = d.CheckIntegrity(); err != nil {
		t.Fatal(err)
	}
	if err = session.Close(); err != nil {
		t.Fatal(err)
	}
	if err = d.Close(); err != nil {
		t.Fatal(err)
	}
}
