package db_test

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"slopdb/db"
	"slopdb/storage"
)

func fulltextDatabase(t *testing.T) *db.Database {
	t.Helper()
	o := storage.DefaultOptions()
	o.DisableWALSync = true
	d, err := db.Open(t.TempDir(), o)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := d.Close(); err != nil {
			t.Error(err)
		}
	})
	return d
}
func fulltextExec(t *testing.T, d *db.Database, q string) *db.ExecResult {
	t.Helper()
	r, err := d.Exec(q)
	if err != nil {
		t.Fatalf("%s: %v", q, err)
	}
	return r
}
func fulltextIDs(t *testing.T, d *db.Database, expr string, want []int64) {
	t.Helper()
	r := fulltextExec(t, d, "SELECT id FROM documents WHERE "+expr+" ORDER BY id")
	var got []int64
	for _, row := range r.Rows {
		got = append(got, row["id"].Int)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("%s: got %v want %v", expr, got, want)
	}
}

func TestMySQLFulltextQueriesAndMetadata(t *testing.T) {
	d := fulltextDatabase(t)
	fulltextExec(t, d, "CREATE TABLE documents(id INTEGER PRIMARY KEY,title TEXT,body TEXT,FULLTEXT INDEX ft_title(title),FULLTEXT INDEX ft_body(body),FULLTEXT INDEX ft_both(title,body))")
	fulltextExec(t, d, `INSERT INTO documents VALUES
 (1,'Café quasar','A green apple rests near bright galaxies'),
 (2,'distant stars','Green apples and a nebula in space'),
 (3,'quiet moon','orbital rocks and lunar dust'),
 (4,'another planet','a green apple orchard')`)
	for _, c := range []struct {
		query string
		ids   []int64
	}{
		{`MATCH(body) AGAINST('+green +apple -orchard' IN BOOLEAN MODE)`, []int64{1}},
		{`MATCH(body) AGAINST('+green +appl*' IN BOOLEAN MODE)`, []int64{1, 2, 4}},
		{`MATCH(body) AGAINST('"green apple"' IN BOOLEAN MODE)`, []int64{1, 4}},
		{`MATCH(body) AGAINST('+green +(apple nebula) -orchard' IN BOOLEAN MODE)`, []int64{1, 2}},
		{`MATCH(body) AGAINST('-green' IN BOOLEAN MODE)`, nil},
		{`MATCH(body) AGAINST('the with' IN BOOLEAN MODE)`, nil},
		{`MATCH(title) AGAINST('CAFE' IN BOOLEAN MODE)`, []int64{1}},
		{`MATCH(body) AGAINST('nebula' IN NATURAL LANGUAGE MODE)`, []int64{2}},
	} {
		t.Run(c.query, func(t *testing.T) { fulltextIDs(t, d, c.query, c.ids) })
	}
	rows := fulltextExec(t, d, "SHOW INDEX FROM documents")
	found := 0
	for _, row := range rows.Rows {
		if strings.HasPrefix(row["Key_name"].Text, "ft_") {
			found++
			if row["Index_type"].Text != "FULLTEXT" {
				t.Fatal(row)
			}
		}
	}
	if found != 4 {
		t.Fatalf("FULLTEXT metadata components: %d", found)
	}
	fulltextExec(t, d, `CREATE VIEW matches AS SELECT id FROM documents WHERE MATCH(title) AGAINST('quasar' IN BOOLEAN MODE)`)
	if r := fulltextExec(t, d, "SELECT id FROM matches"); len(r.Rows) != 1 || r.Rows[0]["id"].Int != 1 {
		t.Fatal(r)
	}
	// A FULLTEXT document index remains a normal transactional maintained index.
	if _, err := d.CheckIntegrity(); err != nil {
		t.Fatal(err)
	}
	fulltextExec(t, d, "UPDATE documents SET body='nebula nebula' WHERE id=1")
	fulltextIDs(t, d, `MATCH(body) AGAINST('nebula' IN BOOLEAN MODE)`, []int64{1, 2})
	fulltextExec(t, d, "DELETE FROM documents WHERE id=2")
	fulltextIDs(t, d, `MATCH(body) AGAINST('nebula' IN BOOLEAN MODE)`, []int64{1})
}

func TestMySQLFulltextPreparedSnapshotsAndValidation(t *testing.T) {
	d := fulltextDatabase(t)
	fulltextExec(t, d, "CREATE TABLE documents(id INTEGER PRIMARY KEY,body TEXT)")
	for _, q := range []string{`SELECT MATCH(body) AGAINST('quasar') FROM documents`, `CREATE FULLTEXT INDEX invalid ON documents(id)`} {
		if _, err := d.Exec(q); err == nil {
			t.Fatalf("accepted invalid fulltext: %s", q)
		}
	}
	fulltextExec(t, d, "CREATE FULLTEXT INDEX ft_document ON documents(body)")
	fulltextExec(t, d, "INSERT INTO documents VALUES(1,'quasar green'),(2,'nebula blue')")
	p, err := d.Prepare("SELECT id FROM documents WHERE MATCH(body) AGAINST(? IN BOOLEAN MODE) ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	s, err := d.NewSession()
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for _, word := range []string{"quasar", "nebula", "quasar"} {
		r, err := s.ExecPrepared(p, word)
		if err != nil || len(r.Rows) != 1 {
			t.Fatalf("%s: %+v %v", word, r, err)
		}
	}
	if _, err = s.Exec("BEGIN"); err != nil {
		t.Fatal(err)
	}
	if _, err = s.Exec("UPDATE documents SET body='quasar private' WHERE id=2"); err != nil {
		t.Fatal(err)
	}
	r, err := s.ExecPrepared(p, "quasar")
	if err != nil || len(r.Rows) != 2 {
		t.Fatalf("overlay: %+v %v", r, err)
	}
	fulltextIDs(t, d, `MATCH(body) AGAINST('quasar' IN BOOLEAN MODE)`, []int64{1})
	if _, err = s.Exec("ROLLBACK"); err != nil {
		t.Fatal(err)
	}
	r, err = s.ExecPrepared(p, "quasar")
	if err != nil || len(r.Rows) != 1 {
		t.Fatalf("rollback: %+v %v", r, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err = s.ExecPreparedContext(ctx, p, "quasar"); err == nil {
		t.Fatal("cancelled fulltext query succeeded")
	}
	for _, text := range []string{`MATCH(body) AGAINST('"unterminated' IN BOOLEAN MODE)`, `MATCH(body) AGAINST('((apple)' IN BOOLEAN MODE)`, `MATCH(body,body) AGAINST('apple')`, `MATCH(body) AGAINST(body)`} {
		if _, err := d.Exec("SELECT " + text + " FROM documents"); err == nil {
			t.Fatalf("accepted %s", text)
		}
	}
	fulltextExec(t, d, "DROP INDEX ft_document")
	if _, err := s.ExecPrepared(p, "quasar"); err == nil {
		t.Fatal("stale fulltext plan survived DROP INDEX")
	}
	fulltextExec(t, d, "ALTER TABLE documents ADD FULLTEXT INDEX ft_new(body)")
	if _, err := s.ExecPrepared(p, "quasar"); err != nil {
		t.Fatal(err)
	}
}

func TestMySQLFulltextPersistsOnReopen(t *testing.T) {
	dir := t.TempDir()
	o := storage.DefaultOptions()
	o.DisableWALSync = true
	d, err := db.Open(dir, o)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if d != nil {
			_ = d.Close()
		}
	})
	fulltextExec(t, d, "CREATE TABLE documents(id INTEGER PRIMARY KEY,body TEXT,FULLTEXT f(body));INSERT INTO documents VALUES(1,'quasar bright')")
	fulltextExec(t, d, "CREATE VIEW found AS SELECT id FROM documents WHERE MATCH(body) AGAINST('quasar' IN BOOLEAN MODE)")
	if err = d.Close(); err != nil {
		t.Fatal(err)
	}
	d = nil
	d, err = db.Open(dir, o)
	if err != nil {
		t.Fatal(err)
	}
	fulltextIDs(t, d, `MATCH(body) AGAINST('quasar' IN BOOLEAN MODE)`, []int64{1})
	if r := fulltextExec(t, d, "SELECT id FROM found"); len(r.Rows) != 1 {
		t.Fatalf("persisted view: %+v", r)
	}
	if _, err = d.CheckIntegrity(); err != nil {
		t.Fatal(fmt.Errorf("reopen integrity: %w", err))
	}
	if err = d.Close(); err != nil {
		t.Fatal(err)
	}
	d = nil
}

func TestMySQLFulltextJoinBindingsAndResourceLimits(t *testing.T) {
	d := fulltextDatabase(t)
	fulltextExec(t, d, "CREATE TABLE documents(id INTEGER PRIMARY KEY,body TEXT,FULLTEXT INDEX ft_body(body));CREATE TABLE links(id INTEGER PRIMARY KEY)")
	fulltextExec(t, d, "INSERT INTO documents VALUES(1,'quasar nebula'),(2,'unrelated planet');INSERT INTO links VALUES(1),(2)")
	r := fulltextExec(t, d, "SELECT d.id AS id FROM links AS l JOIN documents AS d ON l.id=d.id AND MATCH(d.body) AGAINST('quasar' IN BOOLEAN MODE)")
	if len(r.Rows) != 1 || r.Rows[0]["id"].Int != 1 {
		t.Fatalf("joined fulltext: %+v", r.Rows)
	}
	p, err := d.Prepare("SELECT id FROM documents WHERE MATCH(body) AGAINST(? IN BOOLEAN MODE)")
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if r, err = p.Exec([]byte("+quasar")); err != nil || len(r.Rows) != 1 {
		t.Fatalf("binary search: %+v %v", r, err)
	}
	for _, query := range []string{strings.Repeat("(", 65) + "quasar" + strings.Repeat(")", 65), strings.Repeat(">", 100) + "quasar", strings.Repeat("quasar ", 1025)} {
		if _, err = p.Exec(query); err == nil {
			t.Fatal("accepted unbounded fulltext search")
		}
	}
	if _, err = d.Exec("SELECT MATCH(body) AGAINST('" + strings.Repeat("quasar ", 1025) + "') FROM documents"); err == nil {
		t.Fatal("accepted unbounded natural search")
	}
	fulltextIDs(t, d, "MATCH(body) AGAINST('quasar' IN BOOLEAN MODE)", []int64{1})
}
