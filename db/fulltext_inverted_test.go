package db

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"testing"
)

func TestFulltextPostingIndexSelectiveReadsAndReopen(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = d.Close() })
	objectExec(t, d, "CREATE TABLE docs(id INTEGER PRIMARY KEY,body TEXT,FULLTEXT KEY ft(body));CREATE TABLE page(id INTEGER PRIMARY KEY)")
	for batch := 0; batch < 4; batch++ {
		var docs, pages []string
		for j := 0; j < 128; j++ {
			id := batch*128 + j + 1
			text := "ordinary archived document payload"
			if id == 311 {
				text = "quasar nebula green apple"
			}
			docs = append(docs, fmt.Sprintf("(%d,'%s')", id, text))
			pages = append(pages, fmt.Sprintf("(%d)", id))
		}
		objectExec(t, d, "INSERT INTO docs VALUES"+strings.Join(docs, ","))
		objectExec(t, d, "INSERT INTO page VALUES"+strings.Join(pages, ","))
	}
	check := func() {
		t.Helper()
		for _, q := range []string{
			"SELECT id FROM docs WHERE MATCH(body) AGAINST('+quasar +the' IN BOOLEAN MODE)",
			"SELECT id FROM docs WHERE MATCH(body) AGAINST('quasar' IN NATURAL LANGUAGE MODE)",
			"SELECT docs.id AS id FROM page JOIN docs ON page.id=docs.id WHERE MATCH(docs.body) AGAINST('+quas*' IN BOOLEAN MODE)",
		} {
			r := objectExec(t, d, q)
			if len(r.Rows) != 1 || r.Rows[0]["id"].Int != 311 {
				t.Fatalf("%s: %+v", q, r.Rows)
			}
			r = objectExec(t, d, "EXPLAIN ANALYZE "+q)
			if r.Profile == nil {
				t.Fatal("missing profile")
			}
			if r.Profile.RowsExamined > 4 {
				t.Fatalf("FULLTEXT fetched entire table: %+v", r.Profile)
			}
			found := false
			for _, op := range r.Profile.Operators {
				found = found || op.Operator == "FulltextIndex"
			}
			if !found {
				t.Fatal("inverted access not used")
			}
		}
	}
	check()
	it := d.lsm.NewIteratorRange([]byte{2}, []byte{3}, false)
	postings, markers := 0, 0
	for ; it.Valid(); it.Next() {
		key := it.Key().UserKey
		if len(key) < 13 {
			t.Fatal("short index key")
		}
		switch string(key[11:13]) {
		case "p:":
			postings++
		case "d:":
			markers++
		default:
			t.Fatalf("ordinary full-value entry remains: %q", key)
		}
		if bytes.Contains(it.Value(), []byte("document payload")) {
			t.Fatal("document body copied into postings")
		}
	}
	it.Close()
	if postings < 512 || markers != 512 {
		t.Fatal(postings, markers)
	}
	if _, err = d.CheckIntegrity(); err != nil {
		t.Fatal(err)
	}
	if err = d.Close(); err != nil {
		t.Fatal(err)
	}
	d, err = Open(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	check()
	objectExec(t, d, "UPDATE docs SET body='galactic elsewhere' WHERE id=311")
	if r := objectExec(t, d, "SELECT id FROM docs WHERE MATCH(body) AGAINST('quasar' IN BOOLEAN MODE)"); len(r.Rows) != 0 {
		t.Fatal(r.Rows)
	}
	objectExec(t, d, "DELETE FROM docs WHERE id=311")
	if _, err = d.CheckIntegrity(); err != nil {
		t.Fatal(err)
	}
}

func TestLegacyFulltextDocumentsUpgradeToPostings(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	objectExec(t, d, "CREATE TABLE docs(id INTEGER PRIMARY KEY,body TEXT);INSERT INTO docs VALUES(1,'quasar nebula'),(2,'ordinary archive');CREATE INDEX ft ON docs(body)")
	data, ok, err := d.lsm.Get([]byte("\x00__schema_indexes__"))
	if err != nil || !ok {
		t.Fatal(ok, err)
	}
	var catalog map[string]map[string]any
	if err = json.Unmarshal(data, &catalog); err != nil {
		t.Fatal(err)
	}
	catalog["ft"]["fulltext"] = true
	catalog["ft"]["fulltext_version"] = 0
	data, err = json.Marshal(catalog)
	if err != nil {
		t.Fatal(err)
	}
	if err = d.lsm.Put([]byte("\x00__schema_indexes__"), data); err != nil {
		t.Fatal(err)
	}
	if err = d.Close(); err != nil {
		t.Fatal(err)
	}
	d, err = Open(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	r := objectExec(t, d, "SELECT id FROM docs WHERE MATCH(body) AGAINST('quasar' IN BOOLEAN MODE)")
	if len(r.Rows) != 1 || r.Rows[0]["id"].Int != 1 {
		t.Fatal(r.Rows)
	}
	if _, err = d.CheckIntegrity(); err != nil {
		t.Fatal(err)
	}
	objectExec(t, d, "ALTER INDEX ft REBUILD")
	if _, err = d.CheckIntegrity(); err != nil {
		t.Fatal(err)
	}
}

func TestOnlineFulltextPostingBuildCatchesConcurrentWrites(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = d.Close() })
	objectExec(t, d, "CREATE TABLE docs(id INTEGER PRIMARY KEY,body TEXT);INSERT INTO docs VALUES(1,'quasar old'),(2,'nebula old')")
	reached, release := onlineBarrier(t, d, "scanned")
	done := onlineStart(t, d, context.Background(), "CREATE FULLTEXT INDEX ft ON docs(body)")
	onlineReached(t, reached)
	objectExec(t, d, "UPDATE docs SET body='nebula new' WHERE id=1;UPDATE docs SET body='quasar new' WHERE id=2;INSERT INTO docs VALUES(3,'quasar inserted')")
	objectExec(t, d, "BEGIN;INSERT INTO docs VALUES(4,'quasar rolledback');ROLLBACK")
	release()
	if err = onlineWait(t, done); err != nil {
		t.Fatal(err)
	}
	check := func() {
		t.Helper()
		r := objectExec(t, d, "SELECT id FROM docs WHERE MATCH(body) AGAINST('quasar' IN BOOLEAN MODE) ORDER BY id")
		if len(r.Rows) != 2 || r.Rows[0]["id"].Int != 2 || r.Rows[1]["id"].Int != 3 {
			t.Fatal(r.Rows)
		}
		if _, err = d.CheckIntegrity(); err != nil {
			t.Fatal(err)
		}
	}
	check()
	if err = d.Close(); err != nil {
		t.Fatal(err)
	}
	d, err = Open(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	check()
}

func TestTemporaryFulltextPostingsAndPrivateRollback(t *testing.T) {
	d, err := Open(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	a, _ := d.NewSession()
	defer a.Close()
	b, _ := d.NewSession()
	defer b.Close()
	if _, err = a.Exec("CREATE TEMPORARY TABLE docs(body TEXT,FULLTEXT KEY ft(body));INSERT INTO docs VALUES('quasar nebula'),('ordinary archive')"); err != nil {
		t.Fatal(err)
	}
	query := "SELECT body FROM docs WHERE MATCH(body) AGAINST('quasar' IN BOOLEAN MODE)"
	r, err := a.Exec(query)
	if err != nil || len(r.Rows) != 1 {
		t.Fatal(r, err)
	}
	if _, err = b.Exec(query); err == nil {
		t.Fatal("temporary fulltext catalog leaked")
	}
	if _, err = a.Exec("BEGIN;UPDATE docs SET body='galactic space' WHERE MATCH(body) AGAINST('quasar' IN BOOLEAN MODE)"); err != nil {
		t.Fatal(err)
	}
	r, err = a.Exec(query)
	if err != nil || len(r.Rows) != 0 {
		t.Fatal(r, err)
	}
	if _, err = a.Exec("ROLLBACK"); err != nil {
		t.Fatal(err)
	}
	r, err = a.Exec(query)
	if err != nil || len(r.Rows) != 1 {
		t.Fatal(r, err)
	}
	if err = a.Close(); err != nil {
		t.Fatal(err)
	}
	if n := temporaryLSMCount(t, d); n != 0 {
		t.Fatal(n)
	}
}

func TestStreamingFulltextUsesPostingSnapshot(t *testing.T) {
	d, err := Open(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	objectExec(t, d, "CREATE TABLE docs(id INTEGER PRIMARY KEY,body TEXT,FULLTEXT KEY ft(body));INSERT INTO docs VALUES(1,'quasar alpha'),(2,'nebula beta'),(3,'quasar gamma')")
	rows, err := d.Query("SELECT id FROM docs WHERE MATCH(body) AGAINST('quasar' IN BOOLEAN MODE) ORDER BY id DESC")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	if rows.Materialized() {
		t.Fatal("simple FULLTEXT stream materialized")
	}
	objectExec(t, d, "UPDATE docs SET body='quasar changed' WHERE id=2;DELETE FROM docs WHERE id=3")
	var got []int64
	for rows.Next() {
		var id int64
		if err = rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		got = append(got, id)
	}
	if err = rows.Err(); err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0] != 3 || got[1] != 1 {
		t.Fatal(got)
	}
}

func TestRepeatableReadFulltextOwnWritesAndCurrentReads(t *testing.T) {
	d, err := Open(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	a, b := isolationSession(t, d), isolationSession(t, d)
	isolationExec(t, b, "CREATE TABLE docs(id INTEGER PRIMARY KEY,body TEXT,FULLTEXT KEY ft(body));INSERT INTO docs VALUES(1,'quasar old'),(2,'archive old')")
	query := "SELECT id FROM docs WHERE MATCH(body) AGAINST('quasar' IN BOOLEAN MODE) ORDER BY id"
	check := func(want ...int64) {
		t.Helper()
		r := isolationExec(t, a, query)
		if len(r.Rows) != len(want) {
			t.Fatal(r.Rows, want)
		}
		for i, id := range want {
			if r.Rows[i]["id"].Int != id {
				t.Fatal(r.Rows, want)
			}
		}
	}
	isolationExec(t, a, "BEGIN ISOLATION LEVEL REPEATABLE READ")
	check(1)
	isolationExec(t, b, "UPDATE docs SET body='nebula committed' WHERE id=1;UPDATE docs SET body='quasar committed' WHERE id=2;INSERT INTO docs VALUES(3,'quasar inserted')")
	check(1)
	r := isolationExec(t, a, "SELECT body FROM docs WHERE id=1 FOR UPDATE")
	if r.Rows[0]["body"].Text != "nebula committed" {
		t.Fatal(r.Rows)
	}
	isolationExec(t, a, "UPDATE docs SET body='galactic private' WHERE id=1")
	check()
	r = isolationExec(t, a, "SELECT id FROM docs WHERE MATCH(body) AGAINST('galactic' IN BOOLEAN MODE)")
	if len(r.Rows) != 1 || r.Rows[0]["id"].Int != 1 {
		t.Fatal(r.Rows)
	}
	isolationExec(t, a, "UPDATE docs SET body='quasar private' WHERE id=2;SAVEPOINT keep")
	check(2)
	r = isolationExec(t, a, "SELECT MATCH(body) AGAINST('quasar' IN NATURAL LANGUAGE MODE) AS score FROM docs WHERE id=2")
	want := math.Pow(math.Log10(3.0/2.0), 2)
	if len(r.Rows) != 1 || math.Abs(r.Rows[0]["score"].Real-want) > 1e-12 {
		t.Fatal(r.Rows, want)
	}
	isolationExec(t, a, "UPDATE docs SET body='archive again' WHERE id=2")
	check()
	isolationExec(t, a, "ROLLBACK TO keep")
	check(2)
	isolationExec(t, a, "ROLLBACK")
	check(2, 3)
	if _, err = d.CheckIntegrity(); err != nil {
		t.Fatal(err)
	}
}

func TestVirtualSchemaFulltextAndPrefixesShareLSM(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = d.Close() })
	objectExec(t, d, "CREATE SCHEMA corpus;USE corpus;CREATE TABLE docs(id BIGINT UNSIGNED PRIMARY KEY,body TEXT,title TEXT,KEY prefix_title(title(2)),FULLTEXT KEY ft(body));INSERT INTO docs VALUES(18446744073709551615,'quasar nebula','ab-tail');USE main")
	check := func() {
		t.Helper()
		r := objectExec(t, d, "SELECT title FROM corpus.docs WHERE MATCH(body) AGAINST('quasar' IN BOOLEAN MODE)")
		if len(r.Rows) != 1 || r.Rows[0]["title"].Text != "ab-tail" {
			t.Fatal(r.Rows)
		}
		r = objectExec(t, d, "SELECT body FROM corpus.docs WHERE title='ab-tail'")
		if len(r.Rows) != 1 || r.Rows[0]["body"].Text != "quasar nebula" {
			t.Fatal(r.Rows)
		}
	}
	check()
	if err = d.Close(); err != nil {
		t.Fatal(err)
	}
	d, err = Open(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	check()
	if _, err = d.CheckIntegrity(); err != nil {
		t.Fatal(err)
	}
	if err = d.Close(); err != nil {
		t.Fatal(err)
	}
}
