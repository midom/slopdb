package db

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"

	"slopdb/sql"
)

func TestFullTextConfigurationSharedAcrossDialectsAndReopen(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = d.Close() })
	options := ` WITH (min_token_length=1,max_token_length=32,stopword_mode='custom',stopwords='["STOP"]',ranking='term_frequency')`
	objectExec(t, d, "CREATE TABLE configured(id INTEGER PRIMARY KEY,body TEXT,FULLTEXT KEY inline_ft(body)"+options+")")
	objectExec(t, d, "INSERT INTO configured VALUES(1,'Go café café the STOP alpha-beta'),(2,'unrelated document')")
	objectExec(t, d, "CREATE TABLE online_docs(id INTEGER PRIMARY KEY,body TEXT);INSERT INTO online_docs VALUES(1,'Go café café the STOP alpha-beta');CREATE FULLTEXT INDEX online_ft ON online_docs(body)"+options)
	objectExec(t, d, "CREATE TABLE altered_docs(id INTEGER PRIMARY KEY,body TEXT);INSERT INTO altered_docs VALUES(1,'Go café café the STOP alpha-beta');ALTER TABLE altered_docs ADD FULLTEXT INDEX alter_ft(body)"+options)
	objectExec(t, d, "CREATE TABLE transactional_docs(id INTEGER PRIMARY KEY,body TEXT);INSERT INTO transactional_docs VALUES(1,'Go café café the STOP alpha-beta');BEGIN;CREATE FULLTEXT INDEX transaction_ft ON transactional_docs(body)"+options+";COMMIT")
	objectExec(t, d, "CREATE TABLE copied LIKE configured;INSERT INTO copied VALUES(1,'Go café café the STOP alpha-beta')")
	objectExec(t, d, "CREATE TABLE exact_docs(id INTEGER PRIMARY KEY,body TEXT);INSERT INTO exact_docs VALUES(1,'Go café alpha-beta');CREATE FULLTEXT INDEX exact_ft ON exact_docs(body) WITH (min_token_length=1,normalization='none',tokenizer='whitespace',stopword_mode='none')")
	objectExec(t, d, "CREATE TABLE bounded_docs(id INTEGER PRIMARY KEY,body TEXT,FULLTEXT KEY bounded_ft(body) WITH (min_token_length=2,max_token_length=3,stopword_mode='none'));INSERT INTO bounded_docs VALUES(1,'a bb ccc dddd the')")
	objectExec(t, d, "CREATE TABLE legacy_docs(id INTEGER PRIMARY KEY,body TEXT,FULLTEXT KEY legacy_ft(body));INSERT INTO legacy_docs VALUES(1,'Go café café the STOP alpha-beta')")
	check := func() {
		t.Helper()
		for _, dialect := range []sql.Dialect{sql.DialectNative, sql.DialectMySQL, sql.DialectPostgreSQL} {
			s, e := d.NewSession()
			if e != nil {
				t.Fatal(e)
			}
			if e = s.SetDialect(dialect); e != nil {
				t.Fatal(e)
			}
			query := func(table, term string, want int) {
				t.Helper()
				r, e := s.Exec("SELECT id FROM main." + table + " WHERE FULLTEXT_MATCH(body,'" + term + "') > 0")
				if e != nil || len(r.Rows) != want {
					t.Fatalf("%s %s %q: %v %v", dialect, table, term, r, e)
				}
			}
			for _, table := range []string{"configured", "online_docs", "altered_docs", "transactional_docs", "copied"} {
				for _, term := range []string{"go", "cafe", "the", "alpha", "beta"} {
					query(table, term, 1)
				}
				query(table, "stop", 0)
				r, e := s.Exec("SELECT FULLTEXT_MATCH(body,'cafe') AS rank FROM main." + table + " WHERE id=1")
				if e != nil || r.Rows[0]["rank"].Real != 2 {
					t.Fatalf("term-frequency rank %s: %v %v", dialect, r, e)
				}
				r, e = s.Exec("SELECT id FROM main." + table + " WHERE MATCH(body) AGAINST('+go +\"cafe the\"' IN BOOLEAN MODE)")
				if e != nil || len(r.Rows) != 1 {
					t.Fatalf("phrase/position %s: %v %v", dialect, r, e)
				}
			}
			query("exact_docs", "Go", 1)
			query("exact_docs", "go", 0)
			query("exact_docs", "café", 1)
			query("exact_docs", "cafe", 0)
			query("exact_docs", "alpha-beta", 1)
			query("exact_docs", "alpha", 0)
			query("bounded_docs", "bb", 1)
			query("bounded_docs", "ccc", 1)
			query("bounded_docs", "the", 1)
			query("bounded_docs", "a", 0)
			query("bounded_docs", "dddd", 0)
			query("legacy_docs", "go", 0)
			query("legacy_docs", "the", 0)
			query("legacy_docs", "cafe", 1)
			s.Close()
		}
		if _, e := d.CheckIntegrity(); e != nil {
			t.Fatal(e)
		}
	}
	check()
	indexSQL := d.GetIndexSQL("online_ft")
	if !strings.Contains(indexSQL, "stopword_mode='custom'") || !strings.Contains(indexSQL, "min_token_length=1") {
		t.Fatal("index SQL lost configuration", indexSQL)
	}
	objectExec(t, d, "ALTER INDEX online_ft REBUILD")
	check()
	if err = d.Close(); err != nil {
		t.Fatal(err)
	}
	d, err = Open(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	check()
	// Nil configuration remains absent from legacy catalog metadata. Explicit
	// configuration and normalized custom stopwords survive JSON and reopen.
	data, ok, e := d.lsm.Get([]byte("\x00__schema_indexes__"))
	if e != nil || !ok {
		t.Fatal(ok, e)
	}
	var catalog map[string]map[string]json.RawMessage
	if e = json.Unmarshal(data, &catalog); e != nil {
		t.Fatal(e)
	}
	if _, ok = catalog["legacy_ft"]["fulltext_config"]; ok {
		t.Fatal("legacy profile was rewritten")
	}
	var config sql.FullTextConfig
	if e = json.Unmarshal(catalog["online_ft"]["fulltext_config"], &config); e != nil {
		t.Fatal(e)
	}
	if config.MinTokenLength != 1 || config.Ranking != "term_frequency" || !reflect.DeepEqual(config.Stopwords, []string{"stop"}) {
		t.Fatal(config)
	}
	objectExec(t, d, "UPDATE configured SET body='Go small revised' WHERE id=1")
	if r := objectExec(t, d, "SELECT id FROM configured WHERE FULLTEXT_MATCH(body,'cafe')"); len(r.Rows) != 0 {
		t.Fatal("stale configured posting", r.Rows)
	}
	objectExec(t, d, "DELETE FROM configured WHERE id=1")
	if _, err = d.CheckIntegrity(); err != nil {
		t.Fatal(err)
	}
}

func TestFullTextConfigurationRejectsInvalidDDLAtomically(t *testing.T) {
	d := projectionDatabase(t)
	objectExec(t, d, "CREATE TABLE docs(id INTEGER PRIMARY KEY,body TEXT);INSERT INTO docs VALUES(1,'valid document')")
	for _, q := range []string{
		"CREATE FULLTEXT INDEX invalid ON docs(body) WITH (tokenizer='unavailable')",
		"CREATE FULLTEXT INDEX invalid ON docs(body) WITH (normalization='unavailable')",
		"CREATE FULLTEXT INDEX invalid ON docs(body) WITH (min_token_length=8,max_token_length=2)",
		"CREATE FULLTEXT INDEX invalid ON docs(body) WITH (min_token_length=0)",
		"CREATE FULLTEXT INDEX invalid ON docs(body) WITH (ranking='unavailable')",
		"CREATE FULLTEXT INDEX invalid ON docs(body) WITH (stopword_mode='none',stopwords='[\"x\"]')",
		"CREATE FULLTEXT INDEX invalid ON docs(body) WITH (stopwords='[\"two words\"]')",
		"CREATE FULLTEXT INDEX invalid ON docs(body) WITH (stopwords='{}')",
		"CREATE INDEX invalid ON docs(body) WITH (min_token_length=1)",
		"ALTER TABLE docs ADD FULLTEXT invalid(body) WITH (min_token_length=1,min_token_length=2)",
	} {
		if _, e := d.Exec(q); e == nil {
			t.Fatal("invalid configuration accepted", q)
		}
		if d.GetIndexSQL("invalid") != "" {
			t.Fatal("failed index published", q)
		}
	}
	objectExec(t, d, "CREATE FULLTEXT INDEX valid ON docs(body) WITH (min_token_length=1,stopword_mode='none')")
	if _, e := d.CheckIntegrity(); e != nil {
		t.Fatal(e)
	}
}

func TestFullTextDifferentConfigurationsRequireUnambiguousColumnLists(t *testing.T) {
	d := projectionDatabase(t)
	objectExec(t, d, "CREATE TABLE docs(id INTEGER PRIMARY KEY,body TEXT);INSERT INTO docs VALUES(1,'go quasar');CREATE FULLTEXT INDEX original ON docs(body);CREATE FULLTEXT INDEX equivalent ON docs(body) WITH (min_token_length=4,max_token_length=84)")
	if r := objectExec(t, d, "SELECT id FROM docs WHERE FULLTEXT_MATCH(body,'quasar')"); len(r.Rows) != 1 {
		t.Fatal(r.Rows)
	}
	objectExec(t, d, "CREATE FULLTEXT INDEX different ON docs(body) WITH (min_token_length=1,stopword_mode='none')")
	if _, err := d.Exec("SELECT id FROM docs WHERE FULLTEXT_MATCH(body,'go')"); err == nil || !strings.Contains(err.Error(), "ambiguous FULLTEXT") {
		t.Fatal("different analysis selected arbitrarily", err)
	}
}

func TestFullTextImmutableAnalyzerSharedByConcurrentSessions(t *testing.T) {
	d := projectionDatabase(t)
	objectExec(t, d, "CREATE TABLE docs(id INTEGER PRIMARY KEY,body TEXT,FULLTEXT KEY ft(body) WITH (min_token_length=1,stopword_mode='custom',stopwords='[\"ignore\"]'));INSERT INTO docs VALUES(1,'go café'),(2,'other text')")
	var wg sync.WaitGroup
	start := make(chan struct{})
	failures := make(chan error, 4)
	for _, dialect := range []sql.Dialect{sql.DialectNative, sql.DialectMySQL, sql.DialectPostgreSQL} {
		wg.Add(1)
		go func(dialect sql.Dialect) {
			defer wg.Done()
			s, e := d.NewSession()
			if e != nil {
				failures <- e
				return
			}
			defer s.Close()
			if e = s.SetDialect(dialect); e != nil {
				failures <- e
				return
			}
			p, e := s.Prepare("SELECT id FROM main.docs WHERE id=1 AND FULLTEXT_MATCH(body,?)")
			if e != nil {
				failures <- e
				return
			}
			defer p.Close()
			<-start
			for i := 0; i < 20; i++ {
				r, e := p.Exec("go")
				if e != nil {
					failures <- e
					return
				}
				if len(r.Rows) != 1 || r.Rows[0]["id"].Int != 1 {
					failures <- fmt.Errorf("%s incorrect answer: %v", dialect, r.Rows)
					return
				}
			}
		}(dialect)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		s, e := d.NewSession()
		if e != nil {
			failures <- e
			return
		}
		defer s.Close()
		<-start
		for i := 0; i < 20; i++ {
			if _, e = s.Exec("UPDATE docs SET body='go revised' WHERE id=2"); e != nil {
				failures <- e
				return
			}
		}
	}()
	close(start)
	wg.Wait()
	close(failures)
	for e := range failures {
		t.Error(e)
	}
	if _, e := d.CheckIntegrity(); e != nil {
		t.Fatal(e)
	}
}
