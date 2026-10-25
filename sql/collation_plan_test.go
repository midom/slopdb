package sql

import (
	"errors"
	"strings"
	"testing"
)

func TestOnlineIndexRevalidatesColumnAndFullTextAnalysis(t *testing.T) {
	info := &TableInfo{Name: "docs", Columns: []ColumnDef{{Name: "word", ColType: ColText, Collation: "UTF8MB4_UNICODE_520_CI"}}}
	cfg := &FullTextConfig{MinTokenLength: 4}
	makeBuild := func(fulltext bool) *OnlineIndexBuild {
		t.Helper()
		idx := newIndexInfo("idx", "docs", []string{"word"}, false)
		idx.Fulltext = fulltext
		if fulltext {
			if e := configureFullTextIndex(idx, cfg); e != nil {
				t.Fatal(e)
			}
		}
		return &OnlineIndexBuild{idx: idx, info: info}
	}
	s := &Schema{collation: BinaryCollation, tables: map[string]*TableInfo{"docs": info}, indexes: map[string]*IndexInfo{"idx": {Name: "idx"}}}
	// A missing/stale column map is unsafe even when database collation and
	// BINARY flags have not changed; reject it before publishing staged keys.
	b := makeBuild(false)
	err := s.FinishOnlineIndex(&CreateIndexStmt{Index: "idx", Table: "docs", Columns: []string{"word"}}, b)
	if !errors.Is(err, ErrConflict) || !strings.Contains(err.Error(), "collation changed") {
		t.Fatal("stale column collation reached publication", err)
	}
	b = makeBuild(true)
	s.bindIndexCollation(b.idx)
	err = s.FinishOnlineIndex(&CreateIndexStmt{Index: "idx", Table: "docs", Fulltext: true, FullTextConfig: &FullTextConfig{MinTokenLength: 1}}, b)
	if !errors.Is(err, ErrConflict) || !strings.Contains(err.Error(), "full-text configuration changed") {
		t.Fatal("changed full-text analyzer reached publication", err)
	}
	b = makeBuild(true)
	s.bindIndexCollation(b.idx)
	err = s.FinishOnlineIndex(&CreateIndexStmt{Index: "idx", Table: "docs", Fulltext: true, FullTextConfig: &FullTextConfig{}}, b)
	if err == nil || errors.Is(err, ErrConflict) || !strings.Contains(err.Error(), "already exists") {
		t.Fatal("equivalent analyzer rejected before ordinary publication checks", err)
	}
}

func TestCatalogCopiesOwnCollationMapsAndFullTextDeclarations(t *testing.T) {
	cfg, e := cloneFullTextConfig(&FullTextConfig{Stopwords: []string{"first"}})
	if e != nil {
		t.Fatal(e)
	}
	s := &Schema{tables: map[string]*TableInfo{"t": {Name: "t", Columns: []ColumnDef{{Name: "word", ColType: ColText, Collation: "UTF8MB4_UNICODE_520_CI"}}, Constraints: []ConstraintDef{{FullTextConfig: cfg}}}}, indexes: map[string]*IndexInfo{"idx": {Name: "idx", FullTextConfig: cfg, columnCollations: map[string]Collation{"word": "UTF8MB4_UNICODE_520_CI"}, binaryColumns: map[string]bool{"exact": true}}}}
	before := s.captureState()
	s.ensureCatalogWritable()
	s.indexes["idx"].columnCollations["word"] = BinaryCollation
	s.indexes["idx"].binaryColumns["exact"] = false
	s.indexes["idx"].FullTextConfig.Stopwords[0] = "changed"
	s.tables["t"].Columns[0].Collation = BinaryCollation
	s.tables["t"].Constraints[0].FullTextConfig.Stopwords[0] = "changed-constraint"
	if before.indexes["idx"].columnCollations["word"] != "UTF8MB4_UNICODE_520_CI" || !before.indexes["idx"].binaryColumns["exact"] || before.indexes["idx"].FullTextConfig.Stopwords[0] != "first" || before.tables["t"].Columns[0].Collation != "UTF8MB4_UNICODE_520_CI" || before.tables["t"].Constraints[0].FullTextConfig.Stopwords[0] != "first" {
		t.Fatal("DDL copy mutated captured catalog")
	}
}

func TestCollatedPayloadDoesNotDisableNumericJoinKeys(t *testing.T) {
	info := &TableInfo{Name: "docs", Columns: []ColumnDef{{Name: "id", ColType: ColInteger}, {Name: "word", ColType: ColText, Collation: "UTF8MB4_UNICODE_520_CI"}}, PKColumns: []string{"id"}}
	p := &joinPlan{relations: []*joinRelation{{alias: "left", info: info}, {alias: "right", info: info}}, owners: map[string]int{"left.id": 0, "left.word": 0}}
	numeric := []joinKeyPair{{left: "left.id", right: "id"}}
	if p.pairsHaveExplicitCollation(1, numeric) {
		t.Fatal("unused collated payload disabled numeric join")
	}
	if !p.pairsHaveExplicitCollation(1, []joinKeyPair{{left: "left.word", right: "word"}}) {
		t.Fatal("collated key used bytewise join")
	}
	s := &Schema{collation: BinaryCollation}
	reader := s.joinReader(p.relations[1], numeric)
	if len(reader.pairs) != 1 {
		t.Fatal("numeric join recipe discarded")
	}
}
