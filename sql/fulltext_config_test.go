package sql

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestFullTextConfigOwnsAnalysisAndValidatesPersistentVersions(t *testing.T) {
	original := &FullTextConfig{MinTokenLength: 1, Stopwords: []string{"CAFÉ"}, Ranking: "term_frequency"}
	idx := &IndexInfo{Fulltext: true}
	if err := configureFullTextIndex(idx, original); err != nil {
		t.Fatal(err)
	}
	original.Stopwords[0] = "different"
	original.MinTokenLength = 20
	a, e := fullTextAnalyzerForIndex(idx)
	if e != nil {
		t.Fatal(e)
	}
	if a.indexed("cafe") || !a.indexed("go") {
		t.Fatal("caller changed private analyzer")
	}
	words, e := a.words("'Go' CAFÉ alpha-beta", 1<<20)
	if e != nil || !reflect.DeepEqual(words, []string{"go", "cafe", "alpha", "beta"}) {
		t.Fatal(words, e)
	}
	bytes, e := json.Marshal(idx.FullTextConfig)
	if e != nil {
		t.Fatal(e)
	}
	var restored FullTextConfig
	if e = json.Unmarshal(bytes, &restored); e != nil {
		t.Fatal(e)
	}
	decoded, e := fullTextAnalyzerForIndex(&IndexInfo{Fulltext: true, FullTextConfig: &restored})
	if e != nil || decoded.indexed("cafe") || !decoded.indexed("go") {
		t.Fatal(decoded, e)
	}
	for _, data := range []string{`{"version":2}`, `{"tokenizer":"missing"}`, `{"stopwords":["two words"]}`, `{"min_token_length":-1}`, `{"ranking":"missing"}`} {
		if e = json.Unmarshal([]byte(data), &restored); e == nil {
			t.Fatal("invalid persistent analyzer accepted", data)
		}
	}
	if _, e = a.words("budget exceeded", 4); e == nil {
		t.Fatal("tokenizer exceeded its memory budget")
	}
	if e = configureFullTextIndex(&IndexInfo{}, &FullTextConfig{}); e == nil {
		t.Fatal("ordinary index accepted fulltext config")
	}
}
