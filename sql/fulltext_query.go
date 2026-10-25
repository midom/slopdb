package sql

import (
	"encoding/binary"
	"fmt"
	"math"
	"slopdb/storage"
	"strings"
	"unicode"
)

const fulltextFunction = "__FULLTEXT_MATCH"

func validateFulltextFunction(f *FuncCallExpr) error {
	if len(f.Args) < 3 || f.Distinct || f.Window != nil {
		return fmt.Errorf("invalid MATCH expression")
	}
	seen := map[string]bool{}
	for _, e := range f.Args[:len(f.Args)-2] {
		c, ok := e.(*ColumnRefExpr)
		if !ok || seen[c.Name] {
			return fmt.Errorf("MATCH requires distinct column names")
		}
		seen[c.Name] = true
	}
	if l, ok := f.Args[len(f.Args)-1].(*LiteralExpr); !ok || (l.Value.Text != "BOOLEAN" && l.Value.Text != "NATURAL") {
		return fmt.Errorf("invalid AGAINST search mode")
	}
	return walkExpression(f.Args[len(f.Args)-2], func(e Expression) (bool, error) {
		switch e.(type) {
		case *ColumnRefExpr, *SubqueryExpr, *ExistsExpr:
			return true, fmt.Errorf("AGAINST search must be constant during a statement")
		}
		return false, nil
	})
}

func validateFulltextIndex(info *TableInfo, idx *IndexInfo) error {
	if !idx.Fulltext {
		if idx.FullTextConfig != nil {
			return fmt.Errorf("full-text configuration requires a FULLTEXT index")
		}
		return nil
	}
	if _, err := fullTextAnalyzerForIndex(idx); err != nil {
		return err
	}
	if idx.IsUnique {
		return fmt.Errorf("FULLTEXT indexes cannot be UNIQUE")
	}
	if idx.hasPrefixes() {
		return fmt.Errorf("FULLTEXT indexes do not support prefix lengths")
	}
	for _, name := range idx.columnNames() {
		found := false
		for _, c := range info.Columns {
			if c.Name == name {
				found = true
				if c.ColType != ColText {
					return fmt.Errorf("FULLTEXT requires TEXT columns: %s", name)
				}
			}
		}
		if !found {
			return fmt.Errorf("no such FULLTEXT column: %s", name)
		}
	}
	return nil
}

type fulltextBinding struct {
	table *TableInfo
	index *IndexInfo
}

func (s *Schema) resolveFulltextIndex(f *FuncCallExpr, stmt *SelectStmt) (*TableInfo, *IndexInfo, error) {
	if err := validateFulltextFunction(f); err != nil {
		return nil, nil, err
	}
	candidates := map[string]string{}
	if stmt != nil {
		if s.tables[stmt.From] != nil {
			a := stmt.FromAlias
			if a == "" {
				a = stmt.From
			}
			candidates[a] = stmt.From
		}
		for _, j := range stmt.Joins {
			if s.tables[j.Table] != nil {
				a := j.Alias
				if a == "" {
					a = j.Table
				}
				candidates[a] = j.Table
			}
		}
	} else {
		for name := range s.tables {
			candidates[name] = name
		}
	}
	for alias, table := range candidates {
		names := make([]string, 0, len(f.Args)-2)
		valid := true
		for _, e := range f.Args[:len(f.Args)-2] {
			c := e.(*ColumnRefExpr)
			name := c.Name
			if dot := strings.LastIndexByte(name, '.'); dot >= 0 {
				if name[:dot] != alias && name[:dot] != table {
					valid = false
					break
				}
				name = name[dot+1:]
			}
			names = append(names, name)
		}
		if !valid {
			continue
		}
		idx, err := s.selectFullTextIndex(table, names)
		if err != nil {
			return nil, nil, err
		}
		if idx != nil {
			return s.tables[table], idx, nil
		}
	}
	return nil, nil, fmt.Errorf("cannot find FULLTEXT index matching MATCH column list")
}

func (s *Schema) bindFulltextExpressions(stmt *SelectStmt) error {
	bind := func(e Expression) (bool, error) {
		if f, ok := e.(*FuncCallExpr); ok && f.Name == fulltextFunction {
			info, idx, err := s.resolveFulltextIndex(f, stmt)
			if err != nil {
				return true, err
			}
			if s.fulltextBindings == nil {
				s.fulltextBindings = map[*FuncCallExpr]fulltextBinding{}
			}
			s.fulltextBindings[f] = fulltextBinding{info, idx}
		}
		return false, nil
	}
	var expressions []Expression
	for _, c := range stmt.Columns {
		expressions = append(expressions, c.Expr)
	}
	expressions = append(expressions, stmt.Where, stmt.Having)
	expressions = append(expressions, stmt.GroupBy...)
	for _, join := range stmt.Joins {
		expressions = append(expressions, join.On)
	}
	for _, c := range stmt.OrderBy {
		expressions = append(expressions, c.Expr)
	}
	for _, e := range expressions {
		if err := walkExpression(e, bind); err != nil {
			return err
		}
	}
	return nil
}

type fulltextTerm struct {
	words            []string
	prefix           bool
	require, exclude bool
	weight           float64
	children         []fulltextTerm
}
type fulltextQuery struct {
	terms     []fulltextTerm
	boolean   bool
	analysis  *fullTextAnalyzer
	docs      int64
	frequency map[string]int64
}

func (q *fulltextQuery) indexed(word string) bool            { return q.analysis.indexed(word) }
func (q *fulltextQuery) words(text string) ([]string, error) { return q.analysis.words(text, 1<<24) }

func parseBooleanFulltext(text string, q *fulltextQuery) ([]fulltextTerm, error) {
	nodes, depth := 0, 0
	var parse func(int, bool) ([]fulltextTerm, int, error)
	parse = func(pos int, group bool) ([]fulltextTerm, int, error) {
		depth++
		defer func() { depth-- }()
		if depth > 64 {
			return nil, pos, fmt.Errorf("fulltext groups exceed nesting limit")
		}
		var terms []fulltextTerm
		for pos < len(text) {
			if unicode.IsSpace(rune(text[pos])) {
				pos++
				continue
			}
			if text[pos] == ')' {
				if !group {
					return nil, pos, fmt.Errorf("unmatched fulltext closing parenthesis")
				}
				return terms, pos + 1, nil
			}
			term := fulltextTerm{weight: 1}
			for pos < len(text) && strings.ContainsRune("+-~><", rune(text[pos])) {
				switch text[pos] {
				case '+':
					term.require = true
				case '-':
					term.exclude = true
				case '~':
					term.weight = -term.weight
				case '>':
					term.weight *= 1.5
				case '<':
					term.weight *= .5
				}
				pos++
			}
			if math.IsInf(term.weight, 0) || math.Abs(term.weight) > 1e6 {
				return nil, pos, fmt.Errorf("fulltext term weight exceeds limit")
			}
			if pos >= len(text) {
				return nil, pos, fmt.Errorf("fulltext operator without a term")
			}
			if text[pos] == '(' {
				var err error
				groupStart := pos + 1
				term.children, pos, err = parse(pos+1, true)
				if err != nil {
					return nil, pos, err
				}
				if len(term.children) == 0 && strings.TrimSpace(text[groupStart:pos-1]) == "" {
					return nil, pos, fmt.Errorf("empty fulltext group")
				}
			} else if text[pos] == '"' {
				pos++
				start := pos
				for pos < len(text) && text[pos] != '"' {
					pos++
				}
				if pos == len(text) {
					return nil, pos, fmt.Errorf("unterminated fulltext phrase")
				}
				var err error
				term.words, err = q.words(text[start:pos])
				if err != nil {
					return nil, pos, err
				}
				pos++
			} else {
				start := pos
				for pos < len(text) && !strings.ContainsRune(" \t\r\n()", rune(text[pos])) {
					pos++
				}
				word := text[start:pos]
				term.prefix = strings.HasSuffix(word, "*")
				word = strings.TrimSuffix(word, "*")
				var err error
				term.words, err = q.words(word)
				if err != nil {
					return nil, pos, err
				}
			}
			searchable := term.prefix || len(term.children) > 0
			for _, word := range term.words {
				searchable = searchable || q.indexed(word)
			}
			if searchable && (len(term.words) > 0 || len(term.children) > 0) {
				nodes++
				terms = append(terms, term)
			}
			if nodes > 1024 {
				return nil, pos, fmt.Errorf("fulltext query has too many terms")
			}
		}
		if group {
			return nil, pos, fmt.Errorf("unterminated fulltext group")
		}
		return terms, pos, nil
	}
	terms, _, err := parse(0, false)
	return terms, err
}

func fulltextTermHits(t fulltextTerm, words []string, q *fulltextQuery) int {
	if len(t.words) == 0 {
		return 0
	}
	indexed := false
	for _, w := range t.words {
		indexed = indexed || q.indexed(w) || t.prefix
	}
	if !indexed {
		return 0
	}
	count := 0
	for i := 0; i+len(t.words) <= len(words); i++ {
		matched := true
		for j, w := range t.words {
			if t.prefix && j == len(t.words)-1 {
				matched = matched && q.indexed(words[i+j]) && strings.HasPrefix(words[i+j], w)
			} else {
				matched = matched && words[i+j] == w
			}
		}
		if matched {
			count++
		}
	}
	return count
}
func scoreBooleanFulltext(terms []fulltextTerm, words []string, q *fulltextQuery) (bool, float64) {
	required, positive, hits := 0, 0, 0
	score := 0.0
	for _, t := range terms {
		n := 0
		weight := 0.0
		if len(t.children) > 0 {
			ok, s := scoreBooleanFulltext(t.children, words, q)
			if ok {
				n = 1
				weight = s
			}
		} else {
			n = fulltextTermHits(t, words, q)
			weight = float64(n)
		}
		if t.exclude {
			if n > 0 {
				return false, 0
			}
			continue
		}
		positive++
		if t.require {
			required++
			if n == 0 {
				return false, 0
			}
		}
		if n > 0 {
			hits++
			score += t.weight * weight
		}
	}
	return positive > 0 && (required > 0 || hits > 0), score
}

// Fulltext scoring uses snapshot-safe inverted posting statistics and verifies
// candidate document tokens without scanning other table rows.
func (s *Schema) compiledFulltextQuery(f *FuncCallExpr, binding fulltextBinding, row map[string]Value) (*fulltextQuery, error) {
	if binding.index.FulltextVersion != fulltextIndexVersion {
		return nil, fmt.Errorf("FULLTEXT index %s requires rebuild", binding.index.Name)
	}
	queryValue, err := s.evalExpr(f.Args[len(f.Args)-2], row)
	if err != nil {
		return nil, err
	}
	if queryValue.Null {
		return nil, nil
	}
	text := fullTextInput(queryValue)
	if len(text) > 64<<10 {
		return nil, fmt.Errorf("fulltext query exceeds 64 KiB")
	}
	mode := f.Args[len(f.Args)-1].(*LiteralExpr).Value.Text
	key := binding.index.Name + "\x00" + mode + "\x00" + text
	q := s.fulltextQueries[key]
	if q == nil {
		analysis, e := fullTextAnalyzerForIndex(binding.index)
		if e != nil {
			return nil, e
		}
		q = &fulltextQuery{boolean: mode == "BOOLEAN", analysis: analysis, frequency: map[string]int64{}}
		if q.boolean {
			q.terms, err = parseBooleanFulltext(text, q)
		} else {
			words, e := q.words(text)
			if e != nil {
				return nil, e
			}
			for _, w := range words {
				if q.indexed(w) {
					q.terms = append(q.terms, fulltextTerm{words: []string{w}, weight: 1})
					if len(q.terms) > 1024 {
						return nil, fmt.Errorf("fulltext query has too many terms")
					}
				}
			}
		}
		if err != nil {
			return nil, err
		}
		if !q.boolean && len(q.terms) > 0 && q.analysis.config.Ranking == "tf_idf" {
			if err = s.fulltextCorpus(q, binding); err != nil {
				return nil, err
			}
		}
		if s.fulltextQueries == nil {
			s.fulltextQueries = map[string]*fulltextQuery{}
		}
		cost := int64(len(text))*8 + int64(len(q.terms))*128 + 256
		if len(s.fulltextQueries) < 128 && s.fulltextQueryBytes+cost <= s.memoryBudget()/8 {
			s.fulltextQueries[key] = q
			s.fulltextQueryBytes += cost
		}
	}
	return q, nil
}

func (s *Schema) evalFulltext(f *FuncCallExpr, row map[string]Value) (Value, error) {
	if err := validateFulltextFunction(f); err != nil {
		return NewNullValue(), err
	}
	binding, ok := s.fulltextBindings[f]
	if !ok {
		info, idx, err := s.resolveFulltextIndex(f, nil)
		if err != nil {
			return NewNullValue(), err
		}
		binding = fulltextBinding{info, idx}
	}
	q, err := s.compiledFulltextQuery(f, binding, row)
	if err != nil {
		return NewNullValue(), err
	}
	if q == nil {
		return NewNullValue(), nil
	}
	var words []string
	for _, expr := range f.Args[:len(f.Args)-2] {
		v, err := s.evalExpr(expr, row)
		if err != nil {
			return NewNullValue(), err
		}
		if !v.Null {
			part, err := q.analysis.words(v.String(), s.memoryBudget()-int64(len(words))*16)
			if err != nil {
				return NewNullValue(), err
			}
			words = append(words, part...)
			words = append(words, "")
		}
	}
	if q.boolean {
		ok, score := scoreBooleanFulltext(q.terms, words, q)
		if !ok {
			return NewRealValue(0), nil
		}
		return NewRealValue(score), nil
	}
	score := 0.0
	for _, term := range q.terms {
		n := fulltextTermHits(term, words, q)
		if n == 0 {
			continue
		}
		if q.analysis.config.Ranking == "term_frequency" {
			score += float64(n)
			continue
		}
		df := q.frequency[term.words[0]]
		idf := math.Log10(float64(q.docs+1) / float64(df+1))
		if idf == 0 {
			idf = 1e-9
		}
		score += float64(n) * idf * idf
	}
	return NewRealValue(score), nil
}

// Corpus counts and document frequencies come entirely from the inverted
// index. Only a transaction's overwritten rows need residual revalidation when
// a repeatable-read snapshot still contains their historical posting entries.
func (s *Schema) fulltextCorpus(q *fulltextQuery, b fulltextBinding) error {
	prefix := fulltextDocumentPrefix(b.index)
	it := s.newRangeIterator(prefix, prefixSuccessor(prefix), false)
	for ; it.Valid(); it.Next() {
		if err := s.checkInterrupt(); err != nil {
			it.Close()
			return err
		}
		_, n := binary.Uvarint(it.Value())
		if n <= 0 || n != len(it.Value()) {
			it.Close()
			return fmt.Errorf("malformed FULLTEXT document marker")
		}
		key := it.Key().UserKey[len(prefix):]
		if pos, ok := s.txLatest[string(key)]; ok && s.txWrites[pos].del {
			continue
		}
		q.docs++
	}
	err := storage.IteratorError(it)
	it.Close()
	if err != nil {
		return err
	}
	for _, term := range q.terms {
		word := term.words[0]
		if _, exists := q.frequency[word]; exists {
			continue
		}
		prefix := fulltextPostingPrefix(b.index, word)
		it := s.newRangeIterator(prefix, prefixSuccessor(prefix), false)
		count := int64(0)
		for ; it.Valid(); it.Next() {
			if err := s.checkInterrupt(); err != nil {
				it.Close()
				return err
			}
			if err := validateFulltextPositions(it.Value()); err != nil {
				it.Close()
				return err
			}
			rowkey := it.Key().UserKey[len(prefix):]
			if _, changed := s.txLatest[string(rowkey)]; changed {
				value, found, e := s.schemaGet(rowkey)
				if e != nil {
					it.Close()
					return e
				}
				if !found {
					continue
				}
				row := decodeRow(value, b.table)
				matched := false
				for _, name := range b.index.columnNames() {
					words, e := q.analysis.words(row[name].String(), s.memoryBudget())
					if e != nil {
						it.Close()
						return e
					}
					for _, token := range words {
						matched = matched || token == word
					}
				}
				if !matched {
					continue
				}
			}
			count++
		}
		err := storage.IteratorError(it)
		it.Close()
		if err != nil {
			return err
		}
		q.frequency[word] = count
	}
	return nil
}
