package sql

import (
	"bytes"
	"fmt"
	"math"
	"strings"
	"unicode"
	"unicode/utf8"

	"golang.org/x/text/cases"
	"golang.org/x/text/unicode/norm"
	"slopdb/storage"
)

const fulltextFunction = "__FULLTEXT_MATCH"

// MATCH is represented by an ordinary private expression tree so prepared
// binding, expression walking and stored-query reparsing retain their usual
// ownership. The final two arguments are the search expression and mode.
func (p *Parser) parseFulltextMatch() (Expression, error) {
	p.advance()
	if err := p.expect(TokLParen); err != nil {
		return nil, err
	}
	f := &FuncCallExpr{Name: fulltextFunction}
	for {
		if p.cur.Type != TokIdent {
			return nil, fmt.Errorf("MATCH requires column names")
		}
		name := strings.ToLower(p.cur.Value)
		p.advance()
		if p.cur.Type == TokDot {
			p.advance()
			if p.cur.Type != TokIdent {
				return nil, fmt.Errorf("MATCH requires a column after qualifier")
			}
			name += "." + strings.ToLower(p.cur.Value)
			p.advance()
		}
		f.Args = append(f.Args, &ColumnRefExpr{Name: name})
		if p.cur.Type != TokComma {
			break
		}
		p.advance()
	}
	if err := p.expect(TokRParen); err != nil {
		return nil, err
	}
	if !p.optionalAlterWord("AGAINST") {
		return nil, fmt.Errorf("expected AGAINST after MATCH")
	}
	if err := p.expect(TokLParen); err != nil {
		return nil, err
	}
	q, err := p.parseBitwise()
	if err != nil {
		return nil, err
	}
	mode := "NATURAL"
	if p.optionalAlterWord("IN") {
		switch {
		case p.optionalAlterWord("BOOLEAN"):
			mode = "BOOLEAN"
		case p.optionalAlterWord("NATURAL"):
			if !p.optionalAlterWord("LANGUAGE") {
				return nil, fmt.Errorf("expected LANGUAGE in fulltext mode")
			}
		default:
			return nil, fmt.Errorf("expected BOOLEAN or NATURAL LANGUAGE mode")
		}
		if !p.optionalAlterWord("MODE") {
			return nil, fmt.Errorf("expected MODE in AGAINST")
		}
	}
	if err := p.expect(TokRParen); err != nil {
		return nil, err
	}
	f.Args = append(f.Args, q, &LiteralExpr{Value: NewTextValue(mode)})
	return f, nil
}

func formatFulltextMatch(f *FuncCallExpr) string {
	if len(f.Args) < 3 {
		return "MATCH() AGAINST('')"
	}
	names := make([]string, len(f.Args)-2)
	for i, e := range f.Args[:len(names)] {
		names[i] = formatExpr(e)
	}
	mode := "IN NATURAL LANGUAGE MODE"
	if l, ok := f.Args[len(f.Args)-1].(*LiteralExpr); ok && l.Value.Text == "BOOLEAN" {
		mode = "IN BOOLEAN MODE"
	}
	return "MATCH(" + strings.Join(names, ", ") + ") AGAINST(" + formatExpr(f.Args[len(f.Args)-2]) + " " + mode + ")"
}

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
		return nil
	}
	if idx.IsUnique {
		return fmt.Errorf("FULLTEXT indexes cannot be UNIQUE")
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
		for _, idx := range s.tableIndexes(table) {
			if !idx.Fulltext || len(idx.columnNames()) != len(names) {
				continue
			}
			same := true
			for _, name := range names {
				found := false
				for _, col := range idx.columnNames() {
					found = found || name == col
				}
				same = same && found
			}
			if same {
				return s.tables[table], idx, nil
			}
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
	terms           []fulltextTerm
	boolean, myisam bool
	minWord         int
	docs            int64
	frequency       map[string]int64
}

func foldFulltext(text string) string {
	text = norm.NFD.String(cases.Fold().String(text))
	return strings.Map(func(r rune) rune {
		if unicode.Is(unicode.Mn, r) {
			return -1
		}
		return r
	}, text)
}
func fulltextWords(text string) []string {
	return strings.FieldsFunc(foldFulltext(text), func(r rune) bool { return !unicode.IsLetter(r) && !unicode.IsDigit(r) && r != '_' && r != '\'' })
}
func (q *fulltextQuery) indexed(word string) bool {
	n := utf8.RuneCountInString(word)
	if n < q.minWord || n > 84 {
		return false
	}
	if q.myisam {
		return !myisamStopwords[word]
	}
	return !innodbStopwords[word]
}

func parseBooleanFulltext(text string, q *fulltextQuery) ([]fulltextTerm, error) {
	var parse func(int, bool) ([]fulltextTerm, int, error)
	parse = func(pos int, group bool) ([]fulltextTerm, int, error) {
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
			if pos >= len(text) {
				return nil, pos, fmt.Errorf("fulltext operator without a term")
			}
			if text[pos] == '(' {
				var err error
				term.children, pos, err = parse(pos+1, true)
				if err != nil {
					return nil, pos, err
				}
				if len(term.children) == 0 {
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
				term.words = fulltextWords(text[start:pos])
				pos++
			} else {
				start := pos
				for pos < len(text) && !strings.ContainsRune(" \t\r\n()", rune(text[pos])) {
					pos++
				}
				word := text[start:pos]
				term.prefix = strings.HasSuffix(word, "*")
				word = strings.TrimSuffix(word, "*")
				term.words = fulltextWords(word)
			}
			if len(term.words) > 0 || len(term.children) > 0 {
				terms = append(terms, term)
			}
			if len(terms) > 1024 {
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
				matched = matched && strings.HasPrefix(words[i+j], w)
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

// Fulltext execution uses a snapshot-safe document scan and bounded per-query
// corpus counters. Persistent FULLTEXT document indexes use the normal atomic
// index maintenance; an inverted-list acceleration can be added independently
// without changing SQL answers or the persistent catalog format.
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
	queryValue, err := s.evalExpr(f.Args[len(f.Args)-2], row)
	if err != nil {
		return NewNullValue(), err
	}
	if queryValue.Null {
		return NewNullValue(), nil
	}
	text := queryValue.String()
	if len(text) > 64<<10 {
		return NewNullValue(), fmt.Errorf("fulltext query exceeds 64 KiB")
	}
	mode := f.Args[len(f.Args)-1].(*LiteralExpr).Value.Text
	key := binding.index.Name + "\x00" + mode + "\x00" + text
	q := s.fulltextQueries[key]
	if q == nil {
		q = &fulltextQuery{boolean: mode == "BOOLEAN", minWord: 3, frequency: map[string]int64{}}
		// Engine metadata is resolved by a helper to keep legacy catalog entries valid.
		q.myisam = fulltextMyISAM(binding.table)
		if q.myisam {
			q.minWord = 4
		}
		if q.boolean {
			q.terms, err = parseBooleanFulltext(text, q)
		} else {
			for _, w := range fulltextWords(text) {
				if q.indexed(w) {
					q.terms = append(q.terms, fulltextTerm{words: []string{w}, weight: 1})
				}
			}
		}
		if err != nil {
			return NewNullValue(), err
		}
		if !q.boolean {
			if err = s.fulltextCorpus(q, binding); err != nil {
				return NewNullValue(), err
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
	var words []string
	for _, expr := range f.Args[:len(f.Args)-2] {
		v, err := s.evalExpr(expr, row)
		if err != nil {
			return NewNullValue(), err
		}
		if !v.Null {
			part, err := s.fulltextDocumentWords(v.String(), s.memoryBudget()-int64(len(words))*16)
			if err != nil {
				return NewNullValue(), err
			}
			words = append(words, part...)
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
		df := q.frequency[term.words[0]]
		if q.myisam && df*2 >= q.docs {
			continue
		}
		idf := math.Log10(float64(q.docs+1) / float64(df+1))
		if idf == 0 && !q.myisam {
			idf = 1e-9
		}
		score += float64(n) * idf * idf
	}
	return NewRealValue(score), nil
}

func (s *Schema) fulltextCorpus(q *fulltextQuery, b fulltextBinding) error {
	prefix := []byte(s.tableDataPrefix(b.table.Name))
	it := s.newIteratorSeek(prefix)
	defer it.Close()
	for it.Valid() && bytes.HasPrefix(it.Key().UserKey, prefix) {
		if err := s.checkInterrupt(); err != nil {
			return err
		}
		r := decodeRow(it.Value(), b.table)
		var words []string
		for _, name := range b.index.columnNames() {
			if v := r[name]; !v.Null {
				part, err := s.fulltextDocumentWords(v.String(), s.memoryBudget()-int64(len(words))*16)
				if err != nil {
					return err
				}
				words = append(words, part...)
			}
		}
		q.docs++
		seen := map[string]bool{}
		for _, term := range q.terms {
			w := term.words[0]
			if !seen[w] && fulltextTermHits(term, words, q) > 0 {
				q.frequency[w]++
				seen[w] = true
			}
		}
		it.Next()
	}
	return storage.IteratorError(it)
}

func fulltextMyISAM(info *TableInfo) bool { return strings.EqualFold(info.Engine, "MyISAM") }

func (s *Schema) fulltextDocumentWords(text string, remaining int64) ([]string, error) {
	if int64(len(text))*3 > remaining {
		return nil, fmt.Errorf("fulltext document exceeds query memory budget")
	}
	text = foldFulltext(text)
	used := int64(len(text)) * 2
	var words []string
	start := -1
	for offset, r := range text {
		word := unicode.IsLetter(r) || unicode.IsDigit(r) || r == '_' || r == '\''
		if word {
			if start < 0 {
				start = offset
			}
			continue
		}
		if start >= 0 {
			if used+16 > remaining {
				return nil, fmt.Errorf("fulltext document exceeds query memory budget")
			}
			words = append(words, strings.Trim(text[start:offset], "'"))
			used += 16
			start = -1
		}
	}
	if start >= 0 {
		if used+16 > remaining {
			return nil, fmt.Errorf("fulltext document exceeds query memory budget")
		}
		words = append(words, strings.Trim(text[start:], "'"))
	}
	return words, nil
}
