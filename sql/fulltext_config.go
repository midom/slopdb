package sql

import (
	"encoding/json"
	"fmt"
	"reflect"
	"slices"
	"sort"
	"strings"
	"unicode"
	"unicode/utf8"

	"golang.org/x/text/cases"
	"golang.org/x/text/unicode/norm"
)

// FullTextConfig describes an index's analysis and ranking independently of SQL
// dialect. Nil uses the original compatibility profile. Explicit configurations
// are normalized, copied and persisted with the index; changing session dialect
// never changes the interpretation of existing posting keys.
type FullTextConfig struct {
	Version        int      `json:"version,omitempty"`
	Tokenizer      string   `json:"tokenizer,omitempty"`     // unicode or whitespace
	Normalization  string   `json:"normalization,omitempty"` // fold, lower or none
	MinTokenLength int      `json:"min_token_length,omitempty"`
	MaxTokenLength int      `json:"max_token_length,omitempty"`
	StopwordMode   string   `json:"stopword_mode,omitempty"` // legacy, none or custom
	Stopwords      []string `json:"stopwords,omitempty"`
	Ranking        string   `json:"ranking,omitempty"` // tf_idf or term_frequency
	analysis       *fullTextAnalyzer
}

type fullTextAnalyzer struct {
	config    FullTextConfig
	stopwords map[string]bool
}

var legacyFullTextAnalyzer = &fullTextAnalyzer{config: FullTextConfig{Version: 1, Tokenizer: "unicode", Normalization: "fold", MinTokenLength: 4, MaxTokenLength: 84, StopwordMode: "legacy", Ranking: "tf_idf"}}

func (c *FullTextConfig) UnmarshalJSON(data []byte) error {
	type plain FullTextConfig
	var raw plain
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	copy, err := cloneFullTextConfig((*FullTextConfig)(&raw))
	if err != nil {
		return err
	}
	*c = *copy
	return nil
}
func cloneFullTextConfig(input *FullTextConfig) (*FullTextConfig, error) {
	if input == nil {
		return nil, nil
	}
	c := *input
	c.analysis = nil
	c.Stopwords = append([]string(nil), input.Stopwords...)
	if c.Version == 0 {
		c.Version = 1
	}
	if c.Version != 1 {
		return nil, fmt.Errorf("unsupported full-text configuration version %d", c.Version)
	}
	c.Tokenizer = strings.ToLower(c.Tokenizer)
	if c.Tokenizer == "" {
		c.Tokenizer = "unicode"
	}
	if c.Tokenizer != "unicode" && c.Tokenizer != "whitespace" {
		return nil, fmt.Errorf("unsupported full-text tokenizer %q", c.Tokenizer)
	}
	c.Normalization = strings.ToLower(c.Normalization)
	if c.Normalization == "" {
		c.Normalization = "fold"
	}
	if c.Normalization != "fold" && c.Normalization != "lower" && c.Normalization != "none" {
		return nil, fmt.Errorf("unsupported full-text normalization %q", c.Normalization)
	}
	if c.MinTokenLength == 0 {
		c.MinTokenLength = 4
	}
	if c.MaxTokenLength == 0 {
		c.MaxTokenLength = 84
	}
	if c.MinTokenLength < 1 || c.MaxTokenLength < c.MinTokenLength || c.MaxTokenLength > 1<<20 {
		return nil, fmt.Errorf("full-text token lengths must satisfy 1 <= minimum <= maximum <= 1048576")
	}
	c.StopwordMode = strings.ToLower(c.StopwordMode)
	if c.StopwordMode == "" {
		c.StopwordMode = "legacy"
		if len(c.Stopwords) > 0 {
			c.StopwordMode = "custom"
		}
	}
	if c.StopwordMode != "legacy" && c.StopwordMode != "none" && c.StopwordMode != "custom" {
		return nil, fmt.Errorf("unsupported full-text stopword mode %q", c.StopwordMode)
	}
	if c.StopwordMode != "custom" && len(c.Stopwords) > 0 {
		return nil, fmt.Errorf("custom stopwords require stopword_mode=custom")
	}
	if len(c.Stopwords) > 65536 {
		return nil, fmt.Errorf("full-text stopword list exceeds 65536 words")
	}
	c.Ranking = strings.ToLower(c.Ranking)
	if c.Ranking == "" {
		c.Ranking = "tf_idf"
	}
	if c.Ranking != "tf_idf" && c.Ranking != "term_frequency" {
		return nil, fmt.Errorf("unsupported full-text ranking %q", c.Ranking)
	}
	a := &fullTextAnalyzer{config: c, stopwords: map[string]bool{}}
	normalized := make([]string, 0, len(c.Stopwords))
	var bytes int
	for _, word := range c.Stopwords {
		bytes += len(word)
		if bytes > 1<<20 {
			return nil, fmt.Errorf("full-text stopwords exceed 1 MiB")
		}
		words, err := a.words(word, 1<<22)
		if err != nil {
			return nil, err
		}
		if len(words) != 1 || words[0] == "" {
			return nil, fmt.Errorf("each full-text stopword must contain exactly one token")
		}
		if !a.stopwords[words[0]] {
			a.stopwords[words[0]] = true
			normalized = append(normalized, words[0])
		}
	}
	sort.Strings(normalized)
	c.Stopwords = normalized
	a.config = c
	c.analysis = a
	return &c, nil
}
func fullTextAnalyzerForIndex(idx *IndexInfo) (*fullTextAnalyzer, error) {
	if idx.FullTextConfig == nil {
		return legacyFullTextAnalyzer, nil
	}
	if idx.FullTextConfig.analysis != nil {
		return idx.FullTextConfig.analysis, nil
	}
	c, err := cloneFullTextConfig(idx.FullTextConfig)
	if err != nil {
		return nil, err
	}
	return c.analysis, nil
}
func configureFullTextIndex(idx *IndexInfo, c *FullTextConfig) error {
	if !idx.Fulltext && c != nil {
		return fmt.Errorf("full-text configuration requires a FULLTEXT index")
	}
	owned, err := cloneFullTextConfig(c)
	if err != nil {
		return err
	}
	idx.FullTextConfig = owned
	return nil
}
func (a *fullTextAnalyzer) normalize(text string) string {
	switch a.config.Normalization {
	case "none":
		return text
	case "lower":
		return strings.ToLower(text)
	default:
		text = norm.NFD.String(cases.Fold().String(text))
		return strings.Map(func(r rune) rune {
			if unicode.Is(unicode.Mn, r) {
				return -1
			}
			return r
		}, text)
	}
}
func (a *fullTextAnalyzer) wordRune(r rune) bool {
	if a.config.Tokenizer == "whitespace" {
		return !unicode.IsSpace(r)
	}
	return unicode.IsLetter(r) || unicode.IsDigit(r) || r == '_' || r == '\''
}
func (a *fullTextAnalyzer) indexed(word string) bool {
	n := utf8.RuneCountInString(word)
	if n < a.config.MinTokenLength || n > a.config.MaxTokenLength {
		return false
	}
	switch a.config.StopwordMode {
	case "legacy":
		return !fulltextStopwords[word]
	case "custom":
		return !a.stopwords[word]
	}
	return true
}
func (a *fullTextAnalyzer) words(text string, remaining int64) ([]string, error) {
	if int64(len(text)) > remaining/3 {
		return nil, fmt.Errorf("fulltext document exceeds query memory budget")
	}
	text = a.normalize(text)
	used := int64(len(text)) * 2
	if used > remaining {
		return nil, fmt.Errorf("fulltext document exceeds query memory budget")
	}
	var words []string
	start := -1
	emit := func(end int) error {
		if used+16 > remaining {
			return fmt.Errorf("fulltext document exceeds query memory budget")
		}
		word := text[start:end]
		if a.config.Tokenizer == "unicode" {
			word = strings.Trim(word, "'")
		}
		words = append(words, word)
		used += 16
		return nil
	}
	for offset, r := range text {
		if a.wordRune(r) {
			if start < 0 {
				start = offset
			}
			continue
		}
		if start >= 0 {
			if err := emit(offset); err != nil {
				return nil, err
			}
			start = -1
		}
	}
	if start >= 0 {
		if err := emit(len(text)); err != nil {
			return nil, err
		}
	}
	return words, nil
}

func fullTextInput(value Value) string {
	if value.Type == ColBlob {
		return string(value.Blob)
	}
	return value.String()
}

// Equivalent indexes may share a MATCH column list. Distinct analysis must not
// be selected according to Go map iteration or change silently after reopen.
func (s *Schema) selectFullTextIndex(table string, names []string) (*IndexInfo, error) {
	var selected *IndexInfo
	var selectedAnalysis *fullTextAnalyzer
	for _, idx := range s.tableIndexes(table) {
		if !idx.Fulltext || len(idx.columnNames()) != len(names) {
			continue
		}
		matches := true
		for _, name := range names {
			if !slices.Contains(idx.columnNames(), name) {
				matches = false
				break
			}
		}
		if !matches {
			continue
		}
		a, err := fullTextAnalyzerForIndex(idx)
		if err != nil {
			return nil, err
		}
		if selected != nil {
			x, y := a.config, selectedAnalysis.config
			wordsEqual := slices.Equal(x.Stopwords, y.Stopwords)
			x.Stopwords = nil
			y.Stopwords = nil
			x.analysis = nil
			y.analysis = nil
			if !wordsEqual || !reflect.DeepEqual(x, y) {
				return nil, fmt.Errorf("ambiguous FULLTEXT column list: indexes %s and %s use different configurations", selected.SQLName(), idx.SQLName())
			}
		}
		if selected == nil || idx.ID < selected.ID || (idx.ID == selected.ID && idx.Name < selected.Name) {
			selected = idx
			selectedAnalysis = a
		}
	}
	return selected, nil
}

func equivalentFullTextConfigs(left, right *FullTextConfig) bool {
	a, err := fullTextAnalyzerForIndex(&IndexInfo{FullTextConfig: left})
	if err != nil {
		return false
	}
	b, err := fullTextAnalyzerForIndex(&IndexInfo{FullTextConfig: right})
	if err != nil {
		return false
	}
	x, y := a.config, b.config
	wordsEqual := slices.Equal(x.Stopwords, y.Stopwords)
	x.Stopwords = nil
	y.Stopwords = nil
	x.analysis = nil
	y.analysis = nil
	return wordsEqual && reflect.DeepEqual(x, y)
}

// Catalog copies own mutable declaration slices while sharing their immutable
// compiled analyzer. No runtime cache is modified through a declaration copy.
func copyFullTextConfig(input *FullTextConfig) *FullTextConfig {
	if input == nil {
		return nil
	}
	copy := *input
	copy.Stopwords = slices.Clone(input.Stopwords)
	return &copy
}
