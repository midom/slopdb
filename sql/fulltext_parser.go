package sql

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

// WITH is a native index option clause, accepted by both dialect adapters.
// Values are literals because this is persistent schema, not session state.
func (p *Parser) parseFullTextConfig() (*FullTextConfig, error) {
	if !p.optionalAlterWord("WITH") {
		return nil, nil
	}
	if err := p.expect(TokLParen); err != nil {
		return nil, err
	}
	c := &FullTextConfig{}
	seen := map[string]bool{}
	for {
		key := strings.ToLower(p.cur.Value)
		if p.cur.Type != TokIdent && p.cur.Type != TokKeyword {
			return nil, fmt.Errorf("expected full-text configuration option")
		}
		p.advance()
		if seen[key] {
			return nil, fmt.Errorf("duplicate full-text option %s", key)
		}
		seen[key] = true
		if err := p.expect(TokEq); err != nil {
			return nil, err
		}
		switch key {
		case "min_token_length", "max_token_length":
			if p.cur.Type != TokInt {
				return nil, fmt.Errorf("full-text token length requires a positive integer")
			}
			n, err := strconv.ParseInt(p.cur.Value, 10, 32)
			if err != nil || n < 1 || n > 1<<20 {
				return nil, fmt.Errorf("invalid full-text token length")
			}
			p.advance()
			if key == "min_token_length" {
				c.MinTokenLength = int(n)
			} else {
				c.MaxTokenLength = int(n)
			}
		case "tokenizer", "normalization", "stopword_mode", "stopwords", "ranking":
			if p.cur.Type != TokString {
				return nil, fmt.Errorf("full-text option %s requires a quoted string", key)
			}
			value := p.cur.Value
			p.advance()
			switch key {
			case "tokenizer":
				c.Tokenizer = value
			case "normalization":
				c.Normalization = value
			case "stopword_mode":
				c.StopwordMode = value
			case "ranking":
				c.Ranking = value
			case "stopwords":
				if err := json.Unmarshal([]byte(value), &c.Stopwords); err != nil {
					return nil, fmt.Errorf("full-text stopwords require a JSON string array: %w", err)
				}
			}
		default:
			return nil, fmt.Errorf("unknown full-text option %s", key)
		}
		if p.cur.Type != TokComma {
			break
		}
		p.advance()
	}
	if err := p.expect(TokRParen); err != nil {
		return nil, err
	}
	return cloneFullTextConfig(c)
}
func formatFullTextConfig(c *FullTextConfig) string {
	if c == nil {
		return ""
	}
	quote := func(text string) string { return "'" + strings.ReplaceAll(text, "'", "''") + "'" }
	words, _ := json.Marshal(c.Stopwords)
	if c.Stopwords == nil {
		words = []byte("[]")
	}
	return fmt.Sprintf(" WITH (tokenizer=%s, normalization=%s, min_token_length=%d, max_token_length=%d, stopword_mode=%s, stopwords=%s, ranking=%s)", quote(c.Tokenizer), quote(c.Normalization), c.MinTokenLength, c.MaxTokenLength, quote(c.StopwordMode), quote(string(words)), quote(c.Ranking))
}
