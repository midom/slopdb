package mysqlserver

import (
	"slopdb/sql"
	"strings"
)

// splitCommands uses the SQL lexer so quoted semicolons and comments cannot
// create commands. Trigger bodies retain their internal statement separators.
func splitCommands(query string, options sql.ParseOptions) []string {
	tokenizer := sql.NewTokenizerWithOptions(query, options)
	start, parens, blocks := 0, 0, 0
	trigger, create := false, false
	var result []string
	for tok := tokenizer.Next(); tok.Type != sql.TokEOF; tok = tokenizer.Next() {
		if tok.Type == sql.TokLParen {
			parens++
		}
		if tok.Type == sql.TokRParen {
			parens--
		}
		if tok.Type == sql.TokKeyword || tok.Type == sql.TokIdent {
			word := strings.ToUpper(tok.Value)
			if word == "CREATE" {
				create = true
			}
			if create && word == "TRIGGER" {
				trigger = true
			}
			if trigger && (word == "BEGIN" || word == "CASE") {
				blocks++
			}
			if trigger && word == "END" && blocks > 0 {
				blocks--
			}
		}
		if tok.Type == sql.TokSemicolon && parens == 0 && blocks == 0 {
			if part := strings.TrimSpace(query[start:tok.Start]); part != "" {
				result = append(result, part)
			}
			start = tok.Start + 1
			trigger, create = false, false
		}
	}
	if part := strings.TrimSpace(query[start:]); part != "" {
		result = append(result, part)
	}
	return result
}

func (c *client) queryResults(query string) error {
	commands := splitCommands(query, c.currentDatabase().ParseOptions())
	if len(commands) > 1 && c.capabilities&clientMultiStatements == 0 {
		return c.writeError(&protocolError{1064, "42000", "CLIENT_MULTI_STATEMENTS is required"})
	}
	if len(commands) == 0 {
		return c.ok(nil)
	}
	defer func() { c.moreResults = false }()
	for i, command := range commands {
		c.moreResults = i+1 < len(commands)
		result, err := c.query(command, nil, nil)
		if err != nil {
			c.moreResults = false
			return c.writeError(err)
		}
		if err = c.result(result, false); err != nil {
			return err
		}
	}
	return nil
}
