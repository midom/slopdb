package postgresserver

import (
	"fmt"
	"strings"

	"slopdb/sql"
)

type copyCommand struct {
	table, tableSQL, query              string
	columns                             []string
	from                                bool
	format                              string
	delimiter, quote, escape            byte
	null                                string
	header, headerMatch                 bool
	forceQuote, forceNull, forceNotNull map[string]bool
}

func copyIdent(s string) string { return `"` + strings.ReplaceAll(s, `"`, `""`) + `"` }
func copySyntax(s string) error { return &protocolError{"42601", s} }
func parseCopy(query string) (*copyCommand, error) {
	t := sql.NewTokenizer(query)
	tok := t.Next()
	if !strings.EqualFold(tok.Value, "COPY") {
		return nil, nil
	}
	c := &copyCommand{format: "text", forceQuote: map[string]bool{}, forceNull: map[string]bool{}, forceNotNull: map[string]bool{}}
	tok = t.Next()
	if strings.EqualFold(tok.Value, "BINARY") {
		c.format = "binary"
		tok = t.Next()
	}
	if tok.Type == sql.TokLParen {
		start := t.Pos()
		depth := 1
		for depth > 0 {
			tok = t.Next()
			if tok.Type == sql.TokEOF {
				return nil, copySyntax("unterminated COPY query")
			}
			if tok.Type == sql.TokLParen {
				depth++
			}
			if tok.Type == sql.TokRParen {
				depth--
			}
		}
		c.query = query[start:tok.Start]
		tok = t.Next()
	} else {
		if tok.Type != sql.TokIdent && tok.Type != sql.TokString && tok.Type != sql.TokKeyword {
			return nil, copySyntax("COPY requires a table")
		}
		c.table = tok.Value
		c.tableSQL = copyIdent(tok.Value)
		tok = t.Next()
		if tok.Type == sql.TokDot {
			tok = t.Next()
			c.table += "." + tok.Value
			c.tableSQL += "." + copyIdent(tok.Value)
			tok = t.Next()
		}
		if tok.Type == sql.TokLParen {
			for {
				tok = t.Next()
				if tok.Type != sql.TokIdent && tok.Type != sql.TokString && tok.Type != sql.TokKeyword {
					return nil, copySyntax("invalid COPY column")
				}
				c.columns = append(c.columns, tok.Value)
				tok = t.Next()
				if tok.Type == sql.TokRParen {
					break
				}
				if tok.Type != sql.TokComma {
					return nil, copySyntax("expected comma in COPY columns")
				}
			}
			tok = t.Next()
		}
	}
	switch strings.ToUpper(tok.Value) {
	case "FROM":
		c.from = true
	case "TO":
	default:
		return nil, copySyntax("COPY requires FROM or TO")
	}
	if c.from && c.query != "" {
		return nil, copySyntax("COPY FROM requires a table")
	}
	tok = t.Next()
	if (c.from && !strings.EqualFold(tok.Value, "STDIN")) || (!c.from && !strings.EqualFold(tok.Value, "STDOUT")) {
		return nil, &protocolError{"0A000", "COPY supports only STDIN and STDOUT"}
	}
	tok = t.Next()
	if strings.EqualFold(tok.Value, "WITH") {
		tok = t.Next()
	}
	parens := tok.Type == sql.TokLParen
	if parens {
		tok = t.Next()
	}
	options := map[string]string{}
	seenOptions := map[string]bool{}
	for tok.Type != sql.TokEOF && tok.Type != sql.TokSemicolon && tok.Type != sql.TokRParen {
		key := strings.ToUpper(tok.Value)
		optionKey := key
		if key == "CSV" || key == "BINARY" {
			optionKey = "FORMAT"
		}
		if seenOptions[optionKey] {
			return nil, copySyntax("conflicting COPY options")
		}
		seenOptions[optionKey] = true
		tok = t.Next()
		if key == "CSV" || key == "BINARY" {
			if parens {
				return nil, copySyntax("use FORMAT with parenthesized COPY options")
			}
			options["FORMAT"] = strings.ToLower(key)
			continue
		}
		if tok.Type == sql.TokEq || strings.EqualFold(tok.Value, "AS") {
			tok = t.Next()
		}
		switch key {
		case "HEADER":
			value := "true"
			if tok.Type == sql.TokString || strings.EqualFold(tok.Value, "TRUE") || strings.EqualFold(tok.Value, "FALSE") || strings.EqualFold(tok.Value, "MATCH") || tok.Type == sql.TokInt {
				value = tok.Value
				tok = t.Next()
			}
			if _, ok := options[key]; ok {
				return nil, copySyntax("conflicting COPY options")
			}
			options[key] = value
		case "FORCE_QUOTE", "FORCE_NULL", "FORCE_NOT_NULL":
			var names []string
			if tok.Type == sql.TokStar {
				names = []string{"*"}
				tok = t.Next()
			} else if tok.Type == sql.TokLParen {
				for {
					tok = t.Next()
					if tok.Type == sql.TokEOF {
						return nil, copySyntax("unterminated COPY column option")
					}
					names = append(names, tok.Value)
					tok = t.Next()
					if tok.Type == sql.TokRParen {
						break
					}
					if tok.Type != sql.TokComma {
						return nil, copySyntax("expected COPY column option comma")
					}
				}
				tok = t.Next()
			} else {
				return nil, copySyntax("COPY force option requires columns")
			}
			dst := c.forceQuote
			if key == "FORCE_NULL" {
				dst = c.forceNull
			}
			if key == "FORCE_NOT_NULL" {
				dst = c.forceNotNull
			}
			for _, n := range names {
				dst[strings.ToLower(n)] = true
			}
		case "FORMAT", "DELIMITER", "NULL", "QUOTE", "ESCAPE", "ENCODING":
			if tok.Type == sql.TokEOF || tok.Type == sql.TokRParen || tok.Type == sql.TokComma {
				return nil, copySyntax("missing COPY option value")
			}
			if _, ok := options[key]; ok {
				return nil, copySyntax("conflicting COPY options")
			}
			options[key] = tok.Value
			tok = t.Next()
		default:
			return nil, &protocolError{"0A000", fmt.Sprintf("unsupported COPY option %s", key)}
		}
		if parens && tok.Type != sql.TokRParen {
			if tok.Type != sql.TokComma {
				return nil, copySyntax("expected comma in COPY options")
			}
			tok = t.Next()
			if tok.Type == sql.TokRParen {
				return nil, copySyntax("missing COPY option after comma")
			}
		}
	}
	if parens {
		if tok.Type != sql.TokRParen {
			return nil, copySyntax("unterminated COPY options")
		}
		tok = t.Next()
	}
	if tok.Type == sql.TokSemicolon {
		tok = t.Next()
	}
	if tok.Type != sql.TokEOF {
		return nil, copySyntax("unexpected input after COPY")
	}
	if s, ok := options["FORMAT"]; ok {
		c.format = strings.ToLower(s)
	}
	switch c.format {
	case "text":
		c.delimiter = '\t'
		c.null = `\N`
	case "csv":
		c.delimiter = ','
		c.quote = '"'
		c.escape = '"'
	case "binary":
	default:
		return nil, &protocolError{"22023", "COPY format must be text, csv or binary"}
	}
	for key, dst := range map[string]*byte{"DELIMITER": &c.delimiter, "QUOTE": &c.quote, "ESCAPE": &c.escape} {
		if v, ok := options[key]; ok {
			if len(v) != 1 || v[0] == 0 || v[0] == '\r' || v[0] == '\n' {
				return nil, &protocolError{"22023", "COPY delimiter, quote and escape must be one non-newline byte"}
			}
			*dst = v[0]
		}
	}
	if _, ok := options["QUOTE"]; ok {
		if _, set := options["ESCAPE"]; !set {
			c.escape = c.quote
		}
	}
	if v, ok := options["NULL"]; ok {
		c.null = v
	}
	if strings.ContainsAny(c.null, "\r\n\x00") {
		return nil, &protocolError{"22023", "COPY null marker cannot contain newline"}
	}
	if v, ok := options["ENCODING"]; ok && !strings.EqualFold(v, "UTF8") && !strings.EqualFold(v, "UTF-8") {
		return nil, &protocolError{"0A000", "COPY supports UTF8 encoding"}
	}
	if v, ok := options["HEADER"]; ok {
		switch strings.ToLower(v) {
		case "true", "1", "on":
			c.header = true
		case "false", "0", "off":
		case "match":
			c.header = true
			c.headerMatch = true
		default:
			return nil, &protocolError{"22023", "invalid COPY HEADER value"}
		}
	}
	if _, set := options["HEADER"]; set && c.format != "csv" {
		return nil, &protocolError{"22023", "COPY HEADER requires CSV format"}
	}
	if c.format != "csv" && (c.header || len(c.forceQuote)+len(c.forceNull)+len(c.forceNotNull) > 0 || options["QUOTE"] != "" || options["ESCAPE"] != "") {
		return nil, &protocolError{"22023", "COPY CSV options require CSV format"}
	}
	if c.format == "binary" && (options["DELIMITER"] != "" || options["NULL"] != "") {
		return nil, &protocolError{"22023", "COPY binary does not use delimiter or null options"}
	}
	if c.format == "text" && c.delimiter == '\\' {
		return nil, &protocolError{"22023", "COPY text delimiter cannot be backslash"}
	}
	if c.format != "binary" && strings.IndexByte(c.null, c.delimiter) >= 0 {
		return nil, &protocolError{"22023", "COPY null marker contains delimiter"}
	}
	if c.format == "csv" && strings.IndexByte(c.null, c.quote) >= 0 {
		return nil, &protocolError{"22023", "COPY null marker contains quote"}
	}
	if c.format == "csv" && c.quote == c.delimiter {
		return nil, &protocolError{"22023", "COPY delimiter and quote must differ"}
	}
	if c.from && len(c.forceQuote) > 0 || !c.from && (len(c.forceNull) > 0 || len(c.forceNotNull) > 0) {
		return nil, &protocolError{"22023", "COPY force option has wrong direction"}
	}
	return c, nil
}
