package main

import (
	"bufio"
	"fmt"
	"io"
	"strings"
)

// Read one bounded SQL statement without splitting quoted literals or comments.
// Canonical dumps use doubled SQL quotes, rather than backslash escapes.
type statementReader struct {
	reader *bufio.Reader
	limit  int
}

func newStatementReader(r io.Reader) *statementReader {
	return &statementReader{bufio.NewReaderSize(r, 64<<10), 8 << 20}
}
func (s *statementReader) next() (string, error) {
	var out strings.Builder
	var quote byte
	var line, block, content bool
	for {
		b, err := s.reader.ReadByte()
		if err == io.EOF {
			if quote != 0 || block {
				return "", fmt.Errorf("unterminated SQL quote or comment")
			}
			if content {
				return "", fmt.Errorf("SQL statement is missing a terminating semicolon")
			}
			return "", io.EOF
		}
		if err != nil {
			return "", err
		}
		out.WriteByte(b)
		if out.Len() > s.limit {
			return "", fmt.Errorf("SQL statement exceeds %d bytes", s.limit)
		}
		if line {
			if b == '\n' {
				line = false
			}
			continue
		}
		if block {
			if b == '*' {
				if p, _ := s.reader.Peek(1); len(p) == 1 && p[0] == '/' {
					_, _ = s.reader.ReadByte()
					out.WriteByte('/')
					block = false
				}
			}
			continue
		}
		if quote != 0 {
			if b == quote {
				if p, _ := s.reader.Peek(1); len(p) == 1 && p[0] == quote {
					_, _ = s.reader.ReadByte()
					out.WriteByte(quote)
				} else {
					quote = 0
				}
			}
			continue
		}
		if b == '-' || b == '/' {
			p, _ := s.reader.Peek(1)
			if len(p) == 1 && ((b == '-' && p[0] == '-') || (b == '/' && p[0] == '*')) {
				_, _ = s.reader.ReadByte()
				out.WriteByte(p[0])
				line = b == '-'
				block = b == '/'
				continue
			}
		}
		if b == ';' {
			if content {
				return out.String(), nil
			}
			out.Reset()
			continue
		}
		if b == '\'' || b == '"' || b == '`' {
			quote = b
			content = true
		} else if b == '[' {
			quote = ']'
			content = true
		} else if b != ' ' && b != '\n' && b != '\r' && b != '\t' {
			content = true
		}
	}
}
