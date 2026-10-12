package sql

import (
	"errors"
	"strings"
	"testing"
)

func TestParseErrorLocation(t *testing.T) {
	for _, tc := range []struct {
		name, query  string
		line, column int
	}{
		{"statement", "SELECT 1;\nSELECT 'é' FROM t;\nWHAT", 3, 1},
		{"unicode column", "SELECT 'é' + FROM t", 1, 14},
		{"semicolon", "SELECT 1\nSELECT 2", 2, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Parse(tc.query)
			var parseErr *ParseError
			if !errors.As(err, &parseErr) {
				t.Fatalf("expected ParseError, got %v", err)
			}
			if parseErr.Line != tc.line || parseErr.Column != tc.column {
				t.Fatalf("location = %d:%d, want %d:%d: %v", parseErr.Line, parseErr.Column, tc.line, tc.column, err)
			}
			if !strings.Contains(err.Error(), "line ") {
				t.Fatalf("missing location in %v", err)
			}
		})
	}
}

func TestParseErrorPreparedAndLiteralOffsets(t *testing.T) {
	for _, tc := range []struct {
		query, suffix string
		prepare       bool
	}{
		{"SELECT 'é', ? + FROM t", "FROM t", true},
		{"SELECT ('é'", "", false},
		{"SELECT 'é', 1e999 FROM t", "1e999 FROM t", false},
		{"SELECT 'é' LIMIT 9223372036854775808", "9223372036854775808", false},
		{"SELECT 'é', X'xyz' FROM t", "X'xyz' FROM t", false},
		{"SELECT 'é', 'unterminated", "'unterminated", false},
	} {
		t.Run(tc.query, func(t *testing.T) {
			var err error
			if tc.prepare {
				_, err = Prepare(tc.query)
			} else {
				_, err = Parse(tc.query)
			}
			var parseErr *ParseError
			if !errors.As(err, &parseErr) {
				t.Fatalf("missing ParseError: %v", err)
			}
			if got := tc.query[parseErr.ByteOffset:]; got != tc.suffix {
				t.Fatalf("location points at %q, want %q: %v", got, tc.suffix, err)
			}
			if !errors.Is(parseErr, parseErr.Err) {
				t.Fatal("ParseError discarded underlying error identity")
			}
		})
	}
}

func TestParseErrorNamesExpectedToken(t *testing.T) {
	_, err := Parse("SELECT (1")
	if err == nil || !strings.Contains(err.Error(), `expected ")", got end of SQL`) {
		t.Fatalf("expected a readable missing-parenthesis error, got %v", err)
	}
}
