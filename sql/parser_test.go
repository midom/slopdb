package sql

import (
	"math/rand"
	"strings"
	"testing"
)

// Adapted from parser_review.go. Rejection is expected; the
// regression is a panic or failure to return on malformed input.
func TestMalformedParserInputs(t *testing.T) {
	tokens := []string{"SELECT", "CREATE", "TABLE", "INSERT", "INTO", "VALUES", "UPDATE", "DELETE", "FROM", "WHERE", "JOIN", "ON", "USING", "BEGIN", "END", "TRIGGER", "DEFAULT", "PRIMARY", "KEY", "UNIQUE", "CHECK", "AS", "IN", "NOT", "WITH", "UNION", "a", "t", "'str'", "0", "1", "NULL", "(", ")", ",", ";", "%", "@", "+", "*", "="}
	seeds := []string{"INSERT INTO t(@) VALUES(1)", "CREATE TABLE t(a INT DEFAULT ())", "SELECT 1 IN (SELECT 1 UNION SELECT 2)", "INSERT INTO t SELECT 1 UNION SELECT 2", "CREATE VIEW v AS SELECT 1 UNION SELECT 2", "SELECT * FROM (SELECT)", "SELECT 1 FROM t JOIN", "SELECT * FROM t WHERE"}
	rng := rand.New(rand.NewSource(31))
	for i := 0; i < 15000; i++ {
		var query string
		if i < len(seeds) {
			query = seeds[i]
		} else {
			var b strings.Builder
			for n := rng.Intn(30) + 1; n > 0; n-- {
				b.WriteString(tokens[rng.Intn(len(tokens))])
				b.WriteByte(' ')
			}
			query = b.String()
		}
		func() {
			defer func() {
				if p := recover(); p != nil {
					t.Fatalf("Parse(%q) panicked: %v", query, p)
				}
			}()
			_, _ = Parse(query)
		}()
	}
}

func FuzzParse(f *testing.F) {
	for _, query := range []string{"SELECT 1", "CREATE TABLE t(id INTEGER PRIMARY KEY)", "SELECT ? LIMIT 4294967296 OFFSET 4294967296", "INSERT INTO t(@) VALUES(1)", "SELECT * FROM t WHERE", "SELECT a.id FROM a LEFT JOIN b ON a.k=b.k"} {
		f.Add(query)
	}
	f.Fuzz(func(t *testing.T, query string) { _, _ = Parse(query) })
}

func TestAlterColumnStatementTerminator(t *testing.T) {
	for _, query := range []string{
		"ALTER TABLE t ADD COLUMN added TEXT DEFAULT 'x';",
		"ALTER TABLE t ADD COLUMN added INTEGER NOT NULL DEFAULT 0; SELECT 1;",
		"ALTER TABLE t ADD COLUMN parent INTEGER REFERENCES parents(id); COMMIT;",
	} {
		t.Run(query, func(t *testing.T) {
			if _, err := Parse(query); err != nil {
				t.Fatal(err)
			}
		})
	}
	if _, err := Parse("CREATE TABLE t(a INTEGER;)"); err == nil {
		t.Fatal("accepted semicolon inside a CREATE TABLE column list")
	}
}
