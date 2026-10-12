package sql

import (
	"testing"
	"time"
)

func TestIndexPublicationPolicyParsing(t *testing.T) {
	for _, query := range []string{
		"CREATE INDEX idx ON t(value)",
		"CREATE UNIQUE INDEX idx ON t(value)",
		"DROP INDEX idx",
		"ALTER TABLE t ADD INDEX idx(value)",
		"ALTER TABLE t ADD UNIQUE KEY idx(value)",
		"ALTER TABLE t DROP INDEX idx",
	} {
		for _, suffix := range []string{"", " KILL TRANSACTIONS AFTER '250ms'", " KILL TRANSACTIONS AFTER '0s'"} {
			t.Run(query+suffix, func(t *testing.T) {
				statements, err := Parse(query + suffix + ";")
				if err != nil {
					t.Fatal(err)
				}
				var got *time.Duration
				switch statement := statements[0].(type) {
				case *CreateIndexStmt:
					got = statement.PublicationAbortAfter
				case *DropIndexStmt:
					got = statement.PublicationAbortAfter
				case *AlterTableStmt:
					got = statement.PublicationAbortAfter
				default:
					t.Fatalf("unexpected statement %T", statement)
				}
				if suffix == "" {
					if got != nil {
						t.Fatal("ordinary index change enables cancellation")
					}
					return
				}
				want := 250 * time.Millisecond
				if suffix == " KILL TRANSACTIONS AFTER '0s'" {
					want = 0
				}
				if got == nil || *got != want {
					t.Fatalf("publication threshold = %v, want %v", got, want)
				}
			})
		}
	}
}

func TestIndexPublicationPolicyRejectsInvalidSQL(t *testing.T) {
	for _, query := range []string{
		"CREATE INDEX idx ON t(value) KILL TRANSACTIONS AFTER 250",
		"CREATE INDEX idx ON t(value) KILL TRANSACTIONS AFTER '-1ms'",
		"CREATE INDEX idx ON t(value) KILL TRANSACTIONS AFTER '9223372036854775808ns'",
		"CREATE INDEX idx ON t(value) KILL TRANSACTIONS AFTER '1d'",
		"CREATE INDEX idx ON t(value) KILL TRANSACTIONS AFTER ''",
		"CREATE INDEX idx ON t(value) KILL TRANSACTIONS AFTER ?",
		"CREATE INDEX idx ON t(value) KILL AFTER '1s'",
		"DROP INDEX idx KILL TRANSACTIONS AFTER",
		"ALTER TABLE t ADD INDEX idx(value), DROP INDEX old KILL TRANSACTIONS AFTER '1s'",
		"ALTER TABLE t ADD INDEX idx(value) KILL TRANSACTIONS AFTER '1s', DROP INDEX old",
		"ALTER TABLE t ADD COLUMN value TEXT KILL TRANSACTIONS AFTER '1s'",
		"ALTER TABLE t DROP COLUMN value KILL TRANSACTIONS AFTER '1s'",
		"ALTER INDEX idx REBUILD KILL TRANSACTIONS AFTER '1s'",
		"DROP TABLE t KILL TRANSACTIONS AFTER '1s'",
	} {
		if _, err := Parse(query); err == nil {
			t.Errorf("accepted invalid publication policy: %s", query)
		}
	}
}
