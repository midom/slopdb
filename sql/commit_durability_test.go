package sql

import "testing"

func TestCommitDurabilitySyntax(t *testing.T) {
	for query, want := range map[string]string{"COMMIT": "", "COMMIT TRANSACTION": "", "COMMIT ASYNC": "async", "COMMIT DURABLE": "durable", "commit transaction /* mode */ durable;": "durable"} {
		statements, e := Parse(query)
		if e != nil || len(statements) != 1 {
			t.Fatalf("%s: %v", query, e)
		}
		commit, ok := statements[0].(*CommitStmt)
		if !ok || commit.Durability != want {
			t.Fatalf("%s: %#v want %s", query, statements[0], want)
		}
	}
	for _, query := range []string{"COMMIT 'DURABLE'", "COMMIT \"DURABLE\"", "COMMIT [ASYNC]", "COMMIT 'TRANSACTION'", "COMMIT ASYNC DURABLE", "COMMIT DURABLE ?"} {
		if statements, e := Parse(query); e == nil {
			t.Fatalf("accepted %s: %#v", query, statements)
		}
	}
}
