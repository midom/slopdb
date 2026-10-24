package sql

import "testing"

func TestReadBarrierSettingSyntax(t *testing.T) {
	for query, want := range map[string]string{"SET READ BARRIER PREVIOUS": "previous", "set read barrier next;": "next", "SET READ BARRIER COMMITTED": "committed", "SET READ BARRIER DEFAULT": ""} {
		statements, err := Parse(query)
		if err != nil || len(statements) != 1 {
			t.Fatalf("%s: %v", query, err)
		}
		setting, ok := statements[0].(*ReadBarrierStmt)
		if !ok || setting.Mode != want {
			t.Fatalf("%s: %#v", query, statements)
		}
		prepared, err := Prepare(query)
		if err != nil {
			t.Fatal(err)
		}
		bound, err := prepared.Bind(nil)
		if err != nil {
			t.Fatal(err)
		}
		if bound.Statements()[0].(*ReadBarrierStmt).Mode != want {
			t.Fatal("prepared mode changed")
		}
		bound.Release()
	}
	for _, query := range []string{"SET READ PREVIOUS", "SET READ BARRIER", "SET READ BARRIER 'NEXT'", "SET READ BARRIER UNKNOWN", "SET READ BARRIER PREVIOUS NEXT"} {
		if _, err := Parse(query); err == nil {
			t.Fatalf("accepted malformed %q", query)
		}
	}
}
