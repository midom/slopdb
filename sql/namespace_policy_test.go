package sql

import "testing"

func TestDialectNamespacePolicy(t *testing.T) {
	for _, d := range []Dialect{DialectNative, DialectMySQL, DialectPostgreSQL} {
		p := d.NamespacePolicy()
		want := "main"
		if d == DialectMySQL {
			want = "public"
		}
		if got := p.Resolve("PUBLIC"); got != want {
			t.Fatal(d, got, want)
		}
		label := "main"
		if d == DialectPostgreSQL {
			label = "public"
		}
		if p.Present("main") != label || !p.Visible("main") || !p.Visible("ordinary") {
			t.Fatal(d, p)
		}
		if p.Visible("public") != (d == DialectMySQL) {
			t.Fatal(d, "shadow visibility")
		}
		s := &Schema{dialect: d, schemaAliases: map[string]string{"public": "explicit"}}
		if s.namespaceAlias("PUBLIC") != "explicit" {
			t.Fatal(d, "explicit alias did not override policy")
		}
	}
}
