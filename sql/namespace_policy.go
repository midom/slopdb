package sql

import "strings"

// NamespacePolicy translates connection-visible schema names while all dialects
// share one physical namespace catalog. Explicit session aliases take priority.
type NamespacePolicy struct{ dialect Dialect }

func (d Dialect) NamespacePolicy() NamespacePolicy { return NamespacePolicy{dialect: d} }

// Resolve returns a canonical physical namespace. Native preserves the legacy
// public alias; MySQL may create and select a distinct database named public.
func (p NamespacePolicy) Resolve(name string) string {
	name = strings.ToLower(name)
	if name == "public" && p.dialect != DialectMySQL {
		return "main"
	}
	return name
}

// Present supplies the standard frontend spelling for a canonical namespace.
// Configured root database aliases are supplied by the session catalog adapter.
func (p NamespacePolicy) Present(name string) string {
	if name == "main" && p.dialect == DialectPostgreSQL {
		return "public"
	}
	return name
}

// Visible excludes a physical namespace shadowed by a reserved frontend alias.
// In particular, a MySQL database named public must not become a second PG
// namespace with the same public label and OID as the physical main namespace.
func (p NamespacePolicy) Visible(name string) bool {
	return name == "main" || p.Resolve(name) == name
}
