package sql

import (
	"reflect"
	"strings"
)

// Identifiers and bare-keyword syntax cannot be supplied by parameter bindings.
// Compile their namespace requirements once with the immutable prepared template,
// rather than walking every bound expression on every execution.
type namespaceRequirements struct {
	qualified, nativeBare bool
	rootRead              bool
	multiPartReference    bool
	catalogReference      bool
}

func compileNamespaceRequirements(stmt Statement) namespaceRequirements {
	var requirements namespaceRequirements
	namespaceVisit(reflect.ValueOf(stmt), func(value any) {
		switch st := value.(type) {
		case *FuncCallExpr:
			requirements.nativeBare = requirements.nativeBare || st.PostgresBare
		case *ColumnRefExpr:
			requirements.multiPartReference = requirements.multiPartReference || strings.Count(st.Name, ".") >= 2
		case SelectColumn:
			requirements.multiPartReference = requirements.multiPartReference || strings.Contains(st.StarQualifier, ".")
		case *SelectStmt:
			requirements.catalogReference = requirements.catalogReference || catalogRelationName(st.From)
			requirements.qualified = requirements.qualified || strings.Contains(st.From, ".")
		case JoinClause:
			requirements.qualified = requirements.qualified || strings.Contains(st.Table, ".")
		case *ShowStmt:
			requirements.qualified = requirements.qualified || st.Database != "" || strings.Contains(st.Object, ".")
		case *AlterTableStmt:
			requirements.qualified = requirements.qualified || strings.Contains(st.Table, ".")
		case *CreateViewStmt:
			requirements.qualified = requirements.qualified || strings.Contains(st.View, ".")
		case *DropViewStmt:
			requirements.qualified = requirements.qualified || strings.Contains(st.View, ".")
		case *InsertStmt:
			requirements.qualified = requirements.qualified || strings.Contains(st.Table, ".")
		case *CreateTableStmt:
			requirements.qualified = requirements.qualified || strings.Contains(st.Table, ".")
		case *UpdateStmt:
			requirements.qualified = requirements.qualified || strings.Contains(st.Table, ".")
		case *DeleteStmt:
			requirements.qualified = requirements.qualified || strings.Contains(st.From, ".")
		case *DropTableStmt:
			requirements.qualified = requirements.qualified || strings.Contains(st.Table, ".")
		case *CreateIndexStmt:
			requirements.qualified = requirements.qualified || strings.Contains(st.Table, ".")
		}
	})
	return requirements
}
func preparedNamespaceRequirements(stmt Statement) (namespaceRequirements, bool) {
	var owner *PreparedQuery
	switch st := stmt.(type) {
	case *SelectStmt:
		if st.prepared != nil {
			owner = st.prepared.owner
		}
	case *UpdateStmt:
		if st.prepared != nil {
			owner = st.prepared.owner
		}
	}
	if owner == nil {
		return namespaceRequirements{}, false
	}
	return owner.namespaces, true
}

// Virtual relation names are immutable parser inputs. Compile the possibility
// once; dialect-specific projection and its live snapshot remain execution work.
func catalogRelationName(name string) bool {
	name = strings.ToLower(name)
	if strings.HasPrefix(name, "information_schema.") || name == "mysql.user" {
		return true
	}
	_, known := postgresCatalogName(name)
	return known
}
