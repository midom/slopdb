package sql

import (
	"reflect"
	"testing"
)

func projectionMetadataSchema() (*Schema, *TableInfo) {
	info := &TableInfo{Name: "typed", Columns: []ColumnDef{
		{Name: "id", OriginalName: "ID", ColType: ColInteger, Declared: "BIGINT", IntegerBits: 64, NotNull: true, PrimaryKey: true, AutoIncrement: true, Default: &LiteralExpr{Value: NewIntValue(7)}, DefaultSQL: "7", Check: &LiteralExpr{Value: NewIntValue(1)}, CheckSQL: "1", DefaultDialect: definitionDialect(DialectMySQL), CheckDialect: definitionDialect(DialectMySQL), ConstraintNames: map[string]string{"primary": "typed_pk"}},
		{Name: "payload", OriginalName: "Payload", ColType: ColBlob, Declared: "VARBINARY", Length: 127, FixedBinary: true, Binary: true, AllowExistingLength: true},
		{Name: "amount", OriginalName: "Amount", DisplayName: "Amount", ColType: ColDecimal, Declared: "DECIMAL", Precision: 25, Scale: 4, MySQL: true, Unsigned: true, TypeModifiers: true, ParameterIndex: 17},
		{Name: "choice", ColType: ColText, Declared: "ENUM", MySQL: true, EnumValues: []string{"one", "two"}, Collation: Collation("NFC_CI"), SemanticCollation: Collation("NFC_CI")},
		{Name: "linked", ColType: ColInteger, Declared: "INTEGER", Postgres: true, IntegerBits: 32, Unique: true, RefTable: "parent", RefColumns: []string{"id"}, RefOnDelete: "RESTRICT", RefOnUpdate: "CASCADE", RefDeferrable: true, RefInitiallyDeferred: true},
	}}
	return &Schema{tables: map[string]*TableInfo{info.Name: info}}, info
}

func metadataSelect(t *testing.T, query string) *SelectStmt {
	t.Helper()
	statements, err := Parse(query)
	if err != nil || len(statements) != 1 {
		t.Fatalf("parse %q: %v", query, err)
	}
	stmt, ok := statements[0].(*SelectStmt)
	if !ok {
		t.Fatalf("not SELECT: %T", statements[0])
	}
	return stmt
}

func TestDirectProjectionMetadataMatchesFullDescribe(t *testing.T) {
	for _, query := range []string{
		"SELECT id,payload,amount,choice,linked FROM typed",
		"SELECT amount AS renamed_amount,id AS renamed_id FROM typed",
		"SELECT id,id,payload,payload FROM typed",
		"SELECT id AS same,amount AS same FROM typed",
		"SELECT id FROM typed WHERE id=7 ORDER BY id LIMIT 1",
		"SELECT id,choice FROM typed AS source_alias",
	} {
		t.Run(query, func(t *testing.T) {
			s, info := projectionMetadataSchema()
			stmt := metadataSelect(t, query)
			before := append([]ColumnDef(nil), info.Columns...)
			full, err := s.Describe(stmt)
			if err != nil {
				t.Fatal(err)
			}
			// Executing results promote semantic identity and erase parameter
			// provenance, whereas static Describe retains those internal facts.
			for i := range full {
				full[i].SemanticType = full[i].LogicalType().Kind
				full[i].ParameterIndex = 0
			}
			columns, _, _ := s.selectProjection(stmt, info)
			if !reflect.DeepEqual(columns, full) {
				for i := range columns {
					if !reflect.DeepEqual(columns[i], full[i]) {
						t.Errorf("column %d\nexecution: %#v\n describe: %#v", i, columns[i], full[i])
					}
				}
				t.Fatal("complete ColumnDef metadata differs")
			}
			if !reflect.DeepEqual(info.Columns, before) {
				t.Fatal("projection mutated catalog column metadata")
			}
			for i := range columns {
				if columns[i].Default != nil || columns[i].Check != nil {
					t.Fatal("result retains catalog expression")
				}
				columns[i] = ColumnDef{Name: "caller-owned-slot"}
			}
			if !reflect.DeepEqual(info.Columns, before) {
				t.Fatal("result descriptors alias catalog column slots")
			}
		})
	}
}

func TestDirectProjectionMetadataPreservesExpandedLabels(t *testing.T) {
	s, info := projectionMetadataSchema()
	stmt := metadataSelect(t, "SELECT id,amount AS renamed FROM typed")
	stmt.Columns[0].starLabel = "expanded ID label"
	stmt.Columns[1].starLabel = "expanded alias label"
	columns := []ColumnDef{{Name: "id", DisplayName: stmt.Columns[0].starLabel}, {Name: "renamed", DisplayName: stmt.Columns[1].starLabel}}
	full, err := s.Describe(stmt)
	if err != nil {
		t.Fatal(err)
	}
	for i := range full {
		full[i].SemanticType = full[i].LogicalType().Kind
		full[i].ParameterIndex = 0
	}
	if !s.directProjectionMetadata(stmt, info, columns) {
		t.Fatal("plain physical projection was not handled")
	}
	if !reflect.DeepEqual(columns, full) {
		t.Fatalf("labels differ:\n got %#v\nwant %#v", columns, full)
	}
	// Changing source struct fields after projecting also cannot change the
	// already-returned descriptor values. Nested immutable declaration slices
	// and maps retain the existing result-metadata ownership rules.
	info.Columns[0].Name, info.Columns[0].OriginalName, info.Columns[0].Declared = "changed", "changed", "TEXT"
	if !reflect.DeepEqual(columns, full) {
		t.Fatal("source descriptor mutation changed projected metadata")
	}
}

func TestDirectProjectionMetadataFallsBackWithoutPartialChanges(t *testing.T) {
	for _, query := range []string{
		"SELECT id,missing FROM typed",
		"SELECT id,amount+1 FROM typed",
		"SELECT source_alias.id FROM typed AS source_alias",
		"SELECT * FROM typed",
		"SELECT *,id FROM typed",
		"SELECT id FROM typed JOIN other ON typed.id=other.id",
		"SELECT id FROM (SELECT id FROM typed) AS derived",
		"SELECT id FROM typed GROUP BY id",
		"SELECT id,id,id,id,id,id,id,id,id FROM typed",
		"WITH c AS (SELECT id FROM typed) SELECT id FROM typed",
	} {
		t.Run(query, func(t *testing.T) {
			s, info := projectionMetadataSchema()
			stmt := metadataSelect(t, query)
			columns := make([]ColumnDef, len(stmt.Columns))
			for i := range columns {
				columns[i].Name = "untouched"
			}
			before := append([]ColumnDef(nil), columns...)
			if s.directProjectionMetadata(stmt, info, columns) {
				t.Fatal("uncertain projection bypassed full metadata resolver")
			}
			if !reflect.DeepEqual(columns, before) {
				t.Fatal("fallback partially changed descriptor inputs")
			}
		})
	}
}

func TestDirectProjectionMetadataPhysicalCatalogIdentity(t *testing.T) {
	s, info := projectionMetadataSchema()
	stmt := metadataSelect(t, "SELECT id FROM typed")
	columns := []ColumnDef{{Name: "id"}}
	copyInfo := *info
	if s.directProjectionMetadata(stmt, &copyInfo, columns) {
		t.Fatal("synthetic table descriptor treated as physical catalog entry")
	}
	s.tables["typed"].Columns = append(s.tables["typed"].Columns, info.Columns[0])
	if s.directProjectionMetadata(stmt, info, columns) {
		t.Fatal("ambiguous source columns bypassed general metadata resolver")
	}
}

func TestDirectProjectionMetadataDoesNotAllocateDescriptorEnvironment(t *testing.T) {
	s, info := projectionMetadataSchema()
	for i := range info.Columns {
		info.Columns[i].OriginalName = info.Columns[i].Name
	}
	stmt := metadataSelect(t, "SELECT id,payload,amount FROM typed")
	columns := make([]ColumnDef, 3)
	allocations := testing.AllocsPerRun(100, func() {
		columns[0], columns[1], columns[2] = ColumnDef{Name: "id"}, ColumnDef{Name: "payload"}, ColumnDef{Name: "amount"}
		if !s.directProjectionMetadata(stmt, info, columns) {
			panic("direct projection not handled")
		}
	})
	if allocations != 0 {
		t.Fatalf("direct projection allocates %g objects", allocations)
	}
}
