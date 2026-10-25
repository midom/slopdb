package sql

import "fmt"

// Schema SHOW layouts are defined once for Prepare/Describe and execution.
// Runtime SHOW commands keep their separate observation providers.
var showSchemas = map[string]string{
	"DATABASES":      "Database",
	"COLUMNS":        "Field Type Null Key Default Extra",
	"FULL COLUMNS":   "Field Type Collation Null Key Default Extra Privileges Comment",
	"INDEX":          "Table Non_unique:i Key_name Seq_in_index:i Column_name Collation Cardinality:i Sub_part:i Packed Null Index_type Comment Index_comment Visible Expression",
	"CREATE TABLE":   "Table Create_table",
	"CREATE VIEW":    "View Create_view character_set_client collation_connection",
	"CREATE TRIGGER": "Trigger sql_mode SQL_Original_Statement character_set_client collation_connection Database_Collation Created",
	"TRIGGERS":       "Trigger Event Table Statement Timing Created sql_mode Definer character_set_client collation_connection Database_Collation",
	"TABLE STATUS":   "Name Engine Version:i Row_format Rows:i Avg_row_length:i Data_length:i Max_data_length:i Index_length:i Data_free:i Auto_increment:i Create_time Update_time Check_time Collation Checksum:i Create_options Comment",
}

func (s *Schema) showMetadata(stmt *ShowStmt) ([]ColumnDef, error) {
	if stmt.Kind == "TABLES" {
		ns := s.SearchPath()[0]
		if stmt.Database != "" {
			ns = s.namespaceAlias(stmt.Database)
		}
		columns := describeColumns("Tables_in_" + s.catalogNamespace(ns))
		if stmt.Full {
			columns = append(columns, ColumnDef{Name: "Table_type", ColType: ColText})
		}
		return columns, nil
	}
	kind := stmt.Kind
	if stmt.Full && kind == "COLUMNS" {
		kind = "FULL COLUMNS"
	}
	if kind == "CREATE TABLE" && stmt.Object != "" {
		if rel, ok := s.catalogFacts().relation(stmt.Object); ok && rel.Kind == "view" {
			kind = "CREATE VIEW"
		}
	}
	layout, ok := showSchemas[kind]
	if !ok {
		return nil, fmt.Errorf("metadata unsupported for SHOW %s", stmt.Kind)
	}
	columns := catalogResult(layout).Columns
	for i := range columns {
		switch columns[i].Name {
		case "Create_table":
			columns[i].Name = "Create table"
		case "Create_view":
			columns[i].Name = "Create view"
		case "SQL_Original_Statement":
			columns[i].Name = "SQL Original Statement"
		case "Database_Collation":
			columns[i].Name = "Database Collation"
		}
	}
	return columns, nil
}
func (s *Schema) showFrame(stmt *ShowStmt) *ResultSet {
	columns, _ := s.showMetadata(stmt)
	return &ResultSet{Columns: columns}
}
