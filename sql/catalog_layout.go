package sql

import "strings"

// catalogResult decodes a static logical schema without consulting catalog data.
func catalogResult(spec string) *ResultSet {
	r := &ResultSet{}
	for _, part := range strings.Fields(spec) {
		name, typ, _ := strings.Cut(part, ":")
		c := ColumnDef{Name: name, ColType: ColText, Declared: "TEXT"}
		switch typ {
		case "i":
			c.ColType = ColInteger
			c.Declared = "BIGINT"
		case "o":
			c.ColType = ColInteger
			c.Declared = "OID"
		case "n":
			c.Declared = "NAME"
		case "b":
			c.ColType = ColInteger
			c.Declared = "BOOLEAN"
		case "u":
			c.ColType = ColUnsigned
			c.Declared = "BIGINT UNSIGNED"
		case "r":
			c.ColType = ColReal
			c.Declared = "DOUBLE PRECISION"
		}
		r.Columns = append(r.Columns, c)
	}
	return r
}
