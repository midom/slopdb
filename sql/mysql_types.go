package sql

import (
	"bytes"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
)

// MySQLMode controls connection-local value affinity and result-count behavior.
// Catalog type declarations remain persistent and independent of session state.
func (s *Schema) SetMySQLMode(enabled bool) { s.mysqlMode = enabled }
func (s *Schema) MySQLMode() bool           { return s.mysqlMode }

// parseColumnType consumes only the type portion, leaving constraints to the
// shared parser. Unknown words are deliberately left for its useful error.
func (p *Parser) parseColumnType(col *ColumnDef) error {
	word := strings.ToUpper(p.cur.Value)
	col.ColType = ColText
	bits := 0
	switch word {
	case "INTEGER", "INT", "BIGINT", "SMALLINT", "MEDIUMINT", "TINYINT", "BOOL", "BOOLEAN":
		col.ColType = ColInteger
		switch word {
		case "TINYINT", "BOOL", "BOOLEAN":
			bits = 8
		case "SMALLINT":
			bits = 16
		case "MEDIUMINT":
			bits = 24
		case "INT", "INTEGER":
			bits = 32
		default:
			bits = 64
		}
		p.advance()
		if p.cur.Type == TokLParen {
			p.advance()
			if p.cur.Type != TokInt {
				return fmt.Errorf("integer display width must be positive")
			}
			n, err := strconv.ParseInt(p.cur.Value, 10, 64)
			if err != nil || n <= 0 {
				return fmt.Errorf("invalid integer display width")
			}
			p.advance()
			if err = p.expect(TokRParen); err != nil {
				return err
			}
		}
		col.Declared = word
		col.IntegerBits = bits
	case "REAL", "DOUBLE", "FLOAT":
		col.ColType = ColReal
		p.advance()
		if word == "DOUBLE" && strings.EqualFold(p.cur.Value, "PRECISION") {
			p.advance()
			word = "DOUBLE PRECISION"
		}
		if word == "FLOAT" {
			col.Declared = word
		}
	case "TEXT", "TINYTEXT", "MEDIUMTEXT", "LONGTEXT":
		col.ColType = ColText
		p.advance()
		if word != "TEXT" {
			col.Declared = word
			switch word {
			case "TINYTEXT":
				col.Length = 255
			case "MEDIUMTEXT":
				col.Length = 16777215
			case "LONGTEXT":
				col.Length = 4294967295
			}
		}
	case "BLOB", "TINYBLOB", "MEDIUMBLOB", "LONGBLOB":
		col.ColType = ColBlob
		p.advance()
		if word != "BLOB" {
			col.Declared = word
			switch word {
			case "TINYBLOB":
				col.Length = 255
			case "MEDIUMBLOB":
				col.Length = 16777215
			case "LONGBLOB":
				col.Length = 4294967295
			}
		}
	case "VARCHAR", "VARBINARY", "BINARY":
		p.advance()
		n, err := p.parseStringLength()
		if err != nil {
			return err
		}
		col.Length = n
		if word == "VARCHAR" {
			col.ColType = ColText
		} else {
			col.ColType = ColBlob
		}
		if word == "BINARY" {
			if n > 255 {
				return fmt.Errorf("BINARY length must be between 1 and 255")
			}
			col.FixedBinary = true
			col.Declared = "BINARY"
		}
	case "TIMESTAMP", "DATETIME", "DATE":
		col.ColType = ColText
		col.Declared = word
		p.advance()
	case "ENUM":
		col.ColType = ColText
		col.Declared = word
		p.advance()
		if err := p.expect(TokLParen); err != nil {
			return err
		}
		seen := make(map[string]bool)
		for {
			if p.cur.Type != TokString {
				return fmt.Errorf("ENUM requires string values")
			}
			if seen[p.cur.Value] {
				return fmt.Errorf("duplicate ENUM value: %s", p.cur.Value)
			}
			seen[p.cur.Value] = true
			col.EnumValues = append(col.EnumValues, p.cur.Value)
			p.advance()
			if p.cur.Type != TokComma {
				break
			}
			p.advance()
		}
		if len(col.EnumValues) > 65535 {
			return fmt.Errorf("too many ENUM values")
		}
		if err := p.expect(TokRParen); err != nil {
			return err
		}
	default:
		return nil
	}
	if strings.EqualFold(p.cur.Value, "UNSIGNED") {
		if col.ColType != ColInteger && col.ColType != ColReal {
			return fmt.Errorf("UNSIGNED requires a numeric column")
		}
		col.Unsigned = true
		if col.Declared == "" {
			col.Declared = word
		}
		col.IntegerBits = bits
		p.advance()
	}
	return nil
}

func decorateColumnValue(v Value, col ColumnDef) Value {
	v.Binary = col.Binary
	if !v.Null && len(col.EnumValues) > 0 {
		for i, x := range col.EnumValues {
			if v.Text == x {
				v.EnumValue = true
				v.EnumOrdinal = int64(i + 1)
				break
			}
		}
	}
	return v
}

// normalizeColumnValue is shared by inserts, updates and ALTER rewrites, before
// validation and key encoding. It never returns borrowed binary storage.
func normalizeColumnValue(v Value, col ColumnDef) (Value, error) {
	if v.Null {
		return v, nil
	}
	if len(col.EnumValues) > 0 && v.Type == ColInteger {
		if v.Int < 1 || v.Int > int64(len(col.EnumValues)) {
			return v, fmt.Errorf("invalid ENUM ordinal for %s", col.Name)
		}
		v = NewTextValue(col.EnumValues[v.Int-1])
	}
	if (col.MySQL || col.FixedBinary) && col.ColType == ColBlob && v.Type == ColText {
		v = NewBlobValue([]byte(v.Text))
	}
	if col.MySQL && col.ColType == ColText && v.Type == ColBlob {
		v = NewTextValue(string(v.Blob))
	}
	v = applyAffinity(v, col.ColType)
	if len(col.EnumValues) > 0 && v.Type == ColText {
		found := false
		for _, x := range col.EnumValues {
			if v.Text == x {
				found = true
				break
			}
		}
		if !found {
			return v, fmt.Errorf("invalid ENUM value for %s: %q", col.Name, v.Text)
		}
	}
	if col.FixedBinary && v.Type == ColBlob && int64(len(v.Blob)) < col.Length {
		padded := make([]byte, int(col.Length))
		copy(padded, v.Blob)
		v.Blob = padded
	}
	if (col.Declared == "TIMESTAMP" || col.Declared == "DATETIME" || col.Declared == "DATE") && v.Type == ColText {
		layouts := []string{"2006-01-02 15:04:05", "20060102150405", time.RFC3339, "2006-01-02"}
		var parsed time.Time
		valid := false
		for _, layout := range layouts {
			if t, err := time.Parse(layout, v.Text); err == nil {
				parsed = t
				valid = true
				break
			}
		}
		if !valid {
			return v, fmt.Errorf("invalid %s value for %s: %q", col.Declared, col.Name, v.Text)
		}
		layout := "2006-01-02 15:04:05"
		if col.Declared == "DATE" {
			layout = "2006-01-02"
		}
		v.Text = parsed.UTC().Format(layout)
	}
	return decorateColumnValue(v, col), nil
}

func mysqlColumnRange(col ColumnDef, v Value) error {
	if v.Null {
		return nil
	}
	if col.Unsigned && ((v.Type == ColInteger && v.Int < 0) || (v.Type == ColReal && v.Real < 0)) {
		return fmt.Errorf("unsigned value out of range for %s", col.Name)
	}
	if v.Type == ColInteger && col.IntegerBits > 0 && col.IntegerBits < 64 && (col.MySQL || col.Unsigned || col.Declared != "INTEGER" && col.Declared != "INT") {
		var min, max int64
		if col.Unsigned {
			max = (int64(1) << col.IntegerBits) - 1
		} else {
			max = (int64(1) << (col.IntegerBits - 1)) - 1
			min = -(int64(1) << (col.IntegerBits - 1))
		}
		if v.Int < min || v.Int > max {
			return fmt.Errorf("integer value out of range for %s", col.Name)
		}
	}
	if col.Declared == "FLOAT" && v.Type == ColReal && math.Abs(v.Real) > math.MaxFloat32 {
		return fmt.Errorf("FLOAT value out of range for %s", col.Name)
	}
	return nil
}

func (s *Schema) autoValueRequested(col *ColumnDef, v Value, supplied bool) bool {
	if col == nil || !col.AutoIncrement {
		return false
	}
	zero := v.Type == ColInteger && v.Int == 0
	if options, ok := MySQLSession(s.ctx); ok && strings.Contains(strings.ToUpper(options.SQLMode), "NO_AUTO_VALUE_ON_ZERO") {
		zero = false
	}
	return v.Null && !supplied || s.mysqlMode && col.MySQLAutoIncrement && (v.Null || zero)
}

func pkName(col *ColumnDef) string {
	if col == nil {
		return ""
	}
	return col.Name
}

func (s *Schema) mysqlFoundRows() bool {
	options, ok := MySQLSession(s.ctx)
	return !ok || options.FoundRows
}
func (s *Schema) mysqlUpsertChanges() int64 {
	if !s.mysqlMode {
		return 1
	}
	if s.mysqlConflictChanged {
		return 2
	}
	if s.mysqlFoundRows() {
		return 1
	}
	return 0
}

func (s *Schema) normalizeMySQLValue(v Value, col ColumnDef) (Value, error) {
	if s.mysqlMode {
		col.MySQL = true
	}
	return normalizeColumnValue(v, col)
}
func (s *Schema) normalizeMySQLRow(info *TableInfo, row map[string]Value) error {
	for _, col := range info.Columns {
		v, err := s.normalizeMySQLValue(row[col.Name], col)
		if err != nil {
			return err
		}
		row[col.Name] = v
	}
	return nil
}
func (s *Schema) comparisonAffinity(v Value, col ColumnDef) Value {
	if s.mysqlMode && col.ColType == ColBlob && v.Type == ColText && !v.Null {
		return NewBlobValue([]byte(v.Text))
	}
	return applyAffinity(v, col.ColType)
}

// Nonunique prefix declarations use full-value physical keys. This keeps every
// answer and ordering correct while retaining the requested prefix metadata.
// Unique prefixes need a different uniqueness domain and must never be silently
// substituted with full-value uniqueness.
func validateIndexPrefixes(columns []ColumnDef, idx *IndexInfo) error {
	for i, n := range idx.PrefixLengths {
		if n == 0 {
			continue
		}
		if i >= len(idx.columnNames()) || n < 0 {
			return fmt.Errorf("invalid index prefix length")
		}
		if idx.IsUnique {
			return fmt.Errorf("unique prefix indexes are not supported")
		}
		for _, col := range columns {
			if col.Name == idx.columnNames()[i] {
				if col.ColType != ColText && col.ColType != ColBlob {
					return fmt.Errorf("index prefix requires a string or binary column")
				}
				if col.Length > 0 && n > col.Length {
					return fmt.Errorf("index prefix exceeds column length")
				}
			}
		}
	}
	return nil
}

func formatIndexColumns(idx *IndexInfo) string {
	columns := idx.columnNames()
	parts := make([]string, len(columns))
	for i, name := range columns {
		parts[i] = quoteIdent(name)
		if i < len(idx.PrefixLengths) && idx.PrefixLengths[i] > 0 {
			parts[i] += fmt.Sprintf("(%d)", idx.PrefixLengths[i])
		}
	}
	return strings.Join(parts, ", ")
}
func copyColumnType(dst, src *ColumnDef) {
	dst.Declared = src.Declared
	dst.Unsigned = src.Unsigned
	dst.IntegerBits = src.IntegerBits
	dst.FixedBinary = src.FixedBinary
	dst.EnumValues = append([]string(nil), src.EnumValues...)
}

func mysqlBinaryCompare(a, b Value) (int, bool) {
	if a.Null || b.Null {
		return 0, false
	}
	if a.Type == ColBlob && b.Type == ColText {
		return bytes.Compare(a.Blob, []byte(b.Text)), true
	}
	if a.Type == ColText && b.Type == ColBlob {
		return bytes.Compare([]byte(a.Text), b.Blob), true
	}
	return 0, false
}

// MySQL options select supported behavior; storage remains the native LSM.
func (p *Parser) parseMySQLTableOptions(stmt *CreateTableStmt) error {
	for p.cur.Type != TokEOF && p.cur.Type != TokSemicolon {
		if p.cur.Type == TokComma {
			p.advance()
			continue
		}
		word := strings.ToUpper(p.cur.Value)
		if word == "DEFAULT" {
			p.advance()
			word = strings.ToUpper(p.cur.Value)
		}
		switch word {
		case "ENGINE":
			p.advance()
			if p.cur.Type == TokEq {
				p.advance()
			}
			if !strings.EqualFold(p.cur.Value, "InnoDB") && !strings.EqualFold(p.cur.Value, "MyISAM") {
				return fmt.Errorf("unsupported storage engine: %s", p.cur.Value)
			}
			stmt.Engine = p.cur.Value
			p.advance()
		case "CHARSET", "CHARACTER":
			p.advance()
			if word == "CHARACTER" {
				if err := p.expectKeyword("SET"); err != nil {
					return err
				}
			}
			if p.cur.Type == TokEq {
				p.advance()
			}
			switch strings.ToLower(p.cur.Value) {
			case "binary", "utf8", "utf8mb3", "utf8mb4":
			default:
				return fmt.Errorf("unsupported character set: %s", p.cur.Value)
			}
			stmt.Charset = strings.ToLower(p.cur.Value)
			p.advance()
		case "COLLATE":
			p.advance()
			if p.cur.Type == TokEq {
				p.advance()
			}
			switch strings.ToLower(p.cur.Value) {
			case "binary", "utf8_bin", "utf8mb4_bin", "utf8mb3_bin":
			default:
				return fmt.Errorf("unsupported table collation: %s", p.cur.Value)
			}
			p.advance()
		default:
			return fmt.Errorf("unsupported table option: %s", p.cur.Value)
		}
	}
	return nil
}
