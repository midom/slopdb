package sql

import (
	"strconv"
	"strings"
)

// TypeID identifies a logical SQL type independently of its row encoding and
// wire protocol. IDs are transient metadata, never physical row type tags.
type TypeID uint16

const (
	TypeUnknown TypeID = iota
	TypeBoolean
	TypeInt8
	TypeInt16
	TypeInt24
	TypeInt32
	TypeInt64
	TypeUint8
	TypeUint16
	TypeUint24
	TypeUint32
	TypeUint64
	TypeFloat32
	TypeFloat64
	TypeDecimal
	TypeText
	TypeBinary
	TypeJSON
	TypeJSONB
	TypeUUID
	TypeDate
	TypeTime
	TypeTimeTZ
	TypeTimestamp
	TypeTimestampTZ
	TypeInterval
	TypeObjectID
	TypeRelationID
	TypeTypeID
	TypeSchemaID
	TypeIdentifier
	TypeEnum
	TypeVarchar
)

// ColUnknown is used only by static description of untyped parameters/NULLs.
// It must not be written to rows; -1 remains the ambiguous-name sentinel.
const ColUnknown ColumnType = -2

// LogicalType contains SQL semantics; storage affinity and wire encodings are
// separate mappings. Modifiers belong to columns/plans, not per-row allocations.
type LogicalType struct {
	Kind              TypeID
	IntegerBits       int
	Unsigned          bool
	Precision, Scale  int
	Length            int64
	TemporalPrecision int
	WithTimeZone      bool
	Collation         Collation
}

func (t LogicalType) Known() bool { return t.Kind != TypeUnknown }
func (t LogicalType) Declaration() string {
	switch t.Kind {
	case TypeBoolean:
		return "BOOLEAN"
	case TypeInt8:
		return "TINYINT"
	case TypeInt16:
		return "SMALLINT"
	case TypeInt24:
		return "MEDIUMINT"
	case TypeInt32:
		return "INTEGER"
	case TypeInt64:
		return "BIGINT"
	case TypeUint8:
		return "TINYINT UNSIGNED"
	case TypeUint16:
		return "SMALLINT UNSIGNED"
	case TypeUint24:
		return "MEDIUMINT UNSIGNED"
	case TypeUint32:
		return "INTEGER UNSIGNED"
	case TypeUint64:
		return "BIGINT UNSIGNED"
	case TypeFloat32:
		return "REAL"
	case TypeFloat64:
		return "DOUBLE PRECISION"
	case TypeDecimal:
		return "NUMERIC"
	case TypeText:
		return "TEXT"
	case TypeVarchar:
		return "VARCHAR"
	case TypeBinary:
		return "BYTEA"
	case TypeJSON:
		return "JSON"
	case TypeJSONB:
		return "JSONB"
	case TypeUUID:
		return "UUID"
	case TypeDate:
		return "DATE"
	case TypeTime:
		return "TIME"
	case TypeTimeTZ:
		return "TIMETZ"
	case TypeTimestamp:
		return "TIMESTAMP"
	case TypeTimestampTZ:
		return "TIMESTAMPTZ"
	case TypeInterval:
		return "INTERVAL"
	case TypeObjectID:
		return "OID"
	case TypeRelationID:
		return "REGCLASS"
	case TypeTypeID:
		return "REGTYPE"
	case TypeSchemaID:
		return "REGNAMESPACE"
	case TypeIdentifier:
		return "NAME"
	case TypeEnum:
		return "ENUM"
	}
	return ""
}
func logicalTypeID(id TypeID) LogicalType {
	t := LogicalType{Kind: id, TemporalPrecision: -1}
	switch id {
	case TypeInt8, TypeUint8:
		t.IntegerBits = 8
	case TypeInt16, TypeUint16:
		t.IntegerBits = 16
	case TypeInt24, TypeUint24:
		t.IntegerBits = 24
	case TypeInt32, TypeUint32, TypeObjectID, TypeRelationID, TypeTypeID, TypeSchemaID:
		t.IntegerBits = 32
	case TypeInt64, TypeUint64:
		t.IntegerBits = 64
	}
	t.Unsigned = id >= TypeUint8 && id <= TypeUint64 || id >= TypeObjectID && id <= TypeSchemaID
	t.WithTimeZone = id == TypeTimeTZ || id == TypeTimestampTZ
	return t
}

// LogicalTypeFromDeclaration recognizes declaration aliases, not value coercion.
// In particular it never consults a connection or changes stored column types.
func LogicalTypeFromDeclaration(name string) LogicalType {
	name = strings.ToUpper(strings.TrimSpace(name))
	name = strings.TrimPrefix(name, "PG_CATALOG.")
	var modifiers string
	if i := strings.IndexByte(name, '('); i >= 0 {
		if j := strings.IndexByte(name[i:], ')'); j >= 0 {
			modifiers = name[i+1 : i+j]
			name = strings.TrimSpace(name[:i] + name[i+j+1:])
		}
	}
	id := TypeUnknown
	switch name {
	case "BOOL", "BOOLEAN":
		id = TypeBoolean
	case "TINYINT":
		id = TypeInt8
	case "SMALLINT", "INT2", "SMALLSERIAL", "SERIAL2":
		id = TypeInt16
	case "MEDIUMINT":
		id = TypeInt24
	case "INT", "INTEGER", "INT4", "SERIAL", "SERIAL4":
		id = TypeInt32
	case "BIGINT", "INT8", "BIGSERIAL", "SERIAL8", "SIGNED":
		id = TypeInt64
	case "TINYINT UNSIGNED":
		id = TypeUint8
	case "SMALLINT UNSIGNED":
		id = TypeUint16
	case "MEDIUMINT UNSIGNED":
		id = TypeUint24
	case "INT UNSIGNED", "INTEGER UNSIGNED":
		id = TypeUint32
	case "BIGINT UNSIGNED", "UNSIGNED":
		id = TypeUint64
	case "REAL", "FLOAT", "FLOAT4":
		id = TypeFloat32
	case "DOUBLE", "DOUBLE PRECISION", "FLOAT8":
		id = TypeFloat64
	case "NUMERIC", "DECIMAL":
		id = TypeDecimal
	case "VARCHAR", "CHARACTER VARYING":
		id = TypeVarchar
	case "TEXT", "CHAR", "CHARACTER":
		id = TypeText
	case "BLOB", "BYTEA", "BINARY", "VARBINARY":
		id = TypeBinary
	case "JSON":
		id = TypeJSON
	case "JSONB":
		id = TypeJSONB
	case "UUID":
		id = TypeUUID
	case "DATE":
		id = TypeDate
	case "TIME", "TIME WITHOUT TIME ZONE":
		id = TypeTime
	case "TIMETZ", "TIME WITH TIME ZONE":
		id = TypeTimeTZ
	case "DATETIME", "TIMESTAMP", "TIMESTAMP WITHOUT TIME ZONE":
		id = TypeTimestamp
	case "TIMESTAMPTZ", "TIMESTAMP WITH TIME ZONE":
		id = TypeTimestampTZ
	case "INTERVAL":
		id = TypeInterval
	case "OID":
		id = TypeObjectID
	case "REGCLASS":
		id = TypeRelationID
	case "REGTYPE":
		id = TypeTypeID
	case "REGNAMESPACE":
		id = TypeSchemaID
	case "NAME":
		id = TypeIdentifier
	case "ENUM":
		id = TypeEnum
	}
	t := logicalTypeID(id)
	if modifiers != "" {
		parts := strings.Split(modifiers, ",")
		first, e := strconv.Atoi(strings.TrimSpace(parts[0]))
		if e == nil {
			switch id {
			case TypeText, TypeVarchar, TypeBinary:
				t.Length = int64(first)
			case TypeDecimal:
				t.Precision = first
				if len(parts) > 1 {
					t.Scale, _ = strconv.Atoi(strings.TrimSpace(parts[1]))
				}
			case TypeTime, TypeTimeTZ, TypeTimestamp, TypeTimestampTZ:
				t.TemporalPrecision = first
			}
		}
	}
	return t
}
func LogicalTypeFromColumn(c ColumnDef) LogicalType {
	if c.ColType == ColUnknown {
		return logicalTypeID(TypeUnknown)
	}
	t := LogicalTypeFromDeclaration(c.Declared)
	if c.SemanticType != TypeUnknown {
		t = logicalTypeID(c.SemanticType)
	}
	// Legacy/native INTEGER and REAL use native widths, regardless of spelling.
	if !c.Postgres && c.SemanticType == TypeUnknown {
		switch c.ColType {
		case ColInteger:
			bits := c.IntegerBits
			if !c.MySQL {
				bits = 64
			}
			if bits == 0 {
				bits = 64
			}
			id := TypeInt64
			switch bits {
			case 8:
				id = TypeInt8
			case 16:
				id = TypeInt16
			case 24:
				id = TypeInt24
			case 32:
				id = TypeInt32
			}
			if c.Unsigned {
				id += TypeUint8 - TypeInt8
			}
			t = logicalTypeID(id)
		case ColUnsigned:
			t = logicalTypeID(TypeUint64)
		case ColReal:
			if c.MySQL && strings.EqualFold(c.Declared, "FLOAT") {
				t = logicalTypeID(TypeFloat32)
			} else {
				t = logicalTypeID(TypeFloat64)
			}
		}
	}
	if !t.Known() {
		switch c.ColType {
		case ColInteger:
			t = logicalTypeID(TypeInt64)
		case ColUnsigned:
			t = logicalTypeID(TypeUint64)
		case ColReal:
			t = logicalTypeID(TypeFloat64)
		case ColDecimal:
			t = logicalTypeID(TypeDecimal)
		case ColText:
			t = logicalTypeID(TypeText)
		case ColBlob:
			t = logicalTypeID(TypeBinary)
		case ColJSON:
			t = logicalTypeID(TypeJSON)
		}
	}
	if c.Unsigned && t.Kind >= TypeInt8 && t.Kind <= TypeInt64 {
		t.Kind += TypeUint8 - TypeInt8
		t.Unsigned = true
	}
	if c.ColType == ColText && c.Length > 0 && t.Kind == TypeText {
		t.Kind = TypeVarchar
	}
	t.Precision, t.Scale, t.Length = c.Precision, c.Scale, c.Length
	switch t.Kind {
	case TypeTime, TypeTimeTZ, TypeTimestamp, TypeTimestampTZ:
		if c.TypeModifiers || c.TemporalPrecision > 0 {
			t.TemporalPrecision = c.TemporalPrecision
		}
	}
	t.Collation = c.SemanticCollation
	if c.Binary {
		t.Collation = BinaryCollation
	}
	return t
}
func (c ColumnDef) LogicalType() LogicalType { return LogicalTypeFromColumn(c) }
func (t LogicalType) Column() ColumnDef {

	temporal := t.Kind == TypeTime || t.Kind == TypeTimeTZ || t.Kind == TypeTimestamp || t.Kind == TypeTimestampTZ
	modifiers := temporal && t.TemporalPrecision >= 0 || t.Kind == TypeDecimal && t.Precision > 0 || (t.Kind == TypeText || t.Kind == TypeVarchar || t.Kind == TypeBinary) && t.Length > 0
	c := ColumnDef{SemanticCollation: t.Collation, SemanticType: t.Kind, ColType: ColText, Declared: t.Declaration(), IntegerBits: t.IntegerBits, Unsigned: t.Unsigned, Precision: t.Precision, Scale: t.Scale, Length: t.Length, TemporalPrecision: t.TemporalPrecision, TypeModifiers: modifiers, Binary: t.Collation == BinaryCollation}
	switch t.Kind {
	case TypeUnknown:
		c.ColType = ColUnknown
		c.Declared = ""
	case TypeBoolean, TypeInt8, TypeInt16, TypeInt24, TypeInt32, TypeInt64, TypeObjectID, TypeRelationID, TypeTypeID, TypeSchemaID:
		c.ColType = ColInteger
	case TypeUint8, TypeUint16, TypeUint24, TypeUint32, TypeUint64:
		c.ColType = ColUnsigned
	case TypeFloat32, TypeFloat64:
		c.ColType = ColReal
	case TypeDecimal:
		c.ColType = ColDecimal
	case TypeBinary, TypeUUID:
		c.ColType = ColBlob
	case TypeJSON, TypeJSONB:
		c.ColType = ColJSON
	}
	return c
}
func LogicalTypeFromValue(v Value) LogicalType {
	id := v.SemanticType
	if id == TypeUnknown && v.PGType != "" {
		id = LogicalTypeFromDeclaration(v.PGType).Kind
	}
	if id != TypeUnknown {
		t := logicalTypeID(id)
		t.Collation = v.Collation
		if v.Binary {
			t.Collation = BinaryCollation
		}
		return t
	}
	if v.Null {
		return logicalTypeID(TypeUnknown)
	}
	return LogicalTypeFromColumn(ColumnDef{ColType: v.Type, Binary: v.Binary, SemanticCollation: v.Collation})
}
func (v Value) LogicalType() LogicalType         { return LogicalTypeFromValue(v) }
func (v Value) WithSemanticType(id TypeID) Value { v.SemanticType = id; v.PGType = ""; return v }
func (v Value) WithDeclaredType(name string) Value {
	return v.WithSemanticType(LogicalTypeFromDeclaration(name).Kind)
}
