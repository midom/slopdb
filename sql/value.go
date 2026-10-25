package sql

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
)

type ColumnType int

const (
	ColInteger ColumnType = iota
	ColText
	ColReal
	ColBlob
	ColUnsigned
	ColDecimal
	ColJSON
)

func (ct ColumnType) String() string {
	switch ct {
	case ColInteger:
		return "INTEGER"
	case ColText:
		return "TEXT"
	case ColReal:
		return "REAL"
	case ColUnsigned:
		return "BIGINT UNSIGNED"
	case ColDecimal:
		return "DECIMAL"
	case ColJSON:
		return "JSON"
	case ColBlob:
		return "BLOB"
	}
	return "UNKNOWN"
}

type ForeignKeyDef struct {
	Name              string `json:"name,omitempty"`
	OnDelete          string `json:"on_delete,omitempty"`
	OnUpdate          string `json:"on_update,omitempty"`
	Deferrable        bool   `json:"deferrable,omitempty"`
	InitiallyDeferred bool   `json:"initially_deferred,omitempty"`
	Columns           []string
	RefTable          string
	RefColumns        []string
}

type ColumnDef struct {
	ParameterIndex int    `json:"-"` // internal static inference provenance; never persisted
	SemanticType   TypeID `json:"-"` // inferred logical type; durable declarations retain existing catalog fields
	// DisplayName preserves a duplicate SQL output label; Name is the unique row-map key.
	DisplayName          string            `json:"-"`
	TypeModifiers        bool              `json:"type_modifiers,omitempty"`
	Postgres             bool              `json:"postgres_type,omitempty"`
	RefOnDelete          string            `json:"ref_on_delete,omitempty"`
	RefOnUpdate          string            `json:"ref_on_update,omitempty"`
	RefDeferrable        bool              `json:"ref_deferrable,omitempty"`
	RefInitiallyDeferred bool              `json:"ref_initially_deferred,omitempty"`
	Precision            int               `json:"decimal_precision,omitempty"`
	Scale                int               `json:"decimal_scale,omitempty"`
	Declared             string            `json:"declared_type,omitempty"`
	Unsigned             bool              `json:"unsigned,omitempty"`
	IntegerBits          int               `json:"integer_bits,omitempty"`
	FixedBinary          bool              `json:"fixed_binary,omitempty"`
	EnumValues           []string          `json:"enum_values,omitempty"`
	MySQL                bool              `json:"mysql_affinity,omitempty"`
	MySQLAutoIncrement   bool              `json:"mysql_auto_increment,omitempty"`
	TemporalPrecision    int               `json:"temporal_precision,omitempty"`
	ConstraintNames      map[string]string `json:"constraint_names,omitempty"`
	Name                 string
	ColType              ColumnType
	Length               int64 `json:"length,omitempty"`                // positive VARCHAR/VARBINARY character/byte limit
	AllowExistingLength  bool  `json:"allow_existing_length,omitempty"` // preserve preexisting overlength values on unrelated updates
	Binary               bool  `json:"binary,omitempty"`
	NotNull              bool
	Default              Expression `json:"-"`
	DefaultSQL           string     `json:"default_sql,omitempty"`
	PrimaryKey           bool
	Unique               bool
	Check                Expression `json:"-"`
	CheckSQL             string     `json:"check_sql,omitempty"`
	AutoIncrement        bool
	RefTable             string
	RefColumns           []string
}

// DeclaredType preserves bounded string declarations for schema introspection.
// The underlying SQL value remains TEXT or BLOB.
func (col ColumnDef) DeclaredType() string {
	if col.Postgres && col.Declared != "" {
		if col.Unsigned {
			return col.Declared + " UNSIGNED"
		}
		if col.ColType == ColDecimal && col.Precision > 0 {
			return fmt.Sprintf("NUMERIC(%d,%d)", col.Precision, col.Scale)
		}
		if col.Length > 0 && col.ColType == ColText {
			return fmt.Sprintf("VARCHAR(%d)", col.Length)
		}
		if col.TypeModifiers && (PostgresColumnOID(col) == 1083 || PostgresColumnOID(col) == 1114 || PostgresColumnOID(col) == 1184 || PostgresColumnOID(col) == 1266) {
			return postgresTemporalTypeModifier(col.Declared, col.TemporalPrecision)
		}
		return col.Declared
	}
	if col.Declared != "" && ((col.Declared != "INTEGER" && col.Declared != "INT" && col.Declared != "BIGINT") || col.MySQL || col.Unsigned) {
		declared := col.Declared
		if col.ColType == ColDecimal {
			declared = fmt.Sprintf("DECIMAL(%d,%d)", col.Precision, col.Scale)
		}
		if col.FixedBinary {
			declared = fmt.Sprintf("BINARY(%d)", col.Length)
		}
		if col.TemporalPrecision > 0 {
			declared += fmt.Sprintf("(%d)", col.TemporalPrecision)
		}
		if len(col.EnumValues) > 0 {
			parts := make([]string, len(col.EnumValues))
			for i, value := range col.EnumValues {
				parts[i] = "'" + strings.ReplaceAll(value, "'", "''") + "'"
			}
			declared = "ENUM(" + strings.Join(parts, ",") + ")"
		}
		if col.Unsigned {
			declared += " UNSIGNED"
		}
		return declared
	}
	if col.Length > 0 {
		switch col.ColType {
		case ColText:
			return fmt.Sprintf("VARCHAR(%d)", col.Length)
		case ColBlob:
			return fmt.Sprintf("VARBINARY(%d)", col.Length)
		}
	}
	return col.ColType.String()
}

type CreateTableStmt struct {
	Engine        string
	Charset       string
	Temporary     bool
	Like          string // source table for CREATE TABLE ... LIKE
	Constraints   []ConstraintDef
	Table         string
	Columns       []ColumnDef
	TableCheck    []Expression `json:"-"`
	TableCheckSQL []string     `json:"table_check_sql,omitempty"`
	TableUnique   [][]string
	ForeignKeys   []ForeignKeyDef
	PKColumns     []string
	IfNotExists   bool
	Indexes       []IndexDef
}

type IndexDef struct {
	DisplayName   string
	Name          string
	Columns       []string
	Unique        bool
	Fulltext      bool
	PrefixLengths []int64
}

type InsertStmt struct {
	With          *WithClause
	Table         string
	ColumnNames   []string
	Values        []Expression
	ValueGroups   [][]Expression
	Select        *SelectStmt
	DefaultValues bool
	Replace       bool
	Ignore        bool
	OnConflict    *OnConflictClause
	Returning     []SelectColumn
}

type SelectStmt struct {
	JoinGroup    bool // internal parenthesized joined table, preserves its qualified namespace
	Windows      []WindowDefinition
	prepared     *preparedExecutionPlan
	With         *WithClause
	Compound     *UnionStmt
	materialized *ResultSet
	Columns      []SelectColumn
	From         string
	FromAlias    string
	FromExpr     Expression
	Joins        []JoinClause
	Virtual      *VirtualTable
	Where        Expression
	OrderBy      []OrderByClause
	Limit        int64
	Offset       int64
	LimitParam   int // one-based parameter index; zero means a constant
	OffsetParam  int
	SelectAll    bool
	Distinct     bool
	GroupBy      []Expression
	Having       Expression
	Lock         *SelectLock
	trackRows    bool // retain physical row origins when expanding a view
	mutationRows bool // retain each physical table alias's row identity for joined DML
}

// SelectLock applies to rows from tables in this SELECT's query block.
type SelectLock struct {
	Exclusive bool
	Wait      LockWaitPolicy
	ShareMode bool // preserve LOCK IN SHARE MODE when formatting stored SQL
}

type VirtualTable struct {
	Name string
	Args []Expression
}

type JoinClause struct {
	Type     string // INNER, LEFT, RIGHT, CROSS, NATURAL
	Table    string
	Alias    string
	Subquery *SelectStmt
	On       Expression
	Using    []string
}

type SelectColumn struct {
	Expr  Expression
	Alias string
	Star  bool
}

type OrderByClause struct {
	Expr       Expression
	Desc       bool
	NullsFirst bool
	NullsLast  bool
}

type DeleteStmt struct {
	Alias       string
	Joins       []JoinClause
	Targets     []string
	With        *WithClause
	From        string
	Where       Expression
	Limit       int64
	Offset      int64
	LimitParam  int
	OffsetParam int
	Returning   []SelectColumn
}

type UpdateStmt struct {
	Alias       string
	Joins       []JoinClause
	prepared    *preparedExecutionPlan
	With        *WithClause
	Table       string
	Set         []UpdateSet
	Where       Expression
	Limit       int64
	Offset      int64
	LimitParam  int
	OffsetParam int
	Returning   []SelectColumn
}

type UpdateSet struct {
	Column string
	Value  Expression
}

type DropTableStmt struct {
	Table     string
	Temporary bool
	IfExists  bool
}

type CreateViewStmt struct {
	View    string
	Columns []string
	Select  *SelectStmt
	SQL     string // raw SQL text of the SELECT for persistence
}

type DropTriggerStmt struct{ Name string }

type DropViewStmt struct {
	View string
}

type CreateTriggerStmt struct {
	Name       string
	InsteadOf  bool
	Event      string
	View       string
	ForEachRow bool
	Timing     string
	When       Expression
	BodySQL    []string
	Body       []Statement
}

type CreateIndexStmt struct {
	PrefixLengths         []int64
	PublicationAbortAfter *time.Duration
	Index                 string
	Table                 string
	Column                string
	Columns               []string
	Unique                bool
	Fulltext              bool
	IfNotExists           bool
}

type ExplainStmt struct {
	Stmt    Statement
	Analyze bool
}

type PragmaStmt struct {
	Name  string
	Value Expression
	Args  []Expression
}

// ShowStmt describes a read-only catalog query. DESCRIBE uses the COLUMNS form.
type ShowStmt struct {
	Kind     string
	Database string
	Object   string
	Full     bool
	Like     *string
	Where    Expression
	Column   string // optional exact column name for DESCRIBE
}

func (*ShowStmt) stmt() {}

type AnalyzeStmt struct {
	Table string
}

type BeginStmt struct {
	Isolation          IsolationLevel
	ConsistentSnapshot bool
}
type SetIsolationStmt struct {
	Level   IsolationLevel
	Session bool
}

func (*SetIsolationStmt) stmt() {}

type CommitStmt struct{ Durability string }
type RollbackStmt struct{}
type SavepointStmt struct {
	Name string
}
type ReleaseStmt struct {
	Name   string
	ToSave bool
}

type DropIndexStmt struct {
	Table                 string // optional MySQL ON table qualifier
	PublicationAbortAfter *time.Duration
	Index                 string
}

type ConstraintDef struct {
	OnDelete          string     `json:"on_delete,omitempty"`
	OnUpdate          string     `json:"on_update,omitempty"`
	Deferrable        bool       `json:"deferrable,omitempty"`
	InitiallyDeferred bool       `json:"initially_deferred,omitempty"`
	PrefixLengths     []int64    `json:"prefix_lengths,omitempty"`
	Name              string     `json:"name"`
	Kind              string     `json:"kind"`
	Columns           []string   `json:"columns,omitempty"`
	Check             Expression `json:"-"`
	CheckSQL          string     `json:"check_sql,omitempty"`
	RefTable          string     `json:"ref_table,omitempty"`
	RefColumns        []string   `json:"ref_columns,omitempty"`
	IndexID           uint64     `json:"index_id,omitempty"`
	Inline            bool       `json:"inline,omitempty"`
	Fulltext          bool       `json:"fulltext,omitempty"`
}

type AlterTableStmt struct {
	PublicationAbortAfter *time.Duration
	ObjectKind            string
	TableIfExists         bool
	IfExists              bool
	IfNotExists           bool
	Cascade               bool
	Actions               []*AlterTableStmt
	Constraint            *ConstraintDef
	Default               Expression
	Using                 Expression
	AllowExisting         bool
	Position              string
	After                 string
	ViewSQL               string
	ViewColumns           []string
	Table                 string
	Operation             string
	NewName               string
	OldColName            string
	NewColName            string
	Column                *ColumnDef
}

type VacuumStmt struct{}

type UnionStmt struct {
	With        *WithClause
	LimitParam  int
	OffsetParam int
	Left        *SelectStmt
	Right       Statement
	All         bool
	Op          string // UNION, INTERSECT, EXCEPT
	OrderBy     []OrderByClause
	Limit       int64
	Offset      int64
}

type Statement interface {
	stmt()
}

func (*CreateTableStmt) stmt()   {}
func (*InsertStmt) stmt()        {}
func (*SelectStmt) stmt()        {}
func (*DeleteStmt) stmt()        {}
func (*UpdateStmt) stmt()        {}
func (*DropTableStmt) stmt()     {}
func (*CreateIndexStmt) stmt()   {}
func (*DropIndexStmt) stmt()     {}
func (*AlterTableStmt) stmt()    {}
func (*VacuumStmt) stmt()        {}
func (*UnionStmt) stmt()         {}
func (*ExplainStmt) stmt()       {}
func (*PragmaStmt) stmt()        {}
func (*AnalyzeStmt) stmt()       {}
func (*CreateViewStmt) stmt()    {}
func (*DropViewStmt) stmt()      {}
func (*CreateTriggerStmt) stmt() {}
func (*BeginStmt) stmt()         {}
func (*CommitStmt) stmt()        {}
func (*RollbackStmt) stmt()      {}
func (*SavepointStmt) stmt()     {}
func (*ReleaseStmt) stmt()       {}

type Expression interface {
	expr()
}

type LiteralExpr struct {
	Value          Value
	BoundParameter bool // bound integers are expressions, not ORDER/GROUP ordinals
}

// ParameterExpr exists only in prepared templates. Binding replaces it with a
// LiteralExpr before planning or evaluation.
type ParameterExpr struct{ Index int }

func (*ParameterExpr) expr() {}

type ColumnRefExpr struct {
	Name string
}

type BinaryExpr struct {
	Left   Expression
	Op     string
	Right  Expression
	Escape byte // for LIKE ESCAPE
}

type UnaryExpr struct {
	Op    string
	Right Expression
}

type FuncCallExpr struct {
	PostgresBare         bool // parser keyword expression; native mode resolves it as a column
	MySQLGroupConcat     bool
	GroupConcatOrder     []OrderByClause
	GroupConcatSeparator Expression
	windowKey            string // resolved privately per SELECT, immutable during evaluation
	Window               *WindowSpec
	Distinct             bool
	Name                 string
	Args                 []Expression
}

type InListExpr struct {
	Expr Expression
	List []Expression
	Not  bool
}

type BetweenExpr struct {
	Expr Expression
	Low  Expression
	High Expression
	Not  bool
}

type CaseExpr struct {
	Expr     Expression
	Whens    []CaseWhen
	ElseExpr Expression
}

type CaseWhen struct {
	Cond Expression
	Then Expression
}

type SubqueryExpr struct {
	Stmt *SelectStmt
}

type ExistsExpr struct {
	Stmt *SelectStmt
	Not  bool
}

func (*LiteralExpr) expr()   {}
func (*ColumnRefExpr) expr() {}
func (*BinaryExpr) expr()    {}
func (*UnaryExpr) expr()     {}
func (*FuncCallExpr) expr()  {}
func (*InListExpr) expr()    {}
func (*BetweenExpr) expr()   {}
func (*CaseExpr) expr()      {}
func (*SubqueryExpr) expr()  {}
func (*ExistsExpr) expr()    {}

type Value struct {
	SemanticType TypeID    `json:"-"` // compact protocol-neutral logical type, never a physical row tag
	PGType       string    `json:"-"` // deprecated compatibility input; use SemanticType/WithDeclaredType
	HexLiteral   bool      `json:"-"` // transient X'..' numeric context, never a stored BLOB property
	Collation    Collation `json:"-"`
	Type         ColumnType
	EnumValue    bool  `json:"-"`
	EnumOrdinal  int64 `json:"-"`
	Binary       bool  `json:"-"` // expression-level BINARY override; never persisted
	Int          int64
	Real         float64
	Uint         uint64
	NumericText  string `json:"-"` // exact source spelling for decimal conversion of numeric literals
	NumericExact string `json:"-"` // immutable rational provenance for native REAL literal expressions
	Text         string
	Blob         []byte
	Null         bool
}

func (v Value) MarshalJSON() ([]byte, error) {
	if v.Null {
		return json.Marshal(nil)
	}
	switch v.Type {
	case ColInteger:
		return json.Marshal(v.Int)
	case ColUnsigned:
		return json.Marshal(v.Uint)
	case ColDecimal, ColJSON:
		return []byte(v.Text), nil
	case ColReal:
		return json.Marshal(v.Real)
	case ColText:
		return json.Marshal(v.Text)
	case ColBlob:
		return json.Marshal(v.Blob)
	}
	return json.Marshal(nil)
}

func (v *Value) UnmarshalJSON(data []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	var raw any
	if err := decoder.Decode(&raw); err != nil {
		return err
	}
	switch val := raw.(type) {
	case nil:
		*v = NewNullValue()
	case json.Number:
		if n, err := strconv.ParseInt(string(val), 10, 64); err == nil {
			*v = NewIntValue(n)
		} else if n, err := strconv.ParseUint(string(val), 10, 64); err == nil {
			*v = NewUnsignedValue(n)
		} else if decimal, err := NewDecimalValue(string(val)); err == nil {
			*v = decimal
		} else {
			return err
		}
	case string:
		*v = NewTextValue(val)
	case bool:
		if val {
			*v = NewIntValue(1)
		} else {
			*v = NewIntValue(0)
		}
	default:
		value, err := NewJSONValue(string(data))
		if err != nil {
			return err
		}
		*v = value
	}
	return nil
}

func NewIntValue(v int64) Value {
	return Value{Type: ColInteger, Int: v}
}

func NewRealValue(v float64) Value {
	return Value{Type: ColReal, Real: v}
}

func NewTextValue(v string) Value {
	return Value{Type: ColText, Text: v}
}

func NewNullValue() Value {
	return Value{Null: true}
}

func NewBlobValue(v []byte) Value {
	return Value{Type: ColBlob, Blob: v}
}

func (v Value) String() string {
	if v.Null {
		return "NULL"
	}
	switch v.Type {
	case ColInteger:
		return fmt.Sprintf("%d", v.Int)
	case ColUnsigned:
		return strconv.FormatUint(v.Uint, 10)
	case ColDecimal, ColJSON:
		return v.Text
	case ColReal:
		return fmt.Sprintf("%g", v.Real)
	case ColText:
		return v.Text
	case ColBlob:
		return fmt.Sprintf("%v", v.Blob)
	}
	return "NULL"
}

func ParseValue(typ ColumnType, s string) Value {
	switch typ {
	case ColUnsigned:
		n, _ := strconv.ParseUint(s, 10, 64)
		return NewUnsignedValue(n)
	case ColDecimal:
		v, _ := NewDecimalValue(s)
		return v
	case ColJSON:
		v, _ := NewJSONValue(s)
		return v
	case ColInteger:
		n, _ := strconv.ParseInt(s, 10, 64)
		return NewIntValue(n)
	case ColReal:
		f, _ := strconv.ParseFloat(s, 64)
		return NewRealValue(f)
	case ColText:
		return NewTextValue(s)
	default:
		return NewTextValue(s)
	}
}

func ValueCompare(a, b Value) int {
	if a.Null && b.Null {
		return 0
	}
	if a.Null {
		return -1
	}
	if b.Null {
		return 1
	}

	if a.Type == ColJSON || b.Type == ColJSON {
		return compareJSONValues(a, b)
	}
	if (a.Type == ColUnsigned || a.Type == ColDecimal || b.Type == ColUnsigned || b.Type == ColDecimal) && isNumeric(a) && isNumeric(b) {
		return exactNumericCompare(a, b)
	}
	if a.Type == ColInteger && b.Type == ColInteger {
		if a.Int < b.Int {
			return -1
		}
		if a.Int > b.Int {
			return 1
		}
		return 0
	}
	if a.Type == ColReal && b.Type == ColReal {
		if a.Real < b.Real {
			return -1
		}
		if a.Real > b.Real {
			return 1
		}
		return 0
	}

	aNum, bNum := isNumeric(a), isNumeric(b)
	if aNum && bNum {
		compare := func(i int64, f float64) int {
			if f >= 9223372036854775808.0 {
				return -1
			}
			if f < -9223372036854775808.0 {
				return 1
			}
			n := int64(f)
			if i < n {
				return -1
			}
			if i > n {
				return 1
			}
			if float64(n) < f {
				return -1
			}
			if float64(n) > f {
				return 1
			}
			return 0
		}
		if a.Type == ColInteger {
			return compare(a.Int, b.Real)
		}
		return -compare(b.Int, a.Real)
	}
	if aNum {
		return -1
	}
	if bNum {
		return 1
	}
	if a.Type == ColBlob && b.Type == ColBlob {
		return bytes.Compare(a.Blob, b.Blob)
	}
	if a.Type == ColBlob {
		return 1
	}
	if b.Type == ColBlob {
		return -1
	}
	return strings.Compare(a.Text, b.Text)
}

func isNumeric(v Value) bool {
	return v.Type == ColInteger || v.Type == ColReal || v.Type == ColUnsigned || v.Type == ColDecimal
}

func toFloatVal(v Value) float64 {
	switch v.Type {
	case ColUnsigned:
		return float64(v.Uint)
	case ColDecimal:
		f, _ := strconv.ParseFloat(v.Text, 64)
		return f
	case ColInteger:
		return float64(v.Int)
	case ColReal:
		return v.Real
	default:
		return 0
	}
}

func ValueSerialize(v Value) []byte {
	if v.Null {
		return []byte{0}
	}
	switch v.Type {
	case ColUnsigned:
		return append([]byte{5}, strconv.FormatUint(v.Uint, 10)...)
	case ColDecimal:
		return append([]byte{6}, v.Text...)
	case ColJSON:
		return append([]byte{7}, v.Text...)
	case ColInteger:
		buf := []byte{1}
		s := fmt.Sprintf("%d", v.Int)
		return append(buf, []byte(s)...)
	case ColReal:
		buf := make([]byte, 9)
		buf[0] = 2
		binary.LittleEndian.PutUint64(buf[1:], math.Float64bits(v.Real))
		return buf
	case ColText:
		buf := []byte{3}
		return append(buf, []byte(v.Text)...)
	case ColBlob:
		buf := []byte{4}
		return append(buf, v.Blob...)
	}
	return []byte{0}
}

func ValueDeserialize(data []byte) Value {
	if len(data) == 0 || data[0] == 0 {
		return NewNullValue()
	}
	switch data[0] {
	case 5:
		n, _ := strconv.ParseUint(string(data[1:]), 10, 64)
		return NewUnsignedValue(n)
	case 6:
		v, _ := NewDecimalValue(string(data[1:]))
		return v
	case 7:
		v, _ := NewJSONValue(string(data[1:]))
		return v
	case 1:
		n, _ := strconv.ParseInt(string(data[1:]), 10, 64)
		return NewIntValue(n)
	case 2:
		if len(data) >= 9 {
			bits := binary.LittleEndian.Uint64(data[1:9])
			return NewRealValue(math.Float64frombits(bits))
		}
		f, _ := strconv.ParseFloat(string(data[1:]), 64)
		return NewRealValue(f)
	case 3:
		return NewTextValue(string(data[1:]))
	case 4:
		blob := make([]byte, len(data)-1)
		copy(blob, data[1:])
		return Value{Type: ColBlob, Blob: blob}
	}
	return NewNullValue()
}

func (*DropTriggerStmt) stmt() {}

type SetConstraintsStmt struct {
	Names         []string
	All, Deferred bool
}

func (*SetConstraintsStmt) stmt() {}
