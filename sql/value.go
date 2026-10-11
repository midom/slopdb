package sql

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
)

type ColumnType int

const (
	ColInteger ColumnType = iota
	ColText
	ColReal
	ColBlob
)

func (ct ColumnType) String() string {
	switch ct {
	case ColInteger:
		return "INTEGER"
	case ColText:
		return "TEXT"
	case ColReal:
		return "REAL"
	case ColBlob:
		return "BLOB"
	}
	return "UNKNOWN"
}

type ForeignKeyDef struct {
	Columns    []string
	RefTable   string
	RefColumns []string
}

type ColumnDef struct {
	Name          string
	ColType       ColumnType
	NotNull       bool
	Default       Expression `json:"-"`
	DefaultSQL    string     `json:"default_sql,omitempty"`
	PrimaryKey    bool
	Unique        bool
	Check         Expression `json:"-"`
	CheckSQL      string     `json:"check_sql,omitempty"`
	AutoIncrement bool
	RefTable      string
	RefColumns    []string
}

type CreateTableStmt struct {
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
	Name    string
	Columns []string
	Unique  bool
}

type InsertStmt struct {
	Table         string
	ColumnNames   []string
	Values        []Expression
	ValueGroups   [][]Expression
	Select        *SelectStmt
	DefaultValues bool
	Replace       bool
	Returning     []SelectColumn
}

type SelectStmt struct {
	Columns     []SelectColumn
	From        string
	FromAlias   string
	FromExpr    Expression
	Joins       []JoinClause
	Virtual     *VirtualTable
	Where       Expression
	OrderBy     []OrderByClause
	Limit       int64
	Offset      int64
	LimitParam  int // one-based parameter index; zero means a constant
	OffsetParam int
	SelectAll   bool
	Distinct    bool
	GroupBy     []Expression
	Having      Expression
	Lock        *SelectLock
	trackRows   bool // retain physical row origins when expanding a view
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
	From        string
	Where       Expression
	Limit       int64
	Offset      int64
	LimitParam  int
	OffsetParam int
	Returning   []SelectColumn
}

type UpdateStmt struct {
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
	Table string
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
	Index       string
	Table       string
	Column      string
	Columns     []string
	Unique      bool
	IfNotExists bool
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

type BeginStmt struct{}
type CommitStmt struct{}
type RollbackStmt struct{}
type SavepointStmt struct {
	Name string
}
type ReleaseStmt struct {
	Name   string
	ToSave bool
}

type DropIndexStmt struct {
	Index string
}

type AlterTableStmt struct {
	Table      string
	Operation  string
	NewName    string
	OldColName string
	NewColName string
	Column     *ColumnDef
}

type VacuumStmt struct{}

type UnionStmt struct {
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
	Distinct bool
	Name     string
	Args     []Expression
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
	Type ColumnType
	Int  int64
	Real float64
	Text string
	Blob []byte
	Null bool
}

func (v Value) MarshalJSON() ([]byte, error) {
	if v.Null {
		return json.Marshal(nil)
	}
	switch v.Type {
	case ColInteger:
		return json.Marshal(v.Int)
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
	var raw interface{}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	switch val := raw.(type) {
	case nil:
		*v = NewNullValue()
	case float64:
		if val == float64(int64(val)) && val < 1e15 {
			*v = NewIntValue(int64(val))
		} else {
			*v = NewRealValue(val)
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
		*v = NewTextValue(fmt.Sprintf("%v", val))
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
	return v.Type == ColInteger || v.Type == ColReal
}

func toFloatVal(v Value) float64 {
	switch v.Type {
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
