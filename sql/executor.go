package sql

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"maps"
	"math"
	"math/rand"
	"reflect"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"slopdb/storage"
)

type TableInfo struct {
	Name          string          `json:"name"`
	Columns       []ColumnDef     `json:"columns"`
	TableCheck    []Expression    `json:"-"`
	TableCheckSQL []string        `json:"table_check_sql,omitempty"`
	TableUnique   [][]string      `json:"table_unique,omitempty"`
	ForeignKeys   []ForeignKeyDef `json:"foreign_keys,omitempty"`
	PKColumns     []string        `json:"pk_columns,omitempty"`
}

type IndexInfo struct {
	Name     string   `json:"name"`
	Table    string   `json:"table"`
	Column   string   `json:"column"`
	IsUnique bool     `json:"is_unique"`
	Columns  []string `json:"columns,omitempty"`
	Auto     bool     `json:"auto,omitempty"`
}

type ViewInfo struct {
	Name    string      `json:"name"`
	SQL     string      `json:"sql"`
	Columns []string    `json:"columns,omitempty"`
	Parsed  *SelectStmt `json:"-"`
}

type Schema struct {
	commitMu           sync.RWMutex // protects the committed root catalog and reservations
	statementSnapshot  uint64
	snapshotReader     uint64
	nextSnapshotReader uint64
	statementActive    bool
	validatedRowKey    string
	lsm                *storage.LSMTree
	tables             map[string]*TableInfo
	indexes            map[string]*IndexInfo
	outerRow           map[string]Value
	stats              map[string]int64
	pageSize           int
	userVersion        int64
	views              map[string]*ViewInfo
	triggers           map[string]*TriggerInfo
	autoincMax         map[string]int64
	rowIDCount         map[string]int64

	ctx              context.Context
	lockHandler      func(LockRequest) error
	lockGroupHandler func([]LockRequest) error

	inTx             bool
	txWrites         []txWrite
	txLatest         map[string]int
	txState          *schemaState
	statementChanges int64
	selectDepth      int
	triggerDepth     int
	viewDepth        int
	savepoints       []savepoint

	// Sessions share a committed catalog, but keep private transaction state.
	catalogShared      bool
	catalogLookup      *catalogLookups
	parent             *Schema
	readVersion        uint64
	readStateVersion   uint64
	stateVersion       uint64
	commitVersion      atomic.Uint64
	catalogVersion     uint64
	readCatalogVersion uint64
	rowIDReservations  map[string]int64
	txRowVersions      map[string]uint64
	txCatalogVersion   uint64
	txWriteVersion     uint64
	txDDLVersion       uint64
	txHasWrites        bool
	txHasDDL           bool
}

func (s *Schema) SetContext(ctx context.Context) {
	s.ctx = ctx
}

func (s *Schema) checkInterrupt() error {
	if s.ctx != nil {
		select {
		case <-s.ctx.Done():
			return s.ctx.Err()
		default:
		}
	}
	return nil
}

type txWrite struct {
	previous int
	key      []byte
	value    []byte
	del      bool
}

type savepoint struct {
	name  string
	pos   int
	state *schemaState
	outer bool
}

type TriggerInfo struct {
	Name       string   `json:"name"`
	InsteadOf  bool     `json:"insteadof"`
	Event      string   `json:"event"`
	Timing     string   `json:"timing"`
	Table      string   `json:"table"`
	View       string   `json:"view"`
	When       string   `json:"when"`
	Body       []string `json:"body"`
	ForEachRow bool     `json:"foreachrow"`
}

const (
	schemaPrefix      = "\x00__schema__"
	statsPrefix       = "\x00__stats__"
	versionPrefix     = "\x00__version__"
	schemaViewsPrefix = "\x00__schema_views__"
	schemaTrigPrefix  = "\x00__schema_trig__"
	schemaIdxPrefix   = "\x00__schema_indexes__"
	autoincPrefix     = "\x00__autoinc__"
	dataPrefix        = "\x01"
	indexPrefix       = "\x02"
)

func NewSchema(lsm *storage.LSMTree, pageSize int) (*Schema, error) {
	return NewSchemaWithOptions(lsm, pageSize, nil)
}

// NewSchemaWithOptions optionally checks indexes for diagnostics. Normal opens
// trust atomically maintained indexes.
func NewSchemaWithOptions(lsm *storage.LSMTree, pageSize int, opt *storage.Options) (*Schema, error) {
	s := &Schema{
		lsm:        lsm,
		tables:     make(map[string]*TableInfo),
		indexes:    make(map[string]*IndexInfo),
		stats:      make(map[string]int64),
		pageSize:   pageSize,
		views:      make(map[string]*ViewInfo),
		triggers:   make(map[string]*TriggerInfo),
		autoincMax: make(map[string]int64),
		rowIDCount: make(map[string]int64),
	}
	if err := s.loadSchema(); err != nil {
		return nil, err
	}
	if err := s.upgradePrimaryKeys(); err != nil {
		return nil, err
	}
	if err := s.loadViews(); err != nil {
		return nil, err
	}
	if err := s.loadTriggers(); err != nil {
		return nil, err
	}
	if err := s.loadIndexes(); err != nil {
		return nil, err
	}
	for _, info := range s.tables {
		if err := s.foreignDefinitions(info); err != nil {
			return nil, err
		}
	}
	s.loadAutoincMax()
	s.loadStats()
	s.loadUserVersion()
	if opt != nil && opt.VerifyIndexesOnOpen {
		if err := s.repairIndexes(); err != nil {
			return nil, err
		}
	}
	return s, nil
}

func (s *Schema) loadSchema() error {
	data, ok, err := s.schemaGet([]byte(schemaPrefix))
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}
	if err := json.Unmarshal(data, &s.tables); err != nil {
		return err
	}
	for _, info := range s.tables {
		for i := range info.Columns {
			col := &info.Columns[i]
			if col.DefaultSQL != "" && col.Default == nil {
				expr, err := parseExpr(col.DefaultSQL)
				if err != nil {
					return fmt.Errorf("invalid stored DEFAULT on %s.%s: %w", info.Name, col.Name, err)
				}
				col.Default = expr
			}
			if col.CheckSQL != "" && col.Check == nil {
				expr, err := parseExpr(col.CheckSQL)
				if err != nil {
					return fmt.Errorf("invalid stored CHECK on %s.%s: %w", info.Name, col.Name, err)
				}
				col.Check = expr
			}
		}
		for i, checkSQL := range info.TableCheckSQL {
			if checkSQL != "" && (i >= len(info.TableCheck) || info.TableCheck[i] == nil) {
				expr, err := parseExpr(checkSQL)
				if err != nil {
					return fmt.Errorf("invalid stored CHECK on %s: %w", info.Name, err)
				}
				if i >= len(info.TableCheck) {
					info.TableCheck = append(info.TableCheck, expr)
				} else {
					info.TableCheck[i] = expr
				}
			}
		}
		if err := validateConstraintExpressions(info); err != nil {
			return fmt.Errorf("invalid constraints on %s: %w", info.Name, err)
		}
	}
	return nil
}

func parseExpr(sql string) (Expression, error) {
	if sql == "" {
		return nil, fmt.Errorf("empty expression")
	}
	parser := &Parser{tok: NewTokenizer(sql)}
	parser.advance()
	expr, err := parser.parseExpression()
	if err != nil {
		return nil, err
	}
	if parser.cur.Type != TokEOF {
		return nil, fmt.Errorf("unexpected token in stored expression: %q", parser.cur.Value)
	}
	return expr, nil
}

func (s *Schema) saveSchema() error {
	s.catalogLookup = nil
	if s.inTx {
		return nil
	}
	data, err := json.Marshal(s.tables)
	if err != nil {
		return err
	}
	return s.write([]byte(schemaPrefix), data)
}

func (s *Schema) loadAutoincMax() {
	data, ok, err := s.schemaGet([]byte(autoincPrefix))
	if err != nil || !ok {
		return
	}
	json.Unmarshal(data, &s.autoincMax)
}

func (s *Schema) saveAutoincMax() error {
	if s.inTx {
		return nil
	}
	data, err := json.Marshal(s.autoincMax)
	if err != nil {
		return err
	}
	return s.write([]byte(autoincPrefix), data)
}

func (s *Schema) getAutoincMax(tableName string) int64 {
	if v, ok := s.autoincMax[tableName]; ok {
		return v
	}
	return 0
}

func (s *Schema) setAutoincMax(tableName string, id int64) {
	s.autoincMax[tableName] = id
}

func (s *Schema) schemaGet(key []byte) ([]byte, bool, error) {
	if s.inTx {
		return s.txGet(key)
	}
	return s.committedGet(key)
}

func (s *Schema) CreateTable(stmt *CreateTableStmt) error {
	s.ensureCatalogWritable()
	name := strings.ToLower(stmt.Table)
	if _, ok := s.tables[name]; ok {
		if stmt.IfNotExists {
			return nil
		}
		return fmt.Errorf("table %q already exists", name)
	}

	if _, exists := s.views[name]; exists {
		return fmt.Errorf("object %q already exists", name)
	}
	if len(stmt.Columns) == 0 {
		return fmt.Errorf("table must have columns")
	}
	names := make(map[string]bool)
	for _, col := range stmt.Columns {
		if names[col.Name] {
			return fmt.Errorf("duplicate column: %s", col.Name)
		}
		names[col.Name] = true
	}
	for _, col := range stmt.PKColumns {
		if !names[col] {
			return fmt.Errorf("no such PRIMARY KEY column: %s", col)
		}
	}
	for _, group := range stmt.TableUnique {
		for _, col := range group {
			if !names[col] {
				return fmt.Errorf("no such UNIQUE column: %s", col)
			}
		}
	}

	for i := range stmt.Columns {
		if stmt.Columns[i].Default != nil {
			stmt.Columns[i].DefaultSQL = formatExpr(stmt.Columns[i].Default)
		}
		if stmt.Columns[i].Check != nil {
			stmt.Columns[i].CheckSQL = formatExpr(stmt.Columns[i].Check)
		}
	}
	tcexpr := make([]Expression, len(stmt.TableCheck))
	tcsql := make([]string, len(stmt.TableCheck))
	for i, tc := range stmt.TableCheck {
		tcexpr[i] = tc
		tcsql[i] = formatExpr(tc)
	}

	tableUnique := stmt.TableUnique
	if len(stmt.PKColumns) > 1 {
		tableUnique = append(tableUnique, stmt.PKColumns)
	}
	if len(stmt.PKColumns) == 0 {
		for _, col := range stmt.Columns {
			if col.PrimaryKey {
				stmt.PKColumns = append(stmt.PKColumns, col.Name)
			}
		}
	}
	inlineIndexes, err := s.prepareInlineIndexes(name, stmt)
	if err != nil {
		return err
	}

	info := &TableInfo{
		Name:          name,
		Columns:       stmt.Columns,
		TableCheck:    tcexpr,
		TableCheckSQL: tcsql,
		TableUnique:   tableUnique,
		ForeignKeys:   stmt.ForeignKeys,
		PKColumns:     stmt.PKColumns,
	}
	if err := validateConstraintExpressions(info); err != nil {
		return err
	}
	if err := primaryKeyColumns(info); err != nil {
		return err
	}
	s.tables[name] = info
	if err := s.saveSchema(); err != nil {
		return err
	}
	for _, col := range stmt.Columns {
		if (col.PrimaryKey || col.Unique) && len(stmt.PKColumns) <= 1 {
			idxName := name + "_" + col.Name + "_idx"
			if _, ok := s.indexes[idxName]; !ok {
				s.indexes[idxName] = &IndexInfo{Name: idxName, Table: name, Column: col.Name, IsUnique: true, Auto: true}
			}
		}
	}
	for _, idx := range inlineIndexes {
		s.indexes[idx.Name] = idx
	}
	if err := s.foreignDefinitions(info); err != nil {
		return err
	}
	s.catalogLookup = nil
	return s.saveIndexes()
}

func (s *Schema) DropTable(stmt *DropTableStmt) error {
	s.ensureCatalogWritable()
	if err := s.checkDropReferencedTable(strings.ToLower(stmt.Table)); err != nil {
		return err
	}
	name := strings.ToLower(stmt.Table)
	if _, ok := s.tables[name]; !ok {
		return fmt.Errorf("table %q does not exist", name)
	}

	for idxName, idx := range s.indexes {
		if idx.Table == name {
			idxPrefix := []byte(indexDataPrefix(idxName))
			s.removeRange(idxPrefix, append(idxPrefix, 0xFF))
			delete(s.indexes, idxName)
		}
	}
	s.saveIndexes()

	prefix := tableDataPrefix(name)
	if s.inTx {
		iter := s.txNewIterator()
		for ; iter.Valid(); iter.Next() {
			key := string(iter.Key().UserKey)
			if !strings.HasPrefix(key, prefix) {
				if key > prefix {
					break
				}
				continue
			}
			s.txDelete(iter.Key().UserKey)
		}
		iter.Close()
	} else {
		s.removeRange([]byte(prefix), []byte(prefix+"\xff"))
	}

	delete(s.tables, name)
	delete(s.autoincMax, name)
	delete(s.rowIDCount, name)
	for k, tr := range s.triggers {
		if tr.Table == name || tr.View == name {
			delete(s.triggers, k)
		}
	}
	delete(s.stats, name)
	return s.saveSchema()
}

func (s *Schema) loadStats() {
	data, ok, err := s.schemaGet([]byte(statsPrefix))
	if err != nil || !ok {
		return
	}
	json.Unmarshal(data, &s.stats)
}

func (s *Schema) saveStats() error {
	if s.inTx {
		return nil
	}
	data, err := json.Marshal(s.stats)
	if err != nil {
		return err
	}
	return s.write([]byte(statsPrefix), data)
}

func (s *Schema) loadUserVersion() {
	data, ok, err := s.schemaGet([]byte(versionPrefix))
	if err != nil || !ok || len(data) < 8 {
		return
	}
	s.userVersion = int64(binary.LittleEndian.Uint64(data))
}

func (s *Schema) saveUserVersion() error {
	if s.inTx {
		return nil
	}
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, uint64(s.userVersion))
	return s.write([]byte(versionPrefix), buf)
}

func (s *Schema) UpdateStats() error {
	for name := range s.tables {
		count := s.countRows(name)
		s.stats[name] = count
	}
	return s.saveStats()
}

func (s *Schema) countRows(tableName string) int64 {
	prefix := tableDataPrefix(tableName)
	var count int64
	var iter storage.Iterator
	if s.inTx {
		iter = s.txNewIterator()
	} else {
		iter = s.newIterator()
	}
	defer iter.Close()

	for iter.Valid() {
		key := string(iter.Key().UserKey)
		if !strings.HasPrefix(key, prefix) {
			if key > prefix {
				break
			}
			iter.Next()
			continue
		}
		count++
		iter.Next()
	}

	if s.inTx {
		for _, w := range s.txWrites {
			keyStr := string(w.key)
			if !strings.HasPrefix(keyStr, prefix) {
				continue
			}
			if w.del {
				count--
			} else {
				count++
			}
		}
	}

	return count
}

func (s *Schema) GetStats() map[string]int64 {
	return s.stats
}

func (s *Schema) GetTableList() []string {
	names := make([]string, 0, len(s.tables))
	for name := range s.tables {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func (s *Schema) GetViewList() []string {
	names := make([]string, 0, len(s.views))
	for name := range s.views {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func (s *Schema) GetViewSQL(name string) string {
	v, ok := s.views[strings.ToLower(name)]
	if !ok {
		return ""
	}
	return v.SQL
}

func (s *Schema) GetCreateViewSQL(name string) string {
	vi, ok := s.views[strings.ToLower(name)]
	if !ok {
		return ""
	}
	text := "CREATE VIEW " + quoteIdent(vi.Name)
	if len(vi.Columns) > 0 {
		names := make([]string, len(vi.Columns))
		for i, name := range vi.Columns {
			names[i] = quoteIdent(name)
		}
		text += " (" + strings.Join(names, ",") + ")"
	}
	return text + " AS " + vi.SQL
}

func (s *Schema) GetTriggerList() []string {
	names := make([]string, 0, len(s.triggers))
	for name := range s.triggers {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func (s *Schema) GetTriggerSQL(name string) string {
	t, ok := s.triggers[strings.ToLower(name)]
	if !ok {
		return ""
	}
	target := t.Table
	if target == "" {
		target = t.View
	}
	timing := t.Timing
	if timing == "" {
		if t.InsteadOf {
			timing = "INSTEAD OF"
		} else {
			timing = "AFTER"
		}
	}
	body := strings.Join(t.Body, "; ")
	if len(t.Body) > 0 {
		body += ";"
	}
	when := ""
	if t.When != "" {
		when = " WHEN " + t.When
	}
	return fmt.Sprintf("CREATE TRIGGER %s %s %s ON %s%s BEGIN %s END", quoteIdent(t.Name), timing, t.Event, quoteIdent(target), when, body)
}

func (s *Schema) GetTableInfo(name string) *TableInfo {
	return s.tables[strings.ToLower(name)]
}

func (s *Schema) GetCreateTableSQL(tableName string) string {
	return s.getCreateTableSQL(tableName, false)
}

func (s *Schema) getCreateTableSQL(tableName string, inlineIndexes bool) string {
	info, ok := s.tables[strings.ToLower(tableName)]
	if !ok {
		return ""
	}
	var b strings.Builder
	quotedNames := func(names []string) string {
		quoted := make([]string, len(names))
		for i, name := range names {
			quoted[i] = quoteIdent(name)
		}
		return strings.Join(quoted, ", ")
	}
	fmt.Fprintf(&b, "CREATE TABLE %s(", quoteIdent(info.Name))
	for i, col := range info.Columns {
		if i > 0 {
			b.WriteString(", ")
		}
		fmt.Fprintf(&b, "%s %s", quoteIdent(col.Name), col.ColType.String())
		if col.NotNull {
			b.WriteString(" NOT NULL")
		}
		if col.Default != nil {
			fmt.Fprintf(&b, " DEFAULT %s", formatExpr(col.Default))
		}
		if (col.PrimaryKey || (len(info.PKColumns) == 1 && info.PKColumns[0] == col.Name)) && len(info.PKColumns) <= 1 {
			b.WriteString(" PRIMARY KEY")
			if col.AutoIncrement {
				b.WriteString(" AUTOINCREMENT")
			}
		}
		if col.Unique {
			b.WriteString(" UNIQUE")
		}
		if col.Check != nil {
			fmt.Fprintf(&b, " CHECK(%s)", formatExpr(col.Check))
		}
		if col.RefTable != "" {
			fmt.Fprintf(&b, " REFERENCES %s", quoteIdent(col.RefTable))
			if len(col.RefColumns) > 0 {
				fmt.Fprintf(&b, "(%s)", quotedNames(col.RefColumns))
			}
		}
	}
	if len(info.PKColumns) > 1 {
		b.WriteString(", PRIMARY KEY (")
		for i, pk := range info.PKColumns {
			if i > 0 {
				b.WriteString(", ")
			}
			b.WriteString(quoteIdent(pk))
		}
		b.WriteString(")")
	}
	for _, cols := range info.TableUnique {
		if colsEqual(cols, info.PKColumns) {
			continue
		}
		b.WriteString(", UNIQUE(")
		for i, c := range cols {
			if i > 0 {
				b.WriteString(", ")
			}
			b.WriteString(quoteIdent(c))
		}
		b.WriteString(")")
	}
	for _, chk := range info.TableCheckSQL {
		if chk != "" {
			b.WriteString(", CHECK(")
			b.WriteString(chk)
			b.WriteString(")")
		}
	}
	for _, fk := range info.ForeignKeys {
		fmt.Fprintf(&b, ", FOREIGN KEY (%s) REFERENCES %s", quotedNames(fk.Columns), quoteIdent(fk.RefTable))
		if len(fk.RefColumns) > 0 {
			fmt.Fprintf(&b, "(%s)", quotedNames(fk.RefColumns))
		}
	}
	if inlineIndexes {
		for _, name := range s.GetIndexList() {
			idx := s.indexes[name]
			if idx.Table != info.Name || s.isAutoIndex(idx) {
				continue
			}
			b.WriteString(", ")
			if idx.IsUnique {
				b.WriteString("UNIQUE ")
			}
			fmt.Fprintf(&b, "KEY %s (%s)", quoteIdent(idx.Name), quotedNames(idx.columnNames()))
		}
	}
	b.WriteString(")")
	return b.String()
}

func (s *Schema) QuerySqliteMaster(filter Expression) (*ResultSet, error) {
	rs := &ResultSet{
		Columns: []ColumnDef{
			{Name: "type", ColType: ColText},
			{Name: "name", ColType: ColText},
			{Name: "tbl_name", ColType: ColText},
			{Name: "rootpage", ColType: ColInteger},
			{Name: "sql", ColType: ColText},
		},
	}

	for _, name := range s.GetTableList() {
		createSQL := s.GetCreateTableSQL(name)
		row := map[string]Value{
			"type":     NewTextValue("table"),
			"name":     NewTextValue(name),
			"tbl_name": NewTextValue(name),
			"rootpage": NewIntValue(0),
			"sql":      NewTextValue(createSQL),
		}
		rs.Rows = append(rs.Rows, row)
	}

	for name := range s.stats {
		if _, isTable := s.tables[name]; !isTable {
			continue
		}
		_ = name
	}

	if filter != nil {
		var filtered []map[string]Value
		for _, row := range rs.Rows {
			match, _ := s.evalCondition(filter, row)
			if match {
				filtered = append(filtered, row)
			}
		}
		rs.Rows = filtered
	}

	return rs, nil
}

func (s *Schema) ExecutePragma(stmt *PragmaStmt) (*ResultSet, error) {
	switch stmt.Name {
	case "table_info":
		if len(stmt.Args) < 1 {
			return nil, fmt.Errorf("PRAGMA table_info requires a table name")
		}
		tableName := exprToName(stmt.Args[0])
		return s.pragmaTableInfo(strings.ToLower(tableName))
	case "table_list":
		return s.pragmaTableList()
	case "database_list":
		return s.pragmaDatabaseList()
	case "user_version":
		if stmt.Value != nil {
			if lit, ok := stmt.Value.(*LiteralExpr); ok {
				s.userVersion = lit.Value.Int
				s.saveUserVersion()
			} else {
				return nil, fmt.Errorf("PRAGMA user_version requires an integer value")
			}
			return &ResultSet{}, nil
		}
		return s.singleValueResult("user_version", NewIntValue(s.userVersion))
	case "page_size":
		return s.singleValueResult("page_size", NewIntValue(int64(s.pageSize)))
	case "compile_options":
		return &ResultSet{
			Columns: []ColumnDef{{Name: "compile_options", ColType: ColText}},
			Rows:    []map[string]Value{},
		}, nil
	case "foreign_keys":
		return &ResultSet{}, nil
	default:
		return nil, fmt.Errorf("unknown pragma: %s", stmt.Name)
	}
}

func exprToName(expr Expression) string {
	if lit, ok := expr.(*LiteralExpr); ok {
		return lit.Value.Text
	}
	if col, ok := expr.(*ColumnRefExpr); ok {
		return col.Name
	}
	return fmt.Sprintf("%v", expr)
}

func (s *Schema) pragmaTableInfo(tableName string) (*ResultSet, error) {
	info, ok := s.tables[tableName]
	if !ok {
		return nil, fmt.Errorf("table %q does not exist", tableName)
	}
	rs := &ResultSet{
		Columns: []ColumnDef{
			{Name: "cid", ColType: ColInteger},
			{Name: "name", ColType: ColText},
			{Name: "type", ColType: ColText},
			{Name: "notnull", ColType: ColInteger},
			{Name: "dflt_value", ColType: ColText},
			{Name: "pk", ColType: ColInteger},
		},
	}
	for i, col := range info.Columns {
		notNullVal := int64(0)
		if col.NotNull {
			notNullVal = 1
		}
		pkVal := int64(0)
		if col.PrimaryKey {
			pkVal = 1
		}
		dflt := NewNullValue()
		if col.Default != nil {
			dflt = NewTextValue(exprString(col.Default))
		}
		row := map[string]Value{
			"cid":        NewIntValue(int64(i)),
			"name":       NewTextValue(col.Name),
			"type":       NewTextValue(col.ColType.String()),
			"notnull":    NewIntValue(notNullVal),
			"dflt_value": dflt,
			"pk":         NewIntValue(pkVal),
		}
		rs.Rows = append(rs.Rows, row)
	}
	return rs, nil
}

func (s *Schema) pragmaTableList() (*ResultSet, error) {
	rs := &ResultSet{
		Columns: []ColumnDef{
			{Name: "schema", ColType: ColText},
			{Name: "name", ColType: ColText},
			{Name: "type", ColType: ColText},
		},
	}
	for _, name := range s.GetTableList() {
		row := map[string]Value{
			"schema": NewTextValue("main"),
			"name":   NewTextValue(name),
			"type":   NewTextValue("table"),
		}
		rs.Rows = append(rs.Rows, row)
	}
	return rs, nil
}

func (s *Schema) pragmaDatabaseList() (*ResultSet, error) {
	rs := &ResultSet{
		Columns: []ColumnDef{
			{Name: "seq", ColType: ColInteger},
			{Name: "name", ColType: ColText},
			{Name: "file", ColType: ColText},
		},
	}
	rs.Rows = append(rs.Rows, map[string]Value{
		"seq":  NewIntValue(0),
		"name": NewTextValue("main"),
		"file": NewTextValue(""),
	})
	return rs, nil
}

func (s *Schema) singleValueResult(name string, val Value) (*ResultSet, error) {
	return &ResultSet{
		Columns: []ColumnDef{{Name: name, ColType: val.Type}},
		Rows:    []map[string]Value{{name: val}},
	}, nil
}

func (s *Schema) findWhereIndex(where Expression, tableName string) *IndexInfo {
	bin, ok := where.(*BinaryExpr)
	if !ok || bin.Op != "=" {
		return nil
	}
	colRef, ok := bin.Left.(*ColumnRefExpr)
	if !ok {
		colRef, ok = bin.Right.(*ColumnRefExpr)
	}
	if !ok {
		return nil
	}
	for _, idx := range s.tableIndexes(tableName) {
		if idx.Table == tableName && idx.Column == colRef.Name {
			return idx
		}
	}
	return nil
}

func (s *Schema) findOrderIndex(stmt *SelectStmt, tableName string) *IndexInfo {
	if len(stmt.OrderBy) != 1 {
		return nil
	}
	colRef, ok := stmt.OrderBy[0].Expr.(*ColumnRefExpr)
	if !ok {
		return nil
	}
	name := strings.ToLower(colRef.Name)
	name = strings.TrimPrefix(name, tableName+".")
	if stmt.FromAlias != "" {
		name = strings.TrimPrefix(name, stmt.FromAlias+".")
	}
	if info := s.tables[tableName]; info != nil && len(info.PKColumns) == 1 && info.PKColumns[0] == name {
		return &IndexInfo{Name: "PRIMARY", Table: tableName, Column: name, IsUnique: true}
	}
	for _, idx := range s.tableIndexes(tableName) {
		if idx.Table == tableName && len(idx.columnNames()) == 1 && idx.Column == name {
			return idx
		}
	}
	return nil
}

func (s *Schema) Explain(stmt *ExplainStmt) (*ResultSet, error) {
	rs := &ResultSet{
		Columns: []ColumnDef{
			{Name: "addr", ColType: ColInteger},
			{Name: "opcode", ColType: ColText},
			{Name: "p1", ColType: ColInteger},
			{Name: "p2", ColType: ColInteger},
			{Name: "p3", ColType: ColInteger},
			{Name: "p4", ColType: ColText},
			{Name: "p5", ColType: ColText},
			{Name: "comment", ColType: ColText},
		},
	}

	switch inner := stmt.Stmt.(type) {
	case *SelectStmt:
		addr := int64(0)
		addRow := func(opcode string, p1, p2, p3 int64, p4, p5, comment string) {
			rs.Rows = append(rs.Rows, map[string]Value{
				"addr":    NewIntValue(addr),
				"opcode":  NewTextValue(opcode),
				"p1":      NewIntValue(p1),
				"p2":      NewIntValue(p2),
				"p3":      NewIntValue(p3),
				"p4":      NewTextValue(p4),
				"p5":      NewTextValue(p5),
				"comment": NewTextValue(comment),
			})
			addr++
		}
		addRow("Init", 0, 0, 0, "", "", "Start of query")
		addRow("Trace", 0, 0, 0, "", "", "")

		tname := inner.From
		columnCount := int64(0)
		if info := s.tables[tname]; info != nil {
			columnCount = int64(len(info.Columns))
		} else if view := s.views[tname]; view != nil {
			columnCount = int64(len(s.viewColumnDefs(view)))
		} else if tname != "" {
			return nil, fmt.Errorf("table %q does not exist", tname)
		}
		if tname == "" {
			tname = "(subquery)"
		}
		info := s.tables[tname]
		tuple := s.tuplePlan(inner, info)
		covering := s.coveringPlan(inner, tname, info)
		_, point := s.primaryKeyLookup(inner.Where, tname, info)
		ordered := false
		if tuple != nil {
			ordered = tuple.ordered
			opcode := "OpenRead"
			if tuple.index != nil {
				opcode = "OpenIndex"
			}
			addRow(opcode, 0, columnCount, 0, tuple.description(), "", "table="+tname)
			seek := "SeekGE"
			if tuple.reverse {
				seek = "SeekLT"
			}
			if tuple.points != nil {
				seek = "MultiGet"
			}
			addRow(seek, 0, 0, 0, tuple.description(), "", "bounded tuple access")
		} else if point {
			ordered = true
			addRow("OpenRead", 0, columnCount, 0, "PRIMARY", "", "table="+tname)
			addRow("PointLookup", 0, 0, 0, "PRIMARY", "", "single primary key")
		} else if covering != nil {
			ordered = covering.ordered
			addRow("OpenIndex", 0, columnCount, 0, covering.index.Name, "", "covering index")
		} else {
			addRow("OpenRead", 0, columnCount, 0, "", "", "table="+tname)
			addRow("Rewind", 0, 0, 0, "", "", "table scan")
		}

		for _, col := range inner.Columns {
			if col.Star {
				addRow("Column", 0, 0, columnCount, "", "", "columns=*")
			} else if cr, ok := col.Expr.(*ColumnRefExpr); ok {
				addRow("Column", 0, 0, 0, "", "", "col="+cr.Name)
			}
		}

		if inner.Where != nil {
			addRow("Filter", 0, 0, 0, exprString(inner.Where), "", "WHERE clause")
		}

		if tuple != nil || point {
			// Bounded access was described above.
		} else if covering != nil {
			direction := "forward"
			if covering.reverse {
				direction = "backward"
			}
			addRow("SeekGE", 0, 0, 0, covering.index.Name, "", "covering index scan "+direction)
		} else if inner.Where != nil {
			if idx := s.findWhereIndex(inner.Where, tname); idx != nil {
				addRow("SeekGE", 0, 0, 0, "", "", "using index: "+idx.Name)
			}
		} else if len(inner.OrderBy) == 1 && inner.Limit >= 0 {
			if idx := s.findOrderIndex(inner, tname); idx != nil {
				dir := "forward"
				if inner.OrderBy[0].Desc {
					dir = "backward"
				}
				addRow("SeekGE", 0, 0, 0, "", "", "index scan "+dir+": "+idx.Name)
			}
		}

		if len(inner.OrderBy) > 0 && (!ordered || hasAggregates(inner) || len(inner.GroupBy) > 0 || inner.Distinct) {
			addRow("SorterOpen", 0, 0, 0, "", "", "")
			addRow("SorterSort", 0, 0, 0, "", "", "")
		}

		if inner.Limit >= 0 {
			addRow("Limit", 0, int64(inner.Limit), 0, "", "", "")
		}

		addRow("ResultRow", 0, 0, 0, "", "", "")
		addRow("Next", 0, 0, 0, "", "", "")
		addRow("Halt", 0, 0, 0, "", "", "")

		if stmt.Analyze {
			start := time.Now()
			execRS, execErr := s.Select(inner)
			elapsed := time.Since(start)
			if execErr == nil {
				addRow("---", 0, 0, 0, "", "", "ANALYZE results:")
				addRow("Rows", 0, int64(len(execRS.Rows)), 0, "", "", fmt.Sprintf("%d rows returned", len(execRS.Rows)))
				addRow("Time", 0, 0, 0, "", "", fmt.Sprintf("real %.3f ms", float64(elapsed.Microseconds())/1000.0))
			} else {
				addRow("Error", 0, 0, 0, "", "", execErr.Error())
			}
		}

	case *DeleteStmt:
		addRow := func(opcode string, p1, p2, p3 int64, p4, p5, comment string) {
			rs.Rows = append(rs.Rows, map[string]Value{
				"addr":    NewIntValue(int64(len(rs.Rows))),
				"opcode":  NewTextValue(opcode),
				"p1":      NewIntValue(p1),
				"p2":      NewIntValue(p2),
				"p3":      NewIntValue(p3),
				"p4":      NewTextValue(p4),
				"p5":      NewTextValue(p5),
				"comment": NewTextValue(comment),
			})
		}
		addRow("Init", 0, 0, 0, "", "", "")
		addRow("OpenWrite", 0, 0, 0, "", "", "table="+inner.From)
		if info := s.tables[inner.From]; info != nil {
			if _, ok := s.primaryKeyLookup(inner.Where, inner.From, info); ok {
				addRow("PointLookup", 0, 0, 0, "PRIMARY", "", "single primary key")
			} else if plan := buildTuplePlan(&SelectStmt{From: inner.From, Where: inner.Where, Limit: -1}, info, nil, tupleConstraints(inner.Where, inner.From, "", info)); plan != nil {
				addRow("SeekGE", 0, 0, 0, plan.description(), "", "bounded primary key access")
			} else {
				addRow("Rewind", 0, 0, 0, "", "", "table scan")
			}
		}
		if inner.Where != nil {
			addRow("Filter", 0, 0, 0, exprString(inner.Where), "", "")
		}
		addRow("Delete", 0, 0, 0, "", "", "")
		addRow("Halt", 0, 0, 0, "", "", "")

	case *UpdateStmt:
		addRow := func(opcode string, p1, p2, p3 int64, p4, p5, comment string) {
			rs.Rows = append(rs.Rows, map[string]Value{
				"addr":    NewIntValue(int64(len(rs.Rows))),
				"opcode":  NewTextValue(opcode),
				"p1":      NewIntValue(p1),
				"p2":      NewIntValue(p2),
				"p3":      NewIntValue(p3),
				"p4":      NewTextValue(p4),
				"p5":      NewTextValue(p5),
				"comment": NewTextValue(comment),
			})
		}
		addRow("Init", 0, 0, 0, "", "", "")
		addRow("OpenWrite", 0, 0, 0, "", "", "table="+inner.Table)
		if info := s.tables[inner.Table]; info != nil {
			if _, ok := s.primaryKeyLookup(inner.Where, inner.Table, info); ok {
				addRow("PointLookup", 0, 0, 0, "PRIMARY", "", "single primary key")
			} else if plan := buildTuplePlan(&SelectStmt{From: inner.Table, Where: inner.Where, Limit: -1}, info, nil, tupleConstraints(inner.Where, inner.Table, "", info)); plan != nil {
				addRow("SeekGE", 0, 0, 0, plan.description(), "", "bounded primary key access")
			} else {
				addRow("Rewind", 0, 0, 0, "", "", "table scan")
			}
		}
		if inner.Where != nil {
			addRow("Filter", 0, 0, 0, exprString(inner.Where), "", "")
		}
		setStr := ""
		for i, s := range inner.Set {
			if i > 0 {
				setStr += ", "
			}
			setStr += s.Column + "=" + exprString(s.Value)
		}
		addRow("SetColumns", 0, 0, 0, setStr, "", "")
		addRow("Update", 0, 0, 0, "", "", "")
		addRow("Halt", 0, 0, 0, "", "", "")

	case *InsertStmt:
		addRow := func(opcode string, p1, p2, p3 int64, p4, p5, comment string) {
			rs.Rows = append(rs.Rows, map[string]Value{
				"addr":    NewIntValue(int64(len(rs.Rows))),
				"opcode":  NewTextValue(opcode),
				"p1":      NewIntValue(p1),
				"p2":      NewIntValue(p2),
				"p3":      NewIntValue(p3),
				"p4":      NewTextValue(p4),
				"p5":      NewTextValue(p5),
				"comment": NewTextValue(comment),
			})
		}
		addRow("Init", 0, 0, 0, "", "", "")
		addRow("OpenWrite", 0, 0, 0, "", "", "table="+inner.Table)
		addRow("NewRowid", 0, 0, 0, "", "", "")
		addRow("Insert", 0, 0, 0, "", "", "")
		addRow("Halt", 0, 0, 0, "", "", "")

	default:
		return nil, fmt.Errorf("EXPLAIN of %T not supported", stmt.Stmt)
	}

	return rs, nil
}

func colsEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func exprString(e Expression) string {
	if e == nil {
		return ""
	}
	switch x := e.(type) {
	case *LiteralExpr:
		return x.Value.String()
	case *ColumnRefExpr:
		return x.Name
	case *BinaryExpr:
		return exprString(x.Left) + " " + x.Op + " " + exprString(x.Right)
	case *UnaryExpr:
		return x.Op + " " + exprString(x.Right)
	case *FuncCallExpr:
		parts := make([]string, len(x.Args))
		for i, a := range x.Args {
			parts[i] = exprString(a)
		}
		modifier := ""
		if x.Distinct {
			modifier = "DISTINCT "
		}
		return x.Name + "(" + modifier + strings.Join(parts, ", ") + ")"
	default:
		return fmt.Sprintf("%T", e)
	}
}
func (s *Schema) loadViews() error {
	data, ok, err := s.schemaGet([]byte(schemaViewsPrefix))
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}
	if err := json.Unmarshal(data, &s.views); err != nil {
		return err
	}
	for _, vi := range s.views {
		if vi.SQL != "" && vi.Parsed == nil {
			stmts, err := Parse(vi.SQL)
			if err == nil && len(stmts) > 0 {
				if sel, ok := stmts[0].(*SelectStmt); ok {
					vi.Parsed = sel
				}
			}
		}
	}
	return nil
}

func (s *Schema) saveViews() error {
	s.catalogLookup = nil
	if s.inTx {
		return nil
	}
	data, err := json.Marshal(s.views)
	if err != nil {
		return err
	}
	return s.write([]byte(schemaViewsPrefix), data)
}

func (s *Schema) loadTriggers() error {
	data, ok, err := s.schemaGet([]byte(schemaTrigPrefix))
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}
	return json.Unmarshal(data, &s.triggers)
}

func (s *Schema) saveTriggers() error {
	s.catalogLookup = nil
	if s.inTx {
		return nil
	}
	data, err := json.Marshal(s.triggers)
	if err != nil {
		return err
	}
	return s.write([]byte(schemaTrigPrefix), data)
}

func (s *Schema) CreateView(stmt *CreateViewStmt) error {
	s.ensureCatalogWritable()
	name := strings.ToLower(stmt.View)
	if _, ok := s.tables[name]; ok {
		return fmt.Errorf("object %q already exists", name)
	}
	if _, ok := s.views[name]; ok {
		return fmt.Errorf("view %q already exists", name)
	}
	s.views[name] = &ViewInfo{Name: name, SQL: stmt.SQL, Columns: stmt.Columns, Parsed: stmt.Select}
	return s.saveViews()
}

func (s *Schema) DropView(stmt *DropViewStmt) error {
	s.ensureCatalogWritable()
	name := strings.ToLower(stmt.View)
	if _, ok := s.views[name]; !ok {
		return fmt.Errorf("view %q does not exist", name)
	}
	delete(s.views, name)
	return s.saveViews()
}

func (s *Schema) CreateTrigger(stmt *CreateTriggerStmt) error {
	s.ensureCatalogWritable()
	name := strings.ToLower(stmt.Name)
	target := strings.ToLower(stmt.View)
	if _, exists := s.triggers[name]; exists {
		return fmt.Errorf("trigger %q already exists", name)
	}
	_, table := s.tables[target]
	_, view := s.views[target]
	if !table && !view {
		return fmt.Errorf("trigger target %q does not exist", target)
	}
	if stmt.InsteadOf && !view {
		return fmt.Errorf("INSTEAD OF triggers require a view")
	}
	if !stmt.InsteadOf && view {
		return fmt.Errorf("view triggers require INSTEAD OF")
	}
	for _, body := range stmt.Body {
		switch body.(type) {
		case *InsertStmt, *UpdateStmt, *DeleteStmt, *SelectStmt:
		default:
			return fmt.Errorf("unsupported trigger statement: %T", body)
		}
	}
	when := ""
	if stmt.When != nil {
		when = formatExpr(stmt.When)
	}
	s.triggers[name] = &TriggerInfo{Name: name, Table: target, View: target, Timing: stmt.Timing, InsteadOf: stmt.InsteadOf, Event: stmt.Event, When: when, Body: stmt.BodySQL, ForEachRow: stmt.ForEachRow}
	return s.saveTriggers()
}
func (s *Schema) DropTrigger(stmt *DropTriggerStmt) error {
	s.ensureCatalogWritable()
	if _, ok := s.triggers[stmt.Name]; !ok {
		return fmt.Errorf("trigger %q does not exist", stmt.Name)
	}
	delete(s.triggers, stmt.Name)
	return s.saveTriggers()
}
func (s *Schema) fireTriggers(table, event, timing string, old, new map[string]Value) error {
	var names []string
	for name, tr := range s.triggers {
		target := tr.Table
		if target == "" {
			target = tr.View
		}
		if target == table && tr.Event == event && tr.Timing == timing {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	if len(names) == 0 {
		return nil
	}
	if s.triggerDepth >= 16 {
		return fmt.Errorf("trigger recursion limit exceeded")
	}
	s.triggerDepth++
	defer func() { s.triggerDepth-- }()
	previous := s.outerRow
	context := make(map[string]Value)
	for k, v := range old {
		context["old."+k] = v
	}
	for k, v := range new {
		context["new."+k] = v
	}
	s.outerRow = context
	defer func() { s.outerRow = previous }()
	changes := s.statementChanges
	defer func() { s.statementChanges = changes }()
	for _, name := range names {
		tr := s.triggers[name]
		if tr.When != "" {
			expr, err := parseExpr(tr.When)
			if err != nil {
				return err
			}
			match, err := s.evalCondition(expr, nil)
			if err != nil {
				return err
			}
			if !match {
				continue
			}
		}
		for _, body := range tr.Body {
			stmts, err := Parse(body)
			if err != nil {
				return err
			}
			for _, stmt := range stmts {
				switch st := stmt.(type) {
				case *InsertStmt:
					_, _, err = s.Insert(st)
				case *UpdateStmt:
					_, _, err = s.Update(st)
				case *DeleteStmt:
					_, _, err = s.Delete(st)
				case *SelectStmt:
					_, err = s.Select(st)
				default:
					err = fmt.Errorf("unsupported trigger statement")
				}
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}

type schemaState struct {
	tables          map[string]*TableInfo
	indexes         map[string]*IndexInfo
	views           map[string]*ViewInfo
	triggers        map[string]*TriggerInfo
	stats           map[string]int64
	autoinc, rowIDs map[string]int64
	userVersion     int64
	lookups         *catalogLookups
}

// Captured catalogs are immutable until a DDL operation copies them. Ordinary
// row statements only snapshot their small mutable counters.
func (s *Schema) captureState() *schemaState {
	if !s.catalogShared {
		s.catalogShared = true
	}
	return &schemaState{tables: s.tables, indexes: s.indexes, views: s.views, triggers: s.triggers,
		stats: maps.Clone(s.stats), autoinc: maps.Clone(s.autoincMax), rowIDs: maps.Clone(s.rowIDCount), userVersion: s.userVersion, lookups: s.catalogLookup}
}

func (s *Schema) ensureCatalogWritable() {
	s.catalogLookup = nil
	if !s.catalogShared {
		return
	}
	tables := make(map[string]*TableInfo, len(s.tables))
	indexes := make(map[string]*IndexInfo, len(s.indexes))
	views := make(map[string]*ViewInfo, len(s.views))
	triggers := make(map[string]*TriggerInfo, len(s.triggers))
	for k, v := range s.tables {
		c := *v
		// Preserve nil versus empty slices so a snapshot compares equal to its
		// source and does not turn an ordinary row update into a catalog change.
		c.Columns = slices.Clone(v.Columns)
		for i := range c.Columns {
			c.Columns[i].RefColumns = slices.Clone(v.Columns[i].RefColumns)
		}
		c.ForeignKeys = slices.Clone(v.ForeignKeys)
		for i := range c.ForeignKeys {
			c.ForeignKeys[i].Columns = slices.Clone(v.ForeignKeys[i].Columns)
			c.ForeignKeys[i].RefColumns = slices.Clone(v.ForeignKeys[i].RefColumns)
		}
		c.TableUnique = slices.Clone(v.TableUnique)
		for i := range c.TableUnique {
			c.TableUnique[i] = slices.Clone(v.TableUnique[i])
		}
		c.PKColumns = slices.Clone(v.PKColumns)
		c.TableCheck = slices.Clone(v.TableCheck)
		c.TableCheckSQL = slices.Clone(v.TableCheckSQL)
		tables[k] = &c
	}
	for k, v := range s.indexes {
		c := *v
		c.Columns = slices.Clone(v.Columns)
		indexes[k] = &c
	}
	for k, v := range s.views {
		c := *v
		views[k] = &c
	}
	for k, v := range s.triggers {
		c := *v
		triggers[k] = &c
	}
	s.tables = tables
	s.indexes = indexes
	s.views = views
	s.triggers = triggers
	s.catalogShared = false
}
func (s *Schema) restoreState(st *schemaState) {
	s.tables = st.tables
	s.catalogShared = true
	s.catalogLookup = st.lookups
	s.indexes = st.indexes
	s.views = st.views
	s.triggers = st.triggers
	s.stats = st.stats
	s.autoincMax = st.autoinc
	s.rowIDCount = st.rowIDs
	s.userVersion = st.userVersion
}
func (s *Schema) Begin() error {
	if s.inTx {
		return fmt.Errorf("already in transaction")
	}
	s.txState = s.captureState()
	s.inTx = true
	s.txWrites = nil
	s.txLatest = nil
	s.savepoints = nil
	s.txRowVersions = make(map[string]uint64)
	s.txHasWrites = false
	s.txHasDDL = false
	return nil
}
func (s *Schema) Commit() error {
	// Logical catalog/row/predicate locks protect data-only commits. Publishing
	// an atomic batch needs no catalog rewrite or exclusive catalog mutex.
	if s.inTx && s.parent != nil && s.lockHandler != nil && !s.persistentStateChanged() {
		s.EndStatement()
		if err := s.RefreshReadCommitted(); err != nil {
			_ = s.Rollback()
			return err
		}
		if err := s.checkInterrupt(); err != nil {
			_ = s.Rollback()
			return err
		}
		batch := make([]storage.Mutation, 0, len(s.txWrites))
		for _, w := range s.txWrites {
			batch = append(batch, storage.Mutation{Key: w.key, Value: w.value, Delete: w.del})
		}
		if len(batch) > 0 {
			if err := s.lsm.WriteBatch(batch); err != nil {
				return err
			}
			s.parent.commitVersion.Add(1)
		}
		s.endTransaction()
		return nil
	}

	root := s
	if s.parent != nil {
		root = s.parent
	}
	root.commitMu.Lock()
	defer root.commitMu.Unlock()
	s.EndStatement()
	if !s.inTx {
		return fmt.Errorf("not in a transaction")
	}
	if err := s.prepareCommit(); err != nil {
		_ = s.Rollback()
		return err
	}
	changed := s.catalogChanged()
	persistent := s.persistentStateChanged()
	if len(s.txWrites) == 0 && !persistent {
		s.endTransaction()
		return nil
	}
	batch := make([]storage.Mutation, 0, len(s.txWrites)+7)
	for _, w := range s.txWrites {
		batch = append(batch, storage.Mutation{Key: w.key, Value: w.value, Delete: w.del})
	}
	for _, meta := range []struct {
		key    string
		value  any
		before any
	}{
		{schemaPrefix, s.tables, s.txState.tables},
		{schemaIdxPrefix, s.indexes, s.txState.indexes},
		{schemaViewsPrefix, s.views, s.txState.views},
		{schemaTrigPrefix, s.triggers, s.txState.triggers},
		{autoincPrefix, s.autoincMax, s.txState.autoinc},
		{statsPrefix, s.stats, s.txState.stats},
	} {
		if reflect.DeepEqual(meta.value, meta.before) {
			continue
		}
		data, err := json.Marshal(meta.value)
		if err != nil {
			return err
		}
		batch = append(batch, storage.Mutation{Key: []byte(meta.key), Value: data})
	}
	if s.userVersion != s.txState.userVersion {
		version := make([]byte, 8)
		binary.LittleEndian.PutUint64(version, uint64(s.userVersion))
		batch = append(batch, storage.Mutation{Key: []byte(versionPrefix), Value: version})
	}
	if err := s.lsm.WriteBatch(batch); err != nil {
		return err
	}
	if s.parent != nil {
		if persistent {
			state := s.captureState()
			// Row-ID reservations are a shared, monotonic runtime cache; do not
			// copy every table's counters into each session and transaction.
			state.rowIDs = s.parent.rowIDCount
			s.parent.restoreState(state)
			if changed {
				for table := range s.parent.rowIDReservations {
					if _, exists := s.tables[table]; !exists {
						delete(s.parent.rowIDReservations, table)
					}
				}
			}
			s.parent.stateVersion++
			s.readStateVersion = s.parent.stateVersion
		}
		s.parent.commitVersion.Add(1)
		s.readVersion = s.parent.commitVersion.Load()
		if changed {
			s.parent.catalogVersion++
		}
	}
	s.endTransaction()
	return nil
}
func (s *Schema) Rollback() error {
	if !s.inTx {
		return fmt.Errorf("not in a transaction")
	}
	s.restoreState(s.txState)
	s.endTransaction()
	return nil
}
func (s *Schema) Savepoint(name string) error {
	outer := !s.inTx
	if outer {
		if err := s.Begin(); err != nil {
			return err
		}
	}
	s.savepoints = append(s.savepoints, savepoint{name: strings.ToLower(name), pos: len(s.txWrites), state: s.captureState(), outer: outer})
	return nil
}
func (s *Schema) savepointIndex(name string) int {
	for i := len(s.savepoints) - 1; i >= 0; i-- {
		if s.savepoints[i].name == strings.ToLower(name) {
			return i
		}
	}
	return -1
}
func (s *Schema) Release(name string) error {
	idx := s.savepointIndex(name)
	if idx < 0 {
		return fmt.Errorf("savepoint %q not found", name)
	}
	if s.savepoints[idx].outer {
		return s.Commit()
	}
	s.savepoints = s.savepoints[:idx]
	return nil
}
func (s *Schema) RollbackTo(name string) error {
	idx := s.savepointIndex(name)
	if idx < 0 {
		return fmt.Errorf("savepoint %q not found", name)
	}
	sp := s.savepoints[idx]
	s.restoreState(sp.state)
	s.truncateTxWrites(sp.pos)
	s.resetRolledBackWrites()
	s.savepoints = s.savepoints[:idx+1]
	// Keep a fresh copy so repeated rollbacks do not mutate the saved state.
	s.savepoints[idx].state = s.captureState()
	return nil
}

// ExecuteStatement supplies statement rollback inside explicit transactions and
// commits a single atomic batch for statements outside a transaction.
func (s *Schema) ExecuteStatement(run func() error) error {
	implicit := !s.inTx
	if implicit {
		if err := s.Begin(); err != nil {
			return err
		}
	}
	state := s.captureState()
	pos := len(s.txWrites)
	err := run()
	if err == nil {
		err = s.checkStatementForeignKeys(pos)
	}
	if err == nil {
		err = s.checkInterrupt()
	}
	if err == nil {
		err = s.recordWrites(pos)
	}
	if err != nil {
		s.restoreState(state)
		s.truncateTxWrites(pos)
		if implicit {
			s.Rollback()
		}
		return err
	}
	if implicit {
		if err := s.Commit(); err != nil {
			s.Rollback()
			return err
		}
	}
	return nil
}
func (s *Schema) StatementChanges() int64 { return s.statementChanges }
func (s *Schema) write(key, value []byte) error {
	if s.inTx {
		s.txPut(key, value)
		return nil
	}
	return s.lsm.Put(key, value)
}
func (s *Schema) remove(key []byte) error {
	if s.inTx {
		s.txDelete(key)
		return nil
	}
	return s.lsm.Delete(key)
}
func (s *Schema) removeRange(start, end []byte) error {
	iter := s.newIterator()
	var keys [][]byte
	iter.Seek(start)
	for iter.Valid() && bytes.Compare(iter.Key().UserKey, end) < 0 {
		keys = append(keys, copyBytes(iter.Key().UserKey))
		iter.Next()
	}
	iter.Close()
	for _, key := range keys {
		if err := s.remove(key); err != nil {
			return err
		}
	}
	return nil
}
func (s *Schema) newIterator() storage.Iterator {
	if s.inTx {
		return s.txNewIterator()
	}
	return s.committedIterator()
}

func (s *Schema) txAppend(key, value []byte, deleted bool) {
	if s.txLatest == nil {
		s.txLatest = make(map[string]int)
	}
	ownedKey := copyBytes(key)
	name := string(ownedKey)
	prev := -1
	if index, ok := s.txLatest[name]; ok {
		prev = index
	}
	s.txLatest[name] = len(s.txWrites)
	s.txWrites = append(s.txWrites, txWrite{key: ownedKey, value: copyBytes(value), del: deleted, previous: prev})
}
func (s *Schema) txPut(key, value []byte) { s.txAppend(key, value, false) }
func (s *Schema) txDelete(key []byte)     { s.txAppend(key, nil, true) }

// Undo only the discarded suffix; savepoints and failed statements restore
// prior values without rebuilding or scanning the transaction's write set.
func (s *Schema) truncateTxWrites(pos int) {
	for i := len(s.txWrites) - 1; i >= pos; i-- {
		w := s.txWrites[i]
		if w.previous >= 0 {
			s.txLatest[string(w.key)] = w.previous
		} else {
			delete(s.txLatest, string(w.key))
		}
	}
	clear(s.txWrites[pos:])
	s.txWrites = s.txWrites[:pos]
}
func (s *Schema) txGet(key []byte) ([]byte, bool, error) {
	if i, ok := s.txLatest[string(key)]; ok {
		w := s.txWrites[i]
		if w.del {
			return nil, false, nil
		}
		return w.value, true, nil
	}
	return s.committedGet(key)
}

func (s *Schema) txNewIterator() storage.Iterator {
	return s.txIterator(s.committedIterator(), nil, nil)
}

func (s *Schema) txIterator(base storage.Iterator, lower, upper []byte) storage.Iterator {
	rows := make([]storage.KeyValue, 0)
	for _, index := range s.txLatest {
		w := s.txWrites[index]
		if lower != nil && bytes.Compare(w.key, lower) < 0 || upper != nil && bytes.Compare(w.key, upper) >= 0 {
			continue
		}
		typ := uint8(1)
		if w.del {
			typ = 0
		}
		rows = append(rows, storage.KeyValue{Key: storage.InternalKey{UserKey: w.key, OpType: typ}, Value: w.value})
	}
	if len(rows) == 0 {
		return base
	}
	sort.Slice(rows, func(i, j int) bool { return bytes.Compare(rows[i].Key.UserKey, rows[j].Key.UserKey) < 0 })
	it := &transactionIterator{base: base, rows: rows}
	it.Next()
	return it
}

type transactionIterator struct {
	err      error
	base     storage.Iterator
	rows     []storage.KeyValue
	pos      int
	current  storage.KeyValue
	valid    bool
	afterEnd bool
}

func (it *transactionIterator) Err() error {
	if it.err != nil {
		return it.err
	}
	return storage.IteratorError(it.base)
}
func (it *transactionIterator) Valid() bool { return it.valid }
func (it *transactionIterator) Next() {
	it.valid = false
	for it.base.Valid() || it.pos < len(it.rows) {
		if err := storage.IteratorError(it.base); err != nil {
			it.err = err
			return
		}
		useOverlay := it.pos < len(it.rows) && (!it.base.Valid() || bytes.Compare(it.rows[it.pos].Key.UserKey, it.base.Key().UserKey) <= 0)
		if useOverlay {
			kv := it.rows[it.pos]
			it.pos++
			if it.base.Valid() && bytes.Equal(it.base.Key().UserKey, kv.Key.UserKey) {
				it.base.Next()
			}
			if kv.Key.OpType == 0 {
				continue
			}
			it.current = kv
		} else {
			it.current = storage.KeyValue{Key: it.base.Key(), Value: it.base.Value()}
			it.base.Next()
		}
		it.valid = true
		it.afterEnd = false
		return
	}
	it.err = storage.IteratorError(it.base)
	it.afterEnd = true
}
func (it *transactionIterator) Prev() {
	if !it.valid && !it.afterEnd {
		return
	}
	var target []byte
	if it.valid {
		target = it.current.Key.UserKey
	}
	for {
		if target == nil {
			it.base.Seek(nil)
			for it.base.Valid() {
				it.base.Next()
			}
		} else {
			it.base.Seek(target)
		}
		it.base.Prev()
		var candidate []byte
		if it.base.Valid() {
			candidate = it.base.Key().UserKey
		}
		position := len(it.rows) - 1
		if target != nil {
			position = sort.Search(len(it.rows), func(i int) bool { return bytes.Compare(it.rows[i].Key.UserKey, target) >= 0 }) - 1
		}
		if position >= 0 && (candidate == nil || bytes.Compare(it.rows[position].Key.UserKey, candidate) > 0) {
			candidate = it.rows[position].Key.UserKey
		}
		if candidate == nil {
			it.valid = false
			it.afterEnd = false
			it.err = storage.IteratorError(it.base)
			return
		}
		it.Seek(candidate)
		if it.valid && bytes.Equal(it.current.Key.UserKey, candidate) {
			return
		}
		if it.Err() != nil {
			return
		}
		target = candidate
	}
}
func (it *transactionIterator) Seek(key []byte) {
	it.base.Seek(key)
	it.pos = sort.Search(len(it.rows), func(i int) bool { return bytes.Compare(it.rows[i].Key.UserKey, key) >= 0 })
	it.Next()
}
func (it *transactionIterator) Key() storage.InternalKey {
	if it.Valid() {
		return it.current.Key
	}
	return storage.InternalKey{}
}
func (it *transactionIterator) Value() []byte {
	if it.Valid() {
		return it.current.Value
	}
	return nil
}
func (it *transactionIterator) Close() { it.base.Close(); it.valid = false; it.afterEnd = false }

func copyBytes(b []byte) []byte {
	if b == nil {
		return nil
	}
	r := make([]byte, len(b))
	copy(r, b)
	return r
}

var _ = strings.HasPrefix

// Reconcile indexes with authoritative rows when verification is requested.
func (s *Schema) repairIndexes() error {
	if len(s.indexes) == 0 {
		hasIndexedColumns := false
		for _, columns := range s.lookups().indexedColumns {
			hasIndexedColumns = hasIndexedColumns || len(columns) > 0
		}
		if !hasIndexedColumns {
			return nil
		}
	}
	expected := make(map[string][]byte)
	actual := make(map[string][]byte)
	iter := s.lsm.NewIterator()
	for iter.Valid() {
		key := iter.Key().UserKey
		if bytes.HasPrefix(key, []byte(indexPrefix)) {
			actual[string(key)] = copyBytes(iter.Value())
		}
		if table, info := s.rowTable(key); info != nil {
			row := decodeRow(iter.Value(), info)
			if err := s.validateIndexedRow(table, row); err != nil {
				iter.Close()
				return fmt.Errorf("cannot open database with invalid indexed data: %w", err)
			}
			for _, idx := range s.tableIndexes(table) {
				if s.isClusteredIndex(idx) {
					continue
				}
				expected[string(encodeRowIndexKey(idx, row, key))] = s.encodeIndexValue(idx, row)
			}
		}
		iter.Next()
	}
	err := storage.IteratorError(iter)
	iter.Close()
	if err != nil {
		return err
	}
	var batch []storage.Mutation
	for key := range actual {
		if _, ok := expected[key]; !ok {
			batch = append(batch, storage.Mutation{Key: []byte(key), Delete: true})
		}
	}
	for key, value := range expected {
		if old, ok := actual[key]; !ok || !bytes.Equal(old, value) {
			batch = append(batch, storage.Mutation{Key: []byte(key), Value: value})
		}
	}
	if len(batch) > 0 {
		return s.lsm.WriteBatch(batch)
	}
	return nil
}

func (s *Schema) loadIndexes() error {
	data, ok, err := s.schemaGet([]byte(schemaIdxPrefix))
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}
	if err := json.Unmarshal(data, &s.indexes); err != nil {
		return err
	}
	return nil
}

func (s *Schema) saveIndexes() error {
	s.catalogLookup = nil
	data, err := json.Marshal(s.indexes)
	if err != nil {
		return err
	}
	return s.write([]byte(schemaIdxPrefix), data)
}

func (s *Schema) GetIndexList() []string {
	names := make([]string, 0, len(s.indexes))
	for name := range s.indexes {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func (s *Schema) GetIndexSQL(name string) string {
	idx, ok := s.indexes[strings.ToLower(name)]
	if !ok {
		return ""
	}
	unique := ""
	if idx.IsUnique {
		unique = "UNIQUE "
	}
	columns := idx.columnNames()
	quoted := make([]string, len(columns))
	for i, name := range columns {
		quoted[i] = quoteIdent(name)
	}
	return fmt.Sprintf("CREATE %sINDEX %s ON %s(%s)", unique, quoteIdent(idx.Name), quoteIdent(idx.Table), strings.Join(quoted, ", "))
}

func tableDataPrefix(tableName string) string {
	return dataPrefix + tableName + ":"
}

func indexDataPrefix(indexName string) string {
	return indexPrefix + indexName + ":"
}

func (s *Schema) CreateIndex(stmt *CreateIndexStmt) error {
	s.ensureCatalogWritable()
	idxName := strings.ToLower(stmt.Index)
	tableName := strings.ToLower(stmt.Table)
	columns := normalizedIndexColumns(stmt.Columns, stmt.Column)

	if _, ok := s.indexes[idxName]; ok {
		if stmt.IfNotExists {
			return nil
		}
		return fmt.Errorf("index %q already exists", idxName)
	}

	info, ok := s.tables[tableName]
	if !ok {
		return fmt.Errorf("table %q does not exist", tableName)
	}

	if err := validateIndexColumns(info.Columns, columns); err != nil {
		return err
	}

	idx := newIndexInfo(idxName, tableName, columns, stmt.Unique)
	var indexEntries []storage.Mutation
	var values [][]Value
	prefix := []byte(tableDataPrefix(tableName))
	iter := s.newIteratorSeek(prefix)
	for iter.Valid() && bytes.HasPrefix(iter.Key().UserKey, prefix) {
		if err := s.checkInterrupt(); err != nil {
			iter.Close()
			return err
		}
		row := decodeRow(iter.Value(), info)
		if err := validateIndexRow(info, columns, row); err != nil {
			iter.Close()
			return err
		}
		if stmt.Unique {
			v, hasNull := indexValues(columns, row)
			if !hasNull {
				for _, old := range values {
					if equalIndexValues(v, old) {
						iter.Close()
						return fmt.Errorf("UNIQUE constraint failed: %s.%s", tableName, strings.Join(columns, ", "))
					}
				}
				values = append(values, v)
			}
		}
		indexEntries = append(indexEntries, storage.Mutation{Key: encodeRowIndexKey(idx, row, iter.Key().UserKey), Value: s.encodeIndexValue(idx, row)})
		iter.Next()
	}
	err := storage.IteratorError(iter)
	iter.Close()
	if err != nil {
		return err
	}

	// Publish the index only after all existing rows pass type and uniqueness
	// validation. A rejected CREATE INDEX leaves the catalog untouched.
	s.indexes[idxName] = idx
	if err := s.saveIndexes(); err != nil {
		delete(s.indexes, idxName)
		return err
	}

	for _, entry := range indexEntries {
		if err := s.write(entry.Key, entry.Value); err != nil {
			return err
		}
	}

	return nil
}

func (s *Schema) DropIndex(stmt *DropIndexStmt) error {
	s.ensureCatalogWritable()
	idxName := strings.ToLower(stmt.Index)
	if _, ok := s.indexes[idxName]; !ok {
		return fmt.Errorf("index %q does not exist", idxName)
	}

	idx := s.indexes[idxName]
	if idx.Auto {
		return fmt.Errorf("cannot drop index backing a constraint: %s", idxName)
	}
	if err := s.checkDropForeignIndex(idx); err != nil {
		return err
	}
	prefix := indexDataPrefix(idxName)
	iter := s.newIterator()

	var keysToDelete [][]byte
	for iter.Valid() {
		key := string(iter.Key().UserKey)
		if !strings.HasPrefix(key, prefix) {
			if key > prefix {
				break
			}
			iter.Next()
			continue
		}
		keysToDelete = append(keysToDelete, append([]byte{}, iter.Key().UserKey...))
		iter.Next()
	}
	iter.Close()

	for _, k := range keysToDelete {
		s.remove(k)
	}

	delete(s.indexes, idxName)
	return s.saveIndexes()
}

func (s *Schema) AlterTable(stmt *AlterTableStmt) error {
	s.ensureCatalogWritable()
	table := strings.ToLower(stmt.Table)
	info, ok := s.tables[table]
	if !ok {
		return fmt.Errorf("table %q does not exist", table)
	}
	entries, err := s.matchingDMLRows(table, info, nil, -1, 0)
	if err != nil {
		return err
	}
	var rows []map[string]Value
	for _, entry := range entries {
		rows = append(rows, decodeRow(entry.Value, info))
	}
	// Remove the old index entries while their column and table names are intact.
	for i, entry := range entries {
		if err := s.updateIndexes(table, info, rows[i], entry.Key.UserKey, false); err != nil {
			return err
		}
	}
	newTable := table
	switch stmt.Operation {
	case "ADD_COLUMN":
		col := *stmt.Column
		for _, old := range info.Columns {
			if old.Name == col.Name {
				return fmt.Errorf("duplicate column: %s", col.Name)
			}
		}
		if col.PrimaryKey || col.Unique {
			return fmt.Errorf("cannot add a PRIMARY KEY or UNIQUE column")
		}
		value := NewNullValue()
		if col.Default != nil {
			value, err = s.evalExpr(col.Default, nil)
			if err != nil {
				return err
			}
			col.DefaultSQL = formatExpr(col.Default)
		}
		if col.NotNull && value.Null && len(rows) > 0 {
			return fmt.Errorf("cannot add a NOT NULL column with NULL default")
		}
		if col.Check != nil {
			col.CheckSQL = formatExpr(col.Check)
		}
		info.Columns = append(info.Columns, col)
		for _, row := range rows {
			row[col.Name] = applyAffinity(value, col.ColType)
		}
		if err := validateConstraintExpressions(info); err != nil {
			return err
		}
		if err := s.foreignDefinitions(info); err != nil {
			return err
		}
	case "RENAME_TO":
		newTable = strings.ToLower(stmt.NewName)
		if _, exists := s.tables[newTable]; exists {
			return fmt.Errorf("table %q already exists", newTable)
		}
		if _, exists := s.views[newTable]; exists {
			return fmt.Errorf("view %q already exists", newTable)
		}
		info.Name = newTable
		s.tables[newTable] = info
		delete(s.tables, table)
		for _, idx := range s.indexes {
			if idx.Table == table {
				idx.Table = newTable
			}
		}
		if v, ok := s.stats[table]; ok {
			s.stats[newTable] = v
			delete(s.stats, table)
		}
		if v, ok := s.autoincMax[table]; ok {
			s.autoincMax[newTable] = v
			delete(s.autoincMax, table)
		}
		if v, ok := s.rowIDCount[table]; ok {
			s.rowIDCount[newTable] = v
			delete(s.rowIDCount, table)
		}
	case "RENAME_COLUMN":
		old, new := strings.ToLower(stmt.OldColName), strings.ToLower(stmt.NewColName)
		found := false
		for _, col := range info.Columns {
			if col.Name == new {
				return fmt.Errorf("duplicate column: %s", new)
			}
		}
		for i := range info.Columns {
			if info.Columns[i].Name == old {
				info.Columns[i].Name = new
				found = true
			}
		}
		if !found {
			return fmt.Errorf("column %q not found", old)
		}
		for _, row := range rows {
			row[new] = row[old]
			delete(row, old)
		}
		for i, name := range info.PKColumns {
			if name == old {
				info.PKColumns[i] = new
			}
		}
		// Copy nested constraint slices before editing the rollback snapshot.
		groups := make([][]string, len(info.TableUnique))
		for i, g := range info.TableUnique {
			groups[i] = append([]string(nil), g...)
			for j, name := range groups[i] {
				if name == old {
					groups[i][j] = new
				}
			}
		}
		info.TableUnique = groups
		for i := range info.Columns {
			col := &info.Columns[i]
			col.Check = renameExpr(col.Check, old, new)
			if col.Check != nil {
				col.CheckSQL = formatExpr(col.Check)
			}
		}
		for i, e := range info.TableCheck {
			info.TableCheck[i] = renameExpr(e, old, new)
			info.TableCheckSQL[i] = formatExpr(info.TableCheck[i])
		}
		for _, idx := range s.indexes {
			if idx.Table == table {
				if idx.Column == old {
					idx.Column = new
				}
				for i, col := range idx.Columns {
					if col == old {
						idx.Columns[i] = new
					}
				}
			}
		}
	default:
		return fmt.Errorf("unknown alter table operation: %s", stmt.Operation)
	}
	if stmt.Operation == "RENAME_TO" || stmt.Operation == "RENAME_COLUMN" {
		s.renameForeignKeys(table, newTable, strings.ToLower(stmt.OldColName), strings.ToLower(stmt.NewColName))
		for _, view := range s.views {
			parsed, err := Parse(view.SQL)
			if err != nil {
				return err
			}
			if len(parsed) != 1 {
				return fmt.Errorf("invalid view definition")
			}
			sel, ok := parsed[0].(*SelectStmt)
			if !ok {
				return fmt.Errorf("invalid view definition")
			}
			if renameViewSelect(sel, table, newTable, strings.ToLower(stmt.OldColName), strings.ToLower(stmt.NewColName), stmt.Operation) {
				view.Parsed = sel
				view.SQL = formatSelectSQL(sel)
			}
		}
	}

	s.catalogLookup = nil

	for i, entry := range entries {
		row := rows[i]
		key := entry.Key.UserKey
		if stmt.Operation == "RENAME_TO" {
			key = append([]byte(tableDataPrefix(newTable)), entry.Key.UserKey[len(tableDataPrefix(table)):]...)
			if err := s.remove(entry.Key.UserKey); err != nil {
				return err
			}
		}
		if err := s.checkConstraints(newTable, info, row, key); err != nil {
			return err
		}
		if err := s.write(key, encodeRow(row, info)); err != nil {
			return err
		}
		if err := s.updateIndexes(newTable, info, row, key, true); err != nil {
			return err
		}
	}
	if err := s.saveIndexes(); err != nil {
		return err
	}
	return s.saveSchema()
}
func renameViewSelect(sel *SelectStmt, table, newTable, oldCol, newCol, operation string) bool {
	changed := false
	qualifiers := make(map[string]bool)
	if sel.From == table {
		qualifiers[sel.FromAlias] = true
		qualifiers[table] = true
		changed = true
		if operation == "RENAME_TO" {
			sel.From = newTable
			if sel.FromAlias == table {
				sel.FromAlias = newTable
			}
		}
	}
	for i := range sel.Joins {
		jc := &sel.Joins[i]
		if jc.Table == table {
			qualifiers[jc.Alias] = true
			qualifiers[table] = true
			changed = true
			if operation == "RENAME_TO" {
				jc.Table = newTable
				if jc.Alias == table {
					jc.Alias = newTable
				}
			}
		}
		if jc.Subquery != nil && renameViewSelect(jc.Subquery, table, newTable, oldCol, newCol, operation) {
			changed = true
		}
	}
	if sub, ok := sel.FromExpr.(*SubqueryExpr); ok && renameViewSelect(sub.Stmt, table, newTable, oldCol, newCol, operation) {
		changed = true
	}
	if !changed {
		return false
	}
	var rewrite func(Expression) Expression
	rewrite = func(expr Expression) Expression {
		switch e := expr.(type) {
		case *ColumnRefExpr:
			c := *e
			parts := strings.Split(c.Name, ".")
			if operation == "RENAME_TO" {
				if len(parts) == 2 && parts[0] == table {
					c.Name = newTable + "." + parts[1]
				}
			} else if len(parts) == 1 && parts[0] == oldCol {
				c.Name = newCol
			} else if len(parts) == 2 && qualifiers[parts[0]] && parts[1] == oldCol {
				c.Name = parts[0] + "." + newCol
			}
			return &c
		case *BinaryExpr:
			c := *e
			c.Left = rewrite(e.Left)
			c.Right = rewrite(e.Right)
			return &c
		case *UnaryExpr:
			c := *e
			c.Right = rewrite(e.Right)
			return &c
		case *FuncCallExpr:
			c := *e
			c.Args = append([]Expression(nil), e.Args...)
			for i, arg := range c.Args {
				c.Args[i] = rewrite(arg)
			}
			return &c
		}
		return expr
	}
	for i := range sel.Columns {
		sel.Columns[i].Expr = rewrite(sel.Columns[i].Expr)
	}
	sel.Where = rewrite(sel.Where)
	sel.Having = rewrite(sel.Having)
	for i := range sel.OrderBy {
		sel.OrderBy[i].Expr = rewrite(sel.OrderBy[i].Expr)
	}
	for i := range sel.GroupBy {
		sel.GroupBy[i] = rewrite(sel.GroupBy[i])
	}
	for i := range sel.Joins {
		sel.Joins[i].On = rewrite(sel.Joins[i].On)
	}
	return true
}

func renameExpr(expr Expression, old, new string) Expression {
	switch e := expr.(type) {
	case *ColumnRefExpr:
		c := *e
		if c.Name == old {
			c.Name = new
		}
		return &c
	case *BinaryExpr:
		c := *e
		c.Left = renameExpr(e.Left, old, new)
		c.Right = renameExpr(e.Right, old, new)
		return &c
	case *UnaryExpr:
		c := *e
		c.Right = renameExpr(e.Right, old, new)
		return &c
	case *FuncCallExpr:
		c := *e
		c.Args = append([]Expression(nil), e.Args...)
		for i, arg := range c.Args {
			c.Args[i] = renameExpr(arg, old, new)
		}
		return &c
	}
	return expr
}

func (s *Schema) Union(stmt *UnionStmt) (*ResultSet, error) {
	left, err := s.Select(stmt.Left)
	if err != nil {
		return nil, err
	}
	right, err := s.execUnionRight(stmt.Right)
	if err != nil {
		return nil, err
	}

	if len(left.Columns) != len(right.Columns) {
		return nil, fmt.Errorf("compound SELECTs must return the same number of columns")
	}

	for i := range right.Rows {
		mapped := make(map[string]Value)
		for j, rc := range right.Columns {
			if j < len(left.Columns) {
				mapped[left.Columns[j].Name] = right.Rows[i][rc.Name]
			}
		}
		right.Rows[i] = mapped
	}

	switch stmt.Op {
	case "INTERSECT":
		left.Rows = intersectRows(left.Rows, right.Rows)
	case "EXCEPT":
		left.Rows = exceptRows(left.Rows, right.Rows)
	default:
		left.Rows = append(left.Rows, right.Rows...)
		if !stmt.All {
			left.Rows = s.distinctRows(left.Rows)
		}
	}

	orders := append([]OrderByClause(nil), stmt.OrderBy...)
	for i := range orders {
		if lit, ok := orders[i].Expr.(*LiteralExpr); ok && !lit.BoundParameter && lit.Value.Type == ColInteger && !lit.Value.Null {
			n := lit.Value.Int
			if n < 1 || n > int64(len(left.Columns)) {
				return nil, fmt.Errorf("ORDER BY position out of range")
			}
			orders[i].Expr = &ColumnRefExpr{Name: left.Columns[n-1].Name}
		}
		for _, row := range left.Rows {
			if _, err := s.evalExpr(orders[i].Expr, row); err != nil {
				return nil, err
			}
		}
	}
	if len(orders) > 0 {
		sort.SliceStable(left.Rows, func(i, j int) bool { return s.compareRows(left.Rows[i], left.Rows[j], orders) })
	}
	if stmt.Offset > 0 {
		if stmt.Offset >= int64(len(left.Rows)) {
			left.Rows = nil
		} else {
			left.Rows = left.Rows[int(stmt.Offset):]
		}
	}
	if stmt.Limit >= 0 && stmt.Limit < int64(len(left.Rows)) {
		left.Rows = left.Rows[:int(stmt.Limit)]
	}

	if len(left.Columns) == 0 && len(right.Columns) > 0 {
		left.Columns = right.Columns
	}
	return left, nil
}

func (s *Schema) execUnionRight(stmt Statement) (*ResultSet, error) {
	switch x := stmt.(type) {
	case *SelectStmt:
		return s.Select(x)
	case *UnionStmt:
		return s.Union(x)
	default:
		return &ResultSet{}, nil
	}
}

func intersectRows(a, b []map[string]Value) []map[string]Value {
	seen := make(map[string]bool)
	for _, r := range b {
		seen[rowKey(r)] = true
	}
	var out []map[string]Value
	for _, r := range a {
		if seen[rowKey(r)] {
			delete(seen, rowKey(r))
			out = append(out, r)
		}
	}
	return out
}

func exceptRows(a, b []map[string]Value) []map[string]Value {
	seen := make(map[string]bool)
	for _, r := range b {
		seen[rowKey(r)] = true
	}
	var out []map[string]Value
	for _, r := range a {
		if !seen[rowKey(r)] {
			seen[rowKey(r)] = true
			out = append(out, r)
		}
	}
	return out
}

func (s *Schema) Vacuum() error {
	if s.inTx {
		return fmt.Errorf("cannot VACUUM within a transaction")
	}
	if err := s.lsm.Flush(); err != nil {
		return err
	}
	return s.lsm.CompactRange()
}

func encodeIndexKey(indexName string, val Value, rowKey []byte) []byte {
	sortable := valueToSortableKey(val)
	key := make([]byte, 0, len(indexPrefix)+len(indexName)+len(sortable)+len(rowKey)+2)
	key = append(key, indexPrefix...)
	key = append(key, indexName...)
	key = append(key, ':')
	key = append(key, sortable...)
	key = append(key, ':')
	return append(key, rowKey...)
}

func valueToSortableKey(v Value) string {
	if v.Null {
		return "\x00"
	}
	switch v.Type {
	case ColInteger:
		buf := make([]byte, 1+8)
		buf[0] = '\x01'
		u := uint64(v.Int) ^ (1 << 63)
		binary.BigEndian.PutUint64(buf[1:], u)
		return string(buf)
	case ColReal:
		buf := make([]byte, 1+8)
		buf[0] = '\x02'
		bits := math.Float64bits(v.Real)
		if v.Real >= 0 {
			bits ^= 1 << 63
		} else {
			bits = ^bits
		}
		binary.BigEndian.PutUint64(buf[1:], bits)
		return string(buf)
	case ColText:
		return "\x03" + v.Text
	case ColBlob:
		return "\x04" + string(v.Blob)
	}
	return "\x00"
}

func (s *Schema) findMatchingRowsViaIndex(where Expression, tableName string) ([][]byte, bool) {
	if keys, ok := s.primaryKeyList(where, tableName, s.tables[tableName]); ok {
		return keys, true
	}
	if key, ok := s.primaryKeyLookup(where, tableName, s.tables[tableName]); ok {
		return [][]byte{key}, true
	}
	bin, ok := where.(*BinaryExpr)
	if !ok || bin.Op != "=" {
		return nil, false
	}

	leftRef, leftOK := bin.Left.(*ColumnRefExpr)
	rightRef, rightOK := bin.Right.(*ColumnRefExpr)
	leftLit, leftLitOK := bin.Left.(*LiteralExpr)
	rightLit, rightLitOK := bin.Right.(*LiteralExpr)

	var colName string
	var val Value

	if leftOK && rightLitOK {
		colName = strings.ToLower(leftRef.Name)
		val = rightLit.Value
	} else if rightOK && leftLitOK {
		colName = strings.ToLower(rightRef.Name)
		val = leftLit.Value
	} else {
		return nil, false
	}

	if info, ok := s.tables[tableName]; ok {
		for _, col := range info.Columns {
			if col.Name == colName {
				val = applyAffinity(val, col.ColType)
				break
			}
		}
	}
	var matchingIdx *IndexInfo
	for _, idx := range s.tableIndexes(tableName) {
		if idx.Table == tableName && idx.Column == colName {
			matchingIdx = idx
			break
		}
	}
	if matchingIdx == nil {
		return nil, false
	}
	if s.isClusteredIndex(matchingIdx) {
		if val.Null {
			return nil, true
		}
		return [][]byte{s.tables[tableName].encodePK(tableName, map[string]Value{colName: val})}, true
	}

	prefixStr := indexValuePrefix(matchingIdx, val)
	prefix := []byte(prefixStr)

	var rowKeys [][]byte
	iter := s.newIterator()
	iter.Seek(prefix)

	for iter.Valid() {
		key := string(iter.Key().UserKey)
		if !strings.HasPrefix(key, prefixStr) {
			break
		}
		rest := indexedRowKey(matchingIdx, []byte(key), len(prefixStr))
		if rest == nil {
			iter.Close()
			return nil, false
		}
		rowKeys = append(rowKeys, append([]byte(nil), rest...))
		iter.Next()
	}
	iter.Close()

	if storage.IteratorError(iter) != nil {
		return nil, false
	}
	return rowKeys, true
}

func (s *Schema) findRowsViaIndexOrder(stmt *SelectStmt, tableName string) ([][]byte, bool) {
	return nil, false
}

// Only exact, typed equalities are safe for a physical key lookup. SQL affinity
// and mixed numeric comparisons still use the normal scan and predicate path.
func (s *Schema) primaryKeyLookup(where Expression, table string, info *TableInfo) ([]byte, bool) {
	if info == nil || len(info.PKColumns) == 0 {
		return nil, false
	}
	values := make(map[string]Value)
	var collect func(Expression)
	collect = func(expr Expression) {
		bin, ok := expr.(*BinaryExpr)
		if !ok {
			return
		}
		if bin.Op == "AND" {
			collect(bin.Left)
			collect(bin.Right)
			return
		}
		if bin.Op != "=" {
			return
		}
		col, colOK := bin.Left.(*ColumnRefExpr)
		lit, litOK := bin.Right.(*LiteralExpr)
		if !colOK || !litOK {
			col, colOK = bin.Right.(*ColumnRefExpr)
			lit, litOK = bin.Left.(*LiteralExpr)
		}
		if !colOK || !litOK || lit.Value.Null {
			return
		}
		name := strings.ToLower(col.Name)
		if prefix, rest, qualified := strings.Cut(name, "."); qualified {
			if prefix != table {
				return
			}
			name = rest
		}
		for _, def := range info.Columns {
			if def.Name == name && def.ColType == lit.Value.Type {
				// NaNs and signed zero do not have a unique physical REAL encoding.
				if lit.Value.Type == ColReal && (lit.Value.Real == 0 || math.IsNaN(lit.Value.Real)) {
					return
				}
				values[name] = lit.Value
				return
			}
		}
	}
	collect(where)
	for _, col := range info.PKColumns {
		if _, ok := values[col]; !ok {
			return nil, false
		}
	}
	return info.encodePK(table, values), true
}

func (s *Schema) makeRowKey(tableName string, info *TableInfo, row map[string]Value, fallbackID int64) []byte {
	if len(info.PKColumns) > 0 {
		return info.encodePK(tableName, row)
	}
	return encodeRowKey(tableName, fallbackID)
}

func encodeRowKey(tableName string, rowID int64) []byte {
	return []byte(fmt.Sprintf("%s%s:%020d", dataPrefix, tableName, rowID))
}

func (s *Schema) Insert(stmt *InsertStmt) (int64, *ResultSet, error) {
	tableName := strings.ToLower(stmt.Table)
	info, ok := s.tables[tableName]
	viewInsert := false
	if !ok {
		if vi, exists := s.views[tableName]; exists {
			info = &TableInfo{Name: tableName, Columns: s.viewColumnDefs(vi)}
			viewInsert = true
		} else {
			return 0, nil, fmt.Errorf("table %q does not exist", tableName)
		}
	}

	s.statementChanges = 0
	seen := make(map[string]bool)
	for _, name := range stmt.ColumnNames {
		if seen[name] {
			continue
		}
		seen[name] = true
		found := false
		for _, col := range info.Columns {
			if col.Name == name {
				found = true
				break
			}
		}
		if !found {
			return 0, nil, fmt.Errorf("no such column: %s", name)
		}
	}

	if stmt.Select != nil {
		return s.insertFromSelect(stmt, info)
	}

	allGroups := make([][]Expression, 0)
	if len(stmt.ValueGroups) > 0 {
		allGroups = append(allGroups, stmt.Values)
		allGroups = append(allGroups, stmt.ValueGroups...)
	} else if len(stmt.Values) > 0 {
		allGroups = append(allGroups, stmt.Values)
	}

	if stmt.DefaultValues {
		allGroups = [][]Expression{nil}
	}
	if len(allGroups) == 0 {
		return 0, nil, fmt.Errorf("no values provided")
	}

	colMap := s.buildColumnMap(stmt.ColumnNames, info.Columns)
	expected := len(info.Columns)
	if len(stmt.ColumnNames) > 0 {
		expected = len(stmt.ColumnNames)
	}
	for _, group := range allGroups {
		if !stmt.DefaultValues && len(group) != expected {
			return 0, nil, fmt.Errorf("expected %d values, got %d", expected, len(group))
		}
	}

	var lastID int64
	var returningRows []map[string]Value

	for _, group := range allGroups {
		row := make(map[string]Value)
		for ci, col := range info.Columns {
			if vi, ok := colMap[ci]; ok && vi < len(group) {
				v, err := s.evalExpr(group[vi], nil)
				if err != nil {
					return 0, nil, err
				}
				row[col.Name] = s.evalLiteral(&LiteralExpr{Value: v}, col.ColType)
			} else if col.Default != nil {
				val, err := s.evalExpr(col.Default, nil)
				if err != nil {
					return 0, nil, err
				}
				row[col.Name] = applyAffinity(val, col.ColType)
			} else {
				row[col.Name] = NewNullValue()
			}
		}

		if viewInsert {
			found := false
			for _, tr := range s.triggers {
				if tr.View == tableName && tr.InsteadOf && tr.Event == "INSERT" {
					found = true
				}
			}
			if !found {
				return 0, nil, fmt.Errorf("cannot modify view %q", tableName)
			}
			if err := s.fireTriggers(tableName, "INSERT", "INSTEAD OF", nil, row); err != nil {
				return 0, nil, err
			}
			returningRows = append(returningRows, row)
			continue
		}

		pkCol := s.getPKColumn(info)
		nextID, err := s.nextRowID(tableName)
		if err != nil {
			return 0, nil, err
		}

		if pkCol != nil && pkCol.AutoIncrement {
			autoMax := s.getAutoincMax(tableName)
			if autoMax >= nextID {
				nextID = autoMax + 1
			}
		}

		if pkCol != nil && pkCol.ColType == ColInteger && len(info.PKColumns) <= 1 {
			pkVal := row[pkCol.Name]
			if pkVal.Null && pkCol.AutoIncrement && !suppliedColumn(stmt.ColumnNames, pkCol.Name, stmt.DefaultValues) {
				if nextID <= 0 {
					return 0, nil, fmt.Errorf("integer primary key exhausted")
				}
				row[pkCol.Name] = NewIntValue(nextID)
			} else {
				if pkVal.Type != ColInteger {
					return 0, nil, fmt.Errorf("datatype mismatch for INTEGER PRIMARY KEY")
				}
				val := pkVal.Int
				if val > nextID-1 {
					nextID = val
				}
			}
		}

		if pkCol != nil && pkCol.ColType == ColInteger && len(info.PKColumns) == 1 {
			nextID = row[pkCol.Name].Int
			if nextID >= s.rowIDCount[tableName] {
				s.rowIDCount[tableName] = nextID + 1
			}
		}
		if err := validatePrimaryKey(info, row); err != nil {
			return 0, nil, err
		}
		key := s.makeRowKey(tableName, info, row, nextID)
		if err := s.lockRow(tableName, info, row, key); err != nil {
			return 0, nil, err
		}
		if err := s.fireTriggers(tableName, "INSERT", "BEFORE", nil, row); err != nil {
			return 0, nil, err
		}
		if stmt.Replace {
			if err := s.deleteConflicts(tableName, info, row); err != nil {
				return 0, nil, err
			}
		}
		if err := s.checkConstraints(tableName, info, row, nil); err != nil {
			return 0, nil, err
		}

		data := encodeRow(row, info)
		if s.inTx {
			s.txPut(key, data)
		} else {
			if err := s.write(key, data); err != nil {
				return 0, nil, err
			}
		}

		if pkCol != nil && pkCol.AutoIncrement {
			if nextID > s.getAutoincMax(tableName) {
				s.setAutoincMax(tableName, nextID)
			}
			s.saveAutoincMax()
		}

		if err := s.updateIndexes(tableName, info, row, key, true); err != nil {
			return 0, nil, err
		}

		if err := s.fireTriggers(tableName, "INSERT", "AFTER", nil, row); err != nil {
			return 0, nil, err
		}
		s.statementChanges++
		lastID = nextID
		returningRows = append(returningRows, row)
	}

	var rs *ResultSet
	if len(stmt.Returning) > 0 {
		var returnErr error
		rs, returnErr = s.buildReturningResult(stmt.Returning, returningRows, info)
		if returnErr != nil {
			return 0, nil, returnErr
		}
	}
	return lastID, rs, nil
}

func (s *Schema) buildColumnMap(colNames []string, columns []ColumnDef) map[int]int {
	result := make(map[int]int)
	if len(colNames) == 0 {
		for i := range columns {
			result[i] = i
		}
		return result
	}
	for vi, name := range colNames {
		for ci, col := range columns {
			if col.Name == name {
				result[ci] = vi
				break
			}
		}
	}
	return result
}

func (s *Schema) getPKColumn(info *TableInfo) *ColumnDef {
	for i := range info.Columns {
		if info.Columns[i].PrimaryKey && info.Columns[i].AutoIncrement {
			return &info.Columns[i]
		}
	}
	for i := range info.Columns {
		if info.Columns[i].PrimaryKey {
			return &info.Columns[i]
		}
	}
	if len(info.PKColumns) == 1 {
		for i := range info.Columns {
			if info.Columns[i].Name == info.PKColumns[0] {
				return &info.Columns[i]
			}
		}
	}

	return nil
}

func (s *Schema) checkConstraints(tableName string, info *TableInfo, row map[string]Value, rowKey []byte) error {
	if s.tableHasNoConstraints(info) {
		return validateRowTypes(info, row)
	}
	var original map[string]Value
	if rowKey != nil {
		value, found, err := s.schemaGet(rowKey)
		if err != nil {
			return err
		}
		if found {
			original = decodeRow(value, info)
		}
	}
	return s.checkConstraintsOriginal(tableName, info, row, rowKey, original)
}

func (s *Schema) checkConstraintsOriginal(tableName string, info *TableInfo, row map[string]Value, rowKey []byte, original map[string]Value) error {
	if err := validatePrimaryKey(info, row); err != nil {
		return err
	}
	if err := validateRowTypes(info, row); err != nil {
		return err
	}
	if err := s.validateIndexedRow(tableName, row); err != nil {
		return err
	}
	checkUnique := func(columns []string) error {
		unchanged := original != nil
		for _, col := range columns {
			if !bytes.Equal(encodeValue(original[col]), encodeValue(row[col])) {
				unchanged = false
				break
			}
		}
		if unchanged {
			return nil
		}
		return s.checkUnique(tableName, columns, row, rowKey, info)
	}
	for _, col := range info.Columns {
		if col.NotNull {
			if v, ok := row[col.Name]; ok && v.Null {
				return fmt.Errorf("NOT NULL constraint failed: %s.%s", tableName, col.Name)
			}
		}
		if col.Check != nil {
			v, err := s.evalExpr(col.Check, row)
			match := v.Null || truthValue(v)
			if err != nil {
				return fmt.Errorf("CHECK constraint failed: %s.%s: %v", tableName, col.Name, err)
			}
			if !match {
				return fmt.Errorf("CHECK constraint failed: %s.%s", tableName, col.Name)
			}
		}
	}

	for _, expr := range info.TableCheck {
		v, err := s.evalExpr(expr, row)
		match := v.Null || truthValue(v)
		if err != nil || !match {
			return fmt.Errorf("CHECK constraint failed: %s", tableName)
		}
	}

	pkCol := s.getPKColumn(info)
	if pkCol != nil && len(info.PKColumns) <= 1 {
		if pkCol.ColType == ColInteger {
			v := row[pkCol.Name]
			if v.Null || v.Type != ColInteger {
				return fmt.Errorf("datatype mismatch for INTEGER PRIMARY KEY")
			}
		}

		pkVal := row[pkCol.Name]
		if !pkVal.Null {
			if err := checkUnique([]string{pkCol.Name}); err != nil {
				return err
			}
		}
	}

	for _, col := range info.Columns {
		if col.Unique {
			if err := checkUnique([]string{col.Name}); err != nil {
				return err
			}
		}
	}

	for _, idx := range s.tableIndexes(tableName) {
		if idx.Table == tableName && idx.IsUnique {
			if err := checkUnique(idx.columnNames()); err != nil {
				return err
			}
		}
	}

	for _, cols := range info.TableUnique {
		if err := checkUnique(cols); err != nil {
			return err
		}
	}

	return nil
}

func (s *Schema) tableHasNoConstraints(info *TableInfo) bool {
	for _, col := range info.Columns {
		if col.NotNull || col.PrimaryKey || col.Unique || col.Check != nil {
			return false
		}
	}
	if len(info.TableCheck) > 0 || len(info.TableUnique) > 0 {
		return false
	}
	for _, idx := range s.tableIndexes(info.Name) {
		if idx.Table == info.Name && idx.IsUnique {
			return false
		}
	}
	return true
}

func (s *Schema) checkUnique(tableName string, colNames []string, row map[string]Value, excludeKey []byte, info *TableInfo) error {
	// Predicate locks protect this value until commit. Check the current state,
	// including our overlay, even when the statement snapshot predates the lock.
	active := s.statementActive
	if s.lockHandler != nil {
		s.statementActive = false
	}
	defer func() { s.statementActive = active }()
	if colsEqual(colNames, info.PKColumns) {
		nonNull := true
		for _, name := range colNames {
			nonNull = nonNull && !row[name].Null
		}
		if nonNull {
			key := info.encodePK(tableName, row)
			_, found, err := s.schemaGet(key)
			if err != nil {
				return err
			}
			if found && !bytes.Equal(key, excludeKey) {
				return fmt.Errorf("UNIQUE constraint failed: %s.%s", tableName, strings.Join(colNames, ", "))
			}
			return nil
		}
	}

	if len(info.PKColumns) == 1 && len(colNames) == 1 && colNames[0] == info.PKColumns[0] {
		if col := s.getPKColumn(info); col != nil && col.ColType == ColInteger {
			v := row[col.Name]
			if !v.Null && v.Type == ColInteger {
				key := info.encodePK(tableName, row)
				_, found, err := s.schemaGet(key)
				if err != nil {
					return err
				}
				if found && !bytes.Equal(key, excludeKey) {
					return fmt.Errorf("UNIQUE constraint failed: %s.%s", tableName, col.Name)
				}
				return nil
			}
		}
	}

	if len(colNames) == 1 {
		if idx := s.findIndexOnColumn(tableName, colNames[0]); idx != nil {
			return s.checkUniqueViaIndex(tableName, colNames[0], row[colNames[0]], excludeKey, idx)
		}
	}
	return s.checkUniqueScan(tableName, colNames, row, excludeKey, info)
}

func (s *Schema) findIndexOnColumn(tableName, colName string) *IndexInfo {
	for _, idx := range s.tableIndexes(tableName) {
		if idx.Table == tableName && len(idx.columnNames()) == 1 && idx.Column == colName {
			return idx
		}
	}
	return nil
}

func (s *Schema) checkUniqueViaIndex(tableName, colName string, val Value, excludeKey []byte, idx *IndexInfo) error {
	if val.Null {
		return nil
	}
	seekPrefix := []byte(fmt.Sprintf("%s%s:%s:", indexPrefix, idx.Name, valueToSortableKey(val)))
	iter := s.newIterator()
	defer iter.Close()

	iter.Seek(seekPrefix)
	for iter.Valid() {
		key := iter.Key().UserKey
		if !bytes.HasPrefix(key, seekPrefix) {
			break
		}
		rowKey := key[len(seekPrefix):]
		if excludeKey != nil && bytes.Equal(rowKey, excludeKey) {
			iter.Next()
			continue
		}
		return fmt.Errorf("UNIQUE constraint failed: %s.%s", tableName, colName)
	}
	return storage.IteratorError(iter)
}

func (s *Schema) checkUniqueScan(tableName string, colNames []string, row map[string]Value, excludeKey []byte, info *TableInfo) error {
	prefix := tableDataPrefix(tableName)
	var iter storage.Iterator
	if s.inTx {
		iter = s.txNewIterator()
	} else {
		iter = s.newIterator()
	}
	defer iter.Close()

	iter.Seek([]byte(prefix))
	for iter.Valid() {
		key := string(iter.Key().UserKey)
		if !strings.HasPrefix(key, prefix) {
			if key > prefix {
				break
			}
			iter.Next()
			continue
		}

		if excludeKey != nil && bytes.Equal(iter.Key().UserKey, excludeKey) {
			iter.Next()
			continue
		}

		existing := decodeRow(iter.Value(), info)
		match := true
		for _, cn := range colNames {
			ev := existing[cn]
			rv := row[cn]
			if ev.Null || rv.Null {
				match = false
				break
			}
			if ValueCompare(ev, rv) != 0 {
				match = false
				break
			}
		}
		if match {
			return fmt.Errorf("UNIQUE constraint failed: %s", tableName)
		}
		iter.Next()
	}

	return storage.IteratorError(iter)
}

func (s *Schema) updateIndexes(tableName string, info *TableInfo, row map[string]Value, rowKey []byte, isInsert bool) error {
	for _, idx := range s.tableIndexes(tableName) {
		if idx.Table != tableName || s.isClusteredIndex(idx) {
			continue
		}
		key := encodeRowIndexKey(idx, row, rowKey)
		var err error
		if isInsert {
			err = s.write(key, s.encodeIndexValue(idx, row))
		} else {
			err = s.remove(key)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *Schema) updateChangedIndexes(table string, oldRow, row map[string]Value, oldKey, key []byte) error {
	for _, idx := range s.tableIndexes(table) {
		if idx.Table != table || s.isClusteredIndex(idx) {
			continue
		}
		before := encodeRowIndexKey(idx, oldRow, oldKey)
		after := encodeRowIndexKey(idx, row, key)
		value := s.encodeIndexValue(idx, row)
		if oldRow != nil && bytes.Equal(before, after) && bytes.Equal(s.encodeIndexValue(idx, oldRow), value) {
			continue
		}
		if oldRow != nil {
			if err := s.remove(before); err != nil {
				return err
			}
		}
		if err := s.write(after, value); err != nil {
			return err
		}
	}
	return nil
}

func (s *Schema) insertFromSelect(stmt *InsertStmt, info *TableInfo) (int64, *ResultSet, error) {
	rs, err := s.Select(stmt.Select)
	if err != nil {
		return 0, nil, err
	}

	expected := len(info.Columns)
	if len(stmt.ColumnNames) > 0 {
		expected = len(stmt.ColumnNames)
	}
	if len(rs.Columns) != expected {
		return 0, nil, fmt.Errorf("expected %d values, got %d", expected, len(rs.Columns))
	}

	tableName := strings.ToLower(stmt.Table)
	var lastID int64
	var returningRows []map[string]Value

	pkCol := s.getPKColumn(info)

	for _, srcRow := range rs.Rows {
		nextID, err := s.nextRowID(tableName)
		if err != nil {
			return 0, nil, err
		}
		if pkCol != nil && pkCol.AutoIncrement {
			autoMax := s.getAutoincMax(tableName)
			if autoMax >= nextID {
				nextID = autoMax + 1
			}
		}

		row := make(map[string]Value)
		for i, col := range info.Columns {
			var val Value
			if len(stmt.ColumnNames) > 0 {
				included := false
				for vi, cn := range stmt.ColumnNames {
					if cn == col.Name && vi < len(rs.Columns) {
						val = srcRow[rs.Columns[vi].Name]
						included = true
						break
					}
				}
				if !included {
					if col.Default != nil {
						var defaultErr error
						val, defaultErr = s.evalExpr(col.Default, nil)
						if defaultErr != nil {
							return 0, nil, defaultErr
						}
					} else {
						val = NewNullValue()
					}
				}
			} else if i < len(rs.Columns) {
				val = srcRow[rs.Columns[i].Name]
			} else {
				val = NewNullValue()
			}
			row[col.Name] = applyAffinity(val, col.ColType)
		}

		if pkCol != nil && pkCol.ColType == ColInteger && len(info.PKColumns) == 1 {
			pkVal := row[pkCol.Name]
			if pkVal.Null && pkCol.AutoIncrement && !suppliedColumn(stmt.ColumnNames, pkCol.Name, false) {
				if nextID <= 0 {
					return 0, nil, fmt.Errorf("integer primary key exhausted")
				}
				row[pkCol.Name] = NewIntValue(nextID)
			}
		}

		if pkCol != nil && pkCol.ColType == ColInteger && len(info.PKColumns) == 1 {
			nextID = row[pkCol.Name].Int
			if nextID >= s.rowIDCount[tableName] {
				s.rowIDCount[tableName] = nextID + 1
			}
		}
		if err := validatePrimaryKey(info, row); err != nil {
			return 0, nil, err
		}
		key := s.makeRowKey(tableName, info, row, nextID)
		if err := s.lockRow(tableName, info, row, key); err != nil {
			return 0, nil, err
		}
		if err := s.fireTriggers(tableName, "INSERT", "BEFORE", nil, row); err != nil {
			return 0, nil, err
		}
		if stmt.Replace {
			if err := s.deleteConflicts(tableName, info, row); err != nil {
				return 0, nil, err
			}
		}
		if err := s.checkConstraints(tableName, info, row, nil); err != nil {
			return 0, nil, err
		}

		data := encodeRow(row, info)
		if s.inTx {
			s.txPut(key, data)
		} else {
			if err := s.write(key, data); err != nil {
				return 0, nil, err
			}
		}

		if pkCol != nil && pkCol.AutoIncrement {
			if nextID > s.getAutoincMax(tableName) {
				s.setAutoincMax(tableName, nextID)
			}
			s.saveAutoincMax()
		}

		if err := s.updateIndexes(tableName, info, row, key, true); err != nil {
			return 0, nil, err
		}
		if err := s.fireTriggers(tableName, "INSERT", "AFTER", nil, row); err != nil {
			return 0, nil, err
		}
		s.statementChanges++
		lastID = nextID
		returningRows = append(returningRows, row)
	}

	var retRs *ResultSet
	if len(stmt.Returning) > 0 {
		var returnErr error
		retRs, returnErr = s.buildReturningResult(stmt.Returning, returningRows, info)
		if returnErr != nil {
			return 0, nil, returnErr
		}
	}
	return lastID, retRs, nil
}

func (s *Schema) buildReturningResult(cols []SelectColumn, rows []map[string]Value, info *TableInfo) (*ResultSet, error) {
	rs := &ResultSet{}
	var expressions []selectExprCol
	for i, sc := range cols {
		if sc.Star {
			for _, col := range info.Columns {
				rs.Columns = append(rs.Columns, col)
				expressions = append(expressions, selectExprCol{name: col.Name, expr: &ColumnRefExpr{Name: col.Name}})
			}
			continue
		}
		name := sc.Alias
		if name == "" {
			if ref, ok := sc.Expr.(*ColumnRefExpr); ok {
				name = ref.Name
			} else {
				name = fmt.Sprintf("expr%d", i)
			}
		}
		rs.Columns = append(rs.Columns, ColumnDef{Name: name})
		expressions = append(expressions, selectExprCol{name: name, expr: sc.Expr})
	}
	for _, row := range rows {
		projected := make(map[string]Value)
		for _, ec := range expressions {
			v, err := s.evalExpr(ec.expr, row)
			if err != nil {
				return nil, err
			}
			projected[ec.name] = v
		}
		rs.Rows = append(rs.Rows, projected)
	}
	return rs, nil
}

func (s *Schema) validateQuery(stmt *SelectStmt, info *TableInfo) error {
	columns := make(map[string]int)
	qualified := make(map[string]bool)
	needQualified := needsQualifiedRows(stmt)
	aliases := make(map[string]bool)
	add := func(defs []ColumnDef, alias string) {
		for _, col := range defs {
			columns[strings.ToLower(col.Name)]++
			if needQualified && alias != "" {
				qualified[strings.ToLower(alias+"."+col.Name)] = true
			}
		}
	}
	if info != nil {
		alias := stmt.FromAlias
		if alias == "" {
			alias = stmt.From
		}
		add(info.Columns, alias)
	}
	for _, jc := range stmt.Joins {
		var defs []ColumnDef
		if jc.Subquery != nil {
			rs, err := s.Select(jc.Subquery)
			if err != nil {
				return err
			}
			defs = rs.Columns
		} else if jt, ok := s.tables[jc.Table]; ok {
			defs = jt.Columns
		} else if vi, ok := s.views[jc.Table]; ok {
			defs = s.viewColumnDefs(vi)
		} else {
			return fmt.Errorf("table %q does not exist", jc.Table)
		}
		alias := jc.Alias
		if alias == "" {
			alias = jc.Table
		}
		add(defs, alias)
		for _, name := range jc.Using {
			columns[name]--
		}
		if jc.Type == "NATURAL" {
			for _, col := range defs {
				if columns[col.Name] > 1 {
					columns[col.Name] = 1
				}
			}
		}
	}
	for _, col := range stmt.Columns {
		if col.Alias != "" {
			aliases[strings.ToLower(col.Alias)] = true
		}
	}
	var validate func(Expression, bool) error
	validate = func(expr Expression, allowAlias bool) error {
		switch e := expr.(type) {
		case *ColumnRefExpr:
			if e.Name == "*" {
				return nil
			}
			name := strings.ToLower(e.Name)
			if allowAlias && aliases[name] {
				return nil
			}
			if _, ok := s.outerRow[name]; ok {
				return nil
			}
			if strings.Contains(name, ".") {
				if qualified[name] {
					return nil
				}
				return fmt.Errorf("no such column: %s", e.Name)
			}
			if columns[name] == 0 {
				return fmt.Errorf("no such column: %s", e.Name)
			}
			if columns[name] > 1 {
				return fmt.Errorf("ambiguous column: %s", e.Name)
			}
		case *BinaryExpr:
			if err := validate(e.Left, allowAlias); err != nil {
				return err
			}
			return validate(e.Right, allowAlias)
		case *UnaryExpr:
			return validate(e.Right, allowAlias)
		case *FuncCallExpr:
			for _, arg := range e.Args {
				if err := validate(arg, allowAlias); err != nil {
					return err
				}
			}
		case *InListExpr:
			if err := validate(e.Expr, allowAlias); err != nil {
				return err
			}
			for _, arg := range e.List {
				if err := validate(arg, allowAlias); err != nil {
					return err
				}
			}
		case *BetweenExpr:
			for _, arg := range []Expression{e.Expr, e.Low, e.High} {
				if err := validate(arg, allowAlias); err != nil {
					return err
				}
			}
		case *CaseExpr:
			if err := validate(e.Expr, allowAlias); err != nil {
				return err
			}
			for _, when := range e.Whens {
				if err := validate(when.Cond, allowAlias); err != nil {
					return err
				}
				if err := validate(when.Then, allowAlias); err != nil {
					return err
				}
			}
			return validate(e.ElseExpr, allowAlias)
		}
		return nil
	}
	for _, col := range stmt.Columns {
		if err := validate(col.Expr, false); err != nil {
			return err
		}
	}
	if err := validate(stmt.Where, false); err != nil {
		return err
	}
	if err := validate(stmt.Having, true); err != nil {
		return err
	}
	for _, order := range stmt.OrderBy {
		if err := validate(order.Expr, true); err != nil {
			return err
		}
	}
	for _, group := range stmt.GroupBy {
		if err := validate(group, true); err != nil {
			return err
		}
	}
	return nil
}

func (s *Schema) Select(stmt *SelectStmt) (*ResultSet, error) {
	if err := s.checkInterrupt(); err != nil {
		return nil, err
	}
	if stmt.Lock != nil {
		// Metadata waits use the normal timeout even with NOWAIT/SKIP LOCKED,
		// which apply only to row locks. Also protects tables from concurrent DDL.
		if err := s.RequestLock(LockRequest{Resource: "catalog"}); err != nil {
			return nil, err
		}
	}
	s.selectDepth++
	defer func() { s.selectDepth-- }()
	if s.selectDepth > 100 {
		return nil, fmt.Errorf("query nesting limit exceeded")
	}
	copyStmt := *stmt
	stmt = &copyStmt
	trackKeys := stmt.Lock != nil || stmt.trackRows
	needQualified := needsQualifiedRows(stmt)
	orderedIndex := false
	coveringProjection := false
	conditionSatisfied := false
	rs := &ResultSet{}
	tableName := strings.ToLower(stmt.From)

	if tableName == "sqlite_master" {
		return s.selectFromSqliteMaster(stmt)
	}
	if tableName == "sqlite_stat1" {
		return s.selectFromStat1(stmt)
	}

	info, ok := s.tables[tableName]
	var viewInfo *ViewInfo
	if !ok && tableName != "" {
		if vi, vok := s.views[tableName]; vok {
			subCols := s.viewColumnDefs(vi)
			info = &TableInfo{Name: tableName, Columns: subCols}
			viewSelect := *vi.Parsed
			viewSelect.trackRows = trackKeys
			stmt.FromExpr = &SubqueryExpr{Stmt: &viewSelect}
			stmt.From = ""
			viewInfo = vi
		} else if stmt.FromExpr == nil {
			return nil, fmt.Errorf("table %q does not exist", tableName)
		}
	}

	if stmt.FromExpr == nil {
		if err := s.validateQuery(stmt, info); err != nil {
			return nil, err
		}
	}

	exprCols := make([]selectExprCol, 0)
	if stmt.SelectAll {
		if info != nil {
			rs.Columns = append([]ColumnDef(nil), info.Columns...)
		}
		base := append([]ColumnDef(nil), rs.Columns...)
		for _, jc := range stmt.Joins {
			s.addJoinColumns(rs, jc, base)
		}
	} else {
		exprIdx := 0
		for _, sc := range stmt.Columns {
			if sc.Star {
				if info != nil {
					rs.Columns = append(rs.Columns, info.Columns...)
				}
				base := append([]ColumnDef(nil), rs.Columns...)
				for _, jc := range stmt.Joins {
					s.addJoinColumns(rs, jc, base)
				}
				continue
			}
			name := sc.Alias
			if name == "" {
				switch e := sc.Expr.(type) {
				case *ColumnRefExpr:
					name = e.Name
				case *FuncCallExpr:
					name = e.Name
				default:
					name = fmt.Sprintf("expr%d", exprIdx)
				}
			}
			exprIdx++
			rs.Columns = append(rs.Columns, ColumnDef{Name: name})
			exprCols = append(exprCols, selectExprCol{name: name, expr: sc.Expr})
		}
	}
	stmt.OrderBy = append([]OrderByClause(nil), stmt.OrderBy...)
	if err := validateGrouping(stmt, info, rs.Columns); err != nil {
		return nil, err
	}
	// Aliases are legal in ORDER BY, GROUP BY and HAVING. Expression evaluation
	// below validates references and preserves any error instead of hiding it.

	prefix := tableDataPrefix(tableName)
	rows := make([]map[string]Value, 0)
	var rowKeys [][]string

	if stmt.FromExpr != nil {
		if subExpr, ok := stmt.FromExpr.(*SubqueryExpr); ok {
			subRS, err := s.Select(subExpr.Stmt)
			if err != nil {
				return nil, err
			}
			if info == nil {
				info = &TableInfo{Columns: subRS.Columns}
			}
			if err := s.validateQuery(stmt, info); err != nil {
				return nil, err
			}

			subRows := subRS.Rows
			if viewInfo != nil && trackKeys {
				rowKeys = subRS.rowKeys
			}
			if stmt.SelectAll && len(rs.Columns) == 0 {
				rs.Columns = subRS.Columns
			}
			if viewInfo != nil {
				if len(viewInfo.Columns) > 0 {
					if len(viewInfo.Columns) != len(subRS.Columns) {
						return nil, fmt.Errorf("view column count mismatch")
					}
					for i, row := range subRows {
						mapped := make(map[string]Value)
						for j, col := range subRS.Columns {
							mapped[viewInfo.Columns[j]] = row[col.Name]
						}
						subRows[i] = mapped
					}
				}
			}
			for _, row := range subRows {
				qualifyRow(row, tableName)
				qualifyRow(row, stmt.FromAlias)
			}
			if len(stmt.Joins) > 0 {
				var joinErr error
				rows, rowKeys, joinErr = s.executeJoins(stmt, subRows, rowKeys)
				if joinErr != nil {
					return nil, joinErr
				}
			} else {
				rows = subRows
			}
			if stmt.Where != nil {
				var filterErr error
				rows, rowKeys, filterErr = s.filterRowsWithKeys(stmt.Where, rows, rowKeys)
				if filterErr != nil {
					return nil, filterErr
				}
			}
		}
	} else if info == nil && tableName == "" && len(stmt.Joins) == 0 && len(stmt.Columns) > 0 {
		row := make(map[string]Value)
		for _, ec := range exprCols {
			if containsAggregate(ec.expr) {
				continue
			}
			val, err := s.evalExpr(ec.expr, row)
			if err != nil {
				return nil, err
			}
			row[ec.name] = val
		}
		rows = append(rows, row)
	} else if info != nil && tableName != "" {
		var baseRows []map[string]Value
		var baseKeys [][]string
		usedIndex := false
		if plan := s.tuplePlan(stmt, info); plan != nil {
			var readErr error
			baseRows, baseKeys, orderedIndex, readErr = s.tupleRows(stmt, plan, trackKeys)
			if readErr != nil {
				return nil, readErr
			}
			usedIndex = true
			conditionSatisfied = true
		}
		if plan := s.coveringPlan(stmt, tableName, info); !usedIndex && plan != nil {
			var readErr error
			baseRows, readErr = s.coveringRows(stmt, plan)
			if readErr != nil {
				return nil, readErr
			}
			usedIndex = true
			conditionSatisfied = true
			orderedIndex = plan.ordered && !hasAggregates(stmt) && len(stmt.GroupBy) == 0
			coveringProjection = exactCoveringProjection(stmt, plan.info)
		}

		if stmt.Where != nil && !usedIndex {
			if rowKeys, ok := s.findMatchingRowsViaIndex(stmt.Where, tableName); ok {
				values, readErr := s.schemaMultiGet(rowKeys)
				if readErr != nil {
					return nil, readErr
				}
				for i, value := range values {
					if value.Found {
						baseRows = append(baseRows, decodeRow(value.Value, info))
						if trackKeys {
							baseKeys = append(baseKeys, []string{string(rowKeys[i])})
						}
					}
				}
				usedIndex = true
				_, conditionSatisfied = stmt.Where.(*InListExpr)
			}
		}

		if !usedIndex {
			var rangeErr error
			baseRows, usedIndex, rangeErr = s.indexRangeRows(stmt, tableName, info)
			orderedIndex = usedIndex
			conditionSatisfied = usedIndex
			if rangeErr != nil {
				return nil, rangeErr
			}
		}

		if !usedIndex {
			scanLimit := int64(-1)
			scanReverse := false
			if stmt.Lock == nil && stmt.Where == nil && len(stmt.Joins) == 0 && !s.inTx && !stmt.Distinct && len(stmt.GroupBy) == 0 &&
				!hasAggregates(stmt) && stmt.Limit >= 0 {
				if rev := s.isOrderByNatural(stmt, info); rev >= 0 {
					scanLimit = limitWindow(stmt.Limit, stmt.Offset)
					scanReverse = rev == 1
				}
			}
			var scanErr error
			if trackKeys {
				baseRows, scanErr = s.scanTableLimit(tableName, info, prefix, scanLimit, scanReverse, &baseKeys)
			} else {
				baseRows, scanErr = s.scanTableLimit(tableName, info, prefix, scanLimit, scanReverse)
			}
			if scanErr != nil {
				return nil, scanErr
			}
		}

		if needQualified {
			for _, row := range baseRows {
				qualifyRow(row, tableName)
				qualifyRow(row, stmt.FromAlias)
			}
		}
		if len(stmt.Joins) > 0 {
			var joinErr error
			rows, rowKeys, joinErr = s.executeJoins(stmt, baseRows, baseKeys)
			if joinErr != nil {
				return nil, joinErr
			}
		} else {
			rows = baseRows
			rowKeys = baseKeys
		}

		if stmt.Where != nil && !conditionSatisfied {
			var filterErr error
			rows, rowKeys, filterErr = s.filterRowsWithKeys(stmt.Where, rows, rowKeys)
			if filterErr != nil {
				return nil, filterErr
			}
		}
	} else if tableName != "" && info == nil {
		return nil, fmt.Errorf("table %q does not exist", tableName)
	} else {
		rows = append(rows, make(map[string]Value))
	}

	if tableName == "" && stmt.FromExpr == nil && stmt.Where != nil {
		var err error
		rows, err = s.filterRows(stmt.Where, rows)
		if err != nil {
			return nil, err
		}
	}

	// Aggregation and DISTINCT collapse source rows. Lock their matching input
	// before collapsing it, so every contributing row is protected. Simple
	// queries acquire locks after sorting and fill LIMIT from available rows.
	lockInputs := stmt.Lock != nil && (hasAggregates(stmt) || len(stmt.GroupBy) > 0 || stmt.Distinct)
	if lockInputs {
		var err error
		rows, rowKeys, err = s.lockSelectRows(stmt, rows, rowKeys, false)
		if err != nil {
			return nil, err
		}
	}

	if len(exprCols) > 0 && !coveringProjection {
		for i := range rows {
			original := make(map[string]Value)
			for k, v := range rows[i] {
				original[k] = v
			}
			for _, ec := range exprCols {
				if containsAggregate(ec.expr) {
					continue
				}
				val, err := s.evalExpr(ec.expr, original)
				if err != nil {
					return nil, err
				}
				rows[i][ec.name] = val
			}
		}

	}
	hasAgg := hasAggregates(stmt)
	hasGroupBy := len(stmt.GroupBy) > 0 || stmt.Having != nil

	if hasAgg || hasGroupBy {
		var err error
		rows, rowKeys, err = s.processGroups(stmt, rows, info, exprCols, rowKeys)
		if err != nil {
			return nil, err
		}
	}

	if stmt.Distinct {
		distinctCols := make([]string, 0, len(rs.Columns))
		for _, col := range rs.Columns {
			distinctCols = append(distinctCols, col.Name)
		}
		rows, rowKeys = s.distinctRowsColsWithKeys(rows, distinctCols, rowKeys)
	}

	for i := range stmt.OrderBy {
		order := &stmt.OrderBy[i]
		if lit, ok := order.Expr.(*LiteralExpr); ok && !lit.BoundParameter && lit.Value.Type == ColInteger && !lit.Value.Null {
			n := lit.Value.Int
			if n < 1 || n > int64(len(rs.Columns)) {
				return nil, fmt.Errorf("ORDER BY position out of range")
			}
			order.Expr = &ColumnRefExpr{Name: rs.Columns[n-1].Name}
		}
		for _, row := range rows {
			if _, err := s.evalExpr(order.Expr, row); err != nil {
				return nil, err
			}
		}
	}

	if len(stmt.OrderBy) > 0 && !orderedIndex {
		sort.Sort(selectRowsWithKeys{rows: rows, keys: rowKeys, less: func(a, b map[string]Value) bool {
			return s.compareRows(a, b, stmt.OrderBy)
		}})
	}
	if stmt.Lock != nil && !lockInputs {
		var err error
		rows, rowKeys, err = s.lockSelectRows(stmt, rows, rowKeys, true)
		if err != nil {
			return nil, err
		}
	}
	if stmt.Offset > 0 {
		if stmt.Offset >= int64(len(rows)) {
			rows = nil
			rowKeys = nil
		} else {
			rows = rows[int(stmt.Offset):]
			if rowKeys != nil {
				rowKeys = rowKeys[int(stmt.Offset):]
			}
		}
	}
	if stmt.Limit >= 0 && stmt.Limit < int64(len(rows)) {
		rows = rows[:int(stmt.Limit)]
		if rowKeys != nil {
			rowKeys = rowKeys[:int(stmt.Limit)]
		}
	}

	if !coveringProjection && !(stmt.SelectAll && !needQualified && info != nil && stmt.FromExpr == nil && len(stmt.Joins) == 0 && !hasAgg && !hasGroupBy) {
		for i, row := range rows {
			projected := make(map[string]Value)
			for _, col := range rs.Columns {
				v, ok := row[col.Name]
				if !ok {
					v = NewNullValue()
				}
				projected[col.Name] = v
			}
			rows[i] = projected
		}
	}
	if err := s.checkInterrupt(); err != nil {
		return nil, err
	}

	rs.Rows = rows
	rs.rowKeys = rowKeys
	return rs, nil
}

func isAggregate(fc *FuncCallExpr) bool {
	if (strings.EqualFold(fc.Name, "MIN") || strings.EqualFold(fc.Name, "MAX")) && len(fc.Args) > 1 {
		return false
	}
	switch strings.ToUpper(fc.Name) {
	case "COUNT", "SUM", "AVG", "MIN", "MAX", "TOTAL", "GROUP_CONCAT":
		return true
	}
	return false
}
func containsAggregate(expr Expression) bool {
	found := false
	walkExpression(expr, func(e Expression) (bool, error) {
		if f, ok := e.(*FuncCallExpr); ok && isAggregate(f) {
			found = true
			return true, nil
		}
		return false, nil
	})
	return found
}
func hasAggregates(stmt *SelectStmt) bool {
	for _, col := range stmt.Columns {
		if containsAggregate(col.Expr) {
			return true
		}
	}
	if containsAggregate(stmt.Having) {
		return true
	}
	for _, o := range stmt.OrderBy {
		if containsAggregate(o.Expr) {
			return true
		}
	}
	return false
}

func (s *Schema) processGroups(stmt *SelectStmt, rows []map[string]Value, info *TableInfo, exprCols []selectExprCol, keys [][]string) ([]map[string]Value, [][]string, error) {
	groups := make(map[string][]map[string]Value)
	groupKeys := make(map[string][]string)
	var order []string
	if len(stmt.GroupBy) == 0 {
		groups[""] = rows
		for _, rowKeys := range keys {
			groupKeys[""] = append(groupKeys[""], rowKeys...)
		}
		order = append(order, "")
	} else {
		for i, row := range rows {
			var key []byte
			for _, e := range stmt.GroupBy {
				v, err := s.evalExpr(e, row)
				if err != nil {
					return nil, nil, err
				}
				key = append(key, valueIdentity(v)...)
			}
			k := string(key)
			if _, ok := groups[k]; !ok {
				order = append(order, k)
			}
			groups[k] = append(groups[k], row)
			if keys != nil {
				groupKeys[k] = append(groupKeys[k], keys[i]...)
			}
		}
	}
	var result []map[string]Value
	var resultKeys [][]string
	for _, key := range order {
		group := groups[key]
		base := make(map[string]Value)
		if len(group) > 0 {
			for k, v := range group[0] {
				base[k] = v
			}
		}
		for _, ec := range exprCols {
			expr, err := s.replaceAggregates(ec.expr, group)
			if err != nil {
				return nil, nil, err
			}
			v, err := s.evalExpr(expr, base)
			if err != nil {
				return nil, nil, err
			}
			base[ec.name] = v
		}
		for i, order := range stmt.OrderBy {
			if containsAggregate(order.Expr) {
				expr, err := s.replaceAggregates(order.Expr, group)
				if err != nil {
					return nil, nil, err
				}
				value, err := s.evalExpr(expr, base)
				if err != nil {
					return nil, nil, err
				}
				base[fmt.Sprintf("\x00order%d", i)] = value
			}
		}
		if stmt.Having != nil {
			expr, err := s.replaceAggregates(stmt.Having, group)
			if err != nil {
				return nil, nil, err
			}
			match, err := s.evalCondition(expr, base)
			if err != nil {
				return nil, nil, err
			}
			if !match {
				continue
			}
		}
		result = append(result, base)
		if keys != nil {
			resultKeys = append(resultKeys, groupKeys[key])
		}
	}
	for i := range stmt.OrderBy {
		if containsAggregate(stmt.OrderBy[i].Expr) {
			stmt.OrderBy[i].Expr = &ColumnRefExpr{Name: fmt.Sprintf("\x00order%d", i)}
		}
	}
	return result, resultKeys, nil
}
func (s *Schema) replaceAggregates(expr Expression, rows []map[string]Value) (Expression, error) {
	return mapExpression(expr, func(e Expression) (Expression, bool, error) {
		if f, ok := e.(*FuncCallExpr); ok && isAggregate(f) {
			value, err := s.evalAggregate(f, rows)
			return &LiteralExpr{Value: value}, true, err
		}
		return e, false, nil
	})
}
func (s *Schema) evalAggregate(fc *FuncCallExpr, rows []map[string]Value) (Value, error) {
	count := len(fc.Args)
	if count > 1 && (!strings.EqualFold(fc.Name, "GROUP_CONCAT") || count > 2) {
		return NewNullValue(), fmt.Errorf("wrong number of arguments to %s", fc.Name)
	}

	name := strings.ToUpper(fc.Name)
	if len(fc.Args) == 0 {
		return NewNullValue(), fmt.Errorf("wrong number of arguments to %s", name)
	}
	if name == "COUNT" {
		if ref, ok := fc.Args[0].(*ColumnRefExpr); ok && ref.Name == "*" {
			return NewIntValue(int64(len(rows))), nil
		}
	}
	var values []Value
	var seen map[string]bool
	if fc.Distinct {
		seen = make(map[string]bool)
	}
	for _, row := range rows {
		v, err := s.evalExpr(fc.Args[0], row)
		if err != nil {
			return NewNullValue(), err
		}
		if !v.Null {
			if fc.Distinct {
				key := string(valueIdentity(v))
				if seen[key] {
					continue
				}
				seen[key] = true
			}
			values = append(values, v)
		}
	}
	if name == "COUNT" {
		return NewIntValue(int64(len(values))), nil
	}
	if len(values) == 0 {
		if name == "TOTAL" {
			return NewRealValue(0), nil
		}
		return NewNullValue(), nil
	}
	switch name {
	case "MIN", "MAX":
		best := values[0]
		for _, v := range values[1:] {
			cmp := ValueCompare(v, best)
			if name == "MIN" && cmp < 0 || name == "MAX" && cmp > 0 {
				best = v
			}
		}
		return best, nil
	case "GROUP_CONCAT":
		separator := ","
		if len(fc.Args) > 1 && len(rows) > 0 {
			v, err := s.evalExpr(fc.Args[1], rows[0])
			if err != nil {
				return NewNullValue(), err
			}
			separator = ""
			if !v.Null {
				separator = v.String()
			}
		}
		parts := make([]string, len(values))
		for i, v := range values {
			parts[i] = v.String()
		}
		return NewTextValue(strings.Join(parts, separator)), nil
	case "SUM", "TOTAL", "AVG":
		allInt := true
		var isum int64
		var total float64
		for _, v := range values {
			if v.Type != ColInteger && v.Type != ColReal {
				return NewNullValue(), fmt.Errorf("%s requires numeric arguments", name)
			}
			total += toFloat(v)
			if v.Type != ColInteger {
				allInt = false
			}
		}
		if name == "SUM" && allInt {
			for _, v := range values {
				next := isum + v.Int
				if v.Int > 0 && next < isum || v.Int < 0 && next > isum {
					return NewNullValue(), fmt.Errorf("integer overflow")
				}
				isum = next
			}
			return NewIntValue(isum), nil
		}
		if name == "AVG" {
			total /= float64(len(values))
		}
		if math.IsNaN(total) || math.IsInf(total, 0) {
			return NewNullValue(), fmt.Errorf("numeric value out of range")
		}
		return NewRealValue(total), nil
	}
	return NewNullValue(), fmt.Errorf("unknown aggregate: %s", name)
}

func (s *Schema) distinctRowsCols(rows []map[string]Value, cols []string) []map[string]Value {
	result, _ := s.distinctRowsColsWithKeys(rows, cols, nil)
	return result
}

func (s *Schema) distinctRowsColsWithKeys(rows []map[string]Value, cols []string, keys [][]string) ([]map[string]Value, [][]string) {
	seen := make(map[string]int)
	var out []map[string]Value
	var outKeys [][]string
	for i, row := range rows {
		var key []byte
		for _, col := range cols {
			key = append(key, valueIdentity(row[col])...)
		}
		k := string(key)
		position, exists := seen[k]
		if !exists {
			position = len(out)
			seen[k] = position
			out = append(out, row)
			if keys != nil {
				outKeys = append(outKeys, nil)
			}
		}
		if keys != nil {
			outKeys[position] = append(outKeys[position], keys[i]...)
		}
	}
	return out, outKeys
}
func (s *Schema) distinctRows(rows []map[string]Value) []map[string]Value {
	seen := make(map[string]bool)
	var out []map[string]Value
	for _, row := range rows {
		k := rowKey(row)
		if !seen[k] {
			seen[k] = true
			out = append(out, row)
		}
	}
	return out
}
func rowKey(row map[string]Value) string {
	var names []string
	for k := range row {
		names = append(names, k)
	}
	sort.Strings(names)
	var key []byte
	for _, name := range names {
		key = append(key, encodeValue(NewTextValue(name))...)
		key = append(key, valueIdentity(row[name])...)
	}
	return string(key)
}

func (s *Schema) selectFromSqliteMaster(stmt *SelectStmt) (*ResultSet, error) {
	masterRS, err := s.QuerySqliteMaster(nil)
	if err != nil {
		return nil, err
	}

	rs := &ResultSet{}
	masterCols := map[string]ColumnDef{
		"type":     {Name: "type", ColType: ColText},
		"name":     {Name: "name", ColType: ColText},
		"tbl_name": {Name: "tbl_name", ColType: ColText},
		"rootpage": {Name: "rootpage", ColType: ColInteger},
		"sql":      {Name: "sql", ColType: ColText},
	}

	if stmt.SelectAll {
		for _, col := range []string{"type", "name", "tbl_name", "rootpage", "sql"} {
			rs.Columns = append(rs.Columns, masterCols[col])
		}
	} else {
		for _, sc := range stmt.Columns {
			if cr, ok := sc.Expr.(*ColumnRefExpr); ok {
				if def, found := masterCols[cr.Name]; found {
					rs.Columns = append(rs.Columns, def)
				} else {
					rs.Columns = append(rs.Columns, ColumnDef{Name: cr.Name, ColType: ColText})
				}
			} else if sc.Star {
				for _, col := range []string{"type", "name", "tbl_name", "rootpage", "sql"} {
					rs.Columns = append(rs.Columns, masterCols[col])
				}
			}
		}
	}

	rows := masterRS.Rows
	if stmt.Where != nil {
		var filterErr error
		rows, filterErr = s.filterRows(stmt.Where, rows)
		if filterErr != nil {
			return nil, filterErr
		}
	}

	if len(stmt.OrderBy) > 0 {
		sort.Slice(rows, func(i, j int) bool {
			return s.compareRows(rows[i], rows[j], stmt.OrderBy)
		})
	}

	if stmt.Offset > 0 {
		if stmt.Offset >= int64(len(rows)) {
			rows = nil
		} else {
			rows = rows[int(stmt.Offset):]
		}
	}
	if stmt.Limit >= 0 && stmt.Limit < int64(len(rows)) {
		rows = rows[:int(stmt.Limit)]
	}

	rs.Rows = rows
	return rs, nil
}

func (s *Schema) selectFromStat1(stmt *SelectStmt) (*ResultSet, error) {
	rs := &ResultSet{
		Columns: []ColumnDef{
			{Name: "tbl", ColType: ColText},
			{Name: "idx", ColType: ColText},
			{Name: "stat", ColType: ColText},
		},
	}

	for name, count := range s.stats {
		rs.Rows = append(rs.Rows, map[string]Value{
			"tbl":  NewTextValue(name),
			"idx":  NewTextValue(name),
			"stat": NewTextValue(fmt.Sprintf("%d", count)),
		})
	}

	rows := rs.Rows
	if stmt.Where != nil {
		var filterErr error
		rows, filterErr = s.filterRows(stmt.Where, rows)
		if filterErr != nil {
			return nil, filterErr
		}
	}
	rs.Rows = rows
	return rs, nil
}

func (s *Schema) isOrderByNatural(stmt *SelectStmt, info *TableInfo) int {
	if len(stmt.OrderBy) == 0 {
		return 0
	}
	return -1
}

func (s *Schema) scanTable(tableName string, info *TableInfo, filter Expression, prefix string) ([]map[string]Value, error) {
	return s.scanTableLimit(tableName, info, prefix, -1, false)
}

func (s *Schema) scanTableLimit(tableName string, info *TableInfo, prefix string, limit int64, reverse bool, keySets ...*[][]string) ([]map[string]Value, error) {
	if limit == 0 {
		return nil, nil
	}
	rowMap := make(map[string]map[string]Value)
	var keys []string
	var iter storage.Iterator
	if s.inTx {
		iter = s.txNewIterator()
	} else {
		iter = s.newIterator()
	}
	defer iter.Close()

	if reverse {
		iter.Seek([]byte(prefix + "\xff"))
		revCount := 0
		for {
			iter.Prev()
			if !iter.Valid() {
				break
			}
			key := string(iter.Key().UserKey)
			if !strings.HasPrefix(key, prefix) {
				break
			}
			row := decodeRow(iter.Value(), info)
			rowMap[key] = row
			keys = append(keys, key)
			revCount++
			if revCount%100 == 0 {
				if err := s.checkInterrupt(); err != nil {
					return nil, err
				}
			}
			if limit >= 0 && int64(len(keys)) >= limit {
				break
			}
		}
		for i, j := 0, len(keys)-1; i < j; i, j = i+1, j-1 {
			keys[i], keys[j] = keys[j], keys[i]
		}
	} else {
		iter.Seek([]byte(prefix))
		scanCount := 0
		for iter.Valid() {
			key := string(iter.Key().UserKey)
			if !strings.HasPrefix(key, prefix) {
				if key > prefix {
					break
				}
				iter.Next()
				continue
			}
			row := decodeRow(iter.Value(), info)
			rowMap[key] = row
			keys = append(keys, key)
			iter.Next()
			scanCount++
			if scanCount%100 == 0 {
				if err := s.checkInterrupt(); err != nil {
					return nil, err
				}
			}
			if limit >= 0 && int64(len(keys)) >= limit {
				break
			}
		}
	}

	var rows []map[string]Value
	for _, k := range keys {
		rows = append(rows, rowMap[k])
		for _, keySet := range keySets {
			*keySet = append(*keySet, []string{k})
		}
	}
	return rows, storage.IteratorError(iter)
}

func (s *Schema) filterRows(where Expression, rows []map[string]Value) ([]map[string]Value, error) {
	result, _, err := s.filterRowsWithKeys(where, rows, nil)
	return result, err
}

func (s *Schema) filterRowsWithKeys(where Expression, rows []map[string]Value, keys [][]string) ([]map[string]Value, [][]string, error) {
	var result []map[string]Value
	var resultKeys [][]string
	for i, row := range rows {
		if err := s.checkInterrupt(); err != nil {
			return nil, nil, err
		}
		match, err := s.evalCondition(where, row)
		if err != nil {
			return nil, nil, err
		}
		if match {
			result = append(result, row)
			if keys != nil {
				resultKeys = append(resultKeys, keys[i])
			}
		}
	}
	return result, resultKeys, nil
}
func qualifyRow(row map[string]Value, alias string) {
	if alias == "" {
		return
	}
	var names []string
	for name := range row {
		if !strings.Contains(name, ".") {
			names = append(names, name)
		}
	}
	for _, name := range names {
		row[alias+"."+name] = row[name]
	}
}

func (s *Schema) addJoinColumns(rs *ResultSet, jc JoinClause, baseColumns []ColumnDef) {
	alias := jc.Alias
	if alias == "" && jc.Subquery != nil {
		return
	}
	if jc.Subquery != nil {
		for _, col := range baseColumns {
			name := col.Name
			exists := false
			for _, ec := range rs.Columns {
				if ec.Name == name {
					exists = true
					break
				}
			}
			if exists && alias != "" {
				name = alias + "." + name
			}
			rs.Columns = append(rs.Columns, ColumnDef{Name: name, ColType: col.ColType})
		}
		return
	}
	info, ok := s.tables[strings.ToLower(jc.Table)]
	if !ok {
		return
	}
	baseExist := make(map[string]bool)
	for _, col := range baseColumns {
		baseExist[col.Name] = true
	}
	for _, col := range info.Columns {
		name := col.Name
		if baseExist[name] {
			name = alias + "." + name
		}
		rs.Columns = append(rs.Columns, ColumnDef{Name: name, ColType: col.ColType})
		baseExist[name] = true
	}
}
func (s *Schema) executeJoins(stmt *SelectStmt, baseRows []map[string]Value, baseKeys [][]string) ([]map[string]Value, [][]string, error) {
	result := baseRows
	resultKeys := baseKeys
	trackKeys := stmt.Lock != nil || stmt.trackRows
	for _, jc := range stmt.Joins {
		joinRows, columns, joinKeys, err := s.getJoinRows(jc, trackKeys)
		if err != nil {
			return nil, nil, err
		}
		alias := jc.Alias
		if alias == "" {
			alias = jc.Table
		}
		var next []map[string]Value
		var nextKeys [][]string
		for i, left := range result {
			if err := s.checkInterrupt(); err != nil {
				return nil, nil, err
			}
			matched := false
			for j, right := range joinRows {
				combined := s.combineRowsWithAlias(left, right, alias)
				match := true
				using := jc.Using
				if jc.Type == "NATURAL" {
					using = nil
					for _, col := range columns {
						if _, ok := left[col.Name]; ok {
							using = append(using, col.Name)
						}
					}
				}
				for _, name := range using {
					a, b := left[name], right[name]
					if a.Null || b.Null || ValueCompare(a, b) != 0 {
						match = false
						break
					}
				}
				if jc.On != nil && match {
					match, err = s.evalCondition(jc.On, combined)
					if err != nil {
						return nil, nil, err
					}
				}
				if match {
					matched = true
					next = append(next, combined)
					if trackKeys {
						var keys []string
						if resultKeys != nil {
							keys = append(keys, resultKeys[i]...)
						}
						if joinKeys != nil {
							keys = append(keys, joinKeys[j]...)
						}
						nextKeys = append(nextKeys, keys)
					}
				}
			}
			if !matched && jc.Type == "LEFT" {
				null := make(map[string]Value)
				for _, col := range columns {
					null[col.Name] = NewNullValue()
				}
				next = append(next, s.combineRowsWithAlias(left, null, alias))
				if trackKeys {
					var keys []string
					if resultKeys != nil {
						keys = resultKeys[i]
					}
					nextKeys = append(nextKeys, keys)
				}
			}
		}
		if jc.Type == "RIGHT" {
			return nil, nil, fmt.Errorf("RIGHT JOIN is not supported")
		}
		result = next
		resultKeys = nextKeys
	}
	return result, resultKeys, nil
}
func (s *Schema) getJoinRows(jc JoinClause, locking bool) ([]map[string]Value, []ColumnDef, [][]string, error) {
	if jc.Subquery != nil {
		rs, err := s.Select(jc.Subquery)
		if err != nil {
			return nil, nil, nil, err
		}
		return rs.Rows, rs.Columns, nil, nil
	}
	name := strings.ToLower(jc.Table)
	if info, ok := s.tables[name]; ok {
		var keys [][]string
		var rows []map[string]Value
		var err error
		if locking {
			rows, err = s.scanTableLimit(name, info, tableDataPrefix(name), -1, false, &keys)
		} else {
			rows, err = s.scanTable(name, info, nil, tableDataPrefix(name))
		}
		return rows, info.Columns, keys, err
	}
	if _, ok := s.views[name]; ok {
		rs, err := s.Select(&SelectStmt{From: name, SelectAll: true, Limit: -1, trackRows: locking})
		if err != nil {
			return nil, nil, nil, err
		}
		return rs.Rows, rs.Columns, rs.rowKeys, nil
	}
	return nil, nil, nil, fmt.Errorf("table %q does not exist", name)
}

func (s *Schema) combineRowsWithAlias(a, b map[string]Value, alias string) map[string]Value {
	result := make(map[string]Value, len(a)+len(b)*2)
	for k, v := range a {
		result[k] = v
	}
	for k, v := range b {
		if alias != "" && !strings.Contains(k, ".") {
			result[alias+"."+k] = v
		}
		if _, exists := result[k]; !exists {
			result[k] = v
		}
	}
	return result
}

func (s *Schema) combineRows(a, b map[string]Value) map[string]Value {
	result := make(map[string]Value, len(a)+len(b))
	for k, v := range a {
		result[k] = v
	}
	for k, v := range b {
		if _, exists := result[k]; exists {
			continue
		}
		result[k] = v
	}
	return result
}

func (s *Schema) makeNullRow(rows []map[string]Value) map[string]Value {
	if len(rows) == 0 {
		return map[string]Value{}
	}
	result := make(map[string]Value)
	for k := range rows[0] {
		result[k] = NewNullValue()
	}
	return result
}

func (s *Schema) evalSubquery(sub *SelectStmt, outer map[string]Value) (*ResultSet, error) {
	prev := s.outerRow
	s.outerRow = outer
	defer func() { s.outerRow = prev }()
	return s.Select(sub)
}
func (s *Schema) evalSubqueryValue(sub *SelectStmt, outer map[string]Value) (Value, error) {
	rs, err := s.evalSubquery(sub, outer)
	if err != nil {
		return NewNullValue(), err
	}
	if len(rs.Columns) != 1 {
		return NewNullValue(), fmt.Errorf("scalar subquery must return one column")
	}
	if len(rs.Rows) == 0 {
		return NewNullValue(), nil
	}
	if len(rs.Rows) != 1 {
		return NewNullValue(), fmt.Errorf("scalar subquery returned more than one row")
	}
	return rs.Rows[0][rs.Columns[0].Name], nil
}

func (s *Schema) compareRows(a, b map[string]Value, orders []OrderByClause) bool {
	for _, o := range orders {
		va, err := s.evalExpr(o.Expr, a)
		if err != nil {
			va = NewNullValue()
		}
		vb, err := s.evalExpr(o.Expr, b)
		if err != nil {
			vb = NewNullValue()
		}

		if va.Null || vb.Null {
			if va.Null && vb.Null {
				continue
			}
			if o.NullsFirst {
				return va.Null
			}
			if o.NullsLast {
				return !va.Null
			}
			if o.Desc {
				return !va.Null
			}
			return va.Null
		}

		cmp := ValueCompare(va, vb)
		if cmp != 0 {
			if o.Desc {
				return cmp > 0
			}
			return cmp < 0
		}
	}
	return false
}

func (s *Schema) matchingDMLRows(tableName string, info *TableInfo, where Expression, limit, offset int64) ([]storage.KeyValue, error) {
	if limit == 0 {
		return nil, nil
	}
	if key, usable := s.primaryKeyLookup(where, tableName, info); usable {
		if err := s.checkInterrupt(); err != nil {
			return nil, err
		}
		value, found, err := s.schemaGet(key)
		if err != nil || !found {
			return nil, err
		}
		match, err := s.evalCondition(where, decodeRow(value, info))
		if err != nil || !match || offset > 0 {
			return nil, err
		}
		return []storage.KeyValue{{Key: storage.InternalKey{UserKey: key}, Value: value}}, nil
	}
	plan := buildTuplePlan(&SelectStmt{From: tableName, Where: where, Limit: -1}, info, nil, tupleConstraints(where, tableName, "", info))
	var intervals []coveringInterval
	residual := where
	if plan != nil {
		intervals = plan.intervals
		residual = plan.residual
	} else {
		prefix := []byte(tableDataPrefix(tableName))
		intervals = []coveringInterval{{prefix, prefixSuccessor(prefix)}}
	}
	var rows []storage.KeyValue
	for _, interval := range intervals {
		iter := s.newRangeIterator(interval.lower, interval.upper, false)
		err := func() error {
			defer iter.Close()
			for iter.Valid() {
				if err := s.checkInterrupt(); err != nil {
					return err
				}
				match := true
				if residual != nil {
					var err error
					match, err = s.evalCondition(residual, decodeRow(iter.Value(), info))
					if err != nil {
						return err
					}
				}
				if match {
					if offset > 0 {
						offset--
					} else {
						key := iter.Key()
						key.UserKey = copyBytes(key.UserKey)
						rows = append(rows, storage.KeyValue{Key: key, Value: copyBytes(iter.Value())})
						if limit >= 0 && int64(len(rows)) >= limit {
							break
						}
					}
				}
				iter.Next()
			}
			return storage.IteratorError(iter)
		}()
		if err != nil {
			return nil, err
		}
		if limit >= 0 && int64(len(rows)) >= limit {
			break
		}
	}
	return rows, nil
}
func (s *Schema) Delete(stmt *DeleteStmt) (int64, *ResultSet, error) {
	table := strings.ToLower(stmt.From)
	info, ok := s.tables[table]
	if !ok {
		if _, view := s.views[table]; view {
			return s.modifyView(table, "DELETE", nil, stmt.Where, stmt.Limit, stmt.Offset, stmt.Returning)
		}
	}

	if !ok {
		return 0, nil, fmt.Errorf("table %q does not exist", table)
	}
	if err := s.validateDMLExpressions(info, stmt.Where, nil, stmt.Returning); err != nil {
		return 0, nil, err
	}
	entries, err := s.matchingDMLRows(table, info, stmt.Where, stmt.Limit, stmt.Offset)
	if err != nil {
		return 0, nil, err
	}
	var returning []map[string]Value
	for _, entry := range entries {
		row := decodeRow(entry.Value, info)
		if err := s.lockRow(table, info, row, entry.Key.UserKey); err != nil {
			return 0, nil, err
		}
		if err := s.fireTriggers(table, "DELETE", "BEFORE", row, nil); err != nil {
			return 0, nil, err
		}
		if err := s.updateIndexes(table, info, row, entry.Key.UserKey, false); err != nil {
			return 0, nil, err
		}
		if err := s.remove(entry.Key.UserKey); err != nil {
			return 0, nil, err
		}
		if err := s.fireTriggers(table, "DELETE", "AFTER", row, nil); err != nil {
			return 0, nil, err
		}
		returning = append(returning, row)
	}
	var rs *ResultSet
	if len(stmt.Returning) > 0 {
		var returnErr error
		rs, returnErr = s.buildReturningResult(stmt.Returning, returning, info)
		if returnErr != nil {
			return 0, nil, returnErr
		}
	}
	return int64(len(entries)), rs, nil
}
func (s *Schema) Update(stmt *UpdateStmt) (int64, *ResultSet, error) {
	if s.inTx && s.lockHandler != nil {
		if plan := s.PreparePointUpdate(stmt); plan != nil {
			ctx := s.ctx
			if ctx == nil {
				ctx = context.Background()
			}
			changes, err := plan.Run(ctx)
			s.statementChanges = changes
			return changes, nil, err
		}
	}
	table := strings.ToLower(stmt.Table)
	info, ok := s.tables[table]
	if !ok {
		if _, view := s.views[table]; view {
			return s.modifyView(table, "UPDATE", stmt.Set, stmt.Where, stmt.Limit, stmt.Offset, stmt.Returning)
		}
	}

	if !ok {
		return 0, nil, fmt.Errorf("table %q does not exist", table)
	}
	for _, set := range stmt.Set {
		found := false
		for _, col := range info.Columns {
			if col.Name == strings.ToLower(set.Column) {
				found = true
				break
			}
		}
		if !found {
			return 0, nil, fmt.Errorf("no such column: %s", set.Column)
		}
	}
	if err := s.validateDMLExpressions(info, stmt.Where, stmt.Set, stmt.Returning); err != nil {
		return 0, nil, err
	}
	entries, err := s.matchingDMLRows(table, info, stmt.Where, stmt.Limit, stmt.Offset)
	if err != nil {
		return 0, nil, err
	}
	var returning []map[string]Value
	for _, entry := range entries {
		original := decodeRow(entry.Value, info)
		if err := s.lockRow(table, info, original, entry.Key.UserKey); err != nil {
			return 0, nil, err
		}
		row := make(map[string]Value)
		for k, v := range original {
			row[k] = v
		}
		for _, set := range stmt.Set {
			v, err := s.evalExpr(set.Value, original)
			if err != nil {
				return 0, nil, err
			}
			for _, col := range info.Columns {
				if col.Name == strings.ToLower(set.Column) {
					v = applyAffinity(v, col.ColType)
					break
				}
			}
			row[strings.ToLower(set.Column)] = v
		}
		beforeTriggers := len(s.txWrites)
		if err := s.fireTriggers(table, "UPDATE", "BEFORE", original, row); err != nil {
			return 0, nil, err
		}

		key := entry.Key.UserKey
		if len(info.PKColumns) > 0 {
			if err := validatePrimaryKey(info, row); err != nil {
				return 0, nil, err
			}
			key = info.encodePK(table, row)
		}
		if err := s.lockRow(table, info, row, key); err != nil {
			return 0, nil, err
		}
		indexedRow := original
		if len(s.txWrites) != beforeTriggers {
			// BEFORE triggers may have changed or removed the indexed row. Compare
			// against its current state before skipping any index mutations.
			data, found, err := s.schemaGet(entry.Key.UserKey)
			if err != nil {
				return 0, nil, err
			}
			indexedRow = nil
			if found {
				indexedRow = decodeRow(data, info)
			}
		}
		if err := s.checkConstraintsOriginal(table, info, row, entry.Key.UserKey, indexedRow); err != nil {
			return 0, nil, err
		}
		if err := s.updateChangedIndexes(table, indexedRow, row, entry.Key.UserKey, key); err != nil {
			return 0, nil, err
		}
		if !bytes.Equal(key, entry.Key.UserKey) {
			if err := s.remove(entry.Key.UserKey); err != nil {
				return 0, nil, err
			}
		}
		if err := s.write(key, encodeRow(row, info)); err != nil {
			return 0, nil, err
		}
		if len(info.PKColumns) == 1 && !bytes.Equal(key, entry.Key.UserKey) {
			v := row[info.PKColumns[0]]
			if v.Type == ColInteger && v.Int >= s.rowIDCount[table] {
				s.rowIDCount[table] = v.Int + 1
			}
		}
		if err := s.fireTriggers(table, "UPDATE", "AFTER", original, row); err != nil {
			return 0, nil, err
		}

		returning = append(returning, row)
	}
	var rs *ResultSet
	if len(stmt.Returning) > 0 {
		var returnErr error
		rs, returnErr = s.buildReturningResult(stmt.Returning, returning, info)
		if returnErr != nil {
			return 0, nil, returnErr
		}
	}
	return int64(len(entries)), rs, nil
}
func (s *Schema) modifyView(table, event string, sets []UpdateSet, where Expression, limit, offset int64, returning []SelectColumn) (int64, *ResultSet, error) {
	found := false
	for _, tr := range s.triggers {
		if tr.View == table && tr.InsteadOf && tr.Event == event {
			found = true
		}
	}
	if !found {
		return 0, nil, fmt.Errorf("cannot modify view %q", table)
	}
	rs, err := s.Select(&SelectStmt{From: table, SelectAll: true, Where: where, Limit: limit, Offset: offset})
	if err != nil {
		return 0, nil, err
	}
	info := &TableInfo{Name: table, Columns: rs.Columns}
	for _, set := range sets {
		valid := false
		for _, col := range info.Columns {
			if col.Name == set.Column {
				valid = true
			}
		}
		if !valid {
			return 0, nil, fmt.Errorf("no such column: %s", set.Column)
		}
	}
	var returned []map[string]Value
	for _, old := range rs.Rows {
		var new map[string]Value
		if event == "UPDATE" {
			new = make(map[string]Value)
			for k, v := range old {
				new[k] = v
			}
			for _, set := range sets {
				v, err := s.evalExpr(set.Value, old)
				if err != nil {
					return 0, nil, err
				}
				new[set.Column] = v
			}
		}
		if err := s.fireTriggers(table, event, "INSTEAD OF", old, new); err != nil {
			return 0, nil, err
		}
		if event == "DELETE" {
			returned = append(returned, old)
		} else {
			returned = append(returned, new)
		}
	}
	var result *ResultSet
	if len(returning) > 0 {
		var returnErr error
		result, returnErr = s.buildReturningResult(returning, returned, info)
		if returnErr != nil {
			return 0, nil, returnErr
		}
	}
	return 0, result, nil
}

func (s *Schema) deleteConflicts(table string, info *TableInfo, row map[string]Value) error {
	groups := s.uniqueGroups(table, info)
	entries, err := s.matchingDMLRows(table, info, nil, -1, 0)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		old := decodeRow(entry.Value, info)
		conflict := false
		for _, group := range groups {
			match := true
			for _, name := range group {
				a, b := old[name], row[name]
				if a.Null || b.Null || ValueCompare(a, b) != 0 {
					match = false
					break
				}
			}
			if match {
				conflict = true
				break
			}
		}
		if conflict {
			if err := s.lockRow(table, info, old, entry.Key.UserKey); err != nil {
				return err
			}
			if err := s.updateIndexes(table, info, old, entry.Key.UserKey, false); err != nil {
				return err
			}
			if err := s.remove(entry.Key.UserKey); err != nil {
				return err
			}
		}
	}
	return nil
}

func truthValue(v Value) bool { return !v.Null && toFloat(v) != 0 }
func (s *Schema) evalCondition(expr Expression, row map[string]Value) (bool, error) {
	v, err := s.evalExpr(expr, row)
	return truthValue(v), err
}

func (s *Schema) evalExpr(expr Expression, row map[string]Value) (Value, error) {
	switch e := expr.(type) {
	case *LiteralExpr:
		if e.Value.Type == ColBlob && !e.Value.Null {
			v := e.Value
			v.Blob = copyBytes(v.Blob)
			return v, nil
		}
		return e.Value, nil
	case *ColumnRefExpr:
		if e.Name == "*" {
			return NewIntValue(1), nil
		}
		if v, ok := row[e.Name]; ok {
			return v, nil
		}
		for name, value := range row {
			if strings.EqualFold(name, e.Name) {
				return value, nil
			}
		}

		if s.outerRow != nil {
			if v, ok := s.outerRow[e.Name]; ok {
				return v, nil
			}
		}
		return NewNullValue(), fmt.Errorf("no such column: %s", e.Name)
	case *BinaryExpr:
		return s.evalBinary(e, row)
	case *UnaryExpr:
		right, err := s.evalExpr(e.Right, row)
		if err != nil {
			return NewNullValue(), err
		}
		if right.Null {
			return right, nil
		}
		switch e.Op {
		case "NOT":
			if !truthValue(right) {
				return NewIntValue(1), nil
			}
			return NewIntValue(0), nil
		case "+":
			if right.Type != ColInteger && right.Type != ColReal {
				return NewNullValue(), fmt.Errorf("unary sign requires a numeric operand")
			}
			return right, nil
		case "-":
			if right.Type != ColInteger && right.Type != ColReal {
				return NewNullValue(), fmt.Errorf("unary sign requires a numeric operand")
			}

			if right.Type == ColInteger {
				if right.Int == math.MinInt64 {
					return NewNullValue(), fmt.Errorf("integer overflow")
				}
				return NewIntValue(-right.Int), nil
			}
			return NewRealValue(-toFloat(right)), nil
		case "~":
			return NewIntValue(^int64(toFloat(right))), nil
		}

	case *FuncCallExpr:
		return s.evalFunc(e, row)
	case *InListExpr:
		return s.evalInList(e, row)
	case *BetweenExpr:
		return s.evalBetween(e, row)
	case *CaseExpr:
		return s.evalCase(e, row)
	case *SubqueryExpr:
		return s.evalSubqueryValue(e.Stmt, row)
	case *ExistsExpr:
		rs, err := s.evalSubquery(e.Stmt, row)
		if err != nil {
			return NewNullValue(), err
		}
		exists := len(rs.Rows) > 0
		if e.Not {
			exists = !exists
		}
		if exists {
			return NewIntValue(1), nil
		}
		return NewIntValue(0), nil
	}
	return NewNullValue(), fmt.Errorf("unsupported expression type: %T", expr)
}

func (s *Schema) evalFunc(e *FuncCallExpr, row map[string]Value) (Value, error) {
	name := strings.ToUpper(e.Name)
	if err := validateScalarFunction(e); err != nil {
		return NewNullValue(), err
	}

	switch strings.ToUpper(e.Name) {
	case "COALESCE", "IFNULL":
		for _, arg := range e.Args {
			v, err := s.evalExpr(arg, row)
			if err != nil {
				return NewNullValue(), err
			}
			if !v.Null {
				return v, nil
			}
		}
		return NewNullValue(), nil
	case "NULLIF":
		if len(e.Args) < 2 {
			return NewNullValue(), nil
		}
		v1, err := s.evalExpr(e.Args[0], row)
		if err != nil {
			return NewNullValue(), err
		}
		v2, err := s.evalExpr(e.Args[1], row)
		if err != nil {
			return NewNullValue(), err
		}
		if ValueCompare(v1, v2) == 0 {
			return NewNullValue(), nil
		}
		return v1, nil
	case "LENGTH":
		if len(e.Args) < 1 {
			return NewIntValue(0), nil
		}
		v, err := s.evalExpr(e.Args[0], row)
		if err != nil {
			return NewNullValue(), err
		}
		if v.Null {
			return v, nil
		}
		if v.Type == ColBlob {
			return NewIntValue(int64(len(v.Blob))), nil
		}
		return NewIntValue(int64(utf8.RuneCountInString(v.String()))), nil
	case "UPPER":
		if len(e.Args) < 1 {
			return NewNullValue(), nil
		}
		v, err := s.evalExpr(e.Args[0], row)
		if err != nil {
			return NewNullValue(), err
		}
		if v.Null {
			return v, nil
		}
		return NewTextValue(strings.ToUpper(v.String())), nil
	case "LOWER":
		if len(e.Args) < 1 {
			return NewNullValue(), nil
		}
		v, err := s.evalExpr(e.Args[0], row)
		if err != nil {
			return NewNullValue(), err
		}
		if v.Null {
			return v, nil
		}
		return NewTextValue(strings.ToLower(v.String())), nil
	case "ABS":
		if len(e.Args) < 1 {
			return NewNullValue(), nil
		}
		v, err := s.evalExpr(e.Args[0], row)
		if err != nil {
			return NewNullValue(), err
		}
		if v.Null {
			return v, nil
		}
		if v.Type != ColInteger && v.Type != ColReal {
			return NewNullValue(), fmt.Errorf("ABS requires a numeric argument")
		}
		if v.Type == ColInteger {
			if v.Int == math.MinInt64 {
				return NewNullValue(), fmt.Errorf("integer overflow")
			}
			if v.Int < 0 {
				return NewIntValue(-v.Int), nil
			}
			return v, nil
		}
		f := toFloat(v)
		if f < 0 {
			return NewRealValue(-f), nil
		}
		return NewRealValue(f), nil
	case "ROUND":
		if len(e.Args) < 1 || len(e.Args) > 2 {
			return NewNullValue(), fmt.Errorf("wrong number of arguments to ROUND")
		}
		v, err := s.evalExpr(e.Args[0], row)
		if err != nil || v.Null {
			return NewNullValue(), err
		}
		if v.Type != ColInteger && v.Type != ColReal {
			return NewNullValue(), fmt.Errorf("ROUND requires a numeric argument")
		}
		digits := 0
		if len(e.Args) == 2 {
			d, err := s.evalExpr(e.Args[1], row)
			if err != nil || d.Null {
				return NewNullValue(), err
			}
			digits = int(toFloat(d))
			if digits < 0 {
				digits = 0
			}
			if digits > 30 {
				digits = 30
			}
		}
		scale := math.Pow10(digits)
		return NewRealValue(math.Round(toFloat(v)*scale) / scale), nil
	case "IIF":
		if len(e.Args) < 3 {
			return NewNullValue(), nil
		}
		cond, err := s.evalCondition(e.Args[0], row)
		if err != nil {
			return NewNullValue(), err
		}
		if cond {
			return s.evalExpr(e.Args[1], row)
		}
		return s.evalExpr(e.Args[2], row)
	case "SUBSTR":
		if len(e.Args) < 2 || len(e.Args) > 3 {
			return NewNullValue(), fmt.Errorf("wrong number of arguments to SUBSTR")
		}
		v, err := s.evalExpr(e.Args[0], row)
		if err != nil || v.Null {
			return NewNullValue(), err
		}
		startValue, err := s.evalExpr(e.Args[1], row)
		if err != nil || startValue.Null {
			return NewNullValue(), err
		}
		chars := []rune(v.String())
		n := len(chars)
		if v.Type == ColBlob {
			n = len(v.Blob)
		}
		start := int(integerValue(startValue))
		length := n
		if len(e.Args) == 3 {
			v, err := s.evalExpr(e.Args[2], row)
			if err != nil || v.Null {
				return NewNullValue(), err
			}
			length = int(integerValue(v))
		}
		if start > 0 {
			start--
		} else if start < 0 {
			start += n
		} else if length > 0 {
			length--
		}
		if start < 0 {
			if length > 0 {
				length += start
			}
			start = 0
		}
		end := start + length
		if length < 0 {
			end = start
			start += length
		}
		if start < 0 {
			start = 0
		}
		if start > n {
			start = n
		}
		if end < start {
			end = start
		}
		if end > n {
			end = n
		}
		if v.Type == ColBlob {
			return NewBlobValue(v.Blob[start:end]), nil
		}
		return NewTextValue(string(chars[start:end])), nil
	case "REPLACE":
		if len(e.Args) < 3 {
			return NewNullValue(), nil
		}
		strVal, err := s.evalExpr(e.Args[0], row)
		if err != nil {
			return NewNullValue(), err
		}
		oldVal, err := s.evalExpr(e.Args[1], row)
		if err != nil {
			return NewNullValue(), err
		}
		newVal, err := s.evalExpr(e.Args[2], row)
		if err != nil {
			return NewNullValue(), err
		}
		if strVal.Null || oldVal.Null || newVal.Null {
			return NewNullValue(), nil
		}
		if oldVal.String() == "" {
			return NewTextValue(strVal.String()), nil
		}
		return NewTextValue(strings.ReplaceAll(strVal.String(), oldVal.String(), newVal.String())), nil
	case "TRIM", "LTRIM", "RTRIM":
		if len(e.Args) < 1 || len(e.Args) > 2 {
			return NewNullValue(), fmt.Errorf("wrong number of arguments to %s", e.Name)
		}
		v, err := s.evalExpr(e.Args[0], row)
		if err != nil || v.Null {
			return NewNullValue(), err
		}
		cut := " "
		if len(e.Args) > 1 {
			c, err := s.evalExpr(e.Args[1], row)
			if err != nil || c.Null {
				return NewNullValue(), err
			}
			cut = c.String()
		}
		switch strings.ToUpper(e.Name) {
		case "LTRIM":
			return NewTextValue(strings.TrimLeft(v.String(), cut)), nil
		case "RTRIM":
			return NewTextValue(strings.TrimRight(v.String(), cut)), nil
		default:
			return NewTextValue(strings.Trim(v.String(), cut)), nil
		}
	case "RANDOM":
		return NewIntValue(rand.Int63()), nil
	case "DATETIME":
		t, err := s.evalDateTimeArgs(e.Args, row)
		if err != nil {
			return NewNullValue(), err
		}
		return NewTextValue(t.Format("2006-01-02 15:04:05")), nil
	case "DATE":
		t, err := s.evalDateTimeArgs(e.Args, row)
		if err != nil {
			return NewNullValue(), err
		}
		return NewTextValue(t.Format("2006-01-02")), nil
	case "TIME":
		t, err := s.evalDateTimeArgs(e.Args, row)
		if err != nil {
			return NewNullValue(), err
		}
		return NewTextValue(t.Format("15:04:05")), nil
	case "STRFTIME":
		if len(e.Args) < 1 {
			return NewNullValue(), nil
		}
		fmtVal, err := s.evalExpr(e.Args[0], row)
		if err != nil {
			return NewNullValue(), err
		}
		t, err := s.evalDateTimeArgs(e.Args[1:], row)
		if err != nil {
			return NewNullValue(), err
		}
		return NewTextValue(dateStrftime(fmtVal.String(), t)), nil
	case "JULIANDAY":
		t, err := s.evalDateTimeArgs(e.Args, row)
		if err != nil {
			return NewNullValue(), err
		}
		return NewRealValue(toJulianDay(t)), nil
	case "CAST":
		v, err := s.evalExpr(e.Args[0], row)
		if err != nil || v.Null {
			return NewNullValue(), err
		}
		t, err := s.evalExpr(e.Args[1], row)
		if err != nil {
			return NewNullValue(), err
		}
		return castSQLValue(v, t.Text)

	case "HEX":
		v, err := s.evalExpr(e.Args[0], row)
		if err != nil {
			return NewNullValue(), err
		}
		if v.Null {
			return NewTextValue(""), nil
		}
		data := v.Blob
		if v.Type != ColBlob {
			data = []byte(v.String())
		}
		return NewTextValue(strings.ToUpper(fmt.Sprintf("%x", data))), nil
	case "TYPEOF":
		v, err := s.evalExpr(e.Args[0], row)
		if err != nil {
			return NewNullValue(), err
		}
		if v.Null {
			return NewTextValue("null"), nil
		}
		switch v.Type {
		case ColInteger:
			return NewTextValue("integer"), nil
		case ColReal:
			return NewTextValue("real"), nil
		case ColBlob:
			return NewTextValue("blob"), nil
		default:
			return NewTextValue("text"), nil
		}
	case "QUOTE":
		v, err := s.evalExpr(e.Args[0], row)
		if err != nil {
			return NewNullValue(), err
		}
		return NewTextValue(formatExpr(&LiteralExpr{Value: v})), nil
	case "INSTR":
		a, err := s.evalExpr(e.Args[0], row)
		if err != nil || a.Null {
			return NewNullValue(), err
		}
		b, err := s.evalExpr(e.Args[1], row)
		if err != nil || b.Null {
			return NewNullValue(), err
		}
		if a.Type == ColBlob && b.Type == ColBlob {
			n := bytes.Index(a.Blob, b.Blob)
			if n < 0 {
				return NewIntValue(0), nil
			}
			return NewIntValue(int64(n + 1)), nil
		}
		n := strings.Index(a.String(), b.String())
		if n < 0 {
			return NewIntValue(0), nil
		}
		return NewIntValue(int64(utf8.RuneCountInString(a.String()[:n]) + 1)), nil
	case "CHANGES":
		return NewIntValue(0), nil
	case "LAST_INSERT_ROWID":
		return NewIntValue(0), nil
	case "MIN", "MAX":
		if len(e.Args) < 2 {
			return NewNullValue(), fmt.Errorf("misuse of aggregate %s", name)
		}
		best := NewNullValue()
		for i, arg := range e.Args {
			v, err := s.evalExpr(arg, row)
			if err != nil || v.Null {
				return NewNullValue(), err
			}
			if i == 0 || name == "MIN" && ValueCompare(v, best) < 0 || name == "MAX" && ValueCompare(v, best) > 0 {
				best = v
			}
		}
		return best, nil
	case "COUNT", "SUM", "AVG", "GROUP_CONCAT", "TOTAL":
		return NewNullValue(), nil
	default:
		return NewNullValue(), fmt.Errorf("no such function: %s", e.Name)
	}
}

func (s *Schema) evalInList(e *InListExpr, row map[string]Value) (Value, error) {
	val, err := s.evalExpr(e.Expr, row)
	if err != nil {
		return NewNullValue(), err
	}
	hasNull := val.Null
	for _, item := range e.List {
		v, err := s.evalExpr(item, row)
		if err != nil {
			return NewNullValue(), err
		}
		if v.Null {
			hasNull = true
		}
		if !val.Null && !v.Null && ValueCompare(val, v) == 0 {
			if e.Not {
				return NewIntValue(0), nil
			}
			return NewIntValue(1), nil
		}
	}
	if len(e.List) > 0 && hasNull {
		return NewNullValue(), nil
	}
	if e.Not {
		return NewIntValue(1), nil
	}
	return NewIntValue(0), nil
}

func (s *Schema) evalBetween(e *BetweenExpr, row map[string]Value) (Value, error) {
	val, err := s.evalExpr(e.Expr, row)
	if err != nil {
		return NewNullValue(), err
	}
	low, err := s.evalExpr(e.Low, row)
	if err != nil {
		return NewNullValue(), err
	}
	high, err := s.evalExpr(e.High, row)
	if err != nil {
		return NewNullValue(), err
	}
	a := s.compareValues(val, low, ">=")
	b := s.compareValues(val, high, "<=")
	result, err := s.evalBinary(&BinaryExpr{Left: &LiteralExpr{Value: a}, Op: "AND", Right: &LiteralExpr{Value: b}}, row)
	if err != nil {
		return NewNullValue(), err
	}
	if e.Not && !result.Null {
		result = NewIntValue(1 - result.Int)
	}
	return result, nil

}

func (s *Schema) evalCase(e *CaseExpr, row map[string]Value) (Value, error) {
	if e.Expr != nil {
		caseVal, err := s.evalExpr(e.Expr, row)
		if err != nil {
			return NewNullValue(), err
		}
		for _, when := range e.Whens {
			condVal, err := s.evalExpr(when.Cond, row)
			if err != nil {
				return NewNullValue(), err
			}
			if !caseVal.Null && !condVal.Null && ValueCompare(caseVal, condVal) == 0 {
				return s.evalExpr(when.Then, row)
			}
		}
	} else {
		for _, when := range e.Whens {
			match, err := s.evalCondition(when.Cond, row)
			if err != nil {
				return NewNullValue(), err
			}
			if match {
				return s.evalExpr(when.Then, row)
			}
		}
	}
	if e.ElseExpr != nil {
		return s.evalExpr(e.ElseExpr, row)
	}
	return NewNullValue(), nil
}

func (s *Schema) evalBinary(e *BinaryExpr, row map[string]Value) (Value, error) {
	switch e.Op {
	case "AND", "OR":
		left, err := s.evalExpr(e.Left, row)
		if err != nil {
			return NewNullValue(), err
		}
		if e.Op == "AND" && !left.Null && !truthValue(left) {
			return NewIntValue(0), nil
		}
		if e.Op == "OR" && truthValue(left) {
			return NewIntValue(1), nil
		}
		right, err := s.evalExpr(e.Right, row)
		if err != nil {
			return NewNullValue(), err
		}
		if e.Op == "AND" && !right.Null && !truthValue(right) {
			return NewIntValue(0), nil
		}
		if e.Op == "OR" && truthValue(right) {
			return NewIntValue(1), nil
		}
		if left.Null || right.Null {
			return NewNullValue(), nil
		}
		if e.Op == "AND" {
			return NewIntValue(1), nil
		}
		return NewIntValue(0), nil
	case "=", "<", ">", "<=", ">=", "<>":
		lv, err := s.evalExpr(e.Left, row)
		if err != nil {
			return NewNullValue(), err
		}
		rv, err := s.evalExpr(e.Right, row)
		if err != nil {
			return NewNullValue(), err
		}
		if _, ok := e.Left.(*ColumnRefExpr); ok {
			if _, literal := e.Right.(*LiteralExpr); literal {
				rv = applyAffinity(rv, lv.Type)
			}
		} else if _, ok := e.Right.(*ColumnRefExpr); ok {
			if _, literal := e.Left.(*LiteralExpr); literal {
				lv = applyAffinity(lv, rv.Type)
			}
		}

		if _, isSub := e.Right.(*SubqueryExpr); isSub {
			return s.compareValues(lv, rv, e.Op), nil
		}
		return s.compareValues(lv, rv, e.Op), nil
	case "IN", "NOT IN":
		lv, err := s.evalExpr(e.Left, row)
		if err != nil {
			return NewNullValue(), err
		}
		if sub, ok := e.Right.(*SubqueryExpr); ok {
			rs, err := s.evalSubquery(sub.Stmt, row)
			if err != nil {
				return NewNullValue(), err
			}
			if len(rs.Columns) != 1 {
				return NewNullValue(), fmt.Errorf("IN subquery must return one column")
			}
			hasNull := lv.Null
			for _, sr := range rs.Rows {
				v := sr[rs.Columns[0].Name]
				if v.Null {
					hasNull = true
				}
				if !lv.Null && !v.Null && ValueCompare(lv, v) == 0 {
					if e.Op == "NOT IN" {
						return NewIntValue(0), nil
					}
					return NewIntValue(1), nil
				}
			}
			if len(rs.Rows) > 0 && hasNull {
				return NewNullValue(), nil
			}
			if e.Op == "NOT IN" {
				return NewIntValue(1), nil
			}
			return NewIntValue(0), nil
		}

		return NewNullValue(), nil
	case "LIKE":
		lv, err := s.evalExpr(e.Left, row)
		if err != nil {
			return NewNullValue(), err
		}
		rv, err := s.evalExpr(e.Right, row)
		if err != nil {
			return NewNullValue(), err
		}
		if lv.Null || rv.Null {
			return NewNullValue(), nil
		}
		if likeMatchEsc(lv.String(), rv.String(), e.Escape) {
			return NewIntValue(1), nil
		}
		return NewIntValue(0), nil
	case "NOT LIKE":
		lv, err := s.evalExpr(e.Left, row)
		if err != nil {
			return NewNullValue(), err
		}
		rv, err := s.evalExpr(e.Right, row)
		if err != nil {
			return NewNullValue(), err
		}
		if lv.Null || rv.Null {
			return NewNullValue(), nil
		}
		if likeMatchEsc(lv.String(), rv.String(), e.Escape) {
			return NewIntValue(0), nil
		}
		return NewIntValue(1), nil
	case "GLOB", "NOT GLOB", "REGEXP", "NOT REGEXP":
		left, err := s.evalExpr(e.Left, row)
		if err != nil || left.Null {
			return NewNullValue(), err
		}
		right, err := s.evalExpr(e.Right, row)
		if err != nil || right.Null {
			return NewNullValue(), err
		}
		matched := false
		if strings.Contains(e.Op, "GLOB") {
			matched = globMatch(left.String(), right.String())
		} else {
			matched, err = regexp.MatchString(right.String(), left.String())
			if err != nil {
				return NewNullValue(), err
			}
		}
		if strings.HasPrefix(e.Op, "NOT ") {
			matched = !matched
		}
		if matched {
			return NewIntValue(1), nil
		}
		return NewIntValue(0), nil
	case "IS", "IS NOT":
		left, err := s.evalExpr(e.Left, row)
		if err != nil {
			return NewNullValue(), err
		}
		right, err := s.evalExpr(e.Right, row)
		if err != nil {
			return NewNullValue(), err
		}
		same := left.Null && right.Null || !left.Null && !right.Null && ValueCompare(left, right) == 0
		if e.Op == "IS NOT" {
			same = !same
		}
		if same {
			return NewIntValue(1), nil
		}
		return NewIntValue(0), nil
	case "+":
		return s.evalArithmetic(e, row, func(a, b float64) float64 { return a + b })
	case "-":
		return s.evalArithmetic(e, row, func(a, b float64) float64 { return a - b })
	case "*":
		return s.evalArithmetic(e, row, func(a, b float64) float64 { return a * b })
	case "%":
		return s.evalArithmetic(e, row, func(a, b float64) float64 { return math.Mod(a, b) })
	case "/":
		return s.evalArithmetic(e, row, func(a, b float64) float64 {
			if b == 0 {
				return 0
			}
			return a / b
		})
	case "||":
		lv, err := s.evalExpr(e.Left, row)
		if err != nil {
			return NewNullValue(), err
		}
		rv, err := s.evalExpr(e.Right, row)
		if err != nil {
			return NewNullValue(), err
		}
		if lv.Null || rv.Null {
			return NewNullValue(), nil
		}
		return NewTextValue(lv.String() + rv.String()), nil
	case "|":
		return s.evalBitwise(e, row, func(a, b int64) int64 { return a | b })
	case "&":
		return s.evalBitwise(e, row, func(a, b int64) int64 { return a & b })
	case "<<":
		return s.evalBitwise(e, row, func(a, b int64) int64 { return a << uint64(b) })
	case ">>":
		return s.evalBitwise(e, row, func(a, b int64) int64 { return a >> uint64(b) })
	}
	return NewNullValue(), nil
}

func (s *Schema) evalArithmetic(e *BinaryExpr, row map[string]Value, fn func(float64, float64) float64) (Value, error) {
	lv, err := s.evalExpr(e.Left, row)
	if err != nil {
		return NewNullValue(), err
	}
	rv, err := s.evalExpr(e.Right, row)
	if err != nil {
		return NewNullValue(), err
	}
	if lv.Null || rv.Null {
		return NewNullValue(), nil
	}
	if (lv.Type != ColInteger && lv.Type != ColReal) || (rv.Type != ColInteger && rv.Type != ColReal) {
		return NewNullValue(), fmt.Errorf("arithmetic requires numeric operands")
	}
	if (e.Op == "/" || e.Op == "%") && toFloat(rv) == 0 {
		return NewNullValue(), fmt.Errorf("division by zero")
	}
	if e.Op == "%" {
		if lv.Type != ColInteger || rv.Type != ColInteger {
			return NewNullValue(), fmt.Errorf("modulo requires integer operands")
		}
		a, b := lv.Int, rv.Int
		if b == 0 {
			return NewNullValue(), fmt.Errorf("division by zero")
		}
		return NewIntValue(a % b), nil
	}
	if lv.Type == ColInteger && rv.Type == ColInteger {
		a, b := lv.Int, rv.Int
		switch e.Op {
		case "+":
			r := a + b
			if (b > 0 && r < a) || (b < 0 && r > a) {
				return NewNullValue(), fmt.Errorf("integer overflow")
			}
			return NewIntValue(r), nil
		case "-":
			r := a - b
			if (b < 0 && r < a) || (b > 0 && r > a) {
				return NewNullValue(), fmt.Errorf("integer overflow")
			}
			return NewIntValue(r), nil
		case "*":
			if a == 0 || b == 0 {
				return NewIntValue(0), nil
			}
			if !(a == math.MinInt64 && b == -1) && !(b == math.MinInt64 && a == -1) {
				r := a * b
				if r/b == a {
					return NewIntValue(r), nil
				}
			}
		case "/":
			if !(a == math.MinInt64 && b == -1) {
				return NewIntValue(a / b), nil
			}
		}
		return NewNullValue(), fmt.Errorf("integer overflow")
	}

	result := fn(toFloat(lv), toFloat(rv))
	if math.IsInf(result, 0) || math.IsNaN(result) {
		return NewNullValue(), fmt.Errorf("numeric value out of range")
	}
	return NewRealValue(result), nil
}

func integerValue(v Value) int64 {
	if v.Type == ColInteger {
		return v.Int
	}
	return int64(toFloat(v))
}

func (s *Schema) evalBitwise(e *BinaryExpr, row map[string]Value, fn func(int64, int64) int64) (Value, error) {
	lv, err := s.evalExpr(e.Left, row)
	if err != nil {
		return NewNullValue(), err
	}
	rv, err := s.evalExpr(e.Right, row)
	if err != nil {
		return NewNullValue(), err
	}
	if lv.Null || rv.Null {
		return NewNullValue(), nil
	}
	return NewIntValue(fn(integerValue(lv), integerValue(rv))), nil
}

func (s *Schema) compareValues(lv, rv Value, op string) Value {
	if lv.Null || rv.Null {
		return NewNullValue()
	}
	cmp := ValueCompare(lv, rv)
	var result bool
	switch op {
	case "=":
		result = cmp == 0
	case "<":
		result = cmp < 0
	case ">":
		result = cmp > 0
	case "<=":
		result = cmp <= 0
	case ">=":
		result = cmp >= 0
	case "<>":
		result = cmp != 0
	}
	if result {
		return NewIntValue(1)
	}
	return NewIntValue(0)
}

func (s *Schema) evalLiteral(expr Expression, colType ColumnType) Value {
	v, err := s.evalExpr(expr, nil)
	if err != nil {
		return NewNullValue()
	}
	return applyAffinity(v, colType)
}
func applyAffinity(v Value, typ ColumnType) Value {
	if v.Null || v.Type == ColBlob {
		return v
	}
	switch typ {
	case ColInteger, ColReal:
		if v.Type == ColText {
			text := strings.TrimSpace(v.Text)
			if n, err := strconv.ParseInt(text, 10, 64); err == nil {
				v = NewIntValue(n)
			} else if f, err := strconv.ParseFloat(text, 64); err == nil {
				v = NewRealValue(f)
			} else {
				return v
			}
		}
		if typ == ColReal && v.Type == ColInteger {
			return NewRealValue(float64(v.Int))
		}
		if typ == ColInteger && v.Type == ColReal && v.Real >= math.MinInt64 && v.Real < 9223372036854775808.0 && v.Real == math.Trunc(v.Real) {
			return NewIntValue(int64(v.Real))
		}
	case ColText:
		if v.Type != ColText {
			return NewTextValue(v.String())
		}
	}
	return v
}
func valueIdentity(v Value) []byte {
	if !v.Null && v.Type == ColReal && v.Real >= math.MinInt64 && v.Real < 9223372036854775808.0 && v.Real == math.Trunc(v.Real) {
		v = NewIntValue(int64(v.Real))
	}
	return encodeValue(v)
}

func toFloat(v Value) float64 {
	switch v.Type {
	case ColInteger:
		return float64(v.Int)
	case ColReal:
		return v.Real
	case ColText:
		var f float64
		fmt.Sscanf(v.Text, "%f", &f)
		return f
	}
	return 0
}

func likeMatch(s, pattern string) bool {
	return likeMatchEsc(s, pattern, 0)
}

func globMatch(str, pattern string) bool {
	var regex strings.Builder
	regex.WriteString("(?s)^")
	runes := []rune(pattern)
	for i := 0; i < len(runes); i++ {
		r := runes[i]
		switch r {
		case '*':
			regex.WriteString(".*")
		case '?':
			regex.WriteString(".")
		case '[':
			j := i + 1
			if j < len(runes) && runes[j] == '^' {
				j++
			}
			if j < len(runes) && runes[j] == ']' {
				j++
			}
			for j < len(runes) && runes[j] != ']' {
				j++
			}
			if j == len(runes) {
				return false
			}
			regex.WriteRune('[')
			for k := i + 1; k < j; k++ {
				if runes[k] == '\\' {
					regex.WriteString(`\\`)
				} else {
					regex.WriteRune(runes[k])
				}
			}
			regex.WriteRune(']')
			i = j
		default:
			regex.WriteString(regexp.QuoteMeta(string(r)))
		}
	}
	regex.WriteString("$")
	matched, err := regexp.MatchString(regex.String(), str)
	return err == nil && matched
}
func likeMatchEsc(str, pattern string, escape byte) bool {
	type token struct {
		r    rune
		wild bool
	}
	var tokens []token
	runes := []rune(pattern)
	fold := func(r rune) rune {
		if r >= 'A' && r <= 'Z' {
			return r + ('a' - 'A')
		}
		return r
	}
	for i := 0; i < len(runes); i++ {
		r := runes[i]
		if escape != 0 && r == rune(escape) {
			i++
			if i == len(runes) {
				return false
			}
			tokens = append(tokens, token{r: fold(runes[i])})
		} else {
			tokens = append(tokens, token{r: fold(r), wild: r == '%' || r == '_'})
		}
	}
	input := []rune(str)
	previous := make([]bool, len(input)+1)
	previous[0] = true
	for _, t := range tokens {
		next := make([]bool, len(input)+1)
		if t.wild && t.r == '%' {
			next[0] = previous[0]
		}
		for j := 1; j <= len(input); j++ {
			if t.wild && t.r == '%' {
				next[j] = previous[j] || next[j-1]
			} else if t.wild && t.r == '_' || t.r == fold(input[j-1]) {
				next[j] = previous[j-1]
			}
		}
		previous = next
	}
	return previous[len(input)]
}

func (s *Schema) evalDateTimeArgs(args []Expression, row map[string]Value) (time.Time, error) {
	if len(args) == 0 {
		return time.Now().UTC(), nil
	}
	tsVal, err := s.evalExpr(args[0], row)
	if err != nil {
		return time.Time{}, err
	}
	t, err := parseDateTimeString(tsVal)
	if err != nil {
		return time.Time{}, err
	}
	for i := 1; i < len(args); i++ {
		modVal, err := s.evalExpr(args[i], row)
		if err != nil {
			return time.Time{}, err
		}
		t, err = applyDateTimeModifier(t, modVal.String())
		if err != nil {
			return time.Time{}, err
		}
	}
	return t, nil
}

func parseDateTimeString(v Value) (time.Time, error) {
	if v.Null {
		return time.Time{}, fmt.Errorf("NULL timestring")
	}
	if v.Type == ColInteger {
		return fromJulianDay(float64(v.Int)), nil
	}
	if v.Type == ColReal {
		return fromJulianDay(v.Real), nil
	}
	s := strings.TrimSpace(v.Text)
	if s == "" || strings.EqualFold(s, "now") {
		return time.Now().UTC(), nil
	}
	if jd, err := strconv.ParseFloat(s, 64); err == nil {
		return fromJulianDay(jd), nil
	}
	for _, layout := range []string{
		"2006-01-02 15:04:05",
		"2006-01-02",
		"2006-01-02 15:04:05.000",
		"2006-01-02T15:04:05",
		"2006-01-02T15:04:05.000",
	} {
		if t, err := time.ParseInLocation(layout, s, time.UTC); err == nil {
			return t, nil
		}
	}
	if t, err := time.ParseInLocation("15:04:05", s, time.UTC); err == nil {
		now := time.Now().UTC()
		return time.Date(now.Year(), now.Month(), now.Day(), t.Hour(), t.Minute(), t.Second(), 0, time.UTC), nil
	}
	if t, err := time.ParseInLocation("15:04", s, time.UTC); err == nil {
		now := time.Now().UTC()
		return time.Date(now.Year(), now.Month(), now.Day(), t.Hour(), t.Minute(), 0, 0, time.UTC), nil
	}
	return time.Time{}, fmt.Errorf("invalid timestring: %s", s)
}

func applyDateTimeModifier(t time.Time, modStr string) (time.Time, error) {
	mod := strings.TrimSpace(modStr)
	lower := strings.ToLower(mod)

	switch {
	case lower == "localtime":
		return t.In(time.Local), nil
	case lower == "utc":
		return t.UTC(), nil
	case lower == "start of month":
		return time.Date(t.Year(), t.Month(), 1, 0, 0, 0, 0, t.Location()), nil
	case lower == "start of year":
		return time.Date(t.Year(), 1, 1, 0, 0, 0, 0, t.Location()), nil
	case lower == "start of day":
		return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location()), nil
	case strings.HasPrefix(lower, "weekday "):
		rest := strings.TrimSpace(mod[8:])
		n, err := strconv.Atoi(rest)
		if err != nil || n < 0 || n > 6 {
			return t, fmt.Errorf("invalid weekday: %s", rest)
		}
		currentDay := int(t.Weekday())
		days := (n - currentDay + 7) % 7
		if days == 0 {
			days = 7
		}
		return t.AddDate(0, 0, days), nil
	}

	parts := strings.Fields(lower)
	if len(parts) >= 2 {
		valStr := parts[0]
		val, err := strconv.ParseFloat(valStr, 64)
		if err != nil {
			return t, fmt.Errorf("invalid modifier: %s", mod)
		}
		unit := parts[len(parts)-1]
		switch unit {
		case "days", "day":
			return t.Add(time.Duration(val * 24 * float64(time.Hour))), nil
		case "hours", "hour":
			return t.Add(time.Duration(val * float64(time.Hour))), nil
		case "minutes", "minute":
			return t.Add(time.Duration(val * float64(time.Minute))), nil
		case "seconds", "second":
			return t.Add(time.Duration(val * float64(time.Second))), nil
		case "months", "month":
			months := int(val)
			frac := val - float64(months)
			if months != 0 {
				t = t.AddDate(0, months, 0)
			}
			if frac != 0 {
				t = t.Add(time.Duration(frac * 30.4375 * 24 * float64(time.Hour)))
			}
			return t, nil
		case "years", "year":
			return t.AddDate(int(val), 0, 0), nil
		}
	}

	return t, fmt.Errorf("invalid modifier: %s", mod)
}

func dateStrftime(format string, t time.Time) string {
	var result strings.Builder
	for i := 0; i < len(format); i++ {
		if format[i] == '%' && i+1 < len(format) {
			i++
			switch format[i] {
			case 'Y':
				result.WriteString(fmt.Sprintf("%04d", t.Year()))
			case 'm':
				result.WriteString(fmt.Sprintf("%02d", int(t.Month())))
			case 'd':
				result.WriteString(fmt.Sprintf("%02d", t.Day()))
			case 'H':
				result.WriteString(fmt.Sprintf("%02d", t.Hour()))
			case 'M':
				result.WriteString(fmt.Sprintf("%02d", t.Minute()))
			case 'S':
				result.WriteString(fmt.Sprintf("%02d", t.Second()))
			case 'w':
				result.WriteString(fmt.Sprintf("%d", int(t.Weekday())))
			case 'j':
				result.WriteString(fmt.Sprintf("%03d", t.YearDay()))
			case 's':
				result.WriteString(fmt.Sprintf("%d", t.Unix()))
			case 'f':
				ms := t.Nanosecond() / 1000000
				result.WriteString(fmt.Sprintf("%03d", ms))
			default:
				result.WriteByte(format[i])
			}
		} else {
			result.WriteByte(format[i])
		}
	}
	return result.String()
}

func toJulianDay(t time.Time) float64 {
	unix := float64(t.UnixNano()) / 1e9
	return 2440587.5 + unix/86400.0
}

func fromJulianDay(jd float64) time.Time {
	unix := (jd - 2440587.5) * 86400.0
	sec := int64(unix)
	nsec := int64((unix - float64(sec)) * 1e9)
	if nsec < 0 {
		nsec += 1e9
		sec--
	}
	return time.Unix(sec, nsec).UTC()
}

const (
	binTypeNull    byte = 0
	binTypeInt64   byte = 1
	binTypeFloat64 byte = 2
	binTypeText    byte = 3
	binTypeBlob    byte = 4
)

func encodeRow(row map[string]Value, info *TableInfo) []byte {
	buf := make([]byte, 0, len(info.Columns)*9)
	for _, col := range info.Columns {
		v := row[col.Name]
		buf = appendValue(buf, v)
	}
	return buf
}

func decodeRow(data []byte, info *TableInfo) map[string]Value {
	row := make(map[string]Value, len(info.Columns))
	offset := 0
	for _, col := range info.Columns {
		if offset >= len(data) {
			row[col.Name] = NewNullValue()
			continue
		}
		v, n := decodeValue(data[offset:])
		row[col.Name] = v
		offset += n
	}
	return row
}

func encodeValue(v Value) []byte { return appendValue(nil, v) }
func appendValue(buf []byte, v Value) []byte {
	if v.Null {
		return append(buf, binTypeNull)
	}
	switch v.Type {
	case ColInteger:
		buf = append(buf, binTypeInt64)
		return binary.LittleEndian.AppendUint64(buf, uint64(v.Int))
	case ColReal:
		buf = append(buf, binTypeFloat64)
		return binary.LittleEndian.AppendUint64(buf, math.Float64bits(v.Real))
	case ColText:
		buf = append(buf, binTypeText)
		buf = binary.AppendUvarint(buf, uint64(len(v.Text)))
		return append(buf, v.Text...)
	case ColBlob:
		buf = append(buf, binTypeBlob)
		buf = binary.AppendUvarint(buf, uint64(len(v.Blob)))
		return append(buf, v.Blob...)
	default:
		return append(buf, binTypeNull)
	}
}

func decodeValue(data []byte) (Value, int) {
	if len(data) < 1 {
		return NewNullValue(), 0
	}
	switch data[0] {
	case binTypeNull:
		return NewNullValue(), 1
	case binTypeInt64:
		if len(data) < 9 {
			return NewNullValue(), 1
		}
		return NewIntValue(int64(binary.LittleEndian.Uint64(data[1:9]))), 9
	case binTypeFloat64:
		if len(data) < 9 {
			return NewNullValue(), 1
		}
		bits := binary.LittleEndian.Uint64(data[1:9])
		return NewRealValue(math.Float64frombits(bits)), 9
	case binTypeText:
		length, n := binary.Uvarint(data[1:])
		if n <= 0 || len(data) < 1+n+int(length) {
			return NewNullValue(), 1 + n
		}
		text := string(data[1+n : 1+n+int(length)])
		return NewTextValue(text), 1 + n + int(length)
	case binTypeBlob:
		length, n := binary.Uvarint(data[1:])
		if n <= 0 || len(data) < 1+n+int(length) {
			return NewNullValue(), 1 + n
		}
		blob := make([]byte, int(length))
		copy(blob, data[1+n:1+n+int(length)])
		return Value{Type: ColBlob, Blob: blob}, 1 + n + int(length)
	}
	return NewNullValue(), 1
}

type ResultSet struct {
	Columns []ColumnDef
	Rows    []map[string]Value
	rowKeys [][]string // physical row origins, used when locking through views
}

func (s *Schema) viewColumnDefs(vi *ViewInfo) []ColumnDef {
	s.viewDepth++
	defer func() { s.viewDepth-- }()
	if s.viewDepth > 100 {
		return nil
	}
	if len(vi.Columns) > 0 {
		cols := make([]ColumnDef, len(vi.Columns))
		for i, name := range vi.Columns {
			cols[i] = ColumnDef{Name: name}
		}
		return cols
	}

	var cols []ColumnDef
	for i, sc := range vi.Parsed.Columns {
		if sc.Star {
			fromName := strings.ToLower(vi.Parsed.From)
			if fromName != "" {
				if fromVi, ok := s.views[fromName]; ok {
					return s.viewColumnDefs(fromVi)
				}
				if fromInfo, ok := s.tables[fromName]; ok {
					return fromInfo.Columns
				}
			}
			continue
		}
		name := ""
		if i < len(vi.Columns) {
			name = vi.Columns[i]
		}
		if name == "" {
			name = sc.Alias
		}
		if name == "" {
			if cr, ok := sc.Expr.(*ColumnRefExpr); ok {
				name = cr.Name
			} else if fc, ok := sc.Expr.(*FuncCallExpr); ok {
				name = fc.Name
			} else {
				name = fmt.Sprintf("expr%d", i)
			}
		}
		if name != "" {
			cols = append(cols, ColumnDef{Name: name, ColType: ColInteger})
		}
	}
	if len(cols) == 0 && len(vi.Parsed.Columns) > 0 && vi.Parsed.Columns[0].Star {
		fromName := strings.ToLower(vi.Parsed.From)
		if fromName != "" {
			if fromVi, ok := s.views[fromName]; ok {
				return s.viewColumnDefs(fromVi)
			}
			if fromInfo, ok := s.tables[fromName]; ok {
				return fromInfo.Columns
			}
		}
		cols = append(cols, ColumnDef{Name: "col0", ColType: ColInteger})
	}
	return cols
}

type selectExprCol struct {
	name string
	expr Expression
}
