package sql

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"math"
	"math/big"
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
	DefaultCollation Collation       `json:"default_collation,omitempty"`
	Engine           string          `json:"engine,omitempty"`
	Charset          string          `json:"charset,omitempty"`
	collation        Collation       `json:"-"`
	ID               uint64          `json:"id,omitempty"`
	Constraints      []ConstraintDef `json:"constraints,omitempty"`
	Name             string          `json:"name"`
	Columns          []ColumnDef     `json:"columns"`
	TableCheck       []Expression    `json:"-"`
	TableCheckSQL    []string        `json:"table_check_sql,omitempty"`
	TableUnique      [][]string      `json:"table_unique,omitempty"`
	ForeignKeys      []ForeignKeyDef `json:"foreign_keys,omitempty"`
	PKColumns        []string        `json:"pk_columns,omitempty"`
}

type IndexInfo struct {
	columnCollations map[string]Collation `json:"-"`
	FulltextVersion  int                  `json:"fulltext_version,omitempty"`
	PrefixEncoding   int                  `json:"prefix_encoding,omitempty"`
	DisplayName      string               `json:"display_name,omitempty"`
	PrefixLengths    []int64              `json:"prefix_lengths,omitempty"`
	collation        Collation            `json:"-"`
	binaryColumns    map[string]bool      `json:"-"`
	ID               uint64               `json:"id,omitempty"`
	Name             string               `json:"name"`
	Table            string               `json:"table"`
	Column           string               `json:"column"`
	IsUnique         bool                 `json:"is_unique"`
	Columns          []string             `json:"columns,omitempty"`
	Auto             bool                 `json:"auto,omitempty"`
	Fulltext         bool                 `json:"fulltext,omitempty"`
}

type ViewInfo struct {
	Name    string      `json:"name"`
	SQL     string      `json:"sql"`
	Columns []string    `json:"columns,omitempty"`
	Parsed  *SelectStmt `json:"-"`
}

type Schema struct {
	catalogView              *CatalogSnapshot
	describeTypes            *preparedTypeInference
	foreignChanges           []foreignChange
	foreignConstraintModes   map[string]bool
	checkingDeferred         bool
	namespaces               map[string]bool
	defaultSchema            string
	searchPath               []string
	schemaAliases            map[string]string
	autoincUnsigned          map[string]uint64
	unsignedRowReservations  map[string]uint64
	lastInsertIDUnsigned     uint64
	isolation                IsolationLevel
	nextIsolation            IsolationLevel
	txIsolation              IsolationLevel
	consistentSnapshot       uint64
	consistentSnapshotActive bool
	statementCurrentSnapshot uint64
	statementQuery           bool
	statementConsistent      bool
	consistentIndexDeletes   map[string]bool
	statementTime            time.Time
	postgresTransactionTime  time.Time
	mysqlRand                map[*FuncCallExpr]*mysqlRandState
	onlineIndexMu            catalogGate
	onlineIndexBuilds        map[*OnlineIndexBuild]struct{}
	queryMemoryBytes         int64
	queryTempDir             string
	queryDisks               []*diskHash
	maxCTEIterations         int64
	maxCTERows               int64
	cteScope                 map[string]*ResultSet
	commitMu                 catalogGate // protects committed catalog publication
	statementSnapshot        uint64
	snapshotReader           uint64
	nextSnapshotReader       atomic.Uint64
	statementActive          bool
	validatedRowKey          string
	lsm                      *storage.LSMTree
	tables                   map[string]*TableInfo
	indexes                  map[string]*IndexInfo
	outerRow                 map[string]Value
	stats                    map[string]int64
	distributions            map[string]*tableStatistics
	nextObjectID             uint64
	userVersion              int64
	collation                Collation
	views                    map[string]*ViewInfo
	triggers                 map[string]*TriggerInfo
	autoincMax               map[string]int64
	autoincQueued            map[string]int64 // high-water snapshots in WAL enqueue order; commitMu protects it
	rowIDCount               map[string]int64

	ctx                   context.Context
	statementSnapshotHook func(context.Context, *Schema) (bool, error)
	replicaReadOnly       bool
	lastCommitPosition    string
	readBarrier           string // private session setting; not catalog state
	lockHandler           func(LockRequest) error
	lockGroupHandler      func([]LockRequest) error

	indexCleanupPending bool // session hint; survives rollback and cleanup failures

	inTx                 bool
	txWrites             []txWrite
	txLatest             map[string]int
	txState              *schemaState
	statementChanges     int64
	subqueryMemos        map[*SelectStmt]*subqueryMemo
	existsMemos          map[*SelectStmt]*subqueryMemo
	selectValues         map[string]storage.PointValue
	selectValueBytes     int
	likePatterns         map[likePatternKey]likePattern
	mysqlRegexpPrograms  map[string]*mysqlRegexpProgram
	fulltextQueries      map[string]*fulltextQuery
	fulltextBindings     map[*FuncCallExpr]fulltextBinding
	fulltextQueryBytes   int64
	subquerySets         map[*ResultSet]*subqueryMembership
	selectDepth          int
	profile              *QueryProfile
	preparedPlan         *preparedExecutionPlan
	triggerDepth         int
	viewDepth            int
	savepoints           []savepoint
	sessionState         SessionState
	dialect              Dialect
	dialectEpoch         uint64
	mysqlFunctions       mysqlFunctionSessionState
	mysqlConflictChanged bool
	temporary            *temporaryCatalog
	txTemporary          *temporaryCatalog
	nextTemporaryID      atomic.Uint64

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
	rowIDMu            sync.Mutex // protects shared row-ID reservations
	rowIDDirty         bool       // explicit IDs need publication to the shared reservations
	txRowVersions      map[string]uint64
	txCatalogVersion   uint64
	txWriteVersion     uint64
	txDDLVersion       uint64
	txHasWrites        bool
	txHasDDL           bool

	disableAutoIncrementPersistence bool
	enableAutoIncrementGroupCommit  bool
}

func (s *Schema) SetContext(ctx context.Context) {
	s.ctx = ctx
}

func (s *Schema) checkInterrupt() error {
	if s.ctx != nil {
		// Err observes cancellation without creating a Done channel on every
		// otherwise nonblocking statement. Actual waits still select on Done.
		return s.ctx.Err()
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
	constraintModes map[string]bool
	name            string
	pos             int
	state           *schemaState
	temporary       *temporaryCatalog
	outer           bool
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
	collationPrefix   = "\x00__collation__"
	schemaViewsPrefix = "\x00__schema_views__"
	schemaTrigPrefix  = "\x00__schema_trig__"
	schemaIdxPrefix   = "\x00__schema_indexes__"
	autoincPrefix     = "\x00__autoinc__"
	dataPrefix        = "\x01"
	indexPrefix       = "\x02"
)

// NewSchema inherits the physical page target from lsm. The pageSize argument
// is retained for source compatibility; configure storage.Options.PageSize.
func NewSchema(lsm *storage.LSMTree, pageSize int) (*Schema, error) {
	return NewSchemaWithOptions(lsm, pageSize, nil)
}

// NewSchemaWithOptions optionally checks indexes for diagnostics. Normal opens
// trust atomically maintained indexes.
func NewSchemaWithOptions(lsm *storage.LSMTree, pageSize int, opt *storage.Options) (*Schema, error) {
	return newSchemaWithOptions(lsm, pageSize, opt, nil)
}

// NewReplicaSchemaAtSnapshot loads metadata from an externally pinned native
// snapshot without migrations, repair or any other storage writes.
func NewReplicaSchemaAtSnapshot(lsm *storage.LSMTree, snapshot uint64, opt *storage.Options) (*Schema, error) {
	if !lsm.ReplicaMode() {
		return nil, fmt.Errorf("historical catalog loading requires replica storage")
	}
	options := *storage.DefaultOptions()
	if opt != nil {
		options = *opt
	}
	options.Replica = true
	return newSchemaWithOptions(lsm, int(lsm.PageSize()), &options, &snapshot)
}

func newSchemaWithOptions(lsm *storage.LSMTree, pageSize int, opt *storage.Options, snapshot *uint64) (*Schema, error) {
	if opt == nil {
		opt = storage.DefaultOptions()
	}
	if opt.MaxCTEIterations < 0 || opt.MaxCTERows < 0 {
		return nil, fmt.Errorf("CTE limits must not be negative")
	}
	s := &Schema{
		maxCTEIterations: opt.MaxCTEIterations,
		maxCTERows:       opt.MaxCTERows,
		lsm:              lsm,
		tables:           make(map[string]*TableInfo),
		indexes:          make(map[string]*IndexInfo),
		stats:            make(map[string]int64),
		distributions:    make(map[string]*tableStatistics),
		views:            make(map[string]*ViewInfo),
		triggers:         make(map[string]*TriggerInfo),
		autoincMax:       make(map[string]int64),
		autoincUnsigned:  make(map[string]uint64),
		rowIDCount:       make(map[string]int64),
	}
	if snapshot != nil {
		s.statementSnapshot, s.statementActive = *snapshot, true
		defer func() { s.statementActive = false }()
	}
	if opt != nil {
		if opt.QueryMemoryBytes < 0 {
			return nil, fmt.Errorf("query memory budget must not be negative")
		}
		s.queryMemoryBytes, s.queryTempDir = opt.QueryMemoryBytes, opt.QueryTempDir
		s.disableAutoIncrementPersistence = opt.DisableAutoIncrementPersistence
		s.enableAutoIncrementGroupCommit = !opt.DisableWAL && !opt.DisableWALSync
	}
	if snapshot == nil {
		if err := cleanupTemporaryRange(lsm); err != nil {
			return nil, err
		}
	}
	if err := s.loadNamespaces(); err != nil {
		return nil, err
	}
	if err := s.loadCollation(); err != nil {
		return nil, err
	}
	if err := s.loadSchema(); err != nil {
		return nil, err
	}
	if !opt.Replica {
		if err := s.upgradePrimaryKeys(); err != nil {
			return nil, err
		}
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
	s.bindCatalogCollation()
	for _, info := range s.tables {
		if err := s.foreignDefinitions(info); err != nil {
			return nil, err
		}
	}
	s.loadAutoincMax()
	if err := s.loadUnsignedAutoinc(); err != nil {
		return nil, err
	}
	s.loadStats()
	s.loadDistributions()
	s.loadUserVersion()
	if err := s.initializeObjectIDs(); err != nil {
		return nil, err
	}
	if !opt.Replica {
		if err := s.CleanupIndexGarbage(); err != nil {
			return nil, err
		}
		if err := s.normalizeConstraintCatalog(); err != nil {
			return nil, err
		}
		if err := s.upgradeFulltextIndexes(); err != nil {
			return nil, err
		}
		if err := s.upgradePrefixIndexes(); err != nil {
			return nil, err
		}
		if opt != nil && opt.VerifyIndexesOnOpen {
			if err := s.repairIndexes(); err != nil {
				return nil, err
			}
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
		if info.DefaultCollation != "" {
			if _, err := ParseCollation(string(info.DefaultCollation)); err != nil {
				return fmt.Errorf("invalid stored table collation on %s: %w", info.Name, err)
			}
		}
		if info.ID == 0 && len(info.PKColumns) > 1 {
			for i, g := range info.TableUnique {
				if slices.Equal(g, info.PKColumns) {
					info.TableUnique = append(info.TableUnique[:i], info.TableUnique[i+1:]...)
					break
				}
			}
		}
		for i := range info.Columns {
			col := &info.Columns[i]
			if col.Collation != "" {
				if _, err := ParseCollation(string(col.Collation)); err != nil {
					return fmt.Errorf("invalid stored column collation on %s.%s: %w", info.Name, col.Name, err)
				}
			}
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
	tables := s.tables
	if s.hasTemporaryTables() {
		tables = s.captureState().tables
	}
	data, err := json.Marshal(tables)
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
	if s.inTx || s.disableAutoIncrementPersistence {
		return nil
	}
	counters := s.autoincMax
	if s.hasTemporaryTables() {
		counters = s.captureState().autoinc
	}
	data, err := json.Marshal(counters)
	if err != nil {
		return err
	}
	return s.write([]byte(autoincPrefix), data)
}

func (s *Schema) getAutoincMax(tableName string) int64 {
	if s.disableAutoIncrementPersistence {
		return 0
	}
	if v, ok := s.autoincMax[tableName]; ok {
		return v
	}
	return 0
}

func (s *Schema) setAutoincMax(tableName string, id int64) {
	if s.disableAutoIncrementPersistence {
		return
	}
	s.autoincMax[tableName] = id
}

func (s *Schema) schemaGet(key []byte) (value []byte, found bool, err error) {
	if s.profile != nil {
		started := time.Now()
		defer func() { s.profilePoint(key, value, found, time.Since(started)) }()
	}
	if s.inTx {
		return s.txGet(key)
	}
	return s.committedGet(key)
}

func (s *Schema) CreateTable(stmt *CreateTableStmt) error {
	if stmt.DefaultCollation != "" {
		copy := *stmt
		copy.Columns = append([]ColumnDef(nil), stmt.Columns...)
		for i := range copy.Columns {
			if copy.Columns[i].ColType == ColText && !copy.Columns[i].Binary && copy.Columns[i].Collation == "" {
				copy.Columns[i].Collation = copy.DefaultCollation
			}
		}
		stmt = &copy
	}
	if stmt.OnCommit != TemporaryCommitUnspecified && (!stmt.Temporary || !s.Dialect().TemporaryDDLPolicy().CommitActions) {
		return fmt.Errorf("ON COMMIT is supported only for PostgreSQL temporary tables")
	}
	for _, c := range stmt.Columns {
		if expressionUsesSessionVariable(c.Default) || expressionUsesSessionVariable(c.Check) {
			return fmt.Errorf("session variables cannot appear in persistent table definitions")
		}
	}
	for _, e := range stmt.TableCheck {
		if expressionUsesSessionVariable(e) {
			return fmt.Errorf("session variables cannot appear in persistent table definitions")
		}
	}
	if s.PostgresMode() {
		if _, known := postgresCatalogName(stmt.Table); known {
			return fmt.Errorf("system catalog %q is read only", stmt.Table)
		}
	}
	if s.PostgresMode() {
		copy := *stmt
		copy.Columns = append([]ColumnDef(nil), stmt.Columns...)
		for i := range copy.Columns {
			postgresColumnDefinition(&copy.Columns[i])
		}
		stmt = &copy
	}
	if stmt.Temporary {
		return s.createTemporaryTable(stmt)
	}
	if stmt.Like != "" {
		var err error
		stmt, err = s.expandCreateTableLike(stmt)
		if err != nil {
			return err
		}
	}
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
		if s.MySQLMode() {
			stmt.Columns[i].MySQL = true
		}
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

	id, err := s.allocateObjectID()
	if err != nil {
		return err
	}
	info := &TableInfo{
		DefaultCollation: stmt.DefaultCollation,
		Engine:           stmt.Engine,
		Charset:          stmt.Charset,
		collation:        s.collation,
		ID:               id,
		Constraints:      stmt.Constraints,
		Name:             name,
		Columns:          stmt.Columns,
		TableCheck:       tcexpr,
		TableCheckSQL:    tcsql,
		TableUnique:      tableUnique,
		ForeignKeys:      stmt.ForeignKeys,
		PKColumns:        stmt.PKColumns,
	}
	if err := validateConstraintExpressions(info); err != nil {
		return err
	}
	if err := primaryKeyColumns(info); err != nil {
		return err
	}
	s.tables[name] = info
	delete(s.distributions, name)
	delete(s.stats, name)
	if err := s.saveSchema(); err != nil {
		return err
	}
	for _, col := range stmt.Columns {
		if (col.PrimaryKey || col.Unique) && len(stmt.PKColumns) <= 1 {
			idxName := name + "_" + col.Name + "_idx"
			if _, ok := s.indexes[idxName]; !ok {
				indexID, err := s.allocateObjectID()
				if err != nil {
					return err
				}
				idx := &IndexInfo{ID: indexID, Name: idxName, Table: name, Column: col.Name, IsUnique: true, Auto: true}
				s.bindIndexCollation(idx)
				s.indexes[idxName] = idx
			}
		}
	}
	for _, idx := range inlineIndexes {
		s.bindIndexCollation(idx)
		idx.ID, err = s.allocateObjectID()
		if err != nil {
			return err
		}
		s.indexes[idx.Name] = idx
	}
	if err := s.foreignDefinitions(info); err != nil {
		return err
	}
	s.catalogLookup = nil
	if err := s.initializeConstraintNames(info); err != nil {
		return err
	}
	if err := s.saveSchema(); err != nil {
		return err
	}
	return s.saveIndexes()
}

func (s *Schema) DropTable(stmt *DropTableStmt) error {
	if stmt.Temporary || s.isTemporaryTable(stmt.Table) {
		return s.dropTemporaryTable(stmt)
	}
	s.ensureCatalogWritable()
	if err := s.checkDropReferencedTable(strings.ToLower(stmt.Table)); err != nil {
		return err
	}
	name := strings.ToLower(stmt.Table)
	if _, ok := s.tables[name]; !ok {
		if stmt.IfExists {
			return nil
		}
		return fmt.Errorf("table %q does not exist", name)
	}

	for idxName, idx := range s.indexes {
		if idx.Table == name {
			idxPrefix := []byte(s.indexDataPrefix(idxName))
			s.removeRange(idxPrefix, append(idxPrefix, 0xFF))
			delete(s.indexes, idxName)
		}
	}
	s.saveIndexes()

	prefix := s.tableDataPrefix(name)
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

	delete(s.autoincUnsigned, s.rowCounterKey(name))
	delete(s.rowIDCount, s.rowCounterKey(name))
	delete(s.tables, name)
	delete(s.autoincMax, name)
	for k, tr := range s.triggers {
		if tr.Table == name || tr.View == name {
			delete(s.triggers, k)
		}
	}
	delete(s.stats, name)
	delete(s.distributions, name)
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
	for name, info := range s.tables {
		distribution, err := s.analyzeTable(info)
		if err != nil {
			return err
		}
		s.stats[name] = distribution.Rows
		s.distributions[name] = distribution
	}
	if err := s.saveStats(); err != nil {
		return err
	}
	return s.saveDistributions()
}

func (s *Schema) countRows(tableName string) int64 {
	prefix := s.tableDataPrefix(tableName)
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
	inlineName := func(kind, col string) string {
		for _, c := range info.Constraints {
			if c.Kind == kind && c.Inline && len(c.Columns) == 1 && c.Columns[0] == col {
				return " CONSTRAINT " + quoteIdent(c.Name)
			}
		}
		return ""
	}
	primaryInline := true
	for _, c := range info.Constraints {
		if c.Kind == "PRIMARY" {
			primaryInline = c.Inline
		}
	}
	fmt.Fprintf(&b, "CREATE TABLE %s(", quoteIdent(info.Name))
	for i, col := range info.Columns {
		if i > 0 {
			b.WriteString(", ")
		}
		fmt.Fprintf(&b, "%s %s", quoteIdent(col.Name), col.DeclaredType())
		if col.Binary {
			b.WriteString(" COLLATE BINARY")
		} else if col.Collation != "" {
			b.WriteString(" COLLATE " + string(col.Collation))
		}
		if col.NotNull {
			b.WriteString(inlineName("NOT_NULL", col.Name) + " NOT NULL")
		}
		if col.Default != nil {
			fmt.Fprintf(&b, " DEFAULT %s", formatExpr(col.Default))
		}
		if (col.PrimaryKey || (len(info.PKColumns) == 1 && info.PKColumns[0] == col.Name)) && len(info.PKColumns) <= 1 && primaryInline {
			b.WriteString(inlineName("PRIMARY", col.Name) + " PRIMARY KEY")
			if col.AutoIncrement {
				if col.MySQLAutoIncrement {
					b.WriteString(" AUTO_INCREMENT")
				} else {
					b.WriteString(" AUTOINCREMENT")
				}
			}
		}
		if col.Unique {
			b.WriteString(inlineName("UNIQUE", col.Name) + " UNIQUE")
		}
		if col.Check != nil {
			fmt.Fprintf(&b, "%s CHECK(%s)", inlineName("CHECK", col.Name), formatExpr(col.Check))
		}
		if col.RefTable != "" {
			fmt.Fprintf(&b, "%s REFERENCES %s", inlineName("FOREIGN", col.Name), quoteIdent(col.RefTable))
			if len(col.RefColumns) > 0 {
				fmt.Fprintf(&b, "(%s)", quotedNames(col.RefColumns))
			}
			b.WriteString(foreignOptionsSQL(ForeignKeyDef{OnDelete: col.RefOnDelete, OnUpdate: col.RefOnUpdate, Deferrable: col.RefDeferrable, InitiallyDeferred: col.RefInitiallyDeferred}))
		}
	}
	for _, c := range info.Constraints {
		if c.Inline {
			continue
		}
		b.WriteString(", CONSTRAINT " + quoteIdent(c.Name) + " ")
		switch c.Kind {
		case "PRIMARY":
			b.WriteString("PRIMARY KEY (" + quotedNames(c.Columns) + ")")
		case "UNIQUE":
			if (&IndexInfo{PrefixLengths: c.PrefixLengths}).hasPrefixes() {
				b.WriteString("UNIQUE KEY (" + formatIndexColumns(&IndexInfo{Columns: c.Columns, PrefixLengths: c.PrefixLengths}) + ")")
			} else {
				b.WriteString("UNIQUE (" + quotedNames(c.Columns) + ")")
			}
		case "CHECK":
			b.WriteString("CHECK (" + c.CheckSQL + ")")
		case "FOREIGN":
			b.WriteString("FOREIGN KEY (" + quotedNames(c.Columns) + ") REFERENCES " + quoteIdent(c.RefTable))
			if len(c.RefColumns) > 0 {
				b.WriteString(" (" + quotedNames(c.RefColumns) + ")")
			}
			b.WriteString(foreignOptionsSQL(ForeignKeyDef{OnDelete: c.OnDelete, OnUpdate: c.OnUpdate, Deferrable: c.Deferrable, InitiallyDeferred: c.InitiallyDeferred}))
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
			if idx.Fulltext {
				b.WriteString("FULLTEXT ")
			}
			fmt.Fprintf(&b, "KEY %s (%s)", quoteIdent(idx.SQLName()), formatIndexColumns(idx))
		}
	}
	b.WriteString(")")
	if info.Engine != "" {
		b.WriteString(" ENGINE=" + info.Engine)
	}
	if info.Charset != "" {
		b.WriteString(" DEFAULT CHARSET=" + info.Charset)
	}
	if info.DefaultCollation != "" {
		b.WriteString(" COLLATE " + string(info.DefaultCollation))
	}
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
		return s.pragmaPageSize(stmt)
	case "collation":
		return s.pragmaCollation(stmt)
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
			"type":       NewTextValue(col.DeclaredType()),
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
		if idx.Table == tableName && !idx.Fulltext && idx.Column == colRef.Name {
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
		if idx.Table == tableName && !idx.Fulltext && !idx.hasPrefixes() && len(idx.columnNames()) == 1 && idx.Column == name {
			return idx
		}
	}
	return nil
}

func (s *Schema) Explain(stmt *ExplainStmt) (*ResultSet, error) {
	if result, handled, err := s.mysqlExplain(stmt); handled {
		return result, err
	}
	if !stmt.Analyze {
		switch stmt.Stmt.(type) {
		case *CreateIndexStmt, *DropIndexStmt, *AlterTableStmt:
			return s.explainSchemaChange(stmt.Stmt)
		}
	}
	if compound, ok := stmt.Stmt.(*UnionStmt); ok {
		cp := *stmt
		cp.Stmt = selectQuery(compound)
		return s.Explain(&cp)
	}
	if sel, ok := stmt.Stmt.(*SelectStmt); ok && sel.With != nil {
		var restore func()
		var err error
		if stmt.Analyze {
			restore, err = s.enterCTEs(sel.With)
		} else {
			restore, err = s.enterCTEMetadata(sel.With)
		}
		if err != nil {
			return nil, err
		}
		defer restore()
		cp := *sel
		cp.With = nil
		resolved, err := s.resolveCTETables(&cp)
		if err != nil {
			return nil, err
		}
		ex := *stmt
		ex.Stmt = resolved
		return s.Explain(&ex)
	}
	if stmt.Analyze {
		if _, ok := stmt.Stmt.(*SelectStmt); !ok {
			return nil, fmt.Errorf("EXPLAIN ANALYZE requires a SELECT statement")
		}
		if s.profile == nil {
			s.StartQueryProfile()
			defer s.StopQueryProfile()
		}
	}
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
		join := s.makeJoinPlan(inner)
		tuple := s.tuplePlan(inner, info)
		covering := s.coveringPlan(inner, tname, info)
		_, point := s.primaryKeyLookup(inner.Where, tname, info)
		ordered := false
		if join != nil {
			addRow("JoinPlan", 0, 0, 0, join.description(), "", "estimated order; selective input counts refine execution")
			var mask uint64
			for _, i := range join.order {
				r := join.relations[i]
				reader := s.joinReader(r, join.pairs(i, mask))
				access := "SCAN"
				if len(reader.columns) > 0 {
					access = "PRIMARY"
					if reader.index != nil {
						access = reader.index.Name
					}
				} else if len(r.paths) > 0 {
					access = "PRIMARY"
					if r.paths[0].index != nil {
						access = r.paths[0].index.Name
					}
				}
				addRow("JoinAccess", 0, 0, 0, access, "", r.alias+": index probes or hash input chosen by cardinality")
				mask |= 1 << i
			}
		} else if ft := s.fulltextPlanIndex(inner.Where, info); ft != nil && !point {
			addRow("OpenFulltext", 0, columnCount, 0, ft.SQLName(), "", "table="+tname)
			addRow("PostingSeek", 0, 0, 0, ft.SQLName(), "", "inverted term postings; residual document verification")
		} else if tuple != nil {
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

		if join != nil || tuple != nil || point {
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
				return nil, execErr
			}
			s.appendProfileResult(rs)
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
			} else if plan := s.tuplePlan(&SelectStmt{From: inner.From, Where: inner.Where, Limit: -1}, info); plan != nil {
				addRow("SeekGE", 0, 0, 0, plan.description(), "", "bounded index access")
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
			} else if plan := s.tuplePlan(&SelectStmt{From: inner.Table, Where: inner.Where, Limit: -1}, info); plan != nil {
				addRow("SeekGE", 0, 0, 0, plan.description(), "", "bounded index access")
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
			if err != nil || len(stmts) != 1 {
				return fmt.Errorf("invalid persisted view %q: %v", vi.Name, err)
			}
			switch stmts[0].(type) {
			case *SelectStmt, *UnionStmt:
				vi.Parsed = selectQuery(stmts[0])
			default:
				return fmt.Errorf("invalid persisted view query %q", vi.Name)
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
	lastID := s.mysqlFunctions.lastID
	defer func() { s.mysqlFunctions.lastID = lastID }()
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
	namespaces      map[string]bool
	tables          map[string]*TableInfo
	indexes         map[string]*IndexInfo
	views           map[string]*ViewInfo
	triggers        map[string]*TriggerInfo
	stats           map[string]int64
	distributions   map[string]*tableStatistics
	autoincUnsigned map[string]uint64
	autoinc, rowIDs map[string]int64
	userVersion     int64
	collation       Collation
	nextObjectID    uint64
	lookups         *catalogLookups
}

// Captured catalogs are immutable until a DDL operation copies them. Ordinary
// row statements only snapshot their small mutable counters.
func (s *Schema) captureState() *schemaState {
	if !s.catalogShared {
		s.catalogShared = true
	}
	return s.persistentTemporaryState(&schemaState{namespaces: s.namespaces, tables: s.tables, indexes: s.indexes, views: s.views, triggers: s.triggers,
		stats: maps.Clone(s.stats), distributions: maps.Clone(s.distributions), autoinc: maps.Clone(s.autoincMax), autoincUnsigned: maps.Clone(s.autoincUnsigned), rowIDs: maps.Clone(s.rowIDCount), userVersion: s.userVersion, collation: s.collation, nextObjectID: s.nextObjectID, lookups: s.catalogLookup})
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
		c.Constraints = slices.Clone(v.Constraints)
		for i := range c.Constraints {
			c.Constraints[i].Columns = slices.Clone(v.Constraints[i].Columns)
			c.Constraints[i].PrefixLengths = slices.Clone(v.Constraints[i].PrefixLengths)
			c.Constraints[i].RefColumns = slices.Clone(v.Constraints[i].RefColumns)
		}
		c.Columns = slices.Clone(v.Columns)
		for i := range c.Columns {
			c.Columns[i].RefColumns = slices.Clone(v.Columns[i].RefColumns)
			c.Columns[i].EnumValues = slices.Clone(v.Columns[i].EnumValues)
			c.Columns[i].ConstraintNames = maps.Clone(v.Columns[i].ConstraintNames)
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
		c.PrefixLengths = slices.Clone(v.PrefixLengths)
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
	s.namespaces = st.namespaces
	s.tables = st.tables
	s.catalogShared = true
	s.catalogLookup = st.lookups
	s.indexes = st.indexes
	s.views = st.views
	s.triggers = st.triggers
	s.stats = st.stats
	s.distributions = st.distributions
	s.autoincMax = st.autoinc
	s.autoincUnsigned = st.autoincUnsigned
	s.rowIDCount = st.rowIDs
	s.userVersion = st.userVersion
	s.collation = st.collation
	s.nextObjectID = st.nextObjectID
	s.overlayTemporaryState(st)
}
func (s *Schema) Begin() error {
	return s.BeginWithOptions(&BeginStmt{})
}

func (s *Schema) BeginWithOptions(stmt *BeginStmt) error {
	if s.inTx {
		return fmt.Errorf("already in transaction")
	}
	level := s.IsolationLevel()
	if s.nextIsolation != "" {
		level = s.nextIsolation
	}
	if stmt.Isolation != "" {
		var err error
		level, err = ParseIsolationLevel(string(stmt.Isolation))
		if err != nil {
			return err
		}
	}
	if stmt.ConsistentSnapshot && level != RepeatableRead {
		return fmt.Errorf("WITH CONSISTENT SNAPSHOT requires REPEATABLE READ")
	}
	s.nextIsolation = ""
	s.txIsolation = level
	s.txState = s.captureState()
	s.txTemporary = s.transactionalTemporarySnapshot()
	s.inTx = true
	s.postgresTransactionTime = s.statementTime
	if s.postgresTransactionTime.IsZero() {
		s.postgresTransactionTime = time.Now().UTC()
	}
	s.txWrites = nil
	s.txLatest = nil
	s.savepoints = nil
	s.txRowVersions = make(map[string]uint64)
	s.txHasWrites = false
	s.txHasDDL = false
	if stmt.ConsistentSnapshot {
		if err := s.ensureConsistentSnapshot(); err != nil {
			s.endTransaction()
			return err
		}
	}
	return nil
}
func (s *Schema) Commit() error {
	if !s.inTx {
		return fmt.Errorf("not in a transaction")
	}

	if err := s.checkDeferredForeignKeys(); err != nil {
		return err
	}
	if err := s.prepareTemporaryCommit(); err != nil {
		return err
	}
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
		if len(batch) > 0 || s.hasPendingTemporaryCleanup() {
			if err := s.writeBatchWithOnlineIndexes(batch); err != nil {
				return err
			}
			s.parent.commitVersion.Add(1)
		}
		s.finishTemporaryCommit()
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
	queueAutoinc := s.enableAutoIncrementGroupCommit && s.parent != nil && s.lockHandler != nil && !s.hasTemporaryTables() && !changed &&
		(!reflect.DeepEqual(s.autoincMax, s.txState.autoinc)) && reflect.DeepEqual(s.autoincUnsigned, s.txState.autoincUnsigned)
	if queueAutoinc {
		// Earlier queued counter snapshots can contain higher IDs than this
		// transaction. Keep the complete metadata records monotonic in the
		// same order as their row batches, including across different tables.
		s.autoincMax = mergeCounter(nil, s.autoincMax, root.autoincQueued)
		root.autoincQueued = maps.Clone(s.autoincMax)
	}
	if len(s.txWrites) == 0 && !persistent && !s.hasPendingTemporaryCleanup() {
		s.finishTemporaryCommit()
		s.endTransaction()
		return nil
	}
	batch := make([]storage.Mutation, 0, len(s.txWrites)+7)
	for _, w := range s.txWrites {
		batch = append(batch, storage.Mutation{Key: w.key, Value: w.value, Delete: w.del})
	}
	currentState := s.captureState()
	for _, meta := range []struct {
		key    string
		value  any
		before any
	}{
		{namespacesPrefix, s.namespaces, s.txState.namespaces},
		{schemaPrefix, currentState.tables, s.txState.tables},
		{schemaIdxPrefix, currentState.indexes, s.txState.indexes},
		{schemaViewsPrefix, s.views, s.txState.views},
		{schemaTrigPrefix, s.triggers, s.txState.triggers},
		{autoincPrefix, currentState.autoinc, s.txState.autoinc},
		{unsignedAutoincPrefix, currentState.autoincUnsigned, s.txState.autoincUnsigned},
		{statsPrefix, currentState.stats, s.txState.stats},
		{distributionPrefix, distributionMetadata{1, currentState.distributions}, distributionMetadata{1, s.txState.distributions}},
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
	if s.nextObjectID != s.txState.nextObjectID {
		batch = append(batch, storage.Mutation{Key: []byte(objectIDPrefix), Value: binary.BigEndian.AppendUint64(nil, s.nextObjectID)})
	}
	if s.userVersion != s.txState.userVersion {
		version := make([]byte, 8)
		binary.LittleEndian.PutUint64(version, uint64(s.userVersion))
		batch = append(batch, storage.Mutation{Key: []byte(versionPrefix), Value: version})
	}
	if s.collation != s.txState.collation {
		encoded, err := json.Marshal(s.collation)
		if err != nil {
			return err
		}
		batch = append(batch, storage.Mutation{Key: []byte(collationPrefix), Value: encoded})
	}
	if queueAutoinc && s.onlyAutoincMetadataChanged() {
		// The logical catalog lock remains held until transaction completion.
		// Release the publication mutex only after queue insertion, so other
		// automatic-key transactions can join the same WAL group. Publish the
		// committed counters after WAL success, merging completion order.
		err := s.writeBatchWithOnlineIndexesQueued(batch, root.commitMu.Unlock)
		root.commitMu.Lock()
		if err != nil {
			return err
		}
		root.autoincMax = mergeCounter(nil, s.autoincMax, root.autoincMax)
		root.stateVersion++
		root.commitVersion.Add(1)
		s.finishTemporaryCommit()
		s.endTransaction()
		return nil
	}
	if err := s.writeBatchWithOnlineIndexes(batch); err != nil {
		return err
	}
	if changed {
		// Exclusive catalog ownership excludes outstanding queued inserts.
		// Dropped or renamed tables must not inherit old counter snapshots.
		root.autoincQueued = nil
	}
	if s.parent != nil {
		if persistent {
			state := s.captureState()
			// Row-ID reservations are a shared, monotonic runtime cache; do not
			// copy every table's counters into each session and transaction.
			state.rowIDs = s.parent.rowIDCount
			s.parent.restoreState(state)
			if changed {
				s.parent.rowIDMu.Lock()
				present := make(map[string]bool)
				for name := range s.tables {
					present[s.rowCounterKey(name)] = true
				}
				for counter := range s.parent.rowIDReservations {
					if !present[counter] {
						delete(s.parent.rowIDReservations, counter)
						delete(s.parent.rowIDCount, counter)
					}
				}
				s.parent.rowIDMu.Unlock()
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
	s.finishTemporaryCommit()
	s.endTransaction()
	return nil
}
func (s *Schema) Rollback() error {
	if !s.inTx {
		return fmt.Errorf("not in a transaction")
	}
	s.restoreTransactionalTemporary(s.txTemporary)
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
	s.savepoints = append(s.savepoints, savepoint{name: strings.ToLower(name), pos: len(s.txWrites), state: s.captureState(), temporary: s.transactionalTemporarySnapshot(), outer: outer, constraintModes: maps.Clone(s.foreignConstraintModes)})
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
	s.restoreTransactionalTemporary(sp.temporary)
	s.restoreState(sp.state)
	s.foreignConstraintModes = maps.Clone(sp.constraintModes)
	s.truncateTxWrites(sp.pos)
	s.resetRolledBackWrites()
	s.savepoints = s.savepoints[:idx+1]
	// Keep a fresh copy so repeated rollbacks do not mutate the saved state.
	s.savepoints[idx].state = s.captureState()
	s.savepoints[idx].temporary = s.transactionalTemporarySnapshot()
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
	temporaryBefore := s.snapshotTemporaryDefinitions()
	pos := len(s.txWrites)
	foreignStart := len(s.foreignChanges)
	defer func() {
		if len(s.foreignChanges) > foreignStart {
			clear(s.foreignChanges[foreignStart:])
			s.foreignChanges = s.foreignChanges[:foreignStart]
		}
	}()
	modesBefore := maps.Clone(s.foreignConstraintModes)
	err := run()
	if err == nil {
		err = s.processForeignActions(foreignStart)
	}
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
		s.foreignConstraintModes = modesBefore
		s.temporary = temporaryBefore
		s.restoreState(state)
		s.truncateTxWrites(pos)
		if implicit {
			s.Rollback()
		}
		return err
	}
	if implicit {
		if err := s.Commit(); err != nil {
			s.temporary = temporaryBefore
			s.Rollback()
			return err
		}
	}
	return nil
}
func (s *Schema) StatementChanges() int64 { return s.statementChanges }
func (s *Schema) write(key, value []byte) error {
	if s.temporary != nil && s.temporary.building && len(key) > 0 && key[0] == 0 {
		return nil
	}
	if s.inTx {
		s.txPut(key, value)
		return nil
	}
	if s.temporaryKey(key) {
		return s.lsm.WriteBatch([]storage.Mutation{{Key: key, Value: value, NoWAL: true}})
	}
	return s.lsm.Put(key, value)
}
func (s *Schema) remove(key []byte) error {
	if s.temporary != nil && s.temporary.building && len(key) > 0 && key[0] == 0 {
		return nil
	}
	if s.inTx {
		s.txDelete(key)
		return nil
	}
	if s.temporaryKey(key) {
		return s.lsm.WriteBatch([]storage.Mutation{{Key: key, Delete: true, NoWAL: true}})
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
	return s.profileReader(s.committedIterator(), "Scan")
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
	for len(s.foreignChanges) > 0 && s.foreignChanges[len(s.foreignChanges)-1].position > pos {
		s.foreignChanges = s.foreignChanges[:len(s.foreignChanges)-1]
	}
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
	return s.profileReader(s.txIterator(s.committedIterator(), nil, nil), "Scan")
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
	deletes, err := s.snapshotIndexDeletes()
	if err != nil {
		return &transactionIterator{base: base, err: err}
	}
	for key := range deletes {
		data := []byte(key)
		if lower != nil && bytes.Compare(data, lower) < 0 || upper != nil && bytes.Compare(data, upper) >= 0 {
			continue
		}
		if _, explicit := s.txLatest[key]; !explicit {
			rows = append(rows, storage.KeyValue{Key: storage.InternalKey{UserKey: data, OpType: 0}})
		}
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
				mutations, e := s.indexRowMutations(idx, row, key, false)
				if e != nil {
					iter.Close()
					return e
				}
				for _, mutation := range mutations {
					expected[string(mutation.Key)] = mutation.Value
				}
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
		return s.writeBatchWithOnlineIndexes(batch)
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
	if s.inTx && s.hasTemporaryTables() {
		return nil
	}
	indexes := s.indexes
	if s.hasTemporaryTables() {
		indexes = s.captureState().indexes
	}
	data, err := json.Marshal(indexes)
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
		if i < len(idx.PrefixLengths) && idx.PrefixLengths[i] > 0 {
			quoted[i] += fmt.Sprintf("(%d)", idx.PrefixLengths[i])
		}
	}
	if idx.Fulltext {
		unique = "FULLTEXT "
	}
	return fmt.Sprintf("CREATE %sINDEX %s ON %s(%s)", unique, quoteIdent(idx.SQLName()), quoteIdent(idx.Table), strings.Join(quoted, ", "))
}

func tableDataPrefix(tableName string) string {
	return dataPrefix + tableName + ":"
}

func indexDataPrefix(indexName string) string {
	return indexPrefix + indexName + ":"
}

func (s *Schema) CreateIndex(stmt *CreateIndexStmt) error {
	if s.isTemporaryTable(stmt.Table) {
		before := s.temporary.building
		s.temporary.building = true
		defer func() { s.temporary.building = before }()
	}
	s.ensureCatalogWritable()
	idxName := strings.ToLower(stmt.Index)
	tableName := strings.ToLower(stmt.Table)
	columns := normalizedIndexColumns(stmt.Columns, stmt.Column)
	if s.isTemporaryTable(tableName) && s.findTableIndex(tableName, idxName) != nil {
		if stmt.IfNotExists {
			return nil
		}
		return fmt.Errorf("index %q already exists", idxName)
	}

	if _, ok := s.indexes[idxName]; ok && !s.isTemporaryTable(tableName) {
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
	idx.Fulltext = stmt.Fulltext
	if idx.Fulltext {
		idx.FulltextVersion = fulltextIndexVersion
	}
	if err := validateFulltextIndex(info, idx); err != nil {
		return err
	}
	idx.PrefixLengths = append([]int64(nil), stmt.PrefixLengths...)
	if idx.hasPrefixes() {
		idx.PrefixEncoding = 1
	}
	if err := validateIndexPrefixes(info.Columns, idx); err != nil {
		return err
	}
	s.bindIndexCollation(idx)
	var err error
	idx.ID, err = s.allocateObjectID()
	if err != nil {
		return err
	}
	if s.isTemporaryTable(tableName) {
		idx.DisplayName = idxName
		idx.Name = fmt.Sprintf("__temporary_%d", idx.ID)
		idxName = idx.Name
	}
	var indexEntries []storage.Mutation
	values := make(map[string]bool)
	prefix := []byte(s.tableDataPrefix(tableName))
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
				var signature []byte
				for i, value := range v {
					signature = appendValueIdentity(signature, idx.keyValue(columns[i], value))
				}
				if values[string(signature)] {
					iter.Close()
					return fmt.Errorf("UNIQUE constraint failed: %s.%s", tableName, strings.Join(columns, ", "))
				}
				values[string(signature)] = true
			}
		}
		mutations, e := s.indexRowMutations(idx, row, iter.Key().UserKey, false)
		if e != nil {
			iter.Close()
			return e
		}
		indexEntries = append(indexEntries, mutations...)
		iter.Next()
	}
	err = storage.IteratorError(iter)
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
	if stmt.Table != "" {
		if idx := s.findTableIndex(stmt.Table, idxName); idx != nil {
			idxName = idx.Name
		} else {
			return fmt.Errorf("index %q does not exist on %s", idxName, stmt.Table)
		}
	}
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
	if err := s.markIndexCleanup(idx); err != nil {
		return err
	}
	delete(s.indexes, idxName)
	return s.saveIndexes()
}

func renameViewSelect(sel *SelectStmt, table, newTable, oldCol, newCol, operation string) bool {
	if sel.With != nil {
		// Lexical CTE names shadow catalog table names in the entire query.
		for _, c := range sel.With.Entries {
			if c.Name == table {
				return false
			}
		}
		changed := false
		for i := range sel.With.Entries {
			if renameCTEQuery(sel.With.Entries[i].Query, table, newTable, oldCol, newCol, operation) {
				changed = true
			}
		}
		with := sel.With
		sel.With = nil
		bodyChanged := renameViewSelect(sel, table, newTable, oldCol, newCol, operation)
		sel.With = with
		return changed || bodyChanged
	}
	if sel.Compound != nil {
		return renameCTEQuery(sel.Compound, table, newTable, oldCol, newCol, operation)
	}
	changed := false
	shadowed := sel.From != table && sel.FromAlias == table
	for _, j := range sel.Joins {
		shadowed = shadowed || j.Table != table && j.Alias == table
	}
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
	direct := len(qualifiers) > 0
	var rewrite func(Expression) Expression
	rewrite = func(expr Expression) Expression {
		result, _ := mapExpression(expr, func(e Expression) (Expression, bool, error) {
			switch x := e.(type) {
			case *ColumnRefExpr:
				c := *x
				parts := strings.Split(c.Name, ".")
				if operation == "RENAME_TO" {
					if len(parts) == 2 && parts[0] == table && !shadowed {
						c.Name = newTable + "." + parts[1]
						changed = changed || c.Name != x.Name
					}
				}
				if operation == "RENAME_COLUMN" {
					if len(parts) == 1 && direct && parts[0] == oldCol {
						c.Name = newCol
						changed = true
					} else if len(parts) == 2 && parts[1] == oldCol && (qualifiers[parts[0]] || parts[0] == table && !shadowed) {
						c.Name = parts[0] + "." + newCol
						changed = true
					}
				}
				return &c, true, nil
			case *SubqueryExpr:
				changed = renameViewSelect(x.Stmt, table, newTable, oldCol, newCol, operation) || changed
				return x, true, nil
			case *ExistsExpr:
				changed = renameViewSelect(x.Stmt, table, newTable, oldCol, newCol, operation) || changed
				return x, true, nil
			}
			return e, false, nil
		})
		return result
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
	for i := range sel.Windows {
		sel.Windows[i].Spec, _ = mapWindowSpec(sel.Windows[i].Spec, func(expr Expression) (Expression, error) {
			return rewrite(expr), nil
		})
	}
	return changed
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
	if stmt.With != nil {
		restore, err := s.enterCTEs(stmt.With)
		if err != nil {
			return nil, err
		}
		defer restore()
		cp := *stmt
		cp.With = nil
		return s.Union(&cp)
	}
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

	for i := range left.Columns {
		name, label := left.Columns[i].Name, left.Columns[i].DisplayName
		t := describeCommon(left.Columns[i], right.Columns[i])
		t.Name, t.DisplayName = name, label
		t.SemanticType = t.LogicalType().Kind
		left.Columns[i] = t
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
		left.Rows = s.intersectRows(left.Rows, right.Rows)
	case "EXCEPT":
		left.Rows = s.exceptRows(left.Rows, right.Rows)
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
		window := int64(-1)
		if stmt.Limit >= 0 {
			window = limitWindow(stmt.Limit, stmt.Offset)
		}
		left.Rows, _, err = s.sortBounded(left.Rows, nil, orders, window)
		if err != nil {
			return nil, err
		}
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

func (s *Schema) intersectRows(a, b []map[string]Value) []map[string]Value {
	seen := make(map[string]bool)
	for _, r := range b {
		seen[s.rowKey(r)] = true
	}
	var out []map[string]Value
	for _, r := range a {
		if seen[s.rowKey(r)] {
			delete(seen, s.rowKey(r))
			out = append(out, r)
		}
	}
	return out
}

func (s *Schema) exceptRows(a, b []map[string]Value) []map[string]Value {
	seen := make(map[string]bool)
	for _, r := range b {
		seen[s.rowKey(r)] = true
	}
	var out []map[string]Value
	for _, r := range a {
		if !seen[s.rowKey(r)] {
			seen[s.rowKey(r)] = true
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
	if v.Type == ColUnsigned || v.Type == ColDecimal || v.Type == ColJSON {
		return string(appendClusteredValue(nil, v))
	}
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
		if v.Real == 0 {
			bits = 0
		}
		if bits&(1<<63) == 0 {
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
	if bin, ok := where.(*BinaryExpr); ok && bin.Op == "AND" {
		info := s.tables[tableName]
		for _, term := range andTerms(where) {
			if typedIndexEquality(term, info) {
				if keys, ok := s.findMatchingRowsViaIndex(term, tableName); ok {
					return keys, true
				}
			}
		}
		return nil, false
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
				if len(col.EnumValues) > 0 && (val.Type == ColInteger || val.Type == ColReal) {
					return nil, false
				}
				val = s.comparisonAffinity(val, col)
				break
			}
		}
	}
	var matchingIdx *IndexInfo
	for _, idx := range s.tableIndexes(tableName) {
		if idx.Table == tableName && !idx.Fulltext && idx.Column == colName {
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
	if p := s.preparedPlan; p != nil && p.where == where && p.info == info {
		return p.primaryKey(table, info)
	}
	return s.uncachedPrimaryKeyLookup(where, table, info)
}
func (s *Schema) uncachedPrimaryKeyLookup(where Expression, table string, info *TableInfo) ([]byte, bool) {
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
	return []byte(fmt.Sprintf("%s%020d", info.storagePrefix(tableName), fallbackID))
}

func encodeRowKey(tableName string, rowID int64) []byte {
	return []byte(fmt.Sprintf("%s%s:%020d", dataPrefix, tableName, rowID))
}

func (s *Schema) Insert(stmt *InsertStmt) (int64, *ResultSet, error) {
	if stmt.With != nil {
		restore, err := s.enterCTEs(stmt.With)
		if err != nil {
			return 0, nil, err
		}
		defer restore()
		cp := *stmt
		cp.With = nil
		return s.Insert(&cp)
	}
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

	if stmt.Ignore {
		if s.MySQLMode() {
			previous := s.ctx
			s.ctx = mysqlIgnoreContext(s.ctx)
			defer func() { s.ctx = previous }()
		}
		copy := *stmt
		copy.Ignore = false
		if copy.OnConflict == nil {
			copy.OnConflict = &OnConflictClause{DoNothing: true}
		}
		return s.Insert(&copy)
	}

	if stmt.OnConflict != nil {
		if viewInsert {
			return 0, nil, fmt.Errorf("ON CONFLICT is not supported on views")
		}
		if err := s.validateOnConflict(stmt.OnConflict, info); err != nil {
			return 0, nil, err
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
	if len(allGroups) == 0 && stmt.Source == nil {
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
	var lastUnsigned uint64
	defer func() { s.lastInsertIDUnsigned = lastUnsigned }()
	var returningRows []map[string]Value

	if stmt.Source != nil {
		if stmt.Source.Next == nil {
			return 0, nil, fmt.Errorf("nil row source")
		}
		if stmt.Source.Reset != nil {
			if err := stmt.Source.Reset(); err != nil {
				return 0, nil, err
			}
		}
	}
	groupIndex := 0
	for {
		var group []Expression
		if stmt.Source != nil {
			if err := s.checkInterrupt(); err != nil {
				return 0, nil, err
			}
			values, err := stmt.Source.Next(s.ctx)
			if err == io.EOF {
				break
			}
			if err != nil {
				return 0, nil, err
			}
			if len(values) != expected {
				return 0, nil, fmt.Errorf("expected %d values, got %d", expected, len(values))
			}
			group = make([]Expression, len(values))
			for i, v := range values {
				group[i] = &LiteralExpr{Value: v, BoundParameter: true}
			}
		} else {
			if groupIndex >= len(allGroups) {
				break
			}
			group = allGroups[groupIndex]
			groupIndex++
		}
		row := make(map[string]Value)
		for ci, col := range info.Columns {
			if vi, ok := colMap[ci]; ok && vi < len(group) {
				v, err := s.evalExpr(group[vi], nil)
				if err != nil {
					return 0, nil, err
				}
				row[col.Name] = assignmentAffinity(v, col)
			} else if col.Default != nil {
				val, err := s.evalExpr(col.Default, nil)
				if err != nil {
					return 0, nil, err
				}
				row[col.Name] = assignmentAffinity(val, col)
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
			if len(stmt.Returning) > 0 {
				returningRows = append(returningRows, row)
			}
			continue
		}

		if err := s.normalizeMySQLRow(info, row); err != nil {
			return 0, nil, err
		}
		pkCol := s.getPKColumn(info)
		generatedAuto := pkCol != nil && pkCol.AutoIncrement && s.autoValueRequested(pkCol, row[pkName(pkCol)], suppliedColumn(stmt.ColumnNames, pkName(pkCol), stmt.DefaultValues))
		if pkCol != nil && pkCol.AutoIncrement && pkCol.Unsigned {
			if err := s.prepareUnsignedAuto(tableName, pkCol, row, suppliedColumn(stmt.ColumnNames, pkCol.Name, stmt.DefaultValues)); err != nil {
				return 0, nil, err
			}
		}
		var nextID int64
		if len(info.PKColumns) == 0 || (pkCol == nil || !pkCol.Unsigned) && s.autoValueRequested(pkCol, row[pkName(pkCol)], suppliedColumn(stmt.ColumnNames, pkName(pkCol), stmt.DefaultValues)) {
			var err error
			nextID, err = s.nextRowID(tableName)
			if err != nil {
				return 0, nil, err
			}
		}

		if pkCol != nil && pkCol.AutoIncrement && !pkCol.Unsigned {
			autoMax := s.getAutoincMax(tableName)
			if autoMax >= nextID {
				nextID = autoMax + 1
			}
		}

		if pkCol != nil && pkCol.ColType == ColInteger && len(info.PKColumns) <= 1 && !(pkCol.AutoIncrement && pkCol.Unsigned) {
			pkVal := row[pkCol.Name]
			if s.autoValueRequested(pkCol, pkVal, suppliedColumn(stmt.ColumnNames, pkCol.Name, stmt.DefaultValues)) {
				if nextID <= 0 {
					return 0, nil, fmt.Errorf("integer primary key exhausted")
				}
				row[pkCol.Name] = NewIntValue(nextID)
			} else {
				if pkVal.Type != ColInteger && !(pkCol.Unsigned && pkVal.Type == ColUnsigned) {
					return 0, nil, fmt.Errorf("datatype mismatch for INTEGER PRIMARY KEY")
				}
				val := pkVal.Int
				if val > nextID-1 {
					nextID = val
				}
			}
		}

		if pkCol != nil && pkCol.ColType == ColInteger && len(info.PKColumns) == 1 && row[pkCol.Name].Type == ColInteger {
			nextID = row[pkCol.Name].Int
			if pkCol.AutoIncrement && !pkCol.Unsigned && nextID >= s.rowIDCount[s.rowCounterKey(tableName)] {
				s.rowIDCount[s.rowCounterKey(tableName)] = nextID + 1
				s.rowIDDirty = true
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
		if stmt.OnConflict != nil {
			handled, updated, err := s.handleInsertConflict(tableName, info, row, stmt.OnConflict)
			if err != nil {
				return 0, nil, err
			}
			if handled {
				// Successful conflict actions reserve generated AUTOINCREMENT IDs
				// just like ordinary insertion, including across database reopening.
				if pkCol != nil && pkCol.AutoIncrement {
					if nextID > s.getAutoincMax(tableName) {
						s.setAutoincMax(tableName, nextID)
					}
					if err := s.saveAutoincMax(); err != nil {
						return 0, nil, err
					}
				}
				if updated != nil {
					s.statementChanges += s.mysqlUpsertChanges()
					if s.MySQLMode() && pkCol != nil && pkCol.AutoIncrement && lastID == 0 {
						lastID = updated[pkCol.Name].Int
					}
					if len(stmt.Returning) > 0 {
						returningRows = append(returningRows, updated)
					}
				}
				continue
			}
		}
		if stmt.Replace {
			if err := s.deleteConflicts(tableName, info, row); err != nil {
				return 0, nil, err
			}
		}
		var constraintErr error
		if stmt.OnConflict != nil {
			// Non-unique checks ran before conflict resolution. Do not evaluate
			// volatile CHECK expressions a second time on an actual insertion.
			constraintErr = s.checkUniqueConstraintsOriginal(tableName, info, row, nil, nil)
		} else {
			constraintErr = s.checkConstraints(tableName, info, row, nil)
		}
		if constraintErr != nil {
			return 0, nil, constraintErr
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
		if generatedAuto {
			s.noteMySQLGeneratedID(row[pkCol.Name])
		}
		if pkCol != nil && row[pkCol.Name].Type == ColUnsigned && (!s.MySQLMode() || pkCol.AutoIncrement) && lastUnsigned == 0 && (!s.MySQLMode() || lastID == 0) {
			lastUnsigned = row[pkCol.Name].Uint
		}
		if !s.MySQLMode() {
			lastID = nextID
		} else if pkCol != nil && pkCol.AutoIncrement && lastID == 0 {
			lastID = nextID
		}
		if len(stmt.Returning) > 0 {
			returningRows = append(returningRows, row)
		}
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
	return s.checkConstraintsOriginalPreserving(tableName, info, row, rowKey, original, nil, nil)
}

func (s *Schema) checkConstraintsOriginalPreserving(tableName string, info *TableInfo, row map[string]Value, rowKey []byte, original, preUpdate map[string]Value, assigned map[string]bool) error {
	if err := s.checkNonUniqueConstraintsPreserving(tableName, info, row, preUpdate, assigned); err != nil {
		return err
	}
	return s.checkUniqueConstraintsOriginal(tableName, info, row, rowKey, original)
}

func (s *Schema) checkUniqueConstraintsOriginal(tableName string, info *TableInfo, row map[string]Value, rowKey []byte, original map[string]Value) error {
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

	pkCol := s.getPKColumn(info)
	if pkCol != nil && len(info.PKColumns) <= 1 {
		if pkCol.ColType == ColInteger {
			v := row[pkCol.Name]
			if v.Null || v.Type != ColInteger && !(pkCol.Unsigned && v.Type == ColUnsigned) {
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
			if idx.hasPrefixes() {
				if err := s.checkPrefixUnique(idx, row, rowKey); err != nil {
					return err
				}
				continue
			}
			if err := checkUnique(idx.columnNames()); err != nil {
				return err
			}
		}
	}

	if len(info.PKColumns) > 1 {
		if err := checkUnique(info.PKColumns); err != nil {
			return err
		}
	}
	for _, cols := range info.TableUnique {
		if err := checkUnique(cols); err != nil {
			return err
		}
	}

	return nil
}

func (s *Schema) checkNonUniqueConstraints(tableName string, info *TableInfo, row map[string]Value) error {
	return s.checkNonUniqueConstraintsPreserving(tableName, info, row, nil, nil)
}

func (s *Schema) checkNonUniqueConstraintsPreserving(tableName string, info *TableInfo, row, original map[string]Value, assigned map[string]bool) error {
	if err := validatePrimaryKey(info, row); err != nil {
		return err
	}
	if err := validateRowTypesPreserving(info, row, original, assigned); err != nil {
		return err
	}
	if err := s.validateIndexedRow(tableName, row); err != nil {
		return err
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

	if len(colNames) > 1 {
		values, hasNull := indexValues(colNames, row)
		if hasNull {
			return nil
		}
		for _, idx := range s.tableIndexes(tableName) {
			if idx.IsUnique && !idx.hasPrefixes() && slices.Equal(idx.columnNames(), colNames) {
				prefix := []byte(indexTuplePrefix(idx, values))
				it := s.newRangeIterator(prefix, prefixSuccessor(prefix), false)
				defer it.Close()
				for it.Valid() {
					key := it.Key().UserKey
					if !bytes.Equal(key[len(prefix):], excludeKey) {
						return fmt.Errorf("UNIQUE constraint failed: %s.%s", tableName, strings.Join(colNames, ","))
					}
					it.Next()
				}
				return storage.IteratorError(it)
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
		if idx.Table == tableName && !idx.hasPrefixes() && !idx.Fulltext && len(idx.columnNames()) == 1 && idx.Column == colName {
			return idx
		}
	}
	return nil
}

func (s *Schema) checkUniqueViaIndex(tableName, colName string, val Value, excludeKey []byte, idx *IndexInfo) error {
	if val.Null {
		return nil
	}
	seekPrefix := []byte(indexValuePrefix(idx, val))
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
	prefix := s.tableDataPrefix(tableName)
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
			if info.binaryColumn(cn) {
				ev.Binary, rv.Binary = true, true
			}
			if ev.Null || rv.Null {
				match = false
				break
			}
			if s.valueCompare(ev, rv) != 0 {
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
		if idx.Fulltext {
			if err := s.writeIndexRow(idx, row, rowKey, !isInsert); err != nil {
				return err
			}
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
		if idx.Fulltext {
			if oldRow != nil {
				if err := s.writeIndexRow(idx, oldRow, oldKey, true); err != nil {
					return err
				}
			}
			if err := s.writeIndexRow(idx, row, key, false); err != nil {
				return err
			}
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
	var lastUnsigned uint64
	defer func() { s.lastInsertIDUnsigned = lastUnsigned }()
	var returningRows []map[string]Value

	pkCol := s.getPKColumn(info)

	for _, srcRow := range rs.Rows {
		var nextID int64
		if pkCol != nil && pkCol.AutoIncrement && !pkCol.Unsigned {
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
			row[col.Name] = assignmentAffinity(val, col)
		}

		if err := s.normalizeMySQLRow(info, row); err != nil {
			return 0, nil, err
		}

		generatedAuto := pkCol != nil && pkCol.AutoIncrement && s.autoValueRequested(pkCol, row[pkName(pkCol)], suppliedColumn(stmt.ColumnNames, pkName(pkCol), false))
		if pkCol != nil && pkCol.AutoIncrement && pkCol.Unsigned {
			if err := s.prepareUnsignedAuto(tableName, pkCol, row, suppliedColumn(stmt.ColumnNames, pkCol.Name, false)); err != nil {
				return 0, nil, err
			}
		}
		if len(info.PKColumns) == 0 || (pkCol == nil || !pkCol.Unsigned) && s.autoValueRequested(pkCol, row[pkName(pkCol)], suppliedColumn(stmt.ColumnNames, pkName(pkCol), false)) {
			var err error
			nextID, err = s.nextRowID(tableName)
			if err != nil {
				return 0, nil, err
			}
			if pkCol != nil && pkCol.AutoIncrement && s.getAutoincMax(tableName) >= nextID {
				nextID = s.getAutoincMax(tableName) + 1
			}
		}
		if pkCol != nil && pkCol.ColType == ColInteger && len(info.PKColumns) == 1 {
			pkVal := row[pkCol.Name]
			if !pkCol.Unsigned && s.autoValueRequested(pkCol, pkVal, suppliedColumn(stmt.ColumnNames, pkCol.Name, false)) {
				if nextID <= 0 {
					return 0, nil, fmt.Errorf("integer primary key exhausted")
				}
				row[pkCol.Name] = NewIntValue(nextID)
			}
		}

		if pkCol != nil && pkCol.ColType == ColInteger && len(info.PKColumns) == 1 && row[pkCol.Name].Type == ColInteger {
			nextID = row[pkCol.Name].Int
			if pkCol.AutoIncrement && !pkCol.Unsigned && nextID >= s.rowIDCount[s.rowCounterKey(tableName)] {
				s.rowIDCount[s.rowCounterKey(tableName)] = nextID + 1
				s.rowIDDirty = true
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
		if stmt.OnConflict != nil {
			handled, updated, err := s.handleInsertConflict(tableName, info, row, stmt.OnConflict)
			if err != nil {
				return 0, nil, err
			}
			if handled {
				// Successful conflict actions reserve generated AUTOINCREMENT IDs
				// just like ordinary insertion, including across database reopening.
				if pkCol != nil && pkCol.AutoIncrement {
					if nextID > s.getAutoincMax(tableName) {
						s.setAutoincMax(tableName, nextID)
					}
					if err := s.saveAutoincMax(); err != nil {
						return 0, nil, err
					}
				}
				if updated != nil {
					s.statementChanges += s.mysqlUpsertChanges()
					if s.MySQLMode() && pkCol != nil && pkCol.AutoIncrement && lastID == 0 {
						lastID = updated[pkCol.Name].Int
					}
					returningRows = append(returningRows, updated)
				}
				continue
			}
		}
		if stmt.Replace {
			if err := s.deleteConflicts(tableName, info, row); err != nil {
				return 0, nil, err
			}
		}
		var constraintErr error
		if stmt.OnConflict != nil {
			// Non-unique checks ran before conflict resolution. Do not evaluate
			// volatile CHECK expressions a second time on an actual insertion.
			constraintErr = s.checkUniqueConstraintsOriginal(tableName, info, row, nil, nil)
		} else {
			constraintErr = s.checkConstraints(tableName, info, row, nil)
		}
		if constraintErr != nil {
			return 0, nil, constraintErr
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
		if generatedAuto {
			s.noteMySQLGeneratedID(row[pkCol.Name])
		}
		if pkCol != nil && row[pkCol.Name].Type == ColUnsigned && (!s.MySQLMode() || pkCol.AutoIncrement) && lastUnsigned == 0 && (!s.MySQLMode() || lastID == 0) {
			lastUnsigned = row[pkCol.Name].Uint
		}
		if !s.MySQLMode() {
			lastID = nextID
		} else if pkCol != nil && pkCol.AutoIncrement && lastID == 0 {
			lastID = nextID
		}
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
	s.returningMetadata(cols, info, rs)
	uniqueResultColumns(rs.Columns, expressions)
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
	if err := s.bindFulltextExpressions(stmt); err != nil {
		return err
	}
	p := stmt.prepared
	if p != nil && p.validated {
		return nil
	}
	err := s.validateQueryUncached(stmt, info)
	if err == nil && p != nil {
		p.validated = true
	}
	return err
}
func (s *Schema) validateQueryUncached(stmt *SelectStmt, info *TableInfo) error {
	columns := make(map[string]int)
	qualified := make(map[string]bool)
	needQualified := needsQualifiedRows(stmt)
	aliases := make(map[string]bool)
	add := func(defs []ColumnDef, alias string) {
		for _, col := range defs {
			if strings.Contains(col.Name, ".") {
				qualified[strings.ToLower(col.Name)] = true
				_, name, _ := strings.Cut(col.Name, ".")
				columns[strings.ToLower(name)]++
			} else {
				columns[strings.ToLower(col.Name)]++
			}
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
			if !isAggregate(e) && e.Window == nil {
				if err := validateResolvedFunction(s.Dialect(), e); err != nil {
					return err
				}
			}
			if err := validate(e.GroupConcatSeparator, allowAlias); err != nil {
				return err
			}
			for _, order := range e.GroupConcatOrder {
				if err := validate(order.Expr, allowAlias); err != nil {
					return err
				}
			}
			for _, arg := range e.Args {
				if err := validate(arg, allowAlias); err != nil {
					return err
				}
			}
			for _, arg := range windowExpressions(e.Window) {
				if err := validate(arg, false); err != nil {
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

func (s *Schema) Select(stmt *SelectStmt) (result *ResultSet, err error) {
	return s.selectInto(stmt, nil)
}

// SelectInto may reuse dst's columns, row maps and BLOB buffers for a prepared
// point SELECT. The caller gives up the old contents on entry. Other SELECT
// shapes use their normal executor and replace dst with an owned result.
// dst and all of its buffers must belong exclusively to this call.
func (s *Schema) SelectInto(stmt *SelectStmt, dst *ResultSet) (*ResultSet, error) {
	if dst == nil {
		return s.Select(stmt)
	}
	result, err := s.selectInto(stmt, dst)
	if err == nil && result != dst {
		*dst = *result
	}
	if err != nil {
		return nil, err
	}
	return dst, nil
}

func (s *Schema) selectInto(stmt *SelectStmt, dst *ResultSet) (result *ResultSet, err error) {
	restoreReadView, err := s.selectReadView(stmt)
	if err != nil {
		return nil, err
	}
	defer restoreReadView()
	if s.selectDepth == 0 && !s.statementActive {
		s.mysqlRand = nil
		s.statementTime = time.Now().UTC()
		defer func() { s.statementTime = time.Time{}; s.mysqlRand = nil }()
	}
	if s.replicaReadOnly && !IsReplicaReadStatement(stmt) {
		return nil, fmt.Errorf("locking reads require the Raft leader")
	}
	if stmt.With != nil {
		restore, err := s.enterCTEs(stmt.With)
		if err != nil {
			return nil, err
		}
		defer restore()
		cp := *stmt
		cp.With = nil
		return s.Select(&cp)
	}
	if stmt.materialized != nil {
		return copyCTEResult(stmt.materialized), nil
	}
	if stmt.Compound != nil {
		return s.Union(stmt.Compound)
	}
	stmt, err = s.resolveCatalogTables(stmt)
	if err != nil {
		return nil, err
	}
	stmt, err = s.resolveCTETables(stmt)
	if err != nil {
		return nil, err
	}
	if finish := s.profileSelect(stmt); finish != nil {
		defer func() { finish(result, err) }()
	}
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
	if s.selectDepth == 0 {
		defer s.clearSelectCaches()
	}
	s.selectDepth++
	defer func() { s.selectDepth-- }()
	if s.selectDepth > 100 {
		return nil, fmt.Errorf("query nesting limit exceeded")
	}
	if stmt.JoinGroup {
		return s.selectJoinGroup(stmt)
	}
	// Prepared physical-table point reads can reuse their validated projection
	// without a heap-allocated statement copy and general scan/result scaffolding.
	// All paths still run inside the normal statement snapshot and lifecycle.
	planUsed := false
	if stmt.prepared != nil && stmt.FromExpr == nil {
		if info := s.tables[strings.ToLower(stmt.From)]; info != nil {
			defer s.restorePreparedPlan(s.usePreparedPlan(stmt.prepared, info))
			planUsed = true
			if rs, used, err := s.selectPreparedPoint(stmt, info, dst); used || err != nil {
				return rs, err
			}
		}
	}
	copyStmt := *stmt
	stmt = &copyStmt
	if err := resolveWindows(stmt); err != nil {
		return nil, err
	}
	windowed := hasWindows(stmt)
	trackKeys := stmt.Lock != nil || stmt.trackRows
	needQualified := needsQualifiedRows(stmt)
	orderedIndex := false
	coveringProjection := false
	conditionSatisfied := false
	expressionsReady := false
	rs := &ResultSet{}
	tableName := strings.ToLower(stmt.From)

	if tableName == "sqlite_master" || tableName == "sqlite_stat1" {
		if !windowed {
			if tableName == "sqlite_master" {
				return s.selectFromSqliteMaster(stmt)
			}
			return s.selectFromStat1(stmt)
		}
		stmt.FromExpr = &SubqueryExpr{Stmt: &SelectStmt{From: tableName, SelectAll: true, Columns: []SelectColumn{{Star: true}}, Limit: -1}}
	}

	info, ok := s.tables[tableName]
	if info == nil {
		if sub, ok := stmt.FromExpr.(*SubqueryExpr); ok && sub.Stmt.materialized != nil {
			info = &TableInfo{Columns: sub.Stmt.materialized.Columns}
		}
	}
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

	if stmt.prepared != nil && info != nil && !planUsed {
		defer s.restorePreparedPlan(s.usePreparedPlan(stmt.prepared, info))
	}

	if stmt.FromExpr == nil {
		if err := s.validateQuery(stmt, info); err != nil {
			return nil, err
		}
	}

	var exprCols []selectExprCol
	var duplicateProjection bool
	rs.Columns, exprCols, duplicateProjection = s.selectProjection(stmt, info)
	stmt.OrderBy = append([]OrderByClause(nil), stmt.OrderBy...)
	if err := validateGrouping(stmt, info, rs.Columns); err != nil {
		return nil, err
	}
	// A physical row already contains unaliased, unqualified column values.
	// Keep that caller-owned decoded map through filtering/sorting, then prune
	// unused fields at the final projection. No expression can overwrite it.
	// Bound linear membership checks for wide projections; their normal map
	// projection stays linear in the number of selected fields.
	identityProjection := len(exprCols) > 0 && len(exprCols) <= 8 && info != nil && stmt.FromExpr == nil && len(stmt.Joins) == 0 &&
		!windowed && !needQualified && len(stmt.GroupBy) == 0 && stmt.Having == nil && !hasAggregates(stmt)
	if identityProjection {
		for _, ec := range exprCols {
			ref, ok := ec.expr.(*ColumnRefExpr)
			if !ok || ref.Name != ec.name {
				identityProjection = false
				break
			}
			found := false
			for _, col := range info.Columns {
				if col.Name == ref.Name {
					found = true
					break
				}
			}
			if !found {
				identityProjection = false
				break
			}
		}
	}
	if sorted, used, sortErr := s.streamOrderedSelectIfNoWindows(stmt, info, exprCols, rs.Columns); used || sortErr != nil {
		if sortErr != nil {
			return nil, sortErr
		}
		rs.Rows = sorted
		return rs, nil
	}
	// Aliases are legal in ORDER BY, GROUP BY and HAVING. Expression evaluation
	// below validates references and preserves any error instead of hiding it.

	prefix := s.tableDataPrefix(tableName)
	rows := make([]map[string]Value, 0)
	var rowKeys [][]string

	streamedGroups := false
	streamedRead := false
	if info != nil && stmt.FromExpr == nil && tableName != "" {
		var streamErr error
		rows, streamedGroups, streamErr = s.streamAggregateScan(stmt, info, exprCols)
		if streamErr != nil {
			return nil, streamErr
		}
		if !streamedGroups {
			rows, streamedRead, streamErr = s.streamSelectScan(stmt, info)
			if streamErr != nil {
				return nil, streamErr
			}
		}
	}
	if streamedGroups || streamedRead {
		// Group output continues through the common DISTINCT/ORDER/LIMIT path.
	} else if plan := s.makeJoinPlanIfNoWindows(stmt); plan != nil {
		expressionsReady = !stmt.SelectAll && !hasAggregates(stmt) && len(stmt.GroupBy) == 0 && stmt.Having == nil
		for _, c := range stmt.Columns {
			if c.Star {
				expressionsReady = false
			}
		}
		orderInputs := make(map[string]bool)
		if expressionsReady {
			for _, o := range stmt.OrderBy {
				names, _ := s.expressionDependencies(o.Expr)
				for _, name := range names {
					orderInputs[name] = true
				}
			}
		}
		if err := s.walkJoinPlan(plan, func(row map[string]Value) error {
			if !expressionsReady {
				rows = append(rows, copyRow(row))
				return nil
			}
			projected := make(map[string]Value, len(exprCols)+len(orderInputs))
			for _, ec := range exprCols {
				v, err := s.evalExpr(ec.expr, row)
				if err != nil {
					return err
				}
				projected[ec.name] = v
			}
			for name := range orderInputs {
				if _, exists := projected[name]; !exists {
					if v, ok := row[name]; ok {
						projected[name] = v
					}
				}
			}
			rows = append(rows, projected)
			return nil
		}); err != nil {
			return nil, err
		}
	} else if stmt.FromExpr != nil {
		if subExpr, ok := stmt.FromExpr.(*SubqueryExpr); ok {
			oldCTEs := s.cteScope
			if viewInfo != nil {
				s.cteScope = nil
			}
			subRS, err := s.Select(subExpr.Stmt)
			s.cteScope = oldCTEs
			if err != nil {
				return nil, err
			}
			if info == nil {
				info = &TableInfo{Columns: subRS.Columns}
			}

			if !stmt.SelectAll {
				rs.Columns, exprCols, duplicateProjection = s.selectProjection(stmt, info)
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
		// Projection runs once below, after WHERE. Evaluating it here too
		// duplicates volatile functions and executes rejected rows' expressions.
		rows = append(rows, make(map[string]Value))
	} else if info != nil && tableName != "" {
		var baseRows []map[string]Value
		var baseKeys [][]string
		usedIndex := false
		accessStmt := stmt
		if windowed {
			copy := *stmt
			copy.Limit = -1
			copy.Offset = 0
			copy.OrderBy = nil
			accessStmt = &copy
		}
		preferScan := s.distributionScanPreferred(stmt, info)
		if plan := s.tuplePlan(accessStmt, info); plan != nil {
			var readErr error
			baseRows, baseKeys, orderedIndex, readErr = s.tupleRows(accessStmt, plan, trackKeys)
			if readErr != nil {
				return nil, readErr
			}
			usedIndex = true
			conditionSatisfied = true
		}
		if plan := s.coveringPlan(stmt, tableName, info); !windowed && !duplicateProjection && !usedIndex && !preferScan && plan != nil {
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

		if stmt.Where != nil && !usedIndex && !preferScan {
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

		if !usedIndex && !preferScan {
			var rangeErr error
			baseRows, usedIndex, rangeErr = s.indexRangeRows(accessStmt, tableName, info)
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
				!windowed && !hasAggregates(stmt) && stmt.Limit >= 0 {
				if rev := s.isOrderByNatural(stmt, info); rev >= 0 {
					scanLimit = limitWindow(stmt.Limit, stmt.Offset)
					scanReverse = rev == 1
				}
			}
			var scanErr error
			if trackKeys {
				baseRows, scanErr = s.scanTableLimit(tableName, info, prefix, scanLimit, scanReverse, &baseKeys)
			} else if len(stmt.Joins) > 0 {
				if needed := joinRequiredNames(stmt); needed != nil {
					baseRows, scanErr = s.scanJoinTable(tableName, stmt.FromAlias, info, needed)
				} else {
					baseRows, scanErr = s.scanTableLimit(tableName, info, prefix, scanLimit, scanReverse)
				}
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

	if !windowed && len(exprCols) > 0 && !coveringProjection && !streamedGroups && !expressionsReady && !identityProjection {
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

	if (hasAgg || hasGroupBy) && !streamedGroups {
		var inputBytes int64
		for _, row := range rows {
			inputBytes += rowMemory(row)
			if inputBytes > s.memoryBudget() {
				return nil, fmt.Errorf("query memory budget exceeded by aggregation shape without a streaming spill plan")
			}
		}
		finish := s.ProfileOperator("Aggregate", tableName)
		inputRows := int64(len(rows))
		var err error
		rows, rowKeys, err = s.processGroups(stmt, rows, info, exprCols, rowKeys)
		if finish != nil {
			finish(inputRows, int64(len(rows)), err)
		}
		if err != nil {
			return nil, err
		}
	}

	if windowed {
		if err := s.processWindows(stmt, rows, exprCols); err != nil {
			return nil, err
		}
		orderedIndex = false
	}
	if stmt.Distinct {
		distinctCols := make([]string, 0, len(rs.Columns))
		for _, col := range rs.Columns {
			distinctCols = append(distinctCols, col.Name)
		}
		var distinctErr error
		rows, rowKeys, distinctErr = s.distinctBounded(rows, distinctCols, rowKeys)
		if distinctErr != nil {
			return nil, distinctErr
		}
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
		finish := s.ProfileOperator("Sort", tableName)
		inputRows := int64(len(rows))
		window := int64(-1)
		if stmt.Lock == nil && stmt.Limit >= 0 {
			window = limitWindow(stmt.Limit, stmt.Offset)
		}
		var sortErr error
		rows, rowKeys, sortErr = s.sortBounded(rows, rowKeys, stmt.OrderBy, window)
		if finish != nil {
			finish(inputRows, int64(len(rows)), sortErr)
		}
		if sortErr != nil {
			return nil, sortErr
		}
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

	if expressionsReady {
		selected := make(map[string]bool)
		for _, col := range rs.Columns {
			selected[col.Name] = true
		}
		for _, row := range rows {
			for name := range row {
				if !selected[name] {
					delete(row, name)
				}
			}
		}
	} else if identityProjection && !coveringProjection {
		for _, row := range rows {
			for name := range row {
				selected := false
				for _, col := range rs.Columns {
					if name == col.Name {
						selected = true
						break
					}
				}
				if !selected {
					delete(row, name)
				}
			}
		}
	} else if !coveringProjection && !(stmt.SelectAll && !needQualified && info != nil && stmt.FromExpr == nil && len(stmt.Joins) == 0 && !hasAgg && !hasGroupBy) {
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
	if fc.Window != nil {
		return false
	}
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
	windowed := hasWindows(stmt)
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
				key = s.appendValueIdentity(key, v)
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
		var originalWindowBase map[string]Value
		if windowed {
			originalWindowBase = copyRow(base)
		}
		for _, ec := range exprCols {
			if containsWindow(ec.expr) {
				if err := s.preserveWindowAggregates(ec.expr, group, base); err != nil {
					return nil, nil, err
				}
				continue
			}
			expr, err := s.replaceAggregates(ec.expr, group)
			if err != nil {
				return nil, nil, err
			}
			v, err := s.evalExpr(expr, base)
			if err != nil {
				return nil, nil, err
			}
			base[ec.name] = v
			if windowed {
				base["\x00selected:"+ec.name] = v
			}
		}
		for i, order := range stmt.OrderBy {
			if containsWindow(order.Expr) {
				if err := s.preserveWindowAggregates(order.Expr, group, base); err != nil {
					return nil, nil, err
				}
				continue
			}
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
		if windowed {
			for k, v := range originalWindowBase {
				base[k] = v
			}
		}
		result = append(result, base)
		if keys != nil {
			resultKeys = append(resultKeys, groupKeys[key])
		}
	}
	for i := range stmt.OrderBy {
		if !containsWindow(stmt.OrderBy[i].Expr) && containsAggregate(stmt.OrderBy[i].Expr) {
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
	if fc.MySQLGroupConcat {
		return s.evalMySQLGroupConcat(fc, rows)
	}
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
				key := string(s.valueIdentity(v))
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
			cmp := s.valueCompare(v, best)
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
		state := &aggregateState{}
		copy := *fc
		copy.Distinct = false
		for _, v := range values {
			if err := addAggregateValue(state, &copy, v); err != nil {
				return NewNullValue(), err
			}
		}
		return aggregateValue(state, &copy)
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
			key = s.appendValueIdentity(key, row[col])
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
		k := s.rowKey(row)
		if !seen[k] {
			seen[k] = true
			out = append(out, row)
		}
	}
	return out
}
func (s *Schema) rowKey(row map[string]Value) string {
	var names []string
	for k := range row {
		names = append(names, k)
	}
	sort.Strings(names)
	var key []byte
	for _, name := range names {
		key = append(key, encodeValue(NewTextValue(name))...)
		key = s.appendValueIdentity(key, row[name])
	}
	return string(key)
}

func (s *Schema) selectCatalogProjection(stmt *SelectStmt, source *ResultSet) (*ResultSet, error) {
	copy := *stmt
	if copy.FromAlias == "" {
		copy.FromAlias = copy.From
	}
	copy.From = ""
	copy.FromExpr = &SubqueryExpr{Stmt: &SelectStmt{materialized: source}}
	return s.Select(&copy)
}
func (s *Schema) selectFromSqliteMaster(stmt *SelectStmt) (*ResultSet, error) {
	source, err := s.QuerySqliteMaster(nil)
	if err != nil {
		return nil, err
	}
	return s.selectCatalogProjection(stmt, source)
}
func (s *Schema) selectFromStat1(stmt *SelectStmt) (*ResultSet, error) {
	source := &ResultSet{Columns: []ColumnDef{{Name: "tbl", ColType: ColText}, {Name: "idx", ColType: ColText}, {Name: "stat", ColType: ColText}}}
	for name, count := range s.stats {
		source.Rows = append(source.Rows, map[string]Value{"tbl": NewTextValue(name), "idx": NewTextValue(name), "stat": NewTextValue(fmt.Sprint(count))})
	}
	return s.selectCatalogProjection(stmt, source)
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
	var rows []map[string]Value
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
		iter.Prev()
	} else {
		iter.Seek([]byte(prefix))
	}
	for n := 0; iter.Valid(); n++ {
		if n%128 == 0 {
			if err := s.checkInterrupt(); err != nil {
				return nil, err
			}
		}
		userKey := iter.Key().UserKey
		if !bytes.HasPrefix(userKey, []byte(prefix)) {
			break
		}
		rows = append(rows, decodeRow(iter.Value(), info))
		if len(keySets) > 0 {
			keys = append(keys, string(userKey))
		}
		if limit >= 0 && int64(len(rows)) >= limit {
			break
		}
		if reverse {
			iter.Prev()
		} else {
			iter.Next()
		}
	}
	if reverse {
		for i, j := 0, len(rows)-1; i < j; i, j = i+1, j-1 {
			rows[i], rows[j] = rows[j], rows[i]
		}
		for i, j := 0, len(keys)-1; i < j; i, j = i+1, j-1 {
			keys[i], keys[j] = keys[j], keys[i]
		}
	}
	for _, keySet := range keySets {
		for _, key := range keys {
			*keySet = append(*keySet, []string{key})
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
		for _, col := range s.cteQueryColumnDefs(jc.Subquery, cteScopeColumns(s.cteScope)) {
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
func (s *Schema) getJoinRows(jc JoinClause, locking bool, needed map[string]bool) ([]map[string]Value, []ColumnDef, [][]string, error) {
	if jc.Subquery != nil {
		if jc.Subquery.JoinGroup {
			copy := *jc.Subquery
			copy.trackRows = locking
			rs, err := s.Select(&copy)
			if err != nil {
				return nil, nil, nil, err
			}
			return rs.Rows, rs.Columns, rs.rowKeys, nil
		}
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
			rows, err = s.scanTableLimit(name, info, s.tableDataPrefix(name), -1, false, &keys)
		} else if needed != nil {
			rows, err = s.scanJoinTable(name, jc.Alias, info, needed)
		} else {
			rows, err = s.scanTable(name, info, nil, s.tableDataPrefix(name))
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
	return s.executeSubquery(sub, outer)
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

		cmp := s.orderValueCompare(va, vb)
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
	plan := s.tuplePlan(&SelectStmt{From: tableName, Where: where, Limit: -1}, info)
	var intervals []coveringInterval
	residual := where
	if plan != nil {
		intervals = plan.intervals
		residual = plan.residual
	} else {
		prefix := []byte(s.tableDataPrefix(tableName))
		intervals = []coveringInterval{{prefix, prefixSuccessor(prefix)}}
	}
	var rows []storage.KeyValue
	var seen map[string]bool
	if plan != nil && variableIndexColumns(info, plan.index) && len(intervals) > 1 {
		seen = make(map[string]bool)
	}
	for _, interval := range intervals {
		iter := s.newRangeIterator(interval.lower, interval.upper, false)
		err := func() error {
			defer iter.Close()
			for iter.Valid() {
				if err := s.checkInterrupt(); err != nil {
					return err
				}
				key, data := iter.Key(), iter.Value()
				if plan != nil && plan.index != nil {
					rowKey, err := s.accessIndexRowKey(&accessPath{info: info, index: plan.index}, interval, key.UserKey, data)
					if err != nil {
						return err
					}
					var found bool
					data, found, err = s.schemaGet(rowKey)
					if err != nil {
						return err
					}
					if !found {
						return fmt.Errorf("missing row for index %s", plan.index.Name)
					}
					key.UserKey = rowKey
				}
				if seen != nil {
					identity := string(key.UserKey)
					if seen[identity] {
						iter.Next()
						continue
					}
					seen[identity] = true
				}
				match := true
				if residual != nil {
					var err error
					match, err = s.evalCondition(residual, decodeRow(data, info))
					if err != nil {
						return err
					}
				}
				if match {
					if offset > 0 {
						offset--
					} else {
						key.UserKey = copyBytes(key.UserKey)
						rows = append(rows, storage.KeyValue{Key: key, Value: copyBytes(data)})
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
	if stmt.With != nil {
		restore, err := s.enterCTEs(stmt.With)
		if err != nil {
			return 0, nil, err
		}
		defer restore()
		cp := *stmt
		cp.With = nil
		return s.Delete(&cp)
	}
	if stmt.Alias != "" || len(stmt.Joins) > 0 || len(stmt.Targets) > 0 {
		return s.deleteJoined(stmt)
	}
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
		if err := s.deletePhysicalRow(table, info, entry.Key.UserKey, row); err != nil {
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
	if stmt.With != nil {
		restore, err := s.enterCTEs(stmt.With)
		if err != nil {
			return 0, nil, err
		}
		defer restore()
		cp := *stmt
		cp.With = nil
		return s.Update(&cp)
	}
	if stmt.Alias != "" || len(stmt.Joins) > 0 || mysqlQualifiedAssignments(stmt.Set) {
		return s.updateJoined(stmt)
	}
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
	var affected int64
	for _, entry := range entries {
		original := decodeRow(entry.Value, info)
		row, err := s.applyRowUpdate(table, info, entry.Key.UserKey, original, stmt.Set, original)
		if err != nil {
			return 0, nil, err
		}
		if !s.MySQLMode() || s.mysqlFoundRows() || !bytes.Equal(encodeRow(original, info), encodeRow(row, info)) {
			affected++
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
	return affected, rs, nil
}

// applyRowUpdate is shared by UPDATE and UPSERT. Both paths retain identical
// locking, trigger, constraint, primary-key, and index mutation semantics.
func (s *Schema) applyRowUpdate(table string, info *TableInfo, oldKey []byte, original map[string]Value, sets []UpdateSet, evaluationRow map[string]Value) (map[string]Value, error) {
	if err := s.lockRow(table, info, original, oldKey); err != nil {
		return nil, err
	}
	row := make(map[string]Value)
	assigned := make(map[string]bool, len(sets))
	for k, v := range original {
		row[k] = v
	}
	for _, set := range sets {
		assigned[strings.ToLower(set.Column)] = true
		v, err := s.evalExpr(set.Value, evaluationRow)
		if err != nil {
			return nil, err
		}
		for _, col := range info.Columns {
			if col.Name == strings.ToLower(set.Column) {
				v = assignmentAffinity(v, col)
				break
			}
		}
		row[strings.ToLower(set.Column)] = v
	}
	if err := s.normalizeMySQLRow(info, row); err != nil {
		return nil, err
	}
	if pk := s.getPKColumn(info); pk != nil && pk.AutoIncrement && pk.Unsigned {
		if err := s.prepareUnsignedAuto(table, pk, row, true); err != nil {
			return nil, err
		}
	}
	beforeTriggers := len(s.txWrites)
	if err := s.fireTriggers(table, "UPDATE", "BEFORE", original, row); err != nil {
		return nil, err
	}

	key := oldKey
	if len(info.PKColumns) > 0 {
		if err := validatePrimaryKey(info, row); err != nil {
			return nil, err
		}
		key = info.encodePK(table, row)
	}
	if err := s.lockRow(table, info, row, key); err != nil {
		return nil, err
	}
	indexedRow := original
	if len(s.txWrites) != beforeTriggers {
		// BEFORE triggers may have changed or removed the indexed row. Compare
		// against its current state before skipping any index mutations.
		data, found, err := s.schemaGet(oldKey)
		if err != nil {
			return nil, err
		}
		indexedRow = nil
		if found {
			indexedRow = decodeRow(data, info)
		}
	}
	if err := s.checkConstraintsOriginalPreserving(table, info, row, oldKey, indexedRow, original, assigned); err != nil {
		return nil, err
	}
	if err := s.updateChangedIndexes(table, indexedRow, row, oldKey, key); err != nil {
		return nil, err
	}
	if !bytes.Equal(key, oldKey) {
		if err := s.remove(oldKey); err != nil {
			return nil, err
		}
	}
	if err := s.write(key, encodeRow(row, info)); err != nil {
		return nil, err
	}
	if len(info.PKColumns) == 1 && !bytes.Equal(key, oldKey) {
		v := row[info.PKColumns[0]]
		if col := s.getPKColumn(info); col != nil && col.AutoIncrement && v.Type == ColInteger && v.Int >= s.rowIDCount[s.rowCounterKey(table)] {
			s.rowIDCount[s.rowCounterKey(table)] = v.Int + 1
			s.rowIDDirty = true
		}
	}
	s.recordForeignChange(table, oldKey, original, row)
	if err := s.fireTriggers(table, "UPDATE", "AFTER", original, row); err != nil {
		return nil, err
	}

	return row, nil
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
		conflict := s.prefixedConflictIndex(table, row, old)
		for _, group := range groups {
			match := true
			for _, name := range group {
				a, b := old[name], row[name]
				if a.Null || b.Null || s.valueCompare(a, b) != 0 {
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
			if err := s.deletePhysicalRow(table, info, entry.Key.UserKey, old); err != nil {
				return err
			}
			if s.MySQLMode() {
				s.statementChanges++
			}
		}
	}
	return nil
}

func truthValue(v Value) bool { return !v.Null && toFloat(v) != 0 }
func (s *Schema) evalCondition(expr Expression, row map[string]Value) (bool, error) {
	v, err := s.evalExpr(expr, row)
	if s.MySQLMode() && v.HexLiteral {
		v = mysqlHexNumber(v)
	}
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
		if (s.MySQLMode() || s.PostgresMode()) && e.Value.NumericText != "" && !strings.ContainsAny(e.Value.NumericText, "eE") {
			return NewDecimalValue(e.Value.NumericText)
		}
		return e.Value, nil
	case *ColumnRefExpr:
		if e.Name == "*" {
			return NewIntValue(1), nil
		}
		if v, ok := row[e.Name]; ok {
			v.HexLiteral = false
			return v, nil
		}
		for name, value := range row {
			if strings.EqualFold(name, e.Name) {
				value.HexLiteral = false
				return value, nil
			}
		}

		if s.outerRow != nil {
			if v, ok := s.outerRow[e.Name]; ok {
				v.HexLiteral = false
				return v, nil
			}
		}
		return NewNullValue(), fmt.Errorf("no such column: %s", e.Name)
	case *BinaryExpr:
		value, err := s.evalBinary(e, row)
		if s.PostgresMode() && postgresBooleanOperator(e.Op) {
			value = value.WithDeclaredType("BOOLEAN")
		}
		return value, err
	case *UnaryExpr:
		right, err := s.evalExpr(e.Right, row)
		if err != nil {
			return NewNullValue(), err
		}
		if strings.HasPrefix(e.Op, "COLLATE:") {
			right.Collation = Collation(strings.TrimPrefix(e.Op, "COLLATE:"))
			if right.Collation == "DEFAULT" {
				right.Collation = s.collation
			}
			right.Binary = false
			return right, nil
		}
		if e.Op == "BINARY" {
			right.HexLiteral = false
			right.Binary = true
			return right, nil
		}
		if right.Null {
			if s.PostgresMode() && e.Op == "NOT" {
				right = right.WithDeclaredType("BOOLEAN")
			}
			return right, nil
		}
		if s.MySQLMode() && right.HexLiteral {
			right = mysqlHexNumber(right)
		}
		if right.EnumValue && (e.Op == "+" || e.Op == "-" || e.Op == "~") {
			right = NewIntValue(right.EnumOrdinal)
		}
		switch e.Op {
		case "NOT":
			return s.postgresBooleanValue(!truthValue(right)), nil
		case "+":
			if !isNumeric(right) {
				return NewNullValue(), fmt.Errorf("unary sign requires a numeric operand")
			}
			return right, nil
		case "-":
			if !isNumeric(right) {
				return NewNullValue(), fmt.Errorf("unary sign requires a numeric operand")
			}

			if right.Type == ColDecimal || right.Type == ColUnsigned {
				return negateExactValue(right)
			}
			if right.Type == ColInteger {
				if right.Int == math.MinInt64 {
					return NewNullValue(), fmt.Errorf("integer overflow")
				}
				return NewIntValue(-right.Int), nil
			}
			return negateRealValue(right), nil
		case "~":
			if right.Type == ColUnsigned {
				return NewUnsignedValue(^right.Uint), nil
			}
			return NewIntValue(^integerValue(right)), nil
		}

	case *FuncCallExpr:
		if e.Window != nil {
			if v, ok := row[windowSlot(e)]; ok {
				v.HexLiteral = false
				return v, nil
			}
			return NewNullValue(), fmt.Errorf("window function evaluated outside window stage")
		}
		if isAggregate(e) {
			if v, ok := row[aggregateSlot(e)]; ok {
				v.HexLiteral = false
				return v, nil
			}
		}
		return s.evalFunc(e, row)
	case *InListExpr:
		value, err := s.evalInList(e, row)
		if s.PostgresMode() {
			value = value.WithDeclaredType("BOOLEAN")
		}
		return value, err
	case *BetweenExpr:
		value, err := s.evalBetween(e, row)
		if s.PostgresMode() {
			value = value.WithDeclaredType("BOOLEAN")
		}
		return value, err
	case *CaseExpr:
		value, err := s.evalCase(e, row)
		value.HexLiteral = false
		return value, err
	case *SubqueryExpr:
		return s.evalSubqueryValue(e.Stmt, row)
	case *ExistsExpr:
		rs, err := s.executeSubqueryMode(e.Stmt, row, true)
		if err != nil {
			return NewNullValue(), err
		}
		exists := len(rs.Rows) > 0
		if e.Not {
			exists = !exists
		}
		return s.postgresBooleanValue(exists), nil
	}
	return NewNullValue(), fmt.Errorf("unsupported expression type: %T", expr)
}

func (s *Schema) evalFunc(e *FuncCallExpr, row map[string]Value) (Value, error) {
	value, err := s.evalFuncResult(e, row)
	value.HexLiteral = false
	return value, err
}

func (s *Schema) evalFuncResult(e *FuncCallExpr, row map[string]Value) (Value, error) {
	return s.evalResolvedFunction(e, row)
}

// Core handlers implement native operators selected by the common registry.
func (s *Schema) evalCoreFunction(e *FuncCallExpr, row map[string]Value) (Value, error) {
	name := strings.ToUpper(e.Name)

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
		if s.valueCompare(v1, v2) == 0 {
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
		if s.MySQLMode() {
			return NewIntValue(int64(len(v.String()))), nil
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
		if !isNumeric(v) {
			return NewNullValue(), fmt.Errorf("ABS requires a numeric argument")
		}
		if v.Type == ColUnsigned {
			return v, nil
		}
		if v.Type == ColDecimal {
			n, scale, _ := valueDecimal(v)
			n.Abs(n)
			return Value{Type: ColDecimal, Text: decimalText(n, scale)}, nil
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
		if !isNumeric(v) {
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
		if v.Type == ColDecimal || v.Type == ColUnsigned {
			n, scale, _ := valueDecimal(v)
			n = roundDecimal(n, scale, digits)
			return Value{Type: ColDecimal, Text: decimalText(n, digits)}, nil
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
		if err != nil {
			return NewNullValue(), err
		}
		t, err := s.evalExpr(e.Args[1], row)
		if err != nil {
			return NewNullValue(), err
		}
		if s.PostgresMode() {
			if r, handled, e := s.CastPostgresCatalogValue(v, t.Text); handled {
				return r, e
			}
			return CastPostgresValue(v, t.Text)
		}
		if s.MySQLMode() && v.HexLiteral {
			target := strings.ToUpper(mysqlCastTarget(t.Text))
			if target == "INTEGER" || target == "INT" || target == "BIGINT" || target == "UNSIGNED" || target == "UNSIGNED INTEGER" || target == "REAL" || target == "DOUBLE" || target == "FLOAT" || strings.HasPrefix(target, "DECIMAL") || strings.HasPrefix(target, "NUMERIC") {
				v = mysqlHexNumber(v)
			}
		}
		if s.MySQLMode() && (strings.EqualFold(t.Text, "DECIMAL") || strings.EqualFold(t.Text, "NUMERIC")) {
			return normalizeDecimal(v, ColumnDef{Name: "CAST", ColType: ColDecimal, Precision: 10, Scale: 0})
		}
		return castSQLValue(v, t.Text)

	case "HEX":
		v, err := s.evalExpr(e.Args[0], row)
		if err != nil {
			return NewNullValue(), err
		}
		if v.Null {
			if s.MySQLMode() {
				return NewNullValue(), nil
			}
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
			if i == 0 || name == "MIN" && s.valueCompare(v, best) < 0 || name == "MAX" && s.valueCompare(v, best) > 0 {
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
		if !val.Null && !v.Null && s.valueCompare(val, v) == 0 {
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
			if !caseVal.Null && !condVal.Null && s.valueCompare(caseVal, condVal) == 0 {
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
				if !lv.EnumValue {
					rv = applyAffinity(rv, lv.Type)
				}
			}
		} else if _, ok := e.Right.(*ColumnRefExpr); ok {
			if _, literal := e.Left.(*LiteralExpr); literal {
				if !rv.EnumValue {
					lv = applyAffinity(lv, rv.Type)
				}
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
			membership := s.membership(rs)
			found := membership.values[string(s.appendEqualityKey(nil, lv))]
			if s.MySQLMode() {
				found = false
				for _, candidate := range rs.Rows {
					v := candidate[rs.Columns[0].Name]
					if !v.Null && s.valueCompare(lv, v) == 0 {
						found = true
						break
					}
				}
			}
			if !lv.Null && found {
				if e.Op == "NOT IN" {
					return NewIntValue(0), nil
				}
				return NewIntValue(1), nil
			}
			if len(rs.Rows) > 0 && (lv.Null || membership.hasNull) {
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
		if s.matchLikeValues(lv, rv, e.Escape) {
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
		if s.matchLikeValues(lv, rv, e.Escape) {
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
		same := left.Null && right.Null || !left.Null && !right.Null && s.valueCompare(left, right) == 0
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
	return arithmeticValuesMode(s.MySQLMode(), e.Op, lv, rv, fn)
}

// arithmeticValues is shared by interpreted and slot-bound execution so exact
// integers, NULL propagation and numeric errors cannot diverge.
func arithmeticValuesMode(mysqlMode bool, op string, lv, rv Value, fn func(float64, float64) float64) (Value, error) {
	if mysqlMode {
		if lv.HexLiteral {
			lv = mysqlHexNumber(lv)
		}
		if rv.HexLiteral {
			rv = mysqlHexNumber(rv)
		}
	}
	if mysqlMode && op != "%" && (lv.Type == ColReal || rv.Type == ColReal) {
		if lv.Type == ColDecimal {
			lv = NewRealValue(toFloatVal(lv))
		}
		if rv.Type == ColDecimal {
			rv = NewRealValue(toFloatVal(rv))
		}
	}
	if mysqlMode && op == "/" && !lv.Null && !rv.Null && isNumeric(lv) && isNumeric(rv) && lv.Type != ColReal && rv.Type != ColReal {
		if lv.Type != ColDecimal {
			n, scale, err := valueDecimal(lv)
			if err != nil {
				return NewNullValue(), err
			}
			lv = Value{Type: ColDecimal, Text: decimalText(n, scale)}
		}
		return extendedArithmetic(op, lv, rv)
	}
	return arithmeticValues(op, lv, rv, fn)
}

func arithmeticValues(op string, lv, rv Value, fn func(float64, float64) float64) (Value, error) {
	if lv.EnumValue {
		lv = NewIntValue(lv.EnumOrdinal)
	}
	if rv.EnumValue {
		rv = NewIntValue(rv.EnumOrdinal)
	}
	if lv.Null || rv.Null {
		return NewNullValue(), nil
	}
	if !isNumeric(lv) || !isNumeric(rv) {
		return NewNullValue(), fmt.Errorf("arithmetic requires numeric operands")
	}
	if lv.Type == ColUnsigned || rv.Type == ColUnsigned || lv.Type == ColDecimal || rv.Type == ColDecimal {
		return extendedArithmetic(op, lv, rv)
	}
	if (op == "/" || op == "%") && toFloat(rv) == 0 {
		return NewNullValue(), fmt.Errorf("division by zero")
	}
	if op == "%" {
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
		switch op {
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
	return realArithmeticProvenance(op, lv, rv, NewRealValue(result))
}

func integerValue(v Value) int64 {
	if v.Type == ColInteger {
		return v.Int
	}
	if v.Type == ColUnsigned {
		if v.Uint > math.MaxInt64 {
			return math.MaxInt64
		}
		return int64(v.Uint)
	}
	if v.Type == ColDecimal {
		n, scale, err := valueDecimal(v)
		if err != nil {
			return 0
		}
		n.Quo(n, decimalPower(scale))
		if n.IsInt64() {
			return n.Int64()
		}
		if n.Sign() < 0 {
			return math.MinInt64
		}
		return math.MaxInt64
	}
	f := toFloat(v)
	if f >= 9223372036854775808.0 {
		return math.MaxInt64
	}
	if f < math.MinInt64 {
		return math.MinInt64
	}
	return int64(f)
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
	if lv.Type == ColUnsigned || rv.Type == ColUnsigned {
		a, b := integerBits(lv), integerBits(rv)
		switch e.Op {
		case "&":
			return NewUnsignedValue(a & b), nil
		case "|":
			return NewUnsignedValue(a | b), nil
		case "<<":
			if b >= 64 {
				return NewUnsignedValue(0), nil
			}
			return NewUnsignedValue(a << b), nil
		case ">>":
			if b >= 64 {
				return NewUnsignedValue(0), nil
			}
			return NewUnsignedValue(a >> b), nil
		}
	}
	return NewIntValue(fn(integerValue(lv), integerValue(rv))), nil
}

func (s *Schema) compareValues(lv, rv Value, op string) Value {
	if lv.Null || rv.Null {
		return NewNullValue()
	}
	return compareSQLValuesBy(lv, rv, op, s.valueCompare(lv, rv))
}

func compareSQLValues(lv, rv Value, op string) Value {
	if lv.Null || rv.Null {
		return NewNullValue()
	}
	return compareSQLValuesBy(lv, rv, op, ValueCompare(lv, rv))
}

func (s *Schema) valueCompare(a, b Value) int {
	if s.MySQLMode() {
		if cmp, ok := mysqlMixedNumericCompare(a, b); ok {
			return cmp
		}
		if cmp, ok := mysqlBinaryCompare(a, b); ok {
			return cmp
		}
	}
	if a.EnumValue && (b.Type == ColInteger || b.Type == ColReal) {
		a = NewIntValue(a.EnumOrdinal)
	}
	if b.EnumValue && (a.Type == ColInteger || a.Type == ColReal) {
		b = NewIntValue(b.EnumOrdinal)
	}
	if !a.Null && !b.Null && a.Type == ColText && b.Type == ColText {
		if collation := explicitValueCollation(a, b); collation != "" {
			return collation.Compare(a.Text, b.Text)
		}
	}
	if !a.Null && !b.Null && a.Type == ColText && b.Type == ColText {
		if a.Binary || b.Binary {
			return strings.Compare(a.Text, b.Text)
		}
		return s.collation.Compare(a.Text, b.Text)
	}
	return ValueCompare(a, b)
}

func compareSQLValuesBy(lv, rv Value, op string, cmp int) Value {
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
	if typ == ColDecimal {
		if v.NumericExact != "" {
			return v
		}
		if coefficient, scale, err := valueDecimal(v); err == nil {
			return Value{Type: ColDecimal, Text: decimalText(coefficient, scale)}
		}
		return v
	}
	if typ == ColJSON {
		return v
	}
	if typ == ColUnsigned {
		if n, err := strconv.ParseUint(v.String(), 10, 64); err == nil {
			return NewUnsignedValue(n)
		}
		return v
	}
	switch typ {
	case ColInteger, ColReal:
		if v.Type == ColText {
			text := strings.TrimSpace(v.Text)
			if n, err := strconv.ParseInt(text, 10, 64); err == nil {
				v = NewIntValue(n)
			} else if u, err := strconv.ParseUint(text, 10, 64); err == nil {
				v = NewUnsignedValue(u)
			} else if f, err := strconv.ParseFloat(text, 64); err == nil {
				v = NewRealValue(f)
			} else {
				return v
			}
		}
		if typ == ColReal && (v.Type == ColUnsigned || v.Type == ColDecimal) {
			return NewRealValue(toFloatVal(v))
		}
		if typ == ColInteger && v.Type == ColUnsigned && v.Uint <= math.MaxInt64 {
			return NewIntValue(int64(v.Uint))
		}
		if typ == ColInteger && v.Type == ColDecimal {
			if n, scale, err := valueDecimal(v); err == nil {
				factor := decimalPower(scale)
				if new(big.Int).Rem(n, factor).Sign() == 0 {
					n.Quo(n, factor)
					if n.IsInt64() {
						return NewIntValue(n.Int64())
					}
				}
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
func valueIdentity(v Value) []byte { return appendValueIdentity(nil, v) }

func (s *Schema) valueIdentity(v Value) []byte { return s.appendValueIdentity(nil, v) }

func (s *Schema) appendValueIdentity(buf []byte, v Value) []byte {
	if !v.Null && v.Type == ColText && (!v.Binary || v.Collation != "") {
		v.Text = valueTextKey(v, s.collation)
	}
	return appendValueIdentity(buf, v)
}

func appendValueIdentity(buf []byte, v Value) []byte {
	if v.Type == ColJSON {
		v.Text = string(jsonSortKey(v.Text))
	}
	if v.Type == ColUnsigned && v.Uint <= math.MaxInt64 {
		v = NewIntValue(int64(v.Uint))
	}
	if v.Type == ColDecimal {
		n, scale, err := valueDecimal(v)
		if err == nil {
			for scale > 0 {
				q, r := new(big.Int), new(big.Int)
				q.QuoRem(n, big.NewInt(10), r)
				if r.Sign() != 0 {
					break
				}
				n = q
				scale--
			}
			if scale == 0 && n.IsInt64() {
				v = NewIntValue(n.Int64())
			} else if scale == 0 && n.Sign() >= 0 && n.BitLen() <= 64 {
				v = NewUnsignedValue(n.Uint64())
			} else {
				v.Text = decimalText(n, scale)
			}
		}
	}
	if !v.Null && v.Type == ColReal && v.Real >= math.MinInt64 && v.Real < 9223372036854775808.0 && v.Real == math.Trunc(v.Real) {
		v = NewIntValue(int64(v.Real))
	}
	return appendValue(buf, v)
}

func toFloat(v Value) float64 {
	if v.EnumValue {
		return float64(v.EnumOrdinal)
	}
	switch v.Type {
	case ColUnsigned, ColDecimal:
		return toFloatVal(v)
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
	return compileLike(pattern, escape).match(str)
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
	binTypeUint64  byte = 5
	binTypeDecimal byte = 6
	binTypeJSON    byte = 7
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
		v = decorateColumnValue(v, col)
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
	case ColUnsigned:
		buf = append(buf, binTypeUint64)
		return binary.LittleEndian.AppendUint64(buf, v.Uint)
	case ColDecimal, ColJSON:
		tag := binTypeDecimal
		if v.Type == ColJSON {
			tag = binTypeJSON
		}
		buf = append(buf, tag)
		buf = binary.AppendUvarint(buf, uint64(len(v.Text)))
		return append(buf, v.Text...)
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
	return decodeValueInto(data, nil)
}

func decodeValueInto(data, blobBuffer []byte) (Value, int) {
	if len(data) < 1 {
		return NewNullValue(), 0
	}
	switch data[0] {
	case binTypeUint64:
		if len(data) < 9 {
			return NewNullValue(), 1
		}
		return NewUnsignedValue(binary.LittleEndian.Uint64(data[1:9])), 9
	case binTypeDecimal, binTypeJSON:
		length, n := binary.Uvarint(data[1:])
		if n <= 0 || length > uint64(len(data)-1-n) {
			return NewNullValue(), 1
		}
		typ := ColDecimal
		if data[0] == binTypeJSON {
			typ = ColJSON
		}
		return Value{Type: typ, Text: string(data[1+n : 1+n+int(length)])}, 1 + n + int(length)
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
		blob := append(blobBuffer[:0], data[1+n:1+n+int(length)]...)
		if blob == nil {
			blob = []byte{} // preserve an empty, non-NULL BLOB
		}
		return Value{Type: ColBlob, Blob: blob}, 1 + n + int(length)
	}
	return NewNullValue(), 1
}

type ResultSet struct {
	Profile *QueryProfile
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
	if vi.Parsed != nil && (vi.Parsed.With != nil || vi.Parsed.Compound != nil) && len(vi.Columns) == 0 {
		return s.cteQueryColumnDefs(vi.Parsed, nil)
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
