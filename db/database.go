package db

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"slopdb/sql"
	"slopdb/storage"
)

type Database struct {
	currentStorage  atomic.Pointer[storage.LSMTree]
	replication     atomic.Pointer[ReplicationGuard]
	generation      atomic.Uint64
	onlineIndexHook func(string) // instance-local synchronization hook for index concurrency tests
	mu              sync.Mutex
	execution       executionGate
	closed          atomic.Bool
	lsm             *storage.LSMTree
	schema          *sql.Schema
	dir             string
	opt             *storage.Options
	sessions        map[*Session]struct{}
	defaultGate     contextMutex
	defaultCancel   context.CancelFunc
	locks           *lockManager
	executionReader uint64
}

func Open(dir string, opt *storage.Options) (*Database, error) {
	if opt == nil {
		opt = storage.DefaultOptions()
	} else {
		copy := *opt
		opt = &copy
	}
	if opt.BlockSize == 0 {
		opt.BlockSize = 4096
	}
	if opt.QueryTimeout < 0 || opt.LockTimeout < 0 {
		return nil, fmt.Errorf("timeouts must not be negative")
	}
	if opt.QueryMemoryBytes < 0 {
		return nil, fmt.Errorf("query memory budget must not be negative")
	}
	tempParent := opt.QueryTempDir
	if tempParent == "" {
		tempParent = os.TempDir()
	}
	dbPath, e := filepath.Abs(dir)
	if e != nil {
		return nil, e
	}
	tempPath, e := filepath.Abs(tempParent)
	if e != nil {
		return nil, e
	}
	if resolved, e := filepath.EvalSymlinks(dbPath); e == nil {
		dbPath = resolved
	}
	if resolved, e := filepath.EvalSymlinks(tempPath); e == nil {
		tempPath = resolved
	}
	if rel, e := filepath.Rel(dbPath, tempPath); e == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(os.PathSeparator)) {
		return nil, fmt.Errorf("query temporary directory must be outside the database directory")
	}

	if opt.MaxCTEIterations < 0 || opt.MaxCTERows < 0 {
		return nil, fmt.Errorf("CTE limits must not be negative")
	}
	lsm, err := storage.NewLSMTree(dir, opt)
	if err != nil {
		return nil, err
	}

	schema, err := sql.NewSchemaWithOptions(lsm, int(lsm.PageSize()), opt)
	if err != nil {
		lsm.Close()
		return nil, err
	}

	database := &Database{
		lsm:      lsm,
		schema:   schema.NewSession(),
		dir:      dir,
		opt:      opt,
		sessions: make(map[*Session]struct{}),
		locks:    newLockManager(),
	}
	database.currentStorage.Store(lsm)
	database.installLocks(database.schema)
	database.executionReader = database.locks.state(database.schema).id
	return database, nil
}

// Close rolls back any open transaction and flushes committed writes to durable
// SSTables. Check its error, especially when storage.Options.DisableWAL is set.
func (db *Database) Close() error {
	db.mu.Lock()
	db.closed.Store(true)
	if db.defaultCancel != nil {
		db.defaultCancel()
	}
	for session := range db.sessions {
		session.cancelActive()
	}
	db.mu.Unlock()
	db.execution.Lock()
	defer db.execution.Unlock()
	db.mu.Lock()
	defer db.mu.Unlock()
	db.closed.Store(true)
	_ = db.schema.Rollback()
	db.locks.release(db.schema)
	for session := range db.sessions {
		session.closeLocked()
	}
	return db.lsm.Close()
}

type ExecResult struct {
	// CommitPosition covers this committed transaction and possibly concurrent
	// commits in the same applied prefix. Empty for uncommitted work/standalone DBs.
	CommitPosition string
	Profile        *sql.QueryProfile
	Columns        []sql.ColumnDef
	Rows           []map[string]sql.Value
	LastID         int64
	Changes        int64
}

type statementExecutor struct {
	schema       *sql.Schema
	parseOptions sql.ParseOptions
	resultTarget *ExecResult
	selectTarget *sql.ResultSet
}

func (db *Database) Exec(sqlText string) (*ExecResult, error) {
	return db.ExecContext(context.Background(), sqlText)
}

func (db *Database) ExecContext(ctx context.Context, sqlText string) (*ExecResult, error) {
	return db.ExecContextWithOptions(ctx, sqlText, ExecOptions{})
}

func (db *Database) ExecWithOptions(sqlText string, options ExecOptions) (*ExecResult, error) {
	return db.ExecContextWithOptions(context.Background(), sqlText, options)
}

func (db *Database) ExecContextWithOptions(ctx context.Context, sqlText string, options ExecOptions) (result *ExecResult, resultErr error) {
	ctx, cancel, lockTimeout, err := db.queryContext(ctx, options)
	if err != nil {
		return nil, err
	}
	defer cancel()
	if err := db.defaultGate.LockContext(ctx); err != nil {
		return nil, err
	}
	defer db.defaultGate.Unlock()
	if err := db.execution.RLockContextForReader(ctx, db.executionReader); err != nil {
		return nil, err
	}
	defer db.execution.RUnlockForReader(db.executionReader)
	db.mu.Lock()
	if db.closed.Load() {
		db.mu.Unlock()
		return nil, errors.New("database closed")
	}
	db.defaultCancel = cancel
	db.mu.Unlock()
	defer func() { db.mu.Lock(); db.defaultCancel = nil; db.mu.Unlock() }()
	ctx, abortCancel := managedCancellation(ctx)
	defer abortCancel(nil)
	defer func() { resultErr = db.finishManagedCall(db.schema, ctx, resultErr, false) }()
	if err := db.beginManagedCall(db.schema, abortCancel); err != nil {
		return nil, err
	}
	return db.execSQL(ctx, sqlText, db.schema, false, lockTimeout)
}

// execSQL runs with the session gate and a shared lifecycle view held.
// Other sessions can evaluate SQL concurrently; commits publish atomically.
func (db *Database) execSQL(ctx context.Context, sqlText string, schema *sql.Schema, transactionBound bool, lockTimeout time.Duration) (*ExecResult, error) {
	if db.closed.Load() {
		return nil, fmt.Errorf("database closed")
	}

	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	schema.SetContext(ctx)
	stmts, err := sql.ParseWithOptions(sqlText, db.ParseOptions())
	if err != nil {
		return nil, fmt.Errorf("parse error: %w", err)
	}
	return db.execStatements(ctx, stmts, schema, transactionBound, lockTimeout)
}

func (db *Database) execStatements(ctx context.Context, stmts []sql.Statement, schema *sql.Schema, transactionBound bool, lockTimeout time.Duration) (*ExecResult, error) {
	return db.execStatementsInto(ctx, stmts, schema, transactionBound, lockTimeout, nil, nil)
}

func (db *Database) execStatementsInto(ctx context.Context, stmts []sql.Statement, schema *sql.Schema, transactionBound bool, lockTimeout time.Duration, dst *ExecResult, selectTarget *sql.ResultSet) (*ExecResult, error) {
	if !hasReadBarrierSetting(stmts) {
		if err := db.checkStatementReplication(withSessionReadBarrier(ctx, schema), stmts); err != nil {
			return nil, err
		}
	}
	return db.execAdmittedStatementsInto(ctx, stmts, schema, transactionBound, lockTimeout, dst, selectTarget)
}

// Independent sessions already checked admission before the prepared point
// update fast path. Do not repeat admission in the shared executor.
func (db *Database) execAdmittedStatementsInto(ctx context.Context, stmts []sql.Statement, schema *sql.Schema, transactionBound bool, lockTimeout time.Duration, dst *ExecResult, selectTarget *sql.ResultSet) (*ExecResult, error) {
	defer schema.StopQueryProfile()
	if db.closed.Load() {
		return nil, fmt.Errorf("database closed")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	schema.SetContext(ctx)
	var err error
	var result *ExecResult
	executor := &statementExecutor{schema: schema, parseOptions: db.ParseOptions(), resultTarget: dst, selectTarget: selectTarget}
	var lockDeadline time.Time
	admitEach := hasReadBarrierSetting(stmts)
	for _, stmt := range stmts {
		if stmt == nil {
			continue
		}
		statementCtx := ctx
		if guard := db.replication.Load(); guard != nil && guard.ReadSnapshot != nil {
			statementCtx = readStatementContext(ctx, schema, stmt)
		}
		schema.SetContext(statementCtx)
		if admitEach {
			if err := db.checkStatementReplication(statementCtx, []sql.Statement{stmt}); err != nil {
				return nil, err
			}
		}
		if setting, ok := stmt.(*sql.ReadBarrierStmt); ok {
			if transactionBound && !schema.InTransaction() {
				return nil, ErrTxDone
			}
			if err := db.setReadBarrier(schema, setting); err != nil {
				return nil, err
			}
			result = &ExecResult{}
			continue
		}
		if position, ok := stmt.(*sql.ReplicationPositionStmt); ok {
			if transactionBound && !schema.InTransaction() {
				return nil, ErrTxDone
			}
			result, err = db.execPosition(ctx, schema, position)
			if err != nil {
				return nil, err
			}
			continue
		}
		if commit, ok := stmt.(*sql.CommitStmt); ok && commit.Durability == "durable" {
			guard := db.replication.Load()
			if (guard == nil || guard.Durable == nil) && (db.opt.DisableWAL || db.opt.DisableWALSync) {
				return nil, fmt.Errorf("COMMIT DURABLE requires WAL and WAL fsync")
			}
		}
		if _, err := indexPublicationPolicy(ctx, schema, stmt); err != nil {
			return nil, err
		}
		if explain, ok := stmt.(*sql.ExplainStmt); ok && explain.Analyze {
			schema.StartQueryProfile()
		}
		held := db.locks.snapshot(schema)
		cleanup := func() {
			schema.EndStatement()
			db.syncManagedTransaction(schema, err)
			if !schema.InTransaction() {
				db.locks.release(schema)
			} else {
				db.locks.restore(schema, held)
			}
		}
		for {
			if db.closed.Load() {
				cleanup()
				return nil, fmt.Errorf("database closed")
			}
			if transactionBound && !schema.InTransaction() {
				cleanup()
				return nil, ErrTxDone
			}
			if err := ctx.Err(); err != nil {
				cleanup()
				return nil, err
			}
			var catalog *ownerLocks
			if _, insert := stmt.(*sql.InsertStmt); insert && !schema.InTransaction() {
				catalog, err = db.locks.acquirePointCatalog(schema)
			} else {
				err = db.statementCatalogLock(schema, stmt)
			}
			if err == nil {
				if _, rollback := stmt.(*sql.RollbackStmt); !rollback {
					reuse, _ := ctx.Value(reuseReadSnapshotKey{}).(bool)
					if !reuse {
						err = schema.StartStatement()
					}
					if reuse {
						ctx = context.WithValue(ctx, reuseReadSnapshotKey{}, false)
					}
					if err != nil {
						_ = schema.Rollback()
					}
				}
			}
			if err == nil && onlineIndexStatement(schema, stmt) {
				result, err = db.execOnlineIndex(ctx, schema, stmt, lockTimeout, &lockDeadline)
			} else if err == nil {
				switch stmt.(type) {
				case *sql.BeginStmt, *sql.CommitStmt, *sql.RollbackStmt, *sql.SavepointStmt, *sql.ReleaseStmt, *sql.SelectStmt, *sql.UnionStmt, *sql.ExplainStmt, *sql.ShowStmt, *sql.VacuumStmt:
					result, err = executor.execStatement(stmt)
				case *sql.PragmaStmt:
					if stmt.(*sql.PragmaStmt).Name == "page_size" {
						result, err = executor.execStatement(stmt)
					} else {
						err = schema.ExecuteStatement(func() error { var e error; result, e = executor.execStatement(stmt); return e })
					}
				default:
					err = schema.ExecuteStatement(func() error { var e error; result, e = executor.execStatement(stmt); return e })
				}
			}
			schema.EndStatement()
			db.syncManagedTransaction(schema, err)
			if !schema.InTransaction() {
				db.locks.release(schema)
			} else if err != nil {
				db.locks.restore(schema, held)
			}
			db.locks.releasePointCatalog(catalog)
			var waiting *lockWait
			if errors.As(err, &waiting) {
				if lockTimeout == 0 {
					cleanup()
					return nil, db.lockDiagnosticError(schema, waiting.request, fmt.Errorf("%w: %w", ErrLockTimeout, context.DeadlineExceeded))
				}
				if lockDeadline.IsZero() {
					lockDeadline = time.Now().Add(lockTimeout)
				}
				policy, _ := indexPublicationPolicy(ctx, schema, stmt)
				if err := db.waitForLockPolicy(ctx, schema, waiting.request, lockDeadline, policy); err != nil {
					if errors.Is(err, ErrDeadlock) {
						_ = schema.Rollback()
					}
					cleanup()
					return nil, err
				}
				continue // Re-read committed rows and re-evaluate the whole statement.
			}
			if errors.Is(err, sql.ErrStatementRetry) {
				continue
			}
			if err != nil {
				return nil, err
			}
			result = db.commitPositionResult(schema, stmt, result)
			if !schema.InTransaction() {
				switch stmt.(type) {
				case *sql.DropIndexStmt, *sql.AlterTableStmt, *sql.CommitStmt, *sql.ReleaseStmt:
					if err := schema.CleanupPendingIndexGarbage(); err != nil {
						return nil, fmt.Errorf("schema change committed; index cleanup pending: %w", err)
					}
				}
			}
			if commit, ok := stmt.(*sql.CommitStmt); ok && commit.Durability == "durable" {
				if g := db.replication.Load(); g != nil && g.Durable != nil {
					if e := g.Durable(ctx); e != nil {
						return nil, fmt.Errorf("%w: %w", ErrDurabilityUnconfirmed, e)
					}
				}
			}
			break
		}
		if err := ctx.Err(); err != nil && !db.publicationCommitWon(schema, ctx) {
			return nil, err
		}
		if result != nil {
			result.Profile = schema.QueryProfile()
		}
		schema.StopQueryProfile()
	}
	if result == nil {
		result = &ExecResult{}
	}
	if dst != nil && result != dst {
		*dst = *result
		result = dst
	}
	return result, nil
}
func (db *statementExecutor) execStatement(stmt sql.Statement) (*ExecResult, error) {
	var result *ExecResult
	switch s := stmt.(type) {
	case *sql.CreateTableStmt:
		if err := db.schema.CreateTable(s); err != nil {
			return nil, fmt.Errorf("CREATE TABLE error: %w", err)
		}
	case *sql.InsertStmt:
		id, returningRS, err := db.schema.Insert(s)
		if err != nil {
			return nil, fmt.Errorf("INSERT error: %w", err)
		}
		if returningRS != nil {
			result = &ExecResult{
				Columns: returningRS.Columns,
				Rows:    returningRS.Rows,
				LastID:  id,
				Changes: db.schema.StatementChanges(),
			}
		} else {
			if id > 0 {
				result = &ExecResult{LastID: id, Changes: db.schema.StatementChanges()}
			} else {
				result = &ExecResult{Changes: db.schema.StatementChanges()}
			}
		}
	case *sql.SelectStmt:
		var rs *sql.ResultSet
		var err error
		if dst := db.resultTarget; dst != nil {
			*db.selectTarget = sql.ResultSet{Columns: dst.Columns, Rows: dst.Rows}
			rs, err = db.schema.SelectInto(s, db.selectTarget)
		} else {
			rs, err = db.schema.Select(s)
		}
		if err != nil {
			return nil, fmt.Errorf("SELECT error: %w", err)
		}
		result = db.resultTarget
		if result == nil {
			result = &ExecResult{}
		}
		*result = ExecResult{Columns: rs.Columns, Rows: rs.Rows, Changes: int64(len(rs.Rows))}
	case *sql.UnionStmt:
		rs, err := db.schema.Union(s)
		if err != nil {
			return nil, fmt.Errorf("UNION error: %w", err)
		}
		result = &ExecResult{
			Columns: rs.Columns,
			Rows:    rs.Rows,
			Changes: int64(len(rs.Rows)),
		}
	case *sql.DeleteStmt:
		n, returningRS, err := db.schema.Delete(s)
		if err != nil {
			return nil, fmt.Errorf("DELETE error: %w", err)
		}
		if returningRS != nil {
			result = &ExecResult{
				Columns: returningRS.Columns,
				Rows:    returningRS.Rows,
				Changes: n,
			}
		} else {
			result = &ExecResult{Changes: n}
		}
	case *sql.UpdateStmt:
		n, returningRS, err := db.schema.Update(s)
		if err != nil {
			return nil, fmt.Errorf("UPDATE error: %w", err)
		}
		if returningRS != nil {
			result = &ExecResult{
				Columns: returningRS.Columns,
				Rows:    returningRS.Rows,
				Changes: n,
			}
		} else {
			result = &ExecResult{Changes: n}
		}
	case *sql.DropTableStmt:
		if err := db.schema.DropTable(s); err != nil {
			return nil, fmt.Errorf("DROP TABLE error: %w", err)
		}
	case *sql.ExplainStmt:
		rs, err := db.schema.Explain(s)
		if err != nil {
			return nil, fmt.Errorf("EXPLAIN error: %w", err)
		}
		result = &ExecResult{
			Columns: rs.Columns,
			Rows:    rs.Rows,
			Changes: int64(len(rs.Rows)),
		}
	case *sql.ShowStmt:
		rs, err := db.schema.ExecuteShow(s)
		if err != nil {
			return nil, fmt.Errorf("SHOW error: %w", err)
		}
		if db.parseOptions.MySQLQuoting && strings.HasPrefix(s.Kind, "CREATE ") {
			for _, row := range rs.Rows {
				for _, name := range []string{"Create table", "Create view", "SQL Original Statement"} {
					if value, ok := row[name]; ok {
						row[name] = sql.NewTextValue(renderMySQLSQL(value.Text))
					}
				}
			}
		}
		result = &ExecResult{Columns: rs.Columns, Rows: rs.Rows, Changes: int64(len(rs.Rows))}
	case *sql.PragmaStmt:
		rs, err := db.schema.ExecutePragma(s)
		if err != nil {
			return nil, fmt.Errorf("PRAGMA error: %w", err)
		}
		result = &ExecResult{
			Columns: rs.Columns,
			Rows:    rs.Rows,
			Changes: int64(len(rs.Rows)),
		}
	case *sql.AnalyzeStmt:
		if err := db.schema.UpdateStats(); err != nil {
			return nil, fmt.Errorf("ANALYZE error: %w", err)
		}
		result = &ExecResult{}
	case *sql.CreateViewStmt:
		if err := db.schema.CreateView(s); err != nil {
			return nil, fmt.Errorf("CREATE VIEW error: %w", err)
		}
	case *sql.DropViewStmt:
		if err := db.schema.DropView(s); err != nil {
			return nil, fmt.Errorf("DROP VIEW error: %w", err)
		}
	case *sql.CreateTriggerStmt:
		if err := db.schema.CreateTrigger(s); err != nil {
			return nil, fmt.Errorf("CREATE TRIGGER error: %w", err)
		}
	case *sql.DropTriggerStmt:
		if err := db.schema.DropTrigger(s); err != nil {
			return nil, fmt.Errorf("DROP TRIGGER error: %w", err)
		}
	case *sql.BeginStmt:
		if err := db.schema.Begin(); err != nil {
			return nil, fmt.Errorf("BEGIN error: %w", err)
		}
		result = &ExecResult{}
	case *sql.CommitStmt:
		if err := db.schema.Commit(); err != nil {
			return nil, fmt.Errorf("COMMIT error: %w", err)
		}
		result = &ExecResult{}
	case *sql.RollbackStmt:
		if err := db.schema.Rollback(); err != nil {
			return nil, fmt.Errorf("ROLLBACK error: %w", err)
		}
		result = &ExecResult{}
	case *sql.SavepointStmt:
		if err := db.schema.Savepoint(s.Name); err != nil {
			return nil, fmt.Errorf("SAVEPOINT error: %w", err)
		}
		result = &ExecResult{}
	case *sql.ReleaseStmt:
		if s.ToSave {
			if err := db.schema.RollbackTo(s.Name); err != nil {
				return nil, fmt.Errorf("ROLLBACK TO SAVEPOINT error: %w", err)
			}
		} else {
			if err := db.schema.Release(s.Name); err != nil {
				return nil, fmt.Errorf("RELEASE SAVEPOINT error: %w", err)
			}
		}
		result = &ExecResult{}
	case *sql.CreateIndexStmt:
		if err := db.schema.CreateIndex(s); err != nil {
			return nil, fmt.Errorf("CREATE INDEX error: %w", err)
		}
	case *sql.DropIndexStmt:
		if err := db.schema.DropIndex(s); err != nil {
			return nil, fmt.Errorf("DROP INDEX error: %w", err)
		}
	case *sql.AlterTableStmt:
		if err := db.schema.AlterTable(s); err != nil {
			return nil, fmt.Errorf("ALTER TABLE error: %w", err)
		}
	case *sql.VacuumStmt:
		if err := db.schema.Vacuum(); err != nil {
			return nil, fmt.Errorf("VACUUM error: %w", err)
		}
	default:
		return nil, fmt.Errorf("unsupported statement type: %T", stmt)
	}
	if result == nil {
		result = &ExecResult{}
	}
	return result, nil
}

func (db *Database) GetTableList() []string {
	db.defaultGate.Lock()
	defer db.defaultGate.Unlock()
	db.execution.RLockContext(context.Background())
	defer db.execution.RUnlock()
	_ = db.schema.RefreshReadCommitted()
	return db.schema.GetTableList()
}

func (db *Database) GetCreateTableSQL(name string) string {
	db.defaultGate.Lock()
	defer db.defaultGate.Unlock()
	db.execution.RLockContext(context.Background())
	defer db.execution.RUnlock()
	_ = db.schema.RefreshReadCommitted()
	return db.renderSQL(db.schema.GetCreateTableSQL(name))
}

func (db *Database) GetViewList() []string {
	db.defaultGate.Lock()
	defer db.defaultGate.Unlock()
	db.execution.RLockContext(context.Background())
	defer db.execution.RUnlock()
	_ = db.schema.RefreshReadCommitted()
	return db.schema.GetViewList()
}

func (db *Database) GetViewSQL(name string) string {
	db.defaultGate.Lock()
	defer db.defaultGate.Unlock()
	db.execution.RLockContext(context.Background())
	defer db.execution.RUnlock()
	_ = db.schema.RefreshReadCommitted()
	return db.renderSQL(db.schema.GetViewSQL(name))
}

func (db *Database) GetCreateViewSQL(name string) string {
	db.defaultGate.Lock()
	defer db.defaultGate.Unlock()
	db.execution.RLockContext(context.Background())
	defer db.execution.RUnlock()
	_ = db.schema.RefreshReadCommitted()
	return db.renderSQL(db.schema.GetCreateViewSQL(name))
}

func (db *Database) GetTriggerList() []string {
	db.defaultGate.Lock()
	defer db.defaultGate.Unlock()
	db.execution.RLockContext(context.Background())
	defer db.execution.RUnlock()
	_ = db.schema.RefreshReadCommitted()
	return db.schema.GetTriggerList()
}

func (db *Database) GetTriggerSQL(name string) string {
	db.defaultGate.Lock()
	defer db.defaultGate.Unlock()
	db.execution.RLockContext(context.Background())
	defer db.execution.RUnlock()
	_ = db.schema.RefreshReadCommitted()
	return db.renderSQL(db.schema.GetTriggerSQL(name))
}

func (db *Database) GetIndexList() []string {
	db.defaultGate.Lock()
	defer db.defaultGate.Unlock()
	db.execution.RLockContext(context.Background())
	defer db.execution.RUnlock()
	_ = db.schema.RefreshReadCommitted()
	return db.schema.GetIndexList()
}

func (db *Database) GetIndexSQL(name string) string {
	db.defaultGate.Lock()
	defer db.defaultGate.Unlock()
	db.execution.RLockContext(context.Background())
	defer db.execution.RUnlock()
	_ = db.schema.RefreshReadCommitted()
	return db.renderSQL(db.schema.GetIndexSQL(name))
}

func (db *Database) Stats() storage.LSMStats {
	return db.currentStorage.Load().Stats()
}

func (db *Database) Dump() string {
	var b strings.Builder

	tables := db.GetTableList()
	sort.Strings(tables)
	for _, name := range tables {
		createSQL := db.GetCreateTableSQL(name)
		b.WriteString(createSQL)
		b.WriteString(";\n")

		rs, err := db.Exec(db.renderSQL("SELECT * FROM " + quoteIdentifier(name)))
		if err != nil || rs == nil || len(rs.Columns) == 0 || len(rs.Rows) == 0 {
			continue
		}
		for _, row := range rs.Rows {
			b.WriteString("INSERT INTO ")
			b.WriteString(db.renderSQL(quoteIdentifier(name)))
			b.WriteString(" VALUES(")
			for i, col := range rs.Columns {
				if i > 0 {
					b.WriteString(",")
				}
				b.WriteString(db.renderSQL(formatValue(row[col.Name])))
			}
			b.WriteString(");\n")
		}
	}

	indexes := db.GetIndexList()
	sort.Strings(indexes)
	for _, name := range indexes {
		if db.isAutoIndex(name) {
			continue
		}
		idxSQL := db.GetIndexSQL(name)
		if idxSQL != "" {
			b.WriteString(idxSQL)
			b.WriteString(";\n")
		}
	}

	views := db.GetViewList()
	sort.Strings(views)
	for _, name := range views {
		viewSQL := db.GetCreateViewSQL(name)
		if viewSQL != "" {
			b.WriteString(viewSQL)
			b.WriteString(";\n")
		}

	}

	triggers := db.GetTriggerList()
	for _, name := range triggers {
		triggerSQL := db.GetTriggerSQL(name)
		if triggerSQL != "" {
			b.WriteString(triggerSQL)
			b.WriteString(";\n")
		}
	}

	b.WriteString("\n")
	return b.String()
}

func formatValue(v sql.Value) string {
	if v.Null {
		return "NULL"
	}
	switch v.Type {
	case sql.ColInteger:
		return fmt.Sprintf("%d", v.Int)
	case sql.ColReal:
		s := strconv.FormatFloat(v.Real, 'g', -1, 64)
		if !strings.Contains(s, ".") && !strings.Contains(s, "e") {
			s += ".0"
		}
		return s
	case sql.ColText:
		return sqlQuote(v.Text)
	case sql.ColBlob:
		return "X'" + hexEncode(v.Blob) + "'"
	default:
		return sqlQuote(v.String())
	}
}

func quoteIdentifier(name string) string { return `"` + strings.ReplaceAll(name, `"`, `""`) + `"` }

func sqlQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}

func hexEncode(data []byte) string {
	const hex = "0123456789ABCDEF"
	out := make([]byte, len(data)*2)
	for i, b := range data {
		out[i*2] = hex[b>>4]
		out[i*2+1] = hex[b&0x0f]
	}
	return string(out)
}

func FormatResult(result *ExecResult) string {
	var b strings.Builder

	if result.LastID > 0 {
		fmt.Fprintf(&b, "last inserted rowid: %d\n", result.LastID)
	}

	if len(result.Columns) > 0 && len(result.Rows) > 0 {
		widths := make([]int, len(result.Columns))
		for i, col := range result.Columns {
			widths[i] = len(col.Name)
		}
		for _, row := range result.Rows {
			for i, col := range result.Columns {
				w := len(row[col.Name].String())
				if w > widths[i] {
					widths[i] = w
				}
			}
		}

		for i, col := range result.Columns {
			if i > 0 {
				b.WriteString(" | ")
			}
			fmt.Fprintf(&b, "%-*s", widths[i], col.Name)
		}
		b.WriteString("\n")
		for i, w := range widths {
			if i > 0 {
				b.WriteString("-+-")
			}
			b.WriteString(strings.Repeat("-", w))
		}
		b.WriteString("\n")

		for _, row := range result.Rows {
			for i, col := range result.Columns {
				if i > 0 {
					b.WriteString(" | ")
				}
				fmt.Fprintf(&b, "%-*s", widths[i], row[col.Name].String())
			}
			b.WriteString("\n")
		}
	}

	if result.Changes > 0 && len(result.Columns) == 0 && result.LastID == 0 {
		fmt.Fprintf(&b, "changes: %d\n", result.Changes)
	}

	return b.String()
}

func (db *Database) isAutoIndex(name string) bool {
	db.defaultGate.Lock()
	defer db.defaultGate.Unlock()
	db.execution.RLockContext(context.Background())
	defer db.execution.RUnlock()
	_ = db.schema.RefreshReadCommitted()
	return db.schema.IsAutoIndex(name)
}
