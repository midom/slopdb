package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/chzyer/readline"

	"slopdb/db"
	"slopdb/storage"
)

var (
	queryRunning     atomic.Int32
	shuttingDown     atomic.Bool
	sigChan          chan os.Signal
	cancelFunc       func()
	cancelMu         sync.Mutex
	currentDB        *db.Database
	currentDBPath    string
	currentDBOptions *storage.Options
	currentDBMu      sync.Mutex // protects database replacement against signal-driven close
)

func main() {
	opt := storage.DefaultOptions()
	flags := flag.NewFlagSet(os.Args[0], flag.ContinueOnError)
	flags.BoolVar(&cliCfg.allowDotCommands, "allow-dot-commands", false, "allow CLI dot commands in non-interactive stdin (including shell and file commands)")
	flags.BoolVar(&opt.DisableWAL, "no-wal", false, "disable WAL; persist writes on flush/clean exit, unflushed writes are lost on crash")
	flags.BoolVar(&opt.DisableWALSync, "no-wal-fsync", false, "write WAL without fsync; power loss may lose recent writes, SSTables and MANIFEST stay synced")
	flags.BoolVar(&opt.DisableWALSync, "no-fsync", false, "alias for --no-wal-fsync")
	flags.DurationVar(&opt.QueryTimeout, "query-timeout", opt.QueryTimeout, "default SQL call timeout (0 disables it)")
	flags.DurationVar(&opt.LockTimeout, "lock-timeout", opt.LockTimeout, "maximum transaction lock wait per SQL call (0 fails immediately on contention)")
	flags.Int64Var(&opt.PageSize, "page-size", 0, "uncompressed SSTable block target in bytes (0 reuses stored setting, new databases default 4096)")
	flags.BoolVar(&opt.DeadlockDetection, "deadlock-detection", false, "detect transaction deadlocks and roll back one victim")
	compression := flags.String("compression", "auto", "SSTable data block compression: auto (none in L0, zstd in L1-L6), none, gzip, zstd, snappy, or lz4")
	l0Compression := flags.String("l0-compression", "auto", "override L0 compression: none, gzip, zstd, snappy, or lz4; auto inherits --compression")
	flags.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s [--no-wal | --no-wal-fsync] [--compression codec] [--l0-compression codec] [--page-size bytes] <database>\n", os.Args[0])
		flags.PrintDefaults()
	}
	if err := flags.Parse(os.Args[1:]); err != nil {
		if err == flag.ErrHelp {
			return
		}
		os.Exit(1)
	}
	if flags.NArg() != 1 {
		flags.Usage()
		os.Exit(1)
	}
	dbPath := flags.Arg(0)
	opt.CompressionType = storage.CompressionType(*compression)
	opt.L0CompressionType = storage.CompressionType(*l0Compression)
	currentDBOptions = opt

	database, err := db.Open(dbPath, opt)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error opening database: %v\n", err)
		os.Exit(1)
	}
	currentDB = database
	currentDBPath = dbPath
	defer func() {
		if err := closeCurrentDatabase(); err != nil {
			fmt.Fprintf(os.Stderr, "Error closing database: %v\n", err)
			os.Exit(1)
		}
	}()

	sigChan = make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		for sig := range sigChan {
			if sig == syscall.SIGTERM {
				shuttingDown.Store(true)
			}
			cancelMu.Lock()
			cancel := cancelFunc
			cancelMu.Unlock()
			if sig == syscall.SIGTERM {
				if cancel != nil {
					cancel()
				}
				exitCLI(0)
			}
			if queryRunning.Load() == 1 && cancel != nil {
				cancel()
				fmt.Fprint(os.Stderr, "\nInterrupt — stopping query\n")
			}
		}
	}()

	if readline.IsTerminal(int(os.Stdin.Fd())) {
		cliCfg.allowDotCommands = true
		processREPL()
	} else {
		processPipe()
	}
}

func closeCurrentDatabase() error {
	currentDBMu.Lock()
	defer currentDBMu.Unlock()
	if currentDB != nil {
		return currentDB.Close()
	}
	return nil
}

// os.Exit skips defers, so every explicit exit after opening must close first.
func exitCLI(status int) {
	shuttingDown.Store(true)
	if err := closeCurrentDatabase(); err != nil {
		fmt.Fprintf(os.Stderr, "Error closing database: %v\n", err)
		status = 1
	}
	os.Exit(status)
}

func processPipe() {
	data, err := io.ReadAll(os.Stdin)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error reading stdin: %v\n", err)
		exitCLI(1)
	}

	statements := splitSQL(string(data))
	for _, sql := range statements {
		sql = strings.TrimSpace(sql)
		if sql == "" {
			continue
		}
		if strings.HasPrefix(sql, ".") {
			handleDotCommand(sql)
			continue
		}
		executeSQL(sql, false)
	}
}

func processREPL() {
	rl, err := readline.NewEx(&readline.Config{
		Prompt:            "\033[32mslopdb> \033[0m",
		HistoryFile:       currentDBPath + ".history",
		InterruptPrompt:   "^C",
		EOFPrompt:         "exit",
		HistorySearchFold: true,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "readline init failed, falling back to basic mode: %v\n", err)
		processBasic()
		return
	}
	defer rl.Close()

	fmt.Printf("slopdb v0.1.0 — SQLite-like database with LSM-tree storage\n")
	fmt.Printf("Database: %s\n", currentDBPath)

	var buffer strings.Builder

	for {
		line, err := rl.Readline()
		if err != nil {
			if err == readline.ErrInterrupt {
				if queryRunning.Load() == 1 {
					continue
				}
				if buffer.Len() > 0 {
					buffer.Reset()
					rl.SetPrompt("\033[32mslopdb> \033[0m")
					fmt.Println("^C — query cancelled")
					continue
				}
				fmt.Println("^C")
				continue
			}
			break
		}

		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		if buffer.Len() == 0 && strings.HasPrefix(line, ".") {
			handleDotCommand(line)
			continue
		}

		buffer.WriteString(line)
		buffer.WriteString(" ")

		if strings.HasSuffix(line, ";") {
			sql := strings.TrimSpace(buffer.String())
			sql = strings.TrimSuffix(sql, ";")
			buffer.Reset()
			rl.SetPrompt("\033[32mslopdb> \033[0m")

			if sql != "" {
				executeSQL(sql, true)
			}
		} else {
			rl.SetPrompt("\033[36m   ..> \033[0m")
		}
	}
	fmt.Println()
}

func processBasic() {
	data, err := io.ReadAll(os.Stdin)
	if err != nil {
		return
	}

	statements := splitSQL(string(data))
	for _, sql := range statements {
		sql = strings.TrimSpace(sql)
		if strings.HasPrefix(sql, ".") {
			handleDotCommand(sql)
			continue
		}
		if sql == "" {
			continue
		}
		executeSQL(sql, false)
	}
}

func executeSQL(sql string, showTiming bool) {
	if shuttingDown.Load() {
		return
	}
	if cliCfg.echo || cliCfg.trace {
		cliPrintf("%s\n", sql)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancelMu.Lock()
	if shuttingDown.Load() {
		cancelMu.Unlock()
		cancel()
		return
	}
	cancelFunc = cancel
	cancelMu.Unlock()
	defer func() {
		cancelMu.Lock()
		cancelFunc = nil
		cancelMu.Unlock()
		cancel()
	}()

	queryRunning.Store(1)
	defer queryRunning.Store(0)

	wallStart := time.Now()

	type execResult struct {
		result *db.ExecResult
		err    error
	}
	done := make(chan execResult, 1)

	go func() {
		result, err := currentDB.ExecContext(ctx, sql)
		done <- execResult{result, err}
	}()

	var result *db.ExecResult
	var err error

	select {
	case <-ctx.Done():
		<-done // Wait for the executor to finish rollback before accepting another query.
		err = fmt.Errorf("query interrupted")
	case r := <-done:
		result = r.result
		err = r.err
	}

	wallElapsed := time.Since(wallStart)

	if err != nil {
		if err.Error() == "context canceled" {
			err = fmt.Errorf("query interrupted")
		}
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		if cliCfg.bail {
			exitCLI(1)
		}
		return
	}

	if result != nil && result.LastID > 0 {
		cliPrintf("last inserted rowid: %d\n", result.LastID)
	}

	if result != nil && len(result.Columns) > 0 && len(result.Rows) > 0 {
		output := formatSQLResult(result)
		if output != "" {
			cliPrint(output)
		}
	}

	if result != nil && result.Changes > 0 && len(result.Columns) == 0 && result.LastID == 0 {
		cliPrintf("changes: %d\n", result.Changes)
	}

	if cliCfg.timer || showTiming {
		fmt.Fprintf(os.Stderr, "Run Time: real %.3f ms\n",
			float64(wallElapsed.Microseconds())/1000.0)
	}

	if cliCfg.onceFile != nil {
		cliCfg.onceFile.Close()
		cliCfg.onceFile = nil
		cliCfg.output = os.Stdout
	}
}

func handleDotCommand(cmd string) {
	// All entry points, including .read content and the basic-mode fallback,
	// share this gate. Input cannot enable its own dot-command permission.
	if !cliCfg.allowDotCommands {
		fmt.Fprintln(os.Stderr, "Error: dot commands are disabled in non-interactive input; use --allow-dot-commands to enable them")
		exitCLI(1)
		return
	}
	switch {
	case cmd == ".exit" || cmd == ".quit":
		fmt.Println("Bye!")
		exitCLI(0)

	case cmd == ".dump":
		fmt.Print(currentDB.Dump())

	case cmd == ".tables":
		names := currentDB.GetTableList()
		if len(names) == 0 {
			fmt.Println("(no tables)")
		} else {
			for _, n := range names {
				fmt.Print("  " + n)
			}
			fmt.Println()
		}

	case cmd == ".views":
		names := currentDB.GetViewList()
		if len(names) == 0 {
			fmt.Println("(no views)")
		} else {
			for _, n := range names {
				fmt.Print("  " + n)
			}
			fmt.Println()
		}

	case cmd == ".triggers":
		names := currentDB.GetTriggerList()
		if len(names) == 0 {
			fmt.Println("(no triggers)")
		} else {
			for _, n := range names {
				fmt.Print("  " + n)
			}
			fmt.Println()
		}

	case cmd == ".schema":
		for _, n := range currentDB.GetTableList() {
			fmt.Println(currentDB.GetCreateTableSQL(n) + ";")
		}
		for _, n := range currentDB.GetIndexList() {
			sql := currentDB.GetIndexSQL(n)
			if sql != "" {
				fmt.Println(sql + ";")
			}
		}
		for _, n := range currentDB.GetViewList() {
			sql := currentDB.GetCreateViewSQL(n)
			if sql != "" {
				fmt.Println(sql + ";")
			}
		}
		for _, n := range currentDB.GetTriggerList() {
			sql := currentDB.GetTriggerSQL(n)
			if sql != "" {
				fmt.Println(sql + ";")
			}
		}

	case cmd == ".indexes":
		names := currentDB.GetIndexList()
		if len(names) == 0 {
			fmt.Println("(no indexes)")
		} else {
			for _, n := range names {
				fmt.Print("  " + n)
			}
			fmt.Println()
		}

	case cmd == ".databases":
		fmt.Printf("main: %s r/w\n", currentDBPath)

	case cmd == ".fullschema":
		tables := currentDB.GetTableList()
		for _, n := range tables {
			fmt.Println(currentDB.GetCreateTableSQL(n) + ";")
		}
		for _, n := range currentDB.GetIndexList() {
			if !isAutoIndexName(n, tables) {
				sql := currentDB.GetIndexSQL(n)
				if sql != "" {
					fmt.Println(sql + ";")
				}
			}
		}
		for _, n := range currentDB.GetViewList() {
			sql := currentDB.GetViewSQL(n)
			if sql != "" {
				fmt.Printf("CREATE VIEW %s AS %s\n", n, sql)
			}
		}
		for _, n := range currentDB.GetTriggerList() {
			sql := currentDB.GetTriggerSQL(n)
			if sql != "" {
				fmt.Println(sql + ";")
			}
		}

	case cmd == ".integrity":
		report, err := currentDB.CheckIntegrity()
		if err != nil {
			fmt.Fprintf(os.Stderr, "Integrity error: %v\n", err)
			if cliCfg.bail {
				exitCLI(1)
			}
			return
		}
		fmt.Printf("ok: %d rows, %d index entries, %d SSTables verified\n", report.SQL.Rows, report.SQL.IndexEntries, report.Storage.Tables)

	case strings.HasPrefix(cmd, ".backup "):
		destination := strings.TrimSpace(strings.TrimPrefix(cmd, ".backup "))
		if err := currentDB.Backup(destination); err != nil {
			fmt.Fprintf(os.Stderr, "Backup error: %v\n", err)
			if cliCfg.bail {
				exitCLI(1)
			}
			return
		}
		fmt.Printf("Backed up database to %q\n", destination)

	case cmd == ".manifest":
		showManifest()

	case cmd == ".status":
		cliPrint(formatDatabaseStatus(currentDB.Stats(), currentDBOptions))

	case cmd == ".locks":
		printLockWaits()

	case cmd == ".explain" || strings.HasPrefix(cmd, ".explain "):
		printQueryExplanation(strings.TrimSpace(strings.TrimPrefix(cmd, ".explain")))

	case strings.HasPrefix(cmd, ".plan "):
		printSchemaChangePlan(strings.TrimSpace(strings.TrimPrefix(cmd, ".plan ")))

	case cmd == ".version":
		fmt.Printf("slopdb v0.1.0 [compiled with %s] DB: %s\n", runtime.Version(), currentDBPath)

	case cmd == ".help":
		fmt.Println("Commands (interactive sessions, or --allow-dot-commands for redirected stdin):")
		fmt.Println("  .exit, .quit      Exit")
		fmt.Println("  .tables           List tables")
		fmt.Println("  .views            List views")
		fmt.Println("  .indexes          List indexes")
		fmt.Println("  .triggers         List triggers")
		fmt.Println("  .schema           Show all schema objects")
		fmt.Println("  .schema NAME      Show CREATE TABLE for a specific table")
		fmt.Println("  .fullschema       Schema excluding auto-indexes")
		fmt.Println("  .databases        List attached databases")
		fmt.Println("  .dump             Render database content as SQL")
		fmt.Println("  .import FILE TBL  Import CSV data into TABLE")
		fmt.Println("  .read FILE        Execute SQL from a file")
		fmt.Println("  .open ?FILE?      Close current and open a new database")
		fmt.Println("  .save FILE        Save current database to FILE")
		fmt.Println("  .cd DIRECTORY     Change working directory")
		fmt.Println("  .output ?FILE?    Send output to FILE or stdout")
		fmt.Println("  .once FILE        Output next command only to FILE")
		fmt.Println("  .mode ?MODE?      Set output mode (table/csv/json/line/markdown/box/tabs)")
		fmt.Println("  .nullvalue STR    Set string for NULL values")
		fmt.Println("  .print STR...     Print literal string")
		fmt.Println("  .echo on|off      Echo SQL before execution")
		fmt.Println("  .trace on|off     Trace SQL execution")
		fmt.Println("  .timer on|off     Toggle SQL timing display")
		fmt.Println("  .stats on|off     Same as .timer")
		fmt.Println("  .bail on|off      Stop after hitting an error")
		fmt.Println("  .changes on|off   Show total row changes per statement")
		fmt.Println("  .shell CMD...     Run command in system shell")
		fmt.Println("  .system CMD...    Same as .shell")
		fmt.Println("  .version          Show version info")
		fmt.Println("  .backup DIRECTORY Create a consistent online database backup")
		fmt.Println("  .integrity        Check storage, rows, constraints and indexes")
		fmt.Println("  .manifest         Show LSM tree diagnostics")
		fmt.Println("  .status           Summarize durability and storage usage")
		fmt.Println("  .locks            Show waiting sessions and their blockers")
		fmt.Println("  .explain SELECT   Explain query access and sorting without running it")
		fmt.Println("  .plan DDL          Explain ALTER or CREATE/DROP INDEX work and locks")
		fmt.Println("  .help             Show this help")
		fmt.Println("SQL metadata commands (end with a semicolon):")
		fmt.Println("  SHOW DATABASES; SHOW [FULL] TABLES;")
		fmt.Println("  SHOW [FULL] COLUMNS FROM table; DESCRIBE table; DESC table;")
		fmt.Println("  SHOW INDEX FROM table; SHOW TRIGGERS;")
		fmt.Println("  SHOW CREATE TABLE table; SHOW CREATE VIEW view; SHOW CREATE TRIGGER trigger;")
		fmt.Println("  Use LIKE 'pattern' or WHERE expression to filter catalog results (WHERE only for indexes).")
		fmt.Println("  Database qualifiers use main.")
		fmt.Println("Start with --no-wal to persist writes on flush/clean exit; unflushed writes are lost on crash.")
		fmt.Println("Use --no-wal-fsync (or --no-fsync) to keep WAL writes without fsync; power loss may lose recent writes.")
		fmt.Println("Compression defaults to auto: none in L0, zstd in L1-L6. Use --compression=none/gzip/zstd/snappy/lz4 to override all levels.")
		fmt.Println("Use --page-size=16384/65536/1048576 to select a persisted SSTable block target; PRAGMA page_size=N changes future writers.")
		fmt.Println("Use --l0-compression=none/gzip/zstd/snappy/lz4 to override L0 only; auto inherits --compression.")
		fmt.Println("Use --query-timeout=5s and --lock-timeout=30s to limit queries and transaction lock waits. Zero disables the query timeout; a zero lock timeout fails immediately on contention.")
		fmt.Println("Use --deadlock-detection to detect lock cycles and roll back one transaction.")
		fmt.Println("SELECT ... FOR UPDATE or FOR SHARE / LOCK IN SHARE MODE locks rows until transaction end (statement end in autocommit).")
		fmt.Println("Append NOWAIT to fail on row contention, or SKIP LOCKED to omit unavailable rows, after the locking clause and any LIMIT.")

	case strings.HasPrefix(cmd, ".schema "):
		tableName := strings.TrimSpace(strings.TrimPrefix(cmd, ".schema "))
		sql := currentDB.GetCreateTableSQL(tableName)
		if sql == "" {
			fmt.Printf("Table %q not found\n", tableName)
		} else {
			fmt.Println(sql + ";")
		}

	case cmd == ".mode" || strings.HasPrefix(cmd, ".mode "):
		handleMode(cmd)

	case strings.HasPrefix(cmd, ".import "):
		parts := strings.Fields(cmd)
		if len(parts) != 3 {
			fmt.Println("Usage: .import FILE TABLE")
			return
		}
		handleImport(parts[1], parts[2])

	case strings.HasPrefix(cmd, ".read "):
		handleRead(strings.TrimSpace(strings.TrimPrefix(cmd, ".read ")))

	case cmd == ".open" || strings.HasPrefix(cmd, ".open "):
		handleOpen(cmd)

	case strings.HasPrefix(cmd, ".save "):
		arg := strings.TrimSpace(strings.TrimPrefix(cmd, ".save "))
		if arg == "" {
			fmt.Println("Usage: .save FILE")
			return
		}
		dumpSQL := currentDB.Dump()
		if err := os.WriteFile(arg, []byte(dumpSQL), 0644); err != nil {
			fmt.Fprintf(os.Stderr, "Error: cannot save to %q: %v\n", arg, err)
			return
		}
		fmt.Printf("Saved database to %q\n", arg)

	case strings.HasPrefix(cmd, ".cd "):
		dir := strings.TrimSpace(strings.TrimPrefix(cmd, ".cd "))
		if err := os.Chdir(dir); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		}

	case cmd == ".output":
		cliCfg.output = os.Stdout

	case strings.HasPrefix(cmd, ".output "):
		arg := strings.TrimSpace(strings.TrimPrefix(cmd, ".output "))
		f, err := os.Create(arg)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		} else {
			cliCfg.output = f
		}

	case strings.HasPrefix(cmd, ".once "):
		arg := strings.TrimSpace(strings.TrimPrefix(cmd, ".once "))
		f, err := os.Create(arg)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		} else {
			cliCfg.onceFile = f
			cliCfg.output = f
		}

	case cmd == ".nullvalue" || strings.HasPrefix(cmd, ".nullvalue "):
		arg := strings.TrimSpace(strings.TrimPrefix(cmd, ".nullvalue "))
		cliCfg.nullStr = arg

	case cmd == ".echo" || strings.HasPrefix(cmd, ".echo "):
		handleBoolFlag(cmd, ".echo")

	case cmd == ".trace" || strings.HasPrefix(cmd, ".trace "):
		handleBoolFlag(cmd, ".trace")

	case cmd == ".timer" || strings.HasPrefix(cmd, ".timer "):
		handleBoolFlag(cmd, ".timer")

	case cmd == ".stats":
		showStats()
	case strings.HasPrefix(cmd, ".stats "):
		handleBoolFlag(cmd, ".stats")

	case cmd == ".bail" || strings.HasPrefix(cmd, ".bail "):
		handleBoolFlag(cmd, ".bail")

	case cmd == ".changes" || strings.HasPrefix(cmd, ".changes "):
		handleBoolFlag(cmd, ".changes")

	case strings.HasPrefix(cmd, ".print "):
		cliPrint(strings.TrimPrefix(cmd, ".print "))
		cliPrint("\n")

	case strings.HasPrefix(cmd, ".shell "):
		runShell(strings.TrimSpace(strings.TrimPrefix(cmd, ".shell ")))

	case strings.HasPrefix(cmd, ".system "):
		runShell(strings.TrimSpace(strings.TrimPrefix(cmd, ".system ")))

	default:
		fmt.Printf("Unknown command: %s\n", cmd)
	}
}

func splitSQL(input string) []string {
	var result []string
	var current strings.Builder
	depth := 0
	trigger := false
	block := false
	cases := 0
	flush := func() {
		text := strings.TrimSpace(current.String())
		if text != "" {
			result = append(result, text)
		}
		current.Reset()
		trigger = false
		block = false
		cases = 0
	}
	for i := 0; i < len(input); {
		ch := input[i]
		if current.Len() == 0 && (ch == ' ' || ch == '\n' || ch == '\r' || ch == '\t') {
			i++
			continue
		}
		if current.Len() == 0 && ch == '.' {
			end := strings.IndexByte(input[i:], '\n')
			if end < 0 {
				end = len(input) - i
			}
			current.WriteString(input[i : i+end])
			i += end
			flush()
			continue
		}
		if ch == '\'' || ch == '"' || ch == '`' || ch == '[' {
			end := ch
			if ch == '[' {
				end = ']'
			}
			current.WriteByte(ch)
			i++
			for i < len(input) {
				c := input[i]
				current.WriteByte(c)
				i++
				if c == end {
					if ch != '[' && i < len(input) && input[i] == end {
						current.WriteByte(input[i])
						i++
						continue
					}
					break
				}
			}
			continue
		}
		if ch == '-' && i+1 < len(input) && input[i+1] == '-' {
			for i < len(input) && input[i] != '\n' {
				current.WriteByte(input[i])
				i++
			}
			continue
		}
		if ch == '/' && i+1 < len(input) && input[i+1] == '*' {
			current.WriteString("/*")
			i += 2
			for i < len(input) {
				if input[i] == '*' && i+1 < len(input) && input[i+1] == '/' {
					current.WriteString("*/")
					i += 2
					break
				}
				current.WriteByte(input[i])
				i++
			}
			continue
		}
		if ch >= 'a' && ch <= 'z' || ch >= 'A' && ch <= 'Z' || ch == '_' {
			start := i
			i++
			for i < len(input) && (input[i] >= 'a' && input[i] <= 'z' || input[i] >= 'A' && input[i] <= 'Z' || input[i] >= '0' && input[i] <= '9' || input[i] == '_') {
				i++
			}
			word := input[start:i]
			upper := strings.ToUpper(word)
			if upper == "TRIGGER" && strings.HasPrefix(strings.ToUpper(strings.TrimSpace(current.String())), "CREATE") {
				trigger = true
			}
			if trigger && upper == "BEGIN" {
				block = true
			}
			if block && upper == "CASE" {
				cases++
			}
			if block && upper == "END" {
				if cases > 0 {
					cases--
				} else {
					block = false
				}
			}
			current.WriteString(word)
			continue
		}
		current.WriteByte(ch)
		i++
		switch ch {
		case '(':
			depth++
		case ')':
			depth--
		case ';':
			if depth == 0 && !block {
				flush()
			}
		}
	}
	flush()
	return result
}

func showManifest() {
	stats := currentDB.Stats()
	fmt.Printf("=== LSM Tree Diagnostics ===\n")
	fmt.Printf("Directory: %s\n", stats.Dir)
	fmt.Printf("Sequence:  %d    Next file: %d\n", stats.SeqNum, stats.FileNum)
	walStatus := map[bool]string{true: "active", false: "inactive"}[stats.WALActive]
	if stats.WALSyncDisabled {
		walStatus += " (fsync disabled)"
	}
	fmt.Printf("WAL:       %s\n", walStatus)
	fmt.Printf("Memtable:  %d bytes\n", stats.MemSize)
	if stats.ImmSize > 0 {
		fmt.Printf("Immutable: %d bytes\n", stats.ImmSize)
	}
	fmt.Println()

	for lv := 0; lv < 7; lv++ {
		ls := stats.Levels[lv]
		if ls.FileCount == 0 {
			continue
		}
		fmt.Printf("Level %d: %d files, %s\n", lv, ls.FileCount, formatBytes(ls.TotalSize))
		for _, f := range ls.Files {
			fmt.Printf("  #%d  %s  [%q .. %q]  seeks=%d\n",
				f.Number,
				formatBytes(int64(f.Size)),
				f.Smallest, f.Largest,
				f.Seeks,
			)
		}
	}
}

func isAutoIndexName(name string, tables []string) bool {
	for _, t := range tables {
		if strings.HasPrefix(name, t+"_") && strings.HasSuffix(name, "_idx") {
			return true
		}
	}
	return false
}

func showStats() {
	stats := currentDB.Stats()
	total := stats.BloomHits + stats.BloomMisses
	bloomRate := 0.0
	if total > 0 {
		bloomRate = float64(stats.BloomHits) / float64(total) * 100
	}
	fmt.Printf("Bloom filter:  hits=%d  misses=%d  rate=%.1f%%\n", stats.BloomHits, stats.BloomMisses, bloomRate)

	totalCache := stats.CacheHits + stats.CacheMisses
	cacheRate := 0.0
	if totalCache > 0 {
		cacheRate = float64(stats.CacheHits) / float64(totalCache) * 100
	}
	fmt.Printf("Block cache:   hits=%d  misses=%d  rate=%.1f%%\n", stats.CacheHits, stats.CacheMisses, cacheRate)
}

func formatBytes(n int64) string {
	switch {
	case n >= 1<<30:
		return fmt.Sprintf("%.1f GB", float64(n)/(1<<30))
	case n >= 1<<20:
		return fmt.Sprintf("%.1f MB", float64(n)/(1<<20))
	case n >= 1<<10:
		return fmt.Sprintf("%.1f KB", float64(n)/(1<<10))
	default:
		return fmt.Sprintf("%d B", n)
	}
}
