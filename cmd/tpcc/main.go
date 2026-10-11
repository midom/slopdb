// tpcc runs a reduced, TPC-C-inspired workload against the embedded SlopDB API.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math"
	"math/rand"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"text/tabwriter"
	"time"

	"slopdb/db"
	"slopdb/sql"
	"slopdb/storage"
)

const initialOrders = 30

var names = [...]string{"new_order", "payment", "order_status", "delivery", "stock_level"}
var invalidItem = errors.New("expected invalid-item rollback")

type config struct {
	Workers     int           `json:"workers"`
	Duration    time.Duration `json:"duration_ns"`
	Warehouses  int           `json:"warehouses"`
	Districts   int           `json:"districts_per_warehouse"`
	Customers   int           `json:"customers_per_district"`
	Items       int           `json:"items"`
	Seed        int64         `json:"seed"`
	Retries     int           `json:"max_retries"`
	LockTimeout time.Duration `json:"lock_timeout_ns"`
	TxTimeout   time.Duration `json:"transaction_timeout_ns"`
	Prepared    bool          `json:"prepared_statements"`
	statements  *preparedStatements
	WALFsync    bool   `json:"wal_fsync"`
	Dir         string `json:"database_dir"`
	JSON        string `json:"report_file,omitempty"`
}

type effect struct {
	NewOrders, Lines, Delivered, DeliveryCents, PaymentCents int64
}

func (e *effect) add(other effect) {
	e.NewOrders += other.NewOrders
	e.Lines += other.Lines
	e.Delivered += other.Delivered
	e.DeliveryCents += other.DeliveryCents
	e.PaymentCents += other.PaymentCents
}

type samples struct {
	committed, failed, rollbacks, canceled, retries int64
	latencies                                       []time.Duration
}

type workerStats struct {
	types       [5]samples
	errors      map[string]int64
	retryErrors map[string]int64
	effects     effect
}

type transactionReport struct {
	Committed int64   `json:"committed"`
	Failed    int64   `json:"failed"`
	Rollbacks int64   `json:"expected_rollbacks"`
	Canceled  int64   `json:"canceled_at_shutdown"`
	Retries   int64   `json:"retries"`
	TPS       float64 `json:"transactions_per_second"`
	MeanMS    float64 `json:"mean_ms"`
	P50MS     float64 `json:"p50_ms"`
	P95MS     float64 `json:"p95_ms"`
	P99MS     float64 `json:"p99_ms"`
	MaxMS     float64 `json:"max_ms"`
}

type report struct {
	Config             config                       `json:"config"`
	Started            time.Time                    `json:"started_at"`
	PrepareSeconds     float64                      `json:"prepare_seconds"`
	LoadSeconds        float64                      `json:"load_seconds"`
	RunSeconds         float64                      `json:"run_seconds"`
	DrainSeconds       float64                      `json:"drain_seconds"`
	Transactions       map[string]transactionReport `json:"transactions"`
	Total              transactionReport            `json:"total"`
	NewOrdersPerMinute float64                      `json:"new_orders_per_minute"`
	Errors             map[string]int64             `json:"errors"`
	RetryErrors        map[string]int64             `json:"retry_errors"`
	Checks             map[string]string            `json:"consistency_checks"`
	ReopenChecks       map[string]string            `json:"reopen_consistency_checks"`
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "tpcc:", err)
		os.Exit(1)
	}
}

func run() (retErr error) {
	var c config
	flag.IntVar(&c.Workers, "workers", 10, "concurrent transaction workers")
	flag.DurationVar(&c.Duration, "duration", time.Minute, "measurement time, excluding loading and validation")
	flag.IntVar(&c.Warehouses, "warehouses", 2, "warehouses")
	flag.IntVar(&c.Districts, "districts", 10, "districts per warehouse")
	flag.IntVar(&c.Customers, "customers", 100, "customers per district")
	flag.IntVar(&c.Items, "items", 1000, "items and stock rows per warehouse")
	flag.Int64Var(&c.Seed, "seed", 42, "random seed")
	flag.IntVar(&c.Retries, "retries", 5, "maximum retries per logical transaction")
	flag.DurationVar(&c.LockTimeout, "lock-timeout", 250*time.Millisecond, "lock wait budget per statement")
	flag.DurationVar(&c.TxTimeout, "tx-timeout", 5*time.Second, "transaction execution timeout")
	flag.BoolVar(&c.Prepared, "prepared", true, "reuse prepared statements (false executes SQL text for comparison)")
	flag.BoolVar(&c.WALFsync, "wal-fsync", false, "enable WAL fsync (WAL is always enabled)")
	flag.StringVar(&c.Dir, "dir", "", "fresh database directory; default creates a directory under /tmp")
	flag.StringVar(&c.JSON, "json", "", "write a JSON results file")
	flag.Parse()
	if flag.NArg() != 0 || c.Workers < 1 || c.Duration <= 0 || c.Warehouses < 1 || c.Districts < 1 || c.Customers < 1 || c.Items < 15 || c.Retries < 0 || c.LockTimeout < 0 || c.TxTimeout <= 0 {
		return errors.New("invalid configuration: positive sizes/timeouts required, items >= 15, retries and lock-timeout >= 0")
	}
	var err error
	if c.Dir == "" {
		c.Dir, err = os.MkdirTemp("/tmp", "slopdb-tpcc-*")
	} else {
		c.Dir, err = filepath.Abs(c.Dir)
		if err == nil {
			err = os.Mkdir(c.Dir, 0755) // Never erase or reuse an existing database.
		}
	}
	if err != nil {
		return err
	}
	options := storage.DefaultOptions()
	options.DisableWALSync = !c.WALFsync
	options.LockTimeout = c.LockTimeout
	options.DeadlockDetection = true
	database, err := db.Open(c.Dir, options)
	if err != nil {
		return err
	}
	defer func() {
		if database != nil {
			retErr = errors.Join(retErr, database.Close())
		}
	}()
	fmt.Printf("TPC-C-inspired workload: workers=%d duration=%s warehouses=%d districts=%d customers=%d items=%d\n", c.Workers, c.Duration, c.Warehouses, c.Districts, c.Customers, c.Items)
	fmt.Printf("Database: %s\nWAL enabled; WAL fsync=%t; deadlock detection enabled\n", c.Dir, c.WALFsync)
	loadStart := time.Now()
	if err = load(database, c); err != nil {
		return fmt.Errorf("load: %w", err)
	}
	r := report{Config: c, LoadSeconds: time.Since(loadStart).Seconds()}
	prepareStart := time.Now()
	c.statements, err = prepareWorkload(database, c)
	if err != nil {
		return err
	}
	defer c.statements.Close()
	r.PrepareSeconds = time.Since(prepareStart).Seconds()
	fmt.Printf("Prepared statements=%t; preparation took %.3fs\n", c.Prepared, r.PrepareSeconds)
	fmt.Printf("Loaded nine tables in %.2fs. Mix: 45%% new-order, 43%% payment, 4%% each order-status/delivery/stock-level.\n", r.LoadSeconds)
	// Check every SQL path before measurement; roll back so counters start clean.
	for kind := range names {
		p := makePlan(rand.New(rand.NewSource(c.Seed+int64(kind))), c, kind)
		p.invalid = false
		tx, beginErr := database.Begin()
		if beginErr != nil {
			return beginErr
		}
		ctx, cancel := context.WithTimeout(context.Background(), c.TxTimeout)
		_, checkErr := execute(ctx, tx, p, c, 1)
		cancel()
		rollbackErr := tx.Rollback()
		if checkErr != nil || rollbackErr != nil {
			return fmt.Errorf("preflight %s: %w", names[kind], errors.Join(checkErr, rollbackErr))
		}
	}
	parent, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	var committed, failed, retried atomic.Int64
	stats := make([]workerStats, c.Workers)
	var historyID atomic.Int64
	var wg sync.WaitGroup
	start := make(chan struct{})
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	for worker := range stats {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			<-start
			stats[id] = work(ctx, database, c, id, &historyID, &committed, &failed, &retried)
		}(worker)
	}
	r.Started = time.Now().UTC()
	deadline := time.NewTimer(c.Duration)
	progress := time.NewTicker(10 * time.Second)
	close(start)
	running := true
	for running {
		select {
		case <-deadline.C:
			running = false
		case <-parent.Done():
			running = false
		case <-progress.C:
			fmt.Printf("%5.1fs: committed=%d failed=%d retries=%d\n", time.Since(r.Started).Seconds(), committed.Load(), failed.Load(), retried.Load())
		}
	}
	deadline.Stop()
	progress.Stop()
	r.RunSeconds = time.Since(r.Started).Seconds()
	cancel()
	drainStart := time.Now()
	wg.Wait()
	r.DrainSeconds = time.Since(drainStart).Seconds()
	effects := summarize(&r, stats)
	fmt.Println("Checking committed data...")
	r.Checks, err = validate(database, c, effects, r.Transactions["payment"].Committed)
	if err != nil {
		retErr = errors.Join(retErr, err)
	}
	closeErr := database.Close()
	database = nil
	if closeErr != nil {
		retErr = errors.Join(retErr, fmt.Errorf("close: %w", closeErr))
	}
	if closeErr == nil {
		database, err = db.Open(c.Dir, options)
		if err == nil {
			r.ReopenChecks, err = validate(database, c, effects, r.Transactions["payment"].Committed)
		}
		if err != nil {
			retErr = errors.Join(retErr, fmt.Errorf("reopen validation: %w", err))
		}
	}
	printReport(r)
	if c.JSON != "" {
		data, marshalErr := json.MarshalIndent(r, "", "  ")
		if marshalErr == nil {
			marshalErr = os.WriteFile(c.JSON, append(data, '\n'), 0644)
		}
		if marshalErr != nil {
			retErr = errors.Join(retErr, marshalErr)
		} else {
			fmt.Println("JSON report:", c.JSON)
		}
	}
	if r.Total.Failed != 0 {
		retErr = errors.Join(retErr, fmt.Errorf("%d unexpected transaction failures", r.Total.Failed))
	}
	return retErr
}

func load(database *db.Database, c config) error {
	ddl := []string{
		"CREATE TABLE warehouse (w_id INTEGER PRIMARY KEY, ytd INTEGER NOT NULL)",
		"CREATE TABLE district (w_id INTEGER, d_id INTEGER, ytd INTEGER NOT NULL, next_o_id INTEGER NOT NULL, PRIMARY KEY(w_id,d_id))",
		"CREATE TABLE customer (w_id INTEGER, d_id INTEGER, c_id INTEGER, balance INTEGER NOT NULL, ytd_payment INTEGER NOT NULL, payment_count INTEGER NOT NULL, delivery_count INTEGER NOT NULL, PRIMARY KEY(w_id,d_id,c_id))",
		"CREATE TABLE item (i_id INTEGER PRIMARY KEY, price INTEGER NOT NULL)",
		"CREATE TABLE stock (w_id INTEGER, i_id INTEGER, quantity INTEGER NOT NULL, ytd INTEGER NOT NULL, order_count INTEGER NOT NULL, remote_count INTEGER NOT NULL, PRIMARY KEY(w_id,i_id))",
		"CREATE TABLE orders (w_id INTEGER, d_id INTEGER, o_id INTEGER, c_id INTEGER NOT NULL, carrier_id INTEGER, line_count INTEGER NOT NULL, total INTEGER NOT NULL, PRIMARY KEY(w_id,d_id,o_id))",
		"CREATE INDEX orders_customer ON orders(w_id,d_id,c_id,o_id)",
		"CREATE TABLE new_order (w_id INTEGER, d_id INTEGER, o_id INTEGER, PRIMARY KEY(w_id,d_id,o_id))",
		"CREATE TABLE order_line (w_id INTEGER, d_id INTEGER, o_id INTEGER, number INTEGER, i_id INTEGER NOT NULL, supply_w_id INTEGER NOT NULL, quantity INTEGER NOT NULL, amount INTEGER NOT NULL, delivered INTEGER NOT NULL, PRIMARY KEY(w_id,d_id,o_id,number))",
		"CREATE TABLE history (h_id INTEGER PRIMARY KEY, w_id INTEGER NOT NULL, d_id INTEGER NOT NULL, c_w_id INTEGER NOT NULL, c_d_id INTEGER NOT NULL, c_id INTEGER NOT NULL, amount INTEGER NOT NULL)",
	}
	for _, query := range ddl {
		if _, err := database.Exec(query); err != nil {
			return err
		}
	}
	tables := []struct {
		name string
		rows []string
	}{}
	add := func(name string, rows []string) {
		tables = append(tables, struct {
			name string
			rows []string
		}{name, rows})
	}
	var items, warehouses, districts, customers, stocks, orders, pending, lines []string
	for i := 1; i <= c.Items; i++ {
		items = append(items, fmt.Sprintf("(%d,%d)", i, price(i)))
	}
	for w := 1; w <= c.Warehouses; w++ {
		warehouses = append(warehouses, fmt.Sprintf("(%d,0)", w))
		for i := 1; i <= c.Items; i++ {
			stocks = append(stocks, fmt.Sprintf("(%d,%d,100,0,0,0)", w, i))
		}
		for d := 1; d <= c.Districts; d++ {
			districts = append(districts, fmt.Sprintf("(%d,%d,0,%d)", w, d, initialOrders+1))
			for customer := 1; customer <= c.Customers; customer++ {
				customers = append(customers, fmt.Sprintf("(%d,%d,%d,0,0,0,0)", w, d, customer))
			}
			for o := 1; o <= initialOrders; o++ {
				total := 0
				carrier, delivered := "1", 1
				if o > initialOrders-10 {
					carrier, delivered = "NULL", 0
					pending = append(pending, fmt.Sprintf("(%d,%d,%d)", w, d, o))
				}
				for n := 1; n <= 5; n++ {
					i := (o*5+n-1)%c.Items + 1
					amount := price(i) * 5
					total += amount
					lines = append(lines, fmt.Sprintf("(%d,%d,%d,%d,%d,%d,5,%d,%d)", w, d, o, n, i, w, amount, delivered))
				}
				orders = append(orders, fmt.Sprintf("(%d,%d,%d,%d,%s,5,%d)", w, d, o, (o-1)%c.Customers+1, carrier, total))
			}
		}
	}
	add("item", items)
	add("warehouse", warehouses)
	add("district", districts)
	add("customer", customers)
	add("stock", stocks)
	add("orders", orders)
	add("new_order", pending)
	add("order_line", lines)
	for _, table := range tables {
		for offset := 0; offset < len(table.rows); offset += 100 {
			end := offset + 100
			if end > len(table.rows) {
				end = len(table.rows)
			}
			if _, err := database.Exec("INSERT INTO " + table.name + " VALUES " + strings.Join(table.rows[offset:end], ",")); err != nil {
				return fmt.Errorf("%s: %w", table.name, err)
			}
		}
	}
	return nil
}

func price(item int) int { return 100 + item%9901 }

type orderItem struct{ id, warehouse, quantity int }
type plan struct {
	kind, w, d, customer, customerW, customerD, amount, carrier, threshold int
	items                                                                  []orderItem
	invalid                                                                bool
}

func makePlan(rng *rand.Rand, c config, kind int) plan {
	p := plan{kind: kind, w: 1 + rng.Intn(c.Warehouses), d: 1 + rng.Intn(c.Districts), customer: 1 + rng.Intn(c.Customers), amount: 100 + rng.Intn(499901), carrier: 1 + rng.Intn(10), threshold: 10 + rng.Intn(11)}
	p.customerW, p.customerD = p.w, p.d
	if kind == 1 && c.Warehouses > 1 && rng.Intn(100) < 15 {
		p.customerW = (p.w+rng.Intn(c.Warehouses-1))%c.Warehouses + 1
		p.customerD = 1 + rng.Intn(c.Districts)
	}
	if kind == 0 {
		seen := make(map[int]bool)
		count := 5 + rng.Intn(11)
		for len(p.items) < count {
			i := 1 + rng.Intn(c.Items)
			if seen[i] {
				continue
			}
			seen[i] = true
			w := p.w
			if c.Warehouses > 1 && rng.Intn(100) == 0 {
				w = (p.w+rng.Intn(c.Warehouses-1))%c.Warehouses + 1
			}
			p.items = append(p.items, orderItem{i, w, 1 + rng.Intn(10)})
		}
		// Consistent stock lock order reduces avoidable deadlocks.
		sort.Slice(p.items, func(i, j int) bool {
			if p.items[i].warehouse != p.items[j].warehouse {
				return p.items[i].warehouse < p.items[j].warehouse
			}
			return p.items[i].id < p.items[j].id
		})
		p.invalid = rng.Intn(100) == 0
	}
	return p
}

type queries struct {
	prepared *preparedStatements

	tx  *db.Tx
	ctx context.Context
	err error
}

func (q *queries) exec(query string, args ...any) *db.ExecResult {
	if q.err != nil {
		return &db.ExecResult{}
	}
	var r *db.ExecResult
	var err error
	if q.prepared.enabled {
		stmt := q.prepared.statements[query]
		if stmt == nil {
			q.err = fmt.Errorf("workload query was not prepared: %s", query)
			return &db.ExecResult{}
		}
		r, err = q.tx.ExecPreparedContext(q.ctx, stmt, args...)
	} else {
		r, err = q.tx.ExecContext(q.ctx, numericQuery(query, args))
	}
	if err != nil {
		q.err = fmt.Errorf("%s (arguments %v): %w", query, args, err)
		return &db.ExecResult{}
	}
	return r
}
func (q *queries) one(query string, args ...any) map[string]sql.Value {
	r := q.exec(query, args...)
	if q.err == nil && len(r.Rows) != 1 {
		q.err = fmt.Errorf("%s: expected one row, got %d", query, len(r.Rows))
	}
	if len(r.Rows) == 1 {
		return r.Rows[0]
	}
	return nil
}
func (q *queries) change(query string, count int64, args ...any) {
	r := q.exec(query, args...)
	if q.err == nil && r.Changes != count {
		q.err = fmt.Errorf("%s: expected %d changes, got %d", query, count, r.Changes)
	}
}

func execute(ctx context.Context, tx *db.Tx, p plan, c config, historyID int64) (effect, error) {
	q := queries{tx: tx, ctx: ctx, prepared: c.statements}
	var e effect
	switch p.kind {
	case 0:
		q.one(warehouseRead, p.w)
		q.one(customerRead, p.customerW, p.customerD, p.customer)
		o := q.one(districtLock, p.w, p.d)["next_o_id"].Int
		q.change(districtNext, 1, p.w, p.d)
		q.change(orderInsert, 1, p.w, p.d, o, p.customer, len(p.items))
		q.change(pendingInsert, 1, p.w, p.d, o)
		total := int64(0)
		for n, item := range p.items {
			cost := q.one(itemRead, item.id)["price"].Int
			quantity := q.one(stockLock, item.warehouse, item.id)["quantity"].Int - int64(item.quantity)
			if quantity < 10 {
				quantity += 91
			}
			remote := 0
			if item.warehouse != p.w {
				remote = 1
			}
			q.change(stockUpdate, 1, quantity, item.quantity, remote, item.warehouse, item.id)
			amount := cost * int64(item.quantity)
			total += amount
			q.change(lineInsert, 1, p.w, p.d, o, n+1, item.id, item.warehouse, item.quantity, amount)
		}
		if p.invalid && q.err == nil {
			r := q.exec(itemRead, c.Items+1)
			if q.err == nil && len(r.Rows) == 0 {
				return e, invalidItem
			}
			if q.err == nil {
				q.err = errors.New("invalid item unexpectedly exists")
			}
		}
		q.change(orderTotal, 1, total, p.w, p.d, o)
		e.NewOrders, e.Lines = 1, int64(len(p.items))
	case 1:
		q.change(warehousePay, 1, p.amount, p.w)
		q.change(districtPay, 1, p.amount, p.w, p.d)
		q.change(customerPay, 1, p.amount, p.amount, p.customerW, p.customerD, p.customer)
		q.change(historyInsert, 1, historyID, p.w, p.d, p.customerW, p.customerD, p.customer, p.amount)
		e.PaymentCents = int64(p.amount)
	case 2:
		q.one(customerRead, p.customerW, p.customerD, p.customer)
		r := q.exec(lastOrder, p.customerW, p.customerD, p.customer)
		if len(r.Rows) > 0 {
			q.exec(orderLines, p.w, p.d, r.Rows[0]["o_id"].Int)
		}
	case 3:
		for d := 1; d <= c.Districts && q.err == nil; d++ {
			r := q.exec(pendingLock, p.w, d)
			if q.err != nil || len(r.Rows) == 0 {
				continue
			}
			o := r.Rows[0]["o_id"].Int
			q.change(pendingDelete, 1, p.w, d, o)
			customerID := q.one(orderCustomer, p.w, d, o)["c_id"].Int
			q.change(orderCarrier, 1, p.carrier, p.w, d, o)
			lineData := q.one(lineSum, p.w, d, o)
			amount := lineData["amount"].Int
			q.change(lineDelivered, lineData["n"].Int, p.w, d, o)
			q.change(customerDelivery, 1, amount, p.w, d, customerID)
			e.Delivered++
			e.DeliveryCents += amount
		}
	case 4:
		next := q.one(districtRead, p.w, p.d)["next_o_id"].Int
		r := q.exec(recentItems, p.w, p.d, next-20, next)
		if len(r.Rows) > 0 {
			args := make([]any, 0, len(r.Rows)+2)
			args = append(args, p.w, p.threshold)
			for _, row := range r.Rows {
				args = append(args, row["i_id"].Int)
			}
			q.one(c.statements.stockCounts[len(r.Rows)], args...)
		}
	}
	return e, q.err
}

func errorClass(err error) string {
	switch {
	case errors.Is(err, db.ErrDeadlock):
		return "deadlock"
	case errors.Is(err, db.ErrConflict):
		return "conflict"
	case errors.Is(err, db.ErrLockTimeout):
		return "lock_timeout"
	case errors.Is(err, context.DeadlineExceeded):
		return "transaction_timeout"
	case errors.Is(err, context.Canceled):
		return "canceled"
	default:
		return err.Error()
	}
}

func work(ctx context.Context, database *db.Database, c config, worker int, historyID, committed, failed, retried *atomic.Int64) workerStats {
	rng := rand.New(rand.NewSource(c.Seed + int64(worker+1)*1000003))
	s := workerStats{errors: make(map[string]int64), retryErrors: make(map[string]int64)}
	for ctx.Err() == nil {
		kind, pick := 0, rng.Intn(100)
		switch {
		case pick < 45:
			kind = 0
		case pick < 88:
			kind = 1
		case pick < 92:
			kind = 2
		case pick < 96:
			kind = 3
		default:
			kind = 4
		}
		p := makePlan(rng, c, kind)
		id := int64(0)
		if kind == 1 {
			id = historyID.Add(1)
		}
		start := time.Now()
		result := &s.types[kind]
		for attempt := 0; ; attempt++ {
			txctx, cancel := context.WithTimeout(ctx, c.TxTimeout)
			tx, err := database.BeginContext(txctx)
			var e effect
			if err == nil {
				e, err = execute(txctx, tx, p, c, id)
				if err == nil {
					err = txctx.Err()
				}
				// Once commit starts, let it finish without deadline cancellation.
				// This avoids reporting a durable commit as a canceled transaction.
				if err == nil {
					if c.Prepared {
						_, err = tx.ExecPrepared(c.statements.statements["COMMIT"])
					} else {
						err = tx.Commit()
					}
				}
				if err != nil {
					rollbackErr := tx.Rollback()
					if rollbackErr != nil && !errors.Is(rollbackErr, db.ErrTxDone) {
						err = fmt.Errorf("rollback failed: %v; original: %v", rollbackErr, err)
					}
				}
			}
			cancel()
			if err == nil {
				result.committed++
				result.latencies = append(result.latencies, time.Since(start))
				s.effects.add(e)
				committed.Add(1)
				break
			}
			if errors.Is(err, invalidItem) {
				result.rollbacks++
				break
			}
			if ctx.Err() != nil {
				result.canceled++
				break
			}
			class := errorClass(err)
			retryable := errors.Is(err, db.ErrDeadlock) || errors.Is(err, db.ErrConflict) || errors.Is(err, db.ErrLockTimeout)
			if !retryable || attempt >= c.Retries {
				result.failed++
				s.errors[class]++
				failed.Add(1)
				break
			}
			result.retries++
			s.retryErrors[class]++
			retried.Add(1)
			backoff := time.NewTimer(time.Duration(1+rng.Intn(5)) * time.Millisecond * time.Duration(1<<min(attempt, 5)))
			select {
			case <-ctx.Done():
				backoff.Stop()
				result.canceled++
			case <-backoff.C:
			}
			if ctx.Err() != nil {
				break
			}
		}
	}
	return s
}

func summarize(r *report, workers []workerStats) effect {
	r.Transactions = make(map[string]transactionReport)
	r.Errors, r.RetryErrors = make(map[string]int64), make(map[string]int64)
	var effects effect
	var total samples
	for kind, name := range names {
		var merged samples
		for _, worker := range workers {
			s := worker.types[kind]
			merged.committed += s.committed
			merged.failed += s.failed
			merged.rollbacks += s.rollbacks
			merged.canceled += s.canceled
			merged.retries += s.retries
			merged.latencies = append(merged.latencies, s.latencies...)
		}
		r.Transactions[name] = describe(merged, r.RunSeconds)
		total.committed += merged.committed
		total.failed += merged.failed
		total.rollbacks += merged.rollbacks
		total.canceled += merged.canceled
		total.retries += merged.retries
		total.latencies = append(total.latencies, merged.latencies...)
	}
	for _, worker := range workers {
		effects.add(worker.effects)
		for key, n := range worker.errors {
			r.Errors[key] += n
		}
		for key, n := range worker.retryErrors {
			r.RetryErrors[key] += n
		}
	}
	r.Total = describe(total, r.RunSeconds)
	r.NewOrdersPerMinute = r.Transactions["new_order"].TPS * 60
	return effects
}

func describe(s samples, seconds float64) transactionReport {
	r := transactionReport{Committed: s.committed, Failed: s.failed, Rollbacks: s.rollbacks, Canceled: s.canceled, Retries: s.retries, TPS: float64(s.committed) / seconds}
	if len(s.latencies) == 0 {
		return r
	}
	sort.Slice(s.latencies, func(i, j int) bool { return s.latencies[i] < s.latencies[j] })
	var sum float64
	for _, latency := range s.latencies {
		sum += float64(latency) / float64(time.Millisecond)
	}
	r.MeanMS = sum / float64(len(s.latencies))
	percentile := func(p float64) float64 {
		return float64(s.latencies[int(math.Ceil(p*float64(len(s.latencies))))-1]) / float64(time.Millisecond)
	}
	r.P50MS, r.P95MS, r.P99MS, r.MaxMS = percentile(.5), percentile(.95), percentile(.99), percentile(1)
	return r
}

func validate(database *db.Database, c config, effects effect, payments int64) (map[string]string, error) {
	checks := make(map[string]string)
	var failures []error
	check := func(name, query string, want int64) {
		r, err := database.Exec(query)
		if err == nil && len(r.Rows) != 1 {
			err = errors.New("expected one aggregate row")
		}
		if err == nil {
			got := r.Rows[0]["n"].Int
			if got != want {
				err = fmt.Errorf("got %d, want %d", got, want)
			}
		}
		if err != nil {
			checks[name] = err.Error()
			failures = append(failures, fmt.Errorf("%s: %w", name, err))
		} else {
			checks[name] = "pass"
		}
	}
	baseOrders := int64(c.Warehouses) * int64(c.Districts) * initialOrders
	check("order_count", "SELECT COUNT(*) AS n FROM orders", baseOrders+effects.NewOrders)
	check("order_line_count", "SELECT COUNT(*) AS n FROM order_line", baseOrders*5+effects.Lines)
	check("pending_order_count", "SELECT COUNT(*) AS n FROM new_order", int64(c.Warehouses)*int64(c.Districts)*10+effects.NewOrders-effects.Delivered)
	check("warehouse_payments", "SELECT SUM(ytd) AS n FROM warehouse", effects.PaymentCents)
	check("district_payments", "SELECT SUM(ytd) AS n FROM district", effects.PaymentCents)
	check("customer_payments", "SELECT SUM(ytd_payment) AS n FROM customer", effects.PaymentCents)
	check("history_payments", "SELECT SUM(amount) AS n FROM history", effects.PaymentCents)
	check("history_count", "SELECT COUNT(*) AS n FROM history", payments)
	check("payment_count", "SELECT SUM(payment_count) AS n FROM customer", payments)
	check("customer_balances", "SELECT SUM(balance) AS n FROM customer", effects.DeliveryCents-effects.PaymentCents)
	check("delivery_count", "SELECT SUM(delivery_count) AS n FROM customer", effects.Delivered)
	check("stock_order_count", "SELECT SUM(order_count) AS n FROM stock", effects.Lines)
	check("stock_quantity_bounds", "SELECT COUNT(*) AS n FROM stock WHERE quantity<10 OR quantity>100", 0)
	check("district_next_order_ids", "SELECT SUM(next_o_id) AS n FROM district", baseOrders+int64(c.Warehouses)*int64(c.Districts)+effects.NewOrders)
	check("order_line_totals", "SELECT SUM(total) AS n FROM orders", sumSeedAmounts(c)+sumNewAmounts(database, &failures))
	return checks, errors.Join(failures...)
}

func sumSeedAmounts(c config) int64 {
	var total int64
	for o := 1; o <= initialOrders; o++ {
		for n := 1; n <= 5; n++ {
			total += int64(price((o*5+n-1)%c.Items+1) * 5)
		}
	}
	return total * int64(c.Warehouses) * int64(c.Districts)
}

func sumNewAmounts(database *db.Database, failures *[]error) int64 {
	r, err := database.Exec(fmt.Sprintf("SELECT SUM(amount) AS n FROM order_line WHERE o_id>%d", initialOrders))
	if err != nil {
		*failures = append(*failures, err)
		return 0
	}
	return r.Rows[0]["n"].Int
}

func printReport(r report) {
	fmt.Printf("\nMeasured %.3fs; drained in %.3fs; committed=%d (%.2f tx/s), new-orders/min=%.2f\n", r.RunSeconds, r.DrainSeconds, r.Total.Committed, r.Total.TPS, r.NewOrdersPerMinute)
	w := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
	fmt.Fprintln(w, "transaction\tcommitted\ttx/s\tmean ms\tp50 ms\tp95 ms\tp99 ms\tfailed\tretries\trollback\tcanceled")
	for _, name := range append(names[:], "TOTAL") {
		t := r.Total
		if name != "TOTAL" {
			t = r.Transactions[name]
		}
		fmt.Fprintf(w, "%s\t%d\t%.2f\t%.2f\t%.2f\t%.2f\t%.2f\t%d\t%d\t%d\t%d\n", name, t.Committed, t.TPS, t.MeanMS, t.P50MS, t.P95MS, t.P99MS, t.Failed, t.Retries, t.Rollbacks, t.Canceled)
	}
	w.Flush()
	fmt.Printf("Errors: %v\nRetry causes: %v\n", r.Errors, r.RetryErrors)
	for _, set := range []struct {
		label  string
		checks map[string]string
	}{{"After run", r.Checks}, {"After close/reopen", r.ReopenChecks}} {
		passed := 0
		for name, result := range set.checks {
			if result == "pass" {
				passed++
			} else {
				fmt.Printf("FAILED %s: %s\n", name, result)
			}
		}
		fmt.Printf("%s consistency checks: %d/%d passed\n", set.label, passed, len(set.checks))
	}
}
