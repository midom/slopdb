package tpcc

import (
	"fmt"

	"strconv"
	"strings"
)

const (
	warehouseRead    = "SELECT ytd FROM warehouse WHERE w_id=?"
	customerRead     = "SELECT balance FROM customer WHERE w_id=? AND d_id=? AND c_id=?"
	districtLock     = "SELECT next_o_id FROM district WHERE w_id=? AND d_id=? FOR UPDATE"
	districtNext     = "UPDATE district SET next_o_id=next_o_id+1 WHERE w_id=? AND d_id=?"
	orderInsert      = "INSERT INTO orders VALUES (?,?,?,?,NULL,?,0)"
	pendingInsert    = "INSERT INTO new_order VALUES (?,?,?)"
	itemRead         = "SELECT price FROM item WHERE i_id=?"
	stockLock        = "SELECT quantity FROM stock WHERE w_id=? AND i_id=? FOR UPDATE"
	stockUpdate      = "UPDATE stock SET quantity=?,ytd=ytd+?,order_count=order_count+1,remote_count=remote_count+? WHERE w_id=? AND i_id=?"
	lineInsert       = "INSERT INTO order_line VALUES (?,?,?,?,?,?,?,?,0)"
	orderTotal       = "UPDATE orders SET total=? WHERE w_id=? AND d_id=? AND o_id=?"
	warehousePay     = "UPDATE warehouse SET ytd=ytd+? WHERE w_id=?"
	districtPay      = "UPDATE district SET ytd=ytd+? WHERE w_id=? AND d_id=?"
	customerPay      = "UPDATE customer SET balance=balance-?,ytd_payment=ytd_payment+?,payment_count=payment_count+1 WHERE w_id=? AND d_id=? AND c_id=?"
	historyInsert    = "INSERT INTO history VALUES (?,?,?,?,?,?,?)"
	lastOrder        = "SELECT o_id,total,carrier_id FROM orders WHERE w_id=? AND d_id=? AND c_id=? ORDER BY o_id DESC LIMIT 1"
	orderLines       = "SELECT i_id,quantity,amount,delivered FROM order_line WHERE w_id=? AND d_id=? AND o_id=?"
	pendingLock      = "SELECT o_id FROM new_order WHERE w_id=? AND d_id=? ORDER BY o_id LIMIT 1 FOR UPDATE SKIP LOCKED"
	pendingDelete    = "DELETE FROM new_order WHERE w_id=? AND d_id=? AND o_id=?"
	orderCustomer    = "SELECT c_id FROM orders WHERE w_id=? AND d_id=? AND o_id=?"
	orderCarrier     = "UPDATE orders SET carrier_id=? WHERE w_id=? AND d_id=? AND o_id=?"
	lineSum          = "SELECT SUM(amount) AS amount,COUNT(*) AS n FROM order_line WHERE w_id=? AND d_id=? AND o_id=?"
	lineDelivered    = "UPDATE order_line SET delivered=1 WHERE w_id=? AND d_id=? AND o_id=?"
	customerDelivery = "UPDATE customer SET balance=balance+?,delivery_count=delivery_count+1 WHERE w_id=? AND d_id=? AND c_id=?"
	districtRead     = "SELECT next_o_id FROM district WHERE w_id=? AND d_id=?"
	recentItems      = "SELECT DISTINCT i_id FROM order_line WHERE w_id=? AND d_id=? AND o_id>=? AND o_id<?"
)

type preparedStatements struct {
	enabled     bool
	statements  map[string]Statement
	stockCounts []string
}

func prepareWorkload(database Database, c Config) (*preparedStatements, error) {
	p := &preparedStatements{enabled: c.Prepared, statements: make(map[string]Statement), stockCounts: make([]string, min(c.Items, 300)+1)}
	templates := []string{warehouseRead, customerRead, districtLock, districtNext, orderInsert, pendingInsert, itemRead, stockLock, stockUpdate, lineInsert, orderTotal, warehousePay, districtPay, customerPay, historyInsert, lastOrder, orderLines, pendingLock, pendingDelete, orderCustomer, orderCarrier, lineSum, lineDelivered, customerDelivery, districtRead, recentItems, "COMMIT"}
	// Twenty recent orders have at most 300 lines. Prepare every IN-list arity
	// before timing, so even stock-level never parses SQL inside the workload.
	for count := 1; count < len(p.stockCounts); count++ {
		p.stockCounts[count] = "SELECT COUNT(*) AS low_stock FROM stock WHERE w_id=? AND quantity<? AND i_id IN (" + strings.TrimSuffix(strings.Repeat("?,", count), ",") + ")"
		templates = append(templates, p.stockCounts[count])
	}
	if c.Prepared {
		for _, query := range templates {
			stmt, err := database.Prepare(query)
			if err != nil {
				p.Close()
				return nil, fmt.Errorf("prepare %s: %w", query, err)
			}
			p.statements[query] = stmt
		}
	}
	if err := database.SetupWorkers(c.Workers, templates, c.Prepared); err != nil {
		p.Close()
		return nil, err
	}
	return p, nil
}

func (p *preparedStatements) Close() {
	for _, stmt := range p.statements {
		stmt.Close()
	}
}

// The text comparison uses the same templates, arguments and transaction order.
// All workload arguments are integers; this private helper does not accept SQL
// text, strings, identifiers or other user-provided values.
func numericQuery(query string, args []any) string {
	var b strings.Builder
	b.Grow(len(query) + len(args)*8)
	i := 0
	for _, part := range strings.Split(query, "?") {
		if i > 0 {
			switch v := args[i-1].(type) {
			case int:
				b.WriteString(strconv.FormatInt(int64(v), 10))
			case int64:
				b.WriteString(strconv.FormatInt(v, 10))
			default:
				panic("non-integer workload argument")
			}
		}
		b.WriteString(part)
		i++
	}
	return b.String()
}
