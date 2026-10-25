package db_test

import (
	"context"
	gosql "database/sql"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"slopdb/db"
)

// Optional independent semantics check. The caller owns the reference server;
// ordinary tests use only the fixed, independently checked answers above.
func TestMySQLTemporalReference(t *testing.T) {
	socket := os.Getenv("SLOPDB_MYSQL_REFERENCE_SOCKET")
	if socket == "" {
		t.Skip("set SLOPDB_MYSQL_REFERENCE_SOCKET to an isolated MySQL reference socket")
	}
	oracle, e := gosql.Open("mysql", "root@unix("+socket+")/?timeout=2s&readTimeout=10s&writeTimeout=10s")
	if e != nil {
		t.Fatal(e)
	}
	defer oracle.Close()
	oracle.SetMaxOpenConns(1)
	if _, e = oracle.Exec("SET time_zone='+00:00'"); e != nil {
		t.Fatal(e)
	}
	d, e := db.Open(t.TempDir(), nil)
	if e != nil {
		t.Fatal(e)
	}
	defer d.Close()
	s := mysqlTypeSession(t, d)
	random := rand.New(rand.NewSource(20261025))
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	for iteration := 0; iteration < 120; iteration++ {
		day := time.Date(1000+random.Intn(8000), time.Month(1+random.Intn(12)), 1+random.Intn(28), random.Intn(24), random.Intn(60), random.Intn(60), 0, time.UTC)
		if iteration < 16 {
			day = time.Date(1990+iteration, 1, 1+iteration%4, 0, 0, 0, 0, time.UTC)
		}
		text := day.Format("2006-01-02 15:04:05")
		quoted := "'" + text + "'"
		offset := random.Intn(500) - 250
		expressions := []string{"YEAR(" + quoted + ")", "DAYOFWEEK(" + quoted + ")", "DAYOFYEAR(" + quoted + ")", "LAST_DAY(" + quoted + ")", "TO_DAYS(" + quoted + ")", "TO_SECONDS(" + quoted + ")", "DATE_FORMAT(" + quoted + ",'%Y %y %m %M %d %D %W %a %j %U %u %V %v %X %x %r')"}
		for mode := 0; mode < 8; mode++ {
			expressions = append(expressions, fmt.Sprintf("WEEK(%s,%d)", quoted, mode), fmt.Sprintf("YEARWEEK(%s,%d)", quoted, mode))
		}
		for _, unit := range []string{"YEAR", "QUARTER", "MONTH", "WEEK", "DAY", "HOUR", "MINUTE", "SECOND", "MICROSECOND"} {
			expressions = append(expressions, fmt.Sprintf("DATE_ADD(%s,INTERVAL %d %s)", quoted, offset, unit), fmt.Sprintf("TIMESTAMPDIFF(%s,%s,'5000-06-15 12:34:56')", unit, quoted))
		}
		for _, unit := range []string{"YEAR_MONTH", "DAY_HOUR", "DAY_MINUTE", "DAY_SECOND", "HOUR_MINUTE", "HOUR_SECOND", "MINUTE_SECOND", "DAY_MICROSECOND", "HOUR_MICROSECOND", "MINUTE_MICROSECOND", "SECOND_MICROSECOND"} {
			expressions = append(expressions, fmt.Sprintf("EXTRACT(%s FROM %s)", unit, quoted))
		}
		selections := make([]string, len(expressions))
		for i, expression := range expressions {
			selections[i] = fmt.Sprintf("%s AS v%d", expression, i)
		}
		query := "SELECT " + strings.Join(selections, ",")
		reference := make([]any, len(expressions))
		dest := make([]any, len(expressions))
		for i := range reference {
			dest[i] = &reference[i]
		}
		if e = oracle.QueryRowContext(ctx, query).Scan(dest...); e != nil {
			t.Fatal(e, query)
		}
		got, e := s.ExecContext(ctx, query)
		if e != nil {
			t.Fatal(e, query)
		}
		for i, expression := range expressions {
			value := got.Rows[0][fmt.Sprintf("v%d", i)]
			if reference[i] == nil {
				if !value.Null {
					t.Errorf("%s: got %s want NULL", expression, value.String())
				}
				continue
			}
			want := fmt.Sprint(reference[i])
			if raw, ok := reference[i].([]byte); ok {
				want = string(raw)
			}
			if value.Null || value.String() != want {
				t.Errorf("%s: got %s want %s", expression, value.String(), want)
			}
		}
		if t.Failed() {
			return
		}
	}
}
