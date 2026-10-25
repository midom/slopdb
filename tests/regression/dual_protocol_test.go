package regression

import (
	"context"
	gosql "database/sql"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strings"
	"testing"
	"time"

	driver "github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5"
	"slopdb/db"
	"slopdb/mysqlserver"
	"slopdb/postgresserver"
	"slopdb/storage"
)

// Both listeners own independent dialect sessions over one physical LSM. The
// native input preference must not determine either client's quoting rules.
func TestSimultaneousMySQLAndPostgresSessions(t *testing.T) {
	for _, mysqlDefault := range []bool{false, true} {
		t.Run(fmt.Sprint(mysqlDefault), func(t *testing.T) {
			opts := storage.DefaultOptions()
			opts.DisableWALSync = true
			opts.MySQLQuoting = mysqlDefault
			d, e := db.Open(t.TempDir(), opts)
			if e != nil {
				t.Fatal(e)
			}
			t.Cleanup(func() {
				if e := d.Close(); e != nil {
					t.Error(e)
				}
			})
			my, e := mysqlserver.New(d, mysqlserver.Config{})
			if e != nil {
				t.Fatal(e)
			}
			pg, e := postgresserver.New(d, postgresserver.Config{})
			if e != nil {
				t.Fatal(e)
			}
			ml, e := net.Listen("tcp", "127.0.0.1:0")
			if e != nil {
				t.Fatal(e)
			}
			pl, e := net.Listen("tcp", "127.0.0.1:0")
			if e != nil {
				ml.Close()
				t.Fatal(e)
			}
			md, pd := make(chan error, 1), make(chan error, 1)
			go func() { md <- my.Serve(ml) }()
			go func() { pd <- pg.Serve(pl) }()
			t.Cleanup(func() {
				my.Close()
				pg.Close()
				for _, done := range []<-chan error{md, pd} {
					if e := <-done; e != nil && !errors.Is(e, net.ErrClosed) {
						t.Error(e)
					}
				}
			})
			cfg := driver.NewConfig()
			cfg.User = "slopdb"
			cfg.Net = "tcp"
			cfg.Addr = ml.Addr().String()
			cfg.DBName = "slopdb"
			cfg.Timeout = 3 * time.Second
			cfg.ReadTimeout = 5 * time.Second
			cfg.WriteTimeout = 5 * time.Second
			mc, e := gosql.Open("mysql", cfg.FormatDSN())
			if e != nil {
				t.Fatal(e)
			}
			defer mc.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			u := url.URL{Scheme: "postgres", Host: pl.Addr().String(), Path: "/slopdb", User: url.User("slopdb")}
			q := url.Values{"sslmode": {"disable"}}
			u.RawQuery = q.Encode()
			pc, e := pgx.Connect(ctx, u.String())
			if e != nil {
				t.Fatal(e)
			}
			defer pc.Close(context.Background())
			mx := func(q string, args ...any) {
				t.Helper()
				if _, e := mc.ExecContext(ctx, q, args...); e != nil {
					t.Fatalf("mysql %s: %v", q, e)
				}
			}
			px := func(q string, args ...any) {
				t.Helper()
				if _, e := pc.Exec(ctx, q, args...); e != nil {
					t.Fatalf("postgres %s: %v", q, e)
				}
			}
			mx("CREATE DATABASE common")
			mx("CREATE TABLE common.`shared table`(id BIGINT PRIMARY KEY,user TEXT,value TEXT)")
			mx("INSERT INTO common.`shared table` VALUES(1,\"mysql user\",\"mysql\\nline\")")
			var user, value string
			if e = pc.QueryRow(ctx, `SELECT "user","value" FROM common."shared table" WHERE id=$1`, int64(1)).Scan(&user, &value); e != nil || user != "mysql user" || value != "mysql\nline" {
				t.Fatalf("PG read MySQL row %q %q %v", user, value, e)
			}
			px(`INSERT INTO common."shared table" VALUES($1,$2,$3)`, int64(2), "pg user", `pg\literal`)
			var marker string
			if e = mc.QueryRowContext(ctx, "SELECT \"mysql literal\" AS marker,user,value FROM common.`shared table` WHERE id=?", int64(2)).Scan(&marker, &user, &value); e != nil || marker != "mysql literal" || user != "pg user" || value != `pg\literal` {
				t.Fatalf("MySQL read PG row %q %q %q %v", marker, user, value, e)
			}
			mx("CREATE VIEW common.`mysql view` AS SELECT id,user,value FROM common.`shared table`")
			px(`CREATE VIEW common."pg view" AS SELECT id,"user","value" FROM common."shared table"`)
			if e = pc.QueryRow(ctx, `SELECT "value" FROM common."mysql view" WHERE id=$1`, int64(1)).Scan(&value); e != nil || value != "mysql\nline" {
				t.Fatal(value, e)
			}
			if e = mc.QueryRowContext(ctx, "SELECT value FROM common.`pg view` WHERE id=?", int64(2)).Scan(&value); e != nil || value != `pg\literal` {
				t.Fatal(value, e)
			}
			// An actual PG integer/boolean/UUID table remains typed when read by either
			// protocol, with no database reopen or alternate storage engine.
			px(`CREATE TABLE common."pg types"(id INTEGER PRIMARY KEY,b BOOLEAN,u UUID)`)
			px(`INSERT INTO common."pg types" VALUES($1,$2,$3::uuid)`, int32(3), true, "550e8400-e29b-41d4-a716-446655440000")
			var b int64
			var uuid []byte
			if e = mc.QueryRowContext(ctx, "SELECT b,u FROM common.`pg types` WHERE id=?", 3).Scan(&b, &uuid); e != nil || b != 1 || len(uuid) != 16 {
				t.Fatalf("typed bridge %d %x %v", b, uuid, e)
			}
			// SHOW rendering is connection-specific as well as parsing.
			var name, create string
			if e = mc.QueryRowContext(ctx, "SHOW CREATE TABLE common.`shared table`").Scan(&name, &create); e != nil || !strings.Contains(create, "`user`") {
				t.Fatalf("MySQL SHOW %q %v", create, e)
			}
			if d.ParseOptions().MySQLQuoting != mysqlDefault {
				t.Fatal("frontend mutated native parse preference")
			}
		})
	}
}
