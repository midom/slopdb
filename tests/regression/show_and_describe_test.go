// Adapted from /tmp/godb/show_test_runner.go; see TESTING.md.
package regression

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"

	"slopdb/db"
	"slopdb/sql"
)

func showAndDescribeCheck(testState *regressionEnv, label string, ok bool) {
	testState.t.Helper()
	testState.checks++
	if !ok {
		testState.t.Fatalf("%v", "FAIL: "+label)
	}
}

func TestShowAndDescribe(testT *testing.T) {
	testState := newRegressionEnv(testT)

	dir := testState.path("godb_show_tests")
	os.RemoveAll(dir)
	d, err := testState.openDatabase(dir, nil)
	if err != nil {
		testState.t.Fatalf("%v", err)
	}
	exec := func(q string) *db.ExecResult {
		r, err := d.Exec(q)
		if err != nil {
			testState.t.Fatalf("%v", fmt.Sprintf("%s: %v", q, err))
		}
		for _, row := range r.Rows {
			showAndDescribeCheck(testState, "row shape: "+q, len(row) == len(r.Columns))
			for _, col := range r.Columns {
				_, ok := row[col.Name]
				showAndDescribeCheck(testState, "row column: "+col.Name, ok)
			}
		}
		return r
	}
	showAndDescribeCheck(testState, "empty tables", len(exec("SHOW TABLES").Rows) == 0)
	showAndDescribeCheck(testState, "empty full tables metadata", len(exec("SHOW FULL TABLES").Columns) == 2)
	showAndDescribeCheck(testState, "database", exec("SHOW DATABASES").Rows[0]["Database"].Text == "main")
	showAndDescribeCheck(testState, "schemas alias", exec("SHOW SCHEMAS LIKE 'm%'").Rows[0]["Database"].Text == "main")
	showAndDescribeCheck(testState, "database where", len(exec("SHOW DATABASES WHERE Database = 'other'").Rows) == 0)
	exec(`CREATE TABLE users (id INTEGER PRIMARY KEY AUTOINCREMENT, email TEXT NOT NULL UNIQUE DEFAULT '', score REAL DEFAULT 1.5, note TEXT DEFAULT NULL, n INTEGER DEFAULT -2, calc INTEGER DEFAULT (1+2)); CREATE INDEX score_idx ON users(score); CREATE UNIQUE INDEX note_idx ON users(note); CREATE TABLE parent (id INTEGER PRIMARY KEY); CREATE TABLE child (id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES parent(id), FOREIGN KEY (id) REFERENCES parent(id)); CREATE TABLE composite (a TEXT, b INTEGER, c TEXT, PRIMARY KEY (a,b), UNIQUE (b,c)); CREATE TABLE plain (v TEXT); CREATE TABLE a_b (v TEXT); CREATE VIEW user_view (uid, mail) AS SELECT id, email FROM users; CREATE VIEW view_chain AS SELECT * FROM user_view; CREATE TRIGGER user_trig AFTER INSERT ON users BEGIN INSERT INTO plain VALUES (NEW.email); END;`)
	r := exec("SHOW FULL TABLES IN MAIN WHERE Table_type = 'VIEW'")
	showAndDescribeCheck(testState, "sorted views", len(r.Rows) == 2 && r.Rows[0]["Tables_in_main"].Text == "user_view")
	showAndDescribeCheck(testState, "table pattern", len(exec("show tables from main like 'us%'").Rows) == 2)
	showAndDescribeCheck(testState, "escaped underscore", len(exec(`SHOW TABLES LIKE 'a\_b'`).Rows) == 1)
	for _, q := range []string{"DESCRIBE users", "DESC users", "SHOW COLUMNS FROM users", "SHOW FIELDS IN main.users", "SHOW COLUMNS IN users FROM main"} {
		r = exec(q)
		showAndDescribeCheck(testState, "describe columns: "+q, len(r.Rows) == 6 && len(r.Columns) == 6)
		showAndDescribeCheck(testState, "pk metadata", r.Rows[0]["Key"].Text == "PRI" && r.Rows[0]["Null"].Text == "NO" && r.Rows[0]["Extra"].Text == "auto_increment")
		showAndDescribeCheck(testState, "unique metadata", r.Rows[1]["Key"].Text == "UNI" && r.Rows[1]["Null"].Text == "NO")
		showAndDescribeCheck(testState, "empty string default", !r.Rows[1]["Default"].Null && r.Rows[1]["Default"].Text == "")
		showAndDescribeCheck(testState, "real metadata", r.Rows[2]["Type"].Text == "REAL" && r.Rows[2]["Key"].Text == "MUL" && r.Rows[2]["Default"].Text == "1.5")
		showAndDescribeCheck(testState, "null default", r.Rows[3]["Default"].Null)
		showAndDescribeCheck(testState, "negative default", r.Rows[4]["Default"].Text == "-2")
		showAndDescribeCheck(testState, "expression default", strings.Contains(r.Rows[5]["Default"].Text, "+"))
	}
	showAndDescribeCheck(testState, "describe exact column", exec("DESC users email").Rows[0]["Field"].Text == "email")
	showAndDescribeCheck(testState, "describe quoted column", exec("DESC `MAIN`.`users` `email`").Rows[0]["Field"].Text == "email")
	showAndDescribeCheck(testState, "describe pattern", exec("DESCRIBE users 's%'").Rows[0]["Field"].Text == "score")
	showAndDescribeCheck(testState, "full columns", len(exec("SHOW FULL FIELDS FROM users LIKE 'email'").Columns) == 9)
	showAndDescribeCheck(testState, "column filters", len(exec("SHOW COLUMNS FROM users WHERE `Key` = 'UNI' AND Field LIKE '%e%'").Rows) == 2)
	r = exec("DESC composite")
	showAndDescribeCheck(testState, "composite pk", r.Rows[0]["Key"].Text == "PRI" && r.Rows[1]["Key"].Text == "PRI")
	for _, q := range []string{"SHOW INDEX FROM users", "SHOW INDEXES IN main.users", "SHOW KEYS FROM users IN main"} {
		r = exec(q)
		showAndDescribeCheck(testState, "index count", len(r.Rows) == 4)
		showAndDescribeCheck(testState, "primary index", r.Rows[0]["Key_name"].Text == "PRIMARY" && r.Rows[0]["Non_unique"].Int == 0)
		showAndDescribeCheck(testState, "lsm metadata", r.Rows[0]["Index_type"].Text == "LSM" && r.Rows[0]["Cardinality"].Null)
	}
	showAndDescribeCheck(testState, "index where", len(exec("SHOW INDEX FROM users WHERE Non_unique = 1").Rows) == 1)
	r = exec("SHOW INDEX FROM composite")
	showAndDescribeCheck(testState, "composite index parts", len(r.Rows) == 4 && r.Rows[1]["Seq_in_index"].Int == 2)
	showAndDescribeCheck(testState, "constraint method", r.Rows[2]["Index_type"].Text == "CONSTRAINT")
	exec("CREATE TABLE composite_unique (a TEXT, b TEXT, c TEXT UNIQUE, PRIMARY KEY (a,b))")
	showAndDescribeCheck(testState, "inline unique with composite primary key", exec("DESC composite_unique c").Rows[0]["Key"].Text == "UNI")
	showAndDescribeCheck(testState, "inline unique logical index", len(exec("SHOW INDEX FROM composite_unique").Rows) == 3)
	showAndDescribeCheck(testState, "no indexes", len(exec("SHOW INDEX FROM plain").Rows) == 0)
	for _, q := range []string{"DESC user_view", "SHOW COLUMNS FROM view_chain"} {
		r = exec(q)
		showAndDescribeCheck(testState, "view column aliases and types", len(r.Rows) == 2 && r.Rows[1]["Field"].Text == "mail" && r.Rows[1]["Type"].Text == "TEXT")
		showAndDescribeCheck(testState, "view constraints", r.Rows[0]["Key"].Text == "" && r.Rows[1]["Default"].Null)
	}
	showAndDescribeCheck(testState, "view metadata preserves table", exec("DESC users").Rows[0]["Key"].Text == "PRI")
	r = exec("SHOW CREATE TABLE users")
	ddl := r.Rows[0]["Create table"].Text
	showAndDescribeCheck(testState, "create table defaults", strings.Contains(ddl, "AUTOINCREMENT") && strings.Contains(ddl, "DEFAULT ''"))
	showAndDescribeCheck(testState, "ddl parses", showAndDescribeParseOK(testState, ddl))
	ddl = exec("SHOW CREATE TABLE child").Rows[0]["Create table"].Text
	showAndDescribeCheck(testState, "foreign keys included", strings.Contains(ddl, "FOREIGN KEY") && strings.Count(ddl, "REFERENCES") == 2 && showAndDescribeParseOK(testState, ddl))
	showAndDescribeCheck(testState, "view ddl", strings.Contains(exec("SHOW CREATE VIEW main.user_view").Rows[0]["Create view"].Text, "CREATE VIEW"))
	showAndDescribeCheck(testState, "create table on view", exec("SHOW CREATE TABLE user_view").Columns[0].Name == "View")
	showAndDescribeCheck(testState, "trigger ddl", strings.Contains(exec("SHOW CREATE TRIGGER user_trig").Rows[0]["SQL Original Statement"].Text, "AFTER INSERT"))
	showAndDescribeCheck(testState, "triggers pattern uses table", len(exec("SHOW TRIGGERS LIKE 'users'").Rows) == 1 && len(exec("SHOW TRIGGERS LIKE 'user_trig'").Rows) == 0)
	showAndDescribeCheck(testState, "trigger where", len(exec("SHOW TRIGGERS FROM main WHERE `Trigger` = 'user_trig'").Rows) == 1)
	exec("INSERT INTO users (email) VALUES ('a');")
	changes := exec("SELECT changes() AS n").Rows[0]["n"].Int
	exec("SHOW TABLES")
	showAndDescribeCheck(testState, "show does not reset changes", exec("SELECT changes() AS n").Rows[0]["n"].Int == changes)
	exec("BEGIN; CREATE TABLE pending (v TEXT); SAVEPOINT s; ALTER TABLE users ADD COLUMN added TEXT DEFAULT 'x';")
	showAndDescribeCheck(testState, "transaction schema visible", len(exec("SHOW TABLES LIKE 'pending'").Rows) == 1 && len(exec("DESC users added").Rows) == 1)
	exec("ROLLBACK TO s")
	showAndDescribeCheck(testState, "savepoint schema rollback", len(exec("DESC users added").Rows) == 0)
	exec("ROLLBACK")
	showAndDescribeCheck(testState, "transaction schema rollback", len(exec("SHOW TABLES LIKE 'pending'").Rows) == 0)
	showAndDescribeCheck(testState, "query describe", len(exec("DESC SELECT * FROM users").Rows) > 0)
	showAndDescribeCheck(testState, "constant query describe", len(exec("DESCRIBE SELECT 1").Rows) > 0)
	for _, q := range []string{
		"SHOW", "SHOW FULL", "SHOW OTHER", "SHOW FULL DATABASES", "SHOW COLUMNS", "SHOW COLUMNS FROM", "SHOW TABLES LIKE", "SHOW TABLES LIKE 1", "SHOW TABLES WHERE", "SHOW INDEX users", "SHOW INDEX FROM users LIKE 'x'", "SHOW CREATE", "SHOW CREATE INDEX score_idx", "SHOW CREATE TABLE", "DESCRIBE", "DESC main.", "DESCRIBE users WHERE email = 'a'", "SHOW TABLES FROM other", "SHOW COLUMNS FROM other.users", "DESC other.users", "SHOW COLUMNS FROM main.users FROM other", "DESC absent", "SHOW CREATE TABLE absent", "SHOW CREATE VIEW users", "SHOW INDEX FROM user_view", "SHOW TABLES WHERE unknown = 1", "SHOW INDEX FROM plain WHERE unknown = 1", "SHOW TABLES LIKE '%' WHERE Tables_in_main = 'users'", "SHOW COLUMNS FROM users WHERE missing_func(Field) = 1", "DESC SELECT * FROM absent",
	} {
		_, err := d.Exec(q)
		showAndDescribeCheck(testState, "reject invalid: "+q, err != nil)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = d.ExecContext(ctx, "SHOW TABLES")
	showAndDescribeCheck(testState, "cancelled context", err != nil)
	exec("CREATE TABLE context_words (full TEXT, tables TEXT, columns TEXT, fields TEXT, databases TEXT, indexes TEXT, schemas TEXT, triggers TEXT); INSERT INTO context_words VALUES ('f','t','c','f','d','i','s','t');")
	showAndDescribeCheck(testState, "context words still identifiers", len(exec("SELECT full, tables, columns FROM context_words").Rows) == 1)
	exec("ALTER TABLE users RENAME COLUMN email TO address")
	showAndDescribeCheck(testState, "renamed column", len(exec("DESC users address").Rows) == 1)
	showAndDescribeCheck(testState, "new name in indexes", len(exec("SHOW INDEX FROM users WHERE Column_name = 'address'").Rows) == 1)
	if err := d.Close(); err != nil {
		testState.t.Fatalf("%v", err)
	}
	d, err = testState.openDatabase(dir, nil)
	if err != nil {
		testState.t.Fatalf("%v", err)
	}
	showAndDescribeCheck(testState, "persisted column metadata", exec("DESC users address").Rows[0]["Default"].Text == "")
	showAndDescribeCheck(testState, "persisted composite metadata", len(exec("SHOW INDEX FROM composite").Rows) == 4)
	showAndDescribeCheck(testState, "persisted ddl", strings.Contains(exec("SHOW CREATE TABLE child").Rows[0]["Create table"].Text, "FOREIGN KEY"))
	showAndDescribeCheck(testState, "persisted trigger", len(exec("SHOW TRIGGERS").Rows) == 1)
	d.Close()
	testState.t.Logf("PASS: %d SHOW/DESCRIBE assertions\n", testState.checks)
}

func showAndDescribeParseOK(testState *regressionEnv, q string) bool {
	testState.t.Helper()
	_, err := sql.Parse(q)
	return err == nil
}
