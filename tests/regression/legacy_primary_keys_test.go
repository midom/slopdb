// Adapted from legacy_primary_review.go; see TESTING.md.
package regression

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"

	"slopdb/sql"
	"slopdb/storage"
)

func TestLegacyPrimaryKeys(testT *testing.T) {
	testState := newRegressionEnv(testT)
	for _, invalid := range []bool{false, true} {
		dir := testState.path(fmt.Sprintf("slopdb_legacy_pk_%t", invalid))
		os.RemoveAll(dir)
		o := storage.DefaultOptions()
		l, e := testState.openStorage(dir, o)
		if e != nil {
			testState.t.Fatalf("%v", e)
		}
		info := &sql.TableInfo{Name: "t", Columns: []sql.ColumnDef{{Name: "id", ColType: sql.ColText, PrimaryKey: true}}, PKColumns: []string{"id"}}
		b, e := json.Marshal(map[string]*sql.TableInfo{"t": info})
		if e != nil {
			testState.t.Fatalf("%v", e)
		}
		if e = l.Put([]byte("\x00__schema__"), b); e != nil {
			testState.t.Fatalf("%v", e)
		}
		if invalid {
			if e = l.Put([]byte("\x01t:\xffnull:00000000000000000001"), []byte{0}); e != nil {
				testState.t.Fatalf("%v", e)
			}
		}
		l.Close()
		d, e := testState.openDatabase(dir, o)
		if invalid {
			if e == nil {
				d.Close()
				testState.t.Fatalf("%v", "legacy NULL primary key accepted")
			}
			if !strings.Contains(e.Error(), "legacy NULL primary keys") {
				testState.t.Fatalf("%v", e)
			}
		} else {
			if e != nil {
				testState.t.Fatalf("%v", e)
			}
			r, e := d.Exec("PRAGMA table_info(t)")
			if e != nil || r.Rows[0]["notnull"].Int != 1 {
				testState.t.Fatalf("%v", fmt.Sprint(r, e))
			}
			_, e = d.Exec("INSERT INTO t VALUES(NULL)")
			if e == nil {
				testState.t.Fatalf("%v", "new NULL allowed")
			}
			d.Close()
			d, e = testState.openDatabase(dir, o)
			if e != nil {
				testState.t.Fatalf("%v", e)
			}
			d.Close()
		}
	}
	testState.t.Log(fmt.Sprint("PASS legacy primary-key metadata upgrade and invalid legacy NULL rejection"))
}
