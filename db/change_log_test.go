package db

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"slopdb/sql"
	"slopdb/storage"
	"testing"
)

func TestCDCTransactionsTypedRowsNamespacesRollbackAndResume(t *testing.T) {
	path := t.TempDir()
	opt := storage.DefaultOptions()
	opt.DisableWALSync = true
	d, e := Open(path, opt)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { _ = d.Close() })
	ctx := context.Background()
	metadata, e := d.EnableCDC(ctx)
	if e != nil || metadata.SourceID == "" {
		t.Fatal(metadata, e)
	}
	session := isolationSession(t, d)
	isolationExec(t, session, "CREATE SCHEMA tenant;CREATE TABLE tenant.records(id BIGINT UNSIGNED PRIMARY KEY,price DECIMAL(20,4),doc JSON)")
	reader, e := d.NewCDCReader(ctx, 0)
	if e != nil {
		t.Fatal(e)
	}
	initial, e := reader.Read(ctx, 100)
	if e != nil || len(initial) != 2 || len(initial[0].Namespaces) != 2 || len(initial[1].SchemaChanges) != 1 {
		t.Fatal(initial, e)
	}
	if initial[1].SchemaChanges[0].After.Schema != "tenant" {
		t.Fatal(initial)
	}
	isolationExec(t, session, "CREATE TEMPORARY TABLE scratch(id INTEGER PRIMARY KEY,v TEXT)")
	isolationExec(t, session, "BEGIN;INSERT INTO tenant.records VALUES(18446744073709551615,1234567890123456.1234,'{\"n\":18446744073709551615}');INSERT INTO scratch VALUES(1,'private');COMMIT")
	txs, e := reader.Read(ctx, 100)
	if e != nil || len(txs) != 1 || len(txs[0].Changes) != 1 {
		t.Fatal(txs, e)
	}
	row := txs[0].Changes[0]
	if row.Kind != "insert" || row.After[0].Type != sql.ColUnsigned || row.After[0].Uint != ^uint64(0) || row.After[1].Text != "1234567890123456.1234" || row.After[2].Type != sql.ColJSON {
		t.Fatal(row)
	}
	// Caller metadata is owned; mutation must not corrupt later event decoding.
	txs[0].Tables[0].Columns[1].ColType = sql.ColBlob
	position := reader.Position()
	isolationExec(t, session, "BEGIN;UPDATE tenant.records SET price=1.0001;ROLLBACK")
	if rows, e := reader.Read(ctx, 100); e != nil || len(rows) != 0 {
		t.Fatal(rows, e)
	}
	isolationExec(t, session, "UPDATE tenant.records SET price=2.0002")
	isolationExec(t, session, "DELETE FROM tenant.records")
	if e = session.Close(); e != nil {
		t.Fatal(e)
	}
	if e = d.Close(); e != nil {
		t.Fatal(e)
	}
	d, e = Open(path, opt)
	if e != nil {
		t.Fatal(e)
	}
	resumed, e := d.NewCDCReader(ctx, position)
	if e != nil {
		t.Fatal(e)
	}
	txs, e = resumed.Read(ctx, 100)
	if e != nil || len(txs) != 2 {
		t.Fatal(txs, e)
	}
	if txs[0].Changes[0].Kind != "update" || txs[0].Changes[0].Before[1].Text != "1234567890123456.1234" || txs[0].Changes[0].After[1].Text != "2.0002" || txs[1].Changes[0].Kind != "delete" {
		t.Fatal(txs)
	}
	reopened, e := d.CDCMetadata(ctx)
	if e != nil || reopened.SourceID != metadata.SourceID {
		t.Fatal(reopened, e)
	}
	if _, e = d.NewCDCReader(ctx, ^uint64(0)); e == nil {
		t.Fatal("accepted future resume position")
	}
	if _, e = d.CheckIntegrity(); e != nil {
		t.Fatal(e)
	}
	if e = d.Close(); e != nil {
		t.Fatal(e)
	}
}
func TestCDCCatalogCheckpointAfterSchemaChanges(t *testing.T) {
	d, e := Open(t.TempDir(), nil)
	if e != nil {
		t.Fatal(e)
	}
	defer d.Close()
	ctx := context.Background()
	if _, e = d.EnableCDC(ctx); e != nil {
		t.Fatal(e)
	}
	s := isolationSession(t, d)
	isolationExec(t, s, "CREATE TABLE oldname(id INTEGER PRIMARY KEY,v TEXT);INSERT INTO oldname VALUES(1,'old')")
	first, e := d.CDCPosition(ctx)
	if e != nil {
		t.Fatal(e)
	}
	isolationExec(t, s, "ALTER TABLE oldname RENAME TO newname;ALTER TABLE newname ADD COLUMN more INTEGER DEFAULT 7")
	position, e := d.CDCPosition(ctx)
	if e != nil {
		t.Fatal(e)
	}
	isolationExec(t, s, "UPDATE newname SET v='new'")
	resumed, e := d.NewCDCReader(ctx, position)
	if e != nil {
		t.Fatal(e)
	}
	rows, e := resumed.Read(ctx, 100)
	if e != nil || len(rows) != 1 || rows[0].Tables[0].Name != "newname" || len(rows[0].Changes[0].After) != 3 || rows[0].Changes[0].After[2].Int != 7 {
		t.Fatal(rows, e)
	}
	old, e := d.NewCDCReader(ctx, first)
	if e != nil {
		t.Fatal(e)
	}
	rows, e = old.Read(ctx, 100)
	if e != nil || len(rows) < 2 {
		t.Fatal(rows, e)
	}
	if rows[0].SchemaChanges[0].Before.Name != "oldname" || rows[0].SchemaChanges[0].After.Name != "newname" {
		t.Fatal(rows)
	}
}

func TestCDCMetadataAndTransactionIndexesAreCallerOwned(t *testing.T) {
	d, err := Open(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	objectExec(t, d, "CREATE TABLE docs(id INTEGER PRIMARY KEY,body TEXT,KEY prefix_body(body(3)),FULLTEXT KEY ft(body))")
	if _, err = d.EnableCDC(context.Background()); err != nil {
		t.Fatal(err)
	}
	reader, err := d.NewCDCReader(context.Background(), 0)
	if err != nil {
		t.Fatal(err)
	}
	metadata := reader.Metadata()
	if len(metadata.Tables) != 1 || len(metadata.Tables[0].Indexes) < 2 {
		t.Fatal(metadata)
	}
	index := func(table CDCTable) *sql.IndexInfo {
		for _, idx := range table.Indexes {
			if idx.SQLName() == "prefix_body" {
				return idx
			}
		}
		t.Fatal("missing prefix index")
		return nil
	}
	index(metadata.Tables[0]).Column = "corrupted"
	index(metadata.Tables[0]).PrefixLengths[0] = 999
	if again := reader.Metadata(); index(again.Tables[0]).Column != "body" || index(again.Tables[0]).PrefixLengths[0] != 3 {
		t.Fatal("metadata aliases reader", again)
	}
	objectExec(t, d, "INSERT INTO docs VALUES(1,'quasar nebula')")
	rows, err := reader.Read(context.Background(), 10)
	if err != nil || len(rows) != 1 {
		t.Fatal(rows, err)
	}
	index(rows[0].Tables[0]).Column = "corrupted"
	objectExec(t, d, "INSERT INTO docs VALUES(2,'galactic');ALTER TABLE docs ADD COLUMN extra INTEGER")
	rows, err = reader.Read(context.Background(), 10)
	if err != nil || len(rows) != 2 {
		t.Fatal(rows, err)
	}
	if index(rows[0].Tables[0]).Column != "body" {
		t.Fatal("transaction index aliases reader", rows)
	}
	if len(rows[1].SchemaChanges) != 1 || index(*rows[1].SchemaChanges[0].Before).Column != "body" {
		t.Fatal("schema before-image corrupted", rows)
	}
}

func TestCDCReaderStorageHandoffWakesAndContinues(t *testing.T) {
	d, err := Open(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	ctx := context.Background()
	if _, err = d.EnableCDC(ctx); err != nil {
		t.Fatal(err)
	}
	objectExec(t, d, "CREATE TABLE docs(id INTEGER PRIMARY KEY,v TEXT);INSERT INTO docs VALUES(1,'old')")
	position, err := d.CDCPosition(ctx)
	if err != nil {
		t.Fatal(err)
	}
	reader, err := d.NewCDCReader(ctx, position)
	if err != nil {
		t.Fatal(err)
	}
	wait := reader.Changes()
	temporary, err := d.NewSession()
	if err != nil {
		t.Fatal(err)
	}
	defer temporary.Close()
	if _, err = temporary.Exec("CREATE TEMPORARY TABLE scratch(id INTEGER PRIMARY KEY,v TEXT);INSERT INTO scratch VALUES(1,'private backup')"); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "snapshot")
	if err = d.Backup(path); err != nil {
		t.Fatal(err)
	}
	next, err := storage.NewLSMTree(path, storage.DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	previous := d.ReplicationStorage()
	sourceHeader, e := previous.ChangeLogHeader()
	if e != nil {
		t.Fatal(e)
	}
	backupHeader, e := next.ChangeLogHeader()
	if e != nil {
		t.Fatal(e)
	}
	sourceJSON, _ := json.Marshal(sourceHeader)
	backupJSON, _ := json.Marshal(backupHeader)
	if !bytes.Equal(sourceJSON, backupJSON) {
		t.Fatal("backup changed CDC baseline")
	}
	sourceRows, e := previous.ReadChanges(ctx, 0, 100)
	if e != nil {
		t.Fatal(e)
	}
	backupRows, e := next.ReadChanges(ctx, 0, 100)
	if e != nil {
		t.Fatal(e)
	}
	sourceJSON, _ = json.Marshal(sourceRows)
	backupJSON, _ = json.Marshal(backupRows)
	if !bytes.Equal(sourceJSON, backupJSON) {
		t.Fatal("backup changed exact journal history")
	}
	it := next.NewIteratorRange([]byte{3}, []byte{4}, false)
	if it.Valid() {
		it.Close()
		t.Fatal("backup contains temporary data")
	}
	it.Close()
	defer previous.Close()
	if err = d.ReplaceReplicaStorage(next, path, func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	select {
	case <-wait:
	default:
		t.Fatal("storage handoff did not wake existing CDC waiter")
	}
	objectExec(t, d, "UPDATE docs SET v='new'")
	rows, err := reader.Read(ctx, 10)
	if err != nil || len(rows) != 1 || rows[0].Changes[0].After[1].Text != "new" {
		t.Fatal(rows, err)
	}
	wait = reader.Changes()
	if err = d.Close(); err != nil {
		t.Fatal(err)
	}
	for _, ch := range []<-chan struct{}{wait, reader.Changes()} {
		select {
		case <-ch:
		default:
			t.Fatal("database close did not wake CDC waiter")
		}
	}
	if _, err = reader.Read(ctx, 10); err == nil {
		t.Fatal("reader accepted closed database")
	}
}

func TestCDCReaderRejectsReplacementRewindAndSourceChange(t *testing.T) {
	for _, changed := range []bool{false, true} {
		t.Run(fmt.Sprint(changed), func(t *testing.T) {
			d, err := Open(t.TempDir(), nil)
			if err != nil {
				t.Fatal(err)
			}
			defer d.Close()
			ctx := context.Background()
			if _, err = d.EnableCDC(ctx); err != nil {
				t.Fatal(err)
			}
			objectExec(t, d, "CREATE TABLE docs(id INTEGER PRIMARY KEY)")
			path := filepath.Join(t.TempDir(), "snapshot")
			if !changed {
				if err = d.Backup(path); err != nil {
					t.Fatal(err)
				}
			}
			objectExec(t, d, "INSERT INTO docs VALUES(1)")
			position, err := d.CDCPosition(ctx)
			if err != nil {
				t.Fatal(err)
			}
			reader, err := d.NewCDCReader(ctx, position)
			if err != nil {
				t.Fatal(err)
			}
			next, err := storage.NewLSMTree(path, storage.DefaultOptions())
			if err != nil {
				t.Fatal(err)
			}
			if changed {
				if _, err = next.EnableChangeLog(ctx); err != nil {
					t.Fatal(err)
				}
			}
			old := d.ReplicationStorage()
			defer old.Close()
			if err = d.ReplaceReplicaStorage(next, path, func() error { return nil }); err != nil {
				t.Fatal(err)
			}
			if _, err = reader.Read(ctx, 10); err == nil {
				t.Fatal("reader accepted changed storage history")
			}
			if reader.Position() != position {
				t.Fatal("failed handoff advanced reader")
			}
		})
	}
}
