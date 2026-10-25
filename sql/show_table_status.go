package sql

import (
	"context"
	"fmt"
	"math"
	"strings"

	"slopdb/storage"
)

func (s *Schema) showTableStatus(stmt *ShowStmt) (*ResultSet, error) {
	rs := s.showFrame(stmt)
	if stmt.Where != nil {
		if err := s.validateQuery(&SelectStmt{Where: stmt.Where}, &TableInfo{Columns: rs.Columns}); err != nil {
			return nil, err
		}
	}
	namespace := s.SearchPath()[0]
	if stmt.Database != "" {
		namespace = s.namespaceAlias(stmt.Database)
	}
	dataRows, err := s.informationSchemaTable("tables", catalogFilter{Schema: s.catalogNamespace(namespace), Pattern: stmt.Like, Status: true})
	if err != nil {
		return nil, err
	}
	for _, catalogRow := range dataRows.Rows {
		row := catalogEmptyRow(rs)
		for label, source := range map[string]string{"Name": "table_name", "Engine": "engine", "Version": "version", "Row_format": "row_format", "Rows": "table_rows", "Avg_row_length": "avg_row_length", "Data_length": "data_length", "Max_data_length": "max_data_length", "Index_length": "index_length", "Data_free": "data_free", "Auto_increment": "auto_increment", "Create_time": "create_time", "Update_time": "update_time", "Check_time": "check_time", "Collation": "table_collation", "Checksum": "checksum", "Create_options": "create_options", "Comment": "table_comment"} {
			row[label] = catalogRow[source]
		}
		if strings.Contains(catalogRow["table_type"].Text, "VIEW") {
			row["Comment"] = NewTextValue("VIEW")
		} else if !row["Collation"].Null {
			row["Collation"] = NewTextValue(strings.ToUpper(row["Collation"].Text))
		}
		rs.Rows = append(rs.Rows, row)
	}
	return rs, nil
}

// Catalog estimates are optional requested facts. The provider calls this only
// after schema/name/pattern filtering; ordinary name/column introspection never
// touches storage. No exact COUNT or user expression evaluation occurs here.
func (s *Schema) catalogStorageFacts(row map[string]Value, fact catalogRelationFact) error {
	info := fact.Relation.Definition
	name := fact.Key
	data, err := s.estimateStatusRange(info.storagePrefix(name), true)
	if err != nil {
		return fmt.Errorf("table %s: %w", name, err)
	}
	var indexBytes int64
	for _, idx := range fact.Relation.Indexes {
		estimate, err := s.estimateStatusRange(idx.storagePrefix(), false)
		if err != nil {
			return fmt.Errorf("index %s: %w", idx.Name, err)
		}
		indexBytes = statusAdd(indexBytes, statusAdd(estimate.DiskBytes, estimate.MemoryBytes))
	}
	dataBytes := statusAdd(data.DiskBytes, data.MemoryBytes)
	average := int64(0)
	if data.Rows > 0 {
		average = dataBytes / data.Rows
	}
	row["table_rows"] = NewIntValue(data.Rows)
	row["avg_row_length"] = NewIntValue(average)
	row["data_length"] = NewIntValue(dataBytes)
	row["index_length"] = NewIntValue(indexBytes)
	row["table_comment"] = NewTextValue("Estimated sizes include compressed SST data blocks and unflushed records, including retained versions; Rows is sampled")
	if col := s.getPKColumn(info); col != nil && col.AutoIncrement {
		last, err := s.lastRowID(name, info)
		if err != nil {
			return err
		}
		last = max(last, s.getAutoincMax(name))
		root := s
		if s.parent != nil {
			root = s.parent
		}
		root.commitMu.RLock()
		root.rowIDMu.Lock()
		reserved := root.rowIDReservations[s.rowCounterKey(name)]
		root.rowIDMu.Unlock()
		root.commitMu.RUnlock()
		if last < math.MaxInt64 {
			row["auto_increment"] = NewIntValue(max(last+1, reserved))
		}
	}
	return nil
}

func statusAdd(a, b int64) int64 {
	if b > 0 && a > math.MaxInt64-b {
		return math.MaxInt64
	}
	return a + b
}

func (s *Schema) estimateStatusRange(prefix string, countRows bool) (storage.RangeEstimate, error) {
	ctx := s.ctx
	if ctx == nil {
		ctx = context.Background()
	}
	snapshot := uint64(math.MaxUint64)
	if s.statementActive {
		snapshot = s.statementSnapshot
	}
	lower := []byte(prefix)
	var estimate storage.RangeEstimate
	var err error
	if countRows {
		estimate, err = s.lsm.EstimateRange(ctx, lower, prefixSuccessor(lower), snapshot)
	} else {
		estimate, err = s.lsm.EstimateRangeBytes(ctx, lower, prefixSuccessor(lower))
	}
	if err != nil {
		return storage.RangeEstimate{}, err
	}
	if !s.inTx {
		return estimate, nil
	}
	// Storage estimates committed versions. Apply only the latest private write
	// for each key so transaction-local inserts, updates and deletes are visible.
	for key, index := range s.txLatest {
		if len(key) < len(prefix) || key[:len(prefix)] != prefix {
			continue
		}
		if err := s.checkInterrupt(); err != nil {
			return storage.RangeEstimate{}, err
		}
		write := s.txWrites[index]
		estimate.MemoryBytes = statusAdd(estimate.MemoryBytes, int64(len(write.key))+8+int64(len(write.value)))
		if !countRows {
			continue
		}
		old, found, err := s.committedGet(write.key)
		if err != nil {
			return storage.RangeEstimate{}, err
		}
		if found {
			estimate.Rows--
			estimate.ValueBytes -= int64(len(old))
		}
		if !write.del {
			estimate.Rows = statusAdd(estimate.Rows, 1)
			estimate.ValueBytes = statusAdd(estimate.ValueBytes, int64(len(write.value)))
		}
	}
	estimate.Rows = max(0, estimate.Rows)
	estimate.ValueBytes = max(0, estimate.ValueBytes)
	return estimate, nil
}
