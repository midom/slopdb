package sql

import (
	"context"
	"fmt"
	"math"

	"slopdb/storage"
)

func (s *Schema) showTableStatus(stmt *ShowStmt) (*ResultSet, error) {
	rs := showResult("Name", "Engine", "Version", "Row_format", "Rows", "Avg_row_length", "Data_length", "Max_data_length", "Index_length", "Data_free", "Auto_increment", "Create_time", "Update_time", "Check_time", "Collation", "Checksum", "Create_options", "Comment")
	for i := range rs.Columns {
		switch rs.Columns[i].Name {
		case "Version", "Rows", "Avg_row_length", "Data_length", "Max_data_length", "Index_length", "Data_free", "Auto_increment", "Checksum":
			rs.Columns[i].ColType = ColInteger
		}
	}
	if stmt.Where != nil {
		if err := s.validateQuery(&SelectStmt{Where: stmt.Where}, &TableInfo{Columns: rs.Columns}); err != nil {
			return nil, err
		}
	}
	namespace := s.SearchPath()[0]
	if stmt.Database != "" {
		namespace = s.namespaceAlias(stmt.Database)
	}
	dataRows, err := s.informationSchemaTable("tables", catalogFilter{Schema: s.catalogNamespace(namespace)})
	if err != nil {
		return nil, err
	}
	facts := s.catalogFacts()
	for _, catalogRow := range dataRows.Rows {
		bare := catalogRow["table_name"].Text
		name := bare
		if namespace != "main" {
			name = namespace + "." + bare
		}
		if err := s.checkInterrupt(); err != nil {
			return nil, err
		}
		// Avoid touching storage for objects rejected by a LIKE filter.
		if stmt.Like != nil && !likeMatchEsc(bare, *stmt.Like, '\\') {
			continue
		}
		row := make(map[string]Value, len(rs.Columns))
		for _, col := range rs.Columns {
			row[col.Name] = NewNullValue()
		}
		for label, source := range map[string]string{"Name": "table_name", "Engine": "engine", "Version": "version", "Row_format": "row_format", "Rows": "table_rows", "Avg_row_length": "avg_row_length", "Data_length": "data_length", "Max_data_length": "max_data_length", "Index_length": "index_length", "Data_free": "data_free", "Auto_increment": "auto_increment", "Create_time": "create_time", "Update_time": "update_time", "Check_time": "check_time", "Collation": "table_collation", "Checksum": "checksum", "Create_options": "create_options", "Comment": "table_comment"} {
			row[label] = catalogRow[source]
		}
		if catalogRow["table_type"].Text == "VIEW" {
			row["Comment"] = NewTextValue("VIEW")
			rs.Rows = append(rs.Rows, row)
			continue
		}
		info := facts.tables[name]
		data, err := s.estimateStatusRange(info.storagePrefix(name), true)
		if err != nil {
			return nil, fmt.Errorf("table %s: %w", name, err)
		}
		var indexBytes int64
		// Include every physical secondary index, including automatic constraint
		// indexes. The clustered primary key is part of Data_length.
		for _, idx := range facts.indexes {
			if idx.Table != name {
				continue
			}
			estimate, err := s.estimateStatusRange(idx.storagePrefix(), false)
			if err != nil {
				return nil, fmt.Errorf("index %s: %w", idx.Name, err)
			}
			indexBytes = statusAdd(indexBytes, statusAdd(estimate.DiskBytes, estimate.MemoryBytes))
		}
		dataBytes := statusAdd(data.DiskBytes, data.MemoryBytes)
		row["Engine"] = NewTextValue("LSM")
		row["Row_format"] = NewTextValue("LSM")
		row["Rows"] = NewIntValue(data.Rows)
		average := int64(0)
		if data.Rows > 0 {
			average = dataBytes / data.Rows
		}
		row["Avg_row_length"] = NewIntValue(average)
		row["Data_length"] = NewIntValue(dataBytes)
		row["Index_length"] = NewIntValue(indexBytes)
		row["Collation"] = NewTextValue(string(s.collation))
		row["Create_options"] = NewTextValue("")
		row["Comment"] = NewTextValue("Estimated sizes include compressed SST data blocks and unflushed records, including retained versions; Rows is sampled")
		if col := s.getPKColumn(info); col != nil && col.AutoIncrement {
			last, err := s.lastRowID(name, info)
			if err != nil {
				return nil, err
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
				row["Auto_increment"] = NewIntValue(max(last+1, reserved))
			}
		}
		rs.Rows = append(rs.Rows, row)
	}
	return rs, nil
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
