package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"strconv"
	"strings"
	"time"

	"slopdb/db"
	"slopdb/sql"
)

type savedQuery struct {
	Number  int             `json:"number"`
	SQL     string          `json:"sql"`
	Columns []string        `json:"columns"`
	Results json.RawMessage `json:"results"`
	Samples []struct {
		Status string `json:"status"`
		Rows   int64  `json:"rows"`
	} `json:"samples"`
}
type savedReport struct {
	TableRows map[string]int64 `json:"table_rows"`
	Queries   []savedQuery     `json:"queries"`
}

// verifyQueryReport reuses independently saved complete answers, not new answers
// computed from the post-maintenance database. Numeric JSON stays exact int64.
func verifyQueryReport(database *db.Database, file string, counts map[string]int64, timeout time.Duration) (int, error) {
	source, err := os.Open(file)
	if err != nil {
		return 0, err
	}
	defer source.Close()
	decoder := json.NewDecoder(source)
	decoder.UseNumber()
	var report savedReport
	if err = decoder.Decode(&report); err != nil {
		return 0, fmt.Errorf("verification report: %w", err)
	}
	var trailing any
	if err = decoder.Decode(&trailing); err != io.EOF {
		return 0, fmt.Errorf("verification report has trailing data")
	}
	if len(report.Queries) == 0 {
		return 0, fmt.Errorf("verification report has no queries")
	}
	for name, want := range report.TableRows {
		got, exists := counts[name]
		if !exists || got != want {
			return 0, fmt.Errorf("verification report %s count: got %d, want %d", name, got, want)
		}
	}
	seen := make(map[int]bool, len(report.Queries))
	checked := 0
	for _, query := range report.Queries {
		if query.Number < 1 || seen[query.Number] {
			return checked, fmt.Errorf("verification report contains invalid/repeated query %d", query.Number)
		}
		seen[query.Number] = true
		if err = verifySavedQuery(database, query, timeout); err != nil {
			return checked, fmt.Errorf("Q%02d after maintenance: %w", query.Number, err)
		}
		checked++
		fmt.Fprintf(os.Stderr, "Q%02d complete answer verified after maintenance\n", query.Number)
	}
	return checked, nil
}
func verifySavedQuery(database *db.Database, query savedQuery, timeout time.Duration) error {
	if strings.TrimSpace(query.SQL) == "" || len(query.Columns) == 0 {
		return fmt.Errorf("saved SQL or output columns missing")
	}
	var expected [][]any
	if len(query.Results) > 0 {
		decoder := json.NewDecoder(bytes.NewReader(query.Results))
		decoder.UseNumber()
		if err := decoder.Decode(&expected); err != nil {
			return fmt.Errorf("saved results: %w", err)
		}
	} else {
		// Report writers omit an empty result set. Accept that omission only with a
		// recorded successful zero-row sample, never for failed/timed-out queries.
		zeroRows := false
		for _, sample := range query.Samples {
			if sample.Status == "ok" && sample.Rows == 0 {
				zeroRows = true
			}
		}
		if !zeroRows {
			return fmt.Errorf("saved complete results missing")
		}
	}
	for i, row := range expected {
		if len(row) != len(query.Columns) {
			return fmt.Errorf("saved row %d has %d columns, want %d", i, len(row), len(query.Columns))
		}
	}
	if len(query.Samples) > 0 {
		successful := false
		for _, sample := range query.Samples {
			if sample.Status == "ok" && sample.Rows == int64(len(expected)) {
				successful = true
			}
		}
		if !successful {
			return fmt.Errorf("report has no successful sample matching saved row count")
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	statement, err := database.PrepareContext(ctx, query.SQL)
	if err != nil {
		return err
	}
	defer statement.Close()
	// Query rejects mutations and multiple statements in an input report. Its
	// blocking fallback evaluates all TPC-H operators with the ordinary executor.
	rows, err := statement.QueryContext(ctx)
	if err != nil {
		return err
	}
	defer rows.Close()
	columns := rows.Columns()
	if len(columns) != len(query.Columns) {
		return fmt.Errorf("column count: got %d, want %d", len(columns), len(query.Columns))
	}
	for i, column := range columns {
		if column.Name != query.Columns[i] {
			return fmt.Errorf("column %d: got %q, want %q", i, column.Name, query.Columns[i])
		}
	}
	actual := make([][]sql.Value, 0, len(expected))
	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			return err
		}
		actual = append(actual, values)
	}
	if err = rows.Err(); err != nil {
		return err
	}
	return compareSavedRows(ctx, actual, expected)
}

// Exact integer/text/null columns partition candidate rows. This keeps TPC-H
// answers such as Q11 linear in their unique key count. Float columns remain
// tolerance-checked within each bucket, so legitimate sort-key ties may reorder.
func compareSavedRows(ctx context.Context, actual [][]sql.Value, expected [][]any) error {
	if len(actual) != len(expected) {
		return fmt.Errorf("row count: got %d, want %d", len(actual), len(expected))
	}
	if len(expected) == 0 {
		return ctx.Err()
	}
	width := len(expected[0])
	exact := make([]bool, width)
	for column := 0; column < width; column++ {
		integers, texts := true, true
		for i, row := range actual {
			if len(row) != width {
				return fmt.Errorf("row %d width differs", i)
			}
			if !row[column].Null {
				integers = integers && row[column].Type == sql.ColInteger
				texts = texts && row[column].Type == sql.ColText
				if row[column].Type == sql.ColText {
					if _, err := strconv.ParseFloat(row[column].Text, 64); err == nil {
						texts = false
					}
				}
			}
			reference := expected[i][column]
			if reference != nil {
				number, isNumber := reference.(json.Number)
				if !isNumber {
					integers = false
				} else {
					_, err := number.Int64()
					integers = integers && err == nil
				}
				text, isText := reference.(string)
				texts = texts && isText
				if isText {
					if _, err := strconv.ParseFloat(text, 64); err == nil {
						texts = false
					}
				}
			}
		}
		exact[column] = integers || texts
	}
	buckets := make(map[string][][]any, len(expected))
	for _, row := range expected {
		key := savedRowKey(row, exact)
		buckets[key] = append(buckets[key], row)
	}
	for i, row := range actual {
		if err := ctx.Err(); err != nil {
			return err
		}
		primitive := make([]any, len(row))
		for j, value := range row {
			if value.Null {
				continue
			}
			switch value.Type {
			case sql.ColInteger:
				primitive[j] = json.Number(strconv.FormatInt(value.Int, 10))
			case sql.ColText:
				primitive[j] = value.Text
			}
		}
		key := savedRowKey(primitive, exact)
		candidates := buckets[key]
		matched := -1
		for j, reference := range candidates {
			equal := true
			for column, value := range row {
				if !savedCellEqual(value, reference[column]) {
					equal = false
					break
				}
			}
			if equal {
				matched = j
				break
			}
		}
		if matched < 0 {
			return fmt.Errorf("row %d has no matching saved answer: %v", i, row)
		}
		// Remove exactly one candidate to preserve duplicate multiplicity.
		candidates[matched] = candidates[len(candidates)-1]
		buckets[key] = candidates[:len(candidates)-1]
	}
	return ctx.Err()
}
func savedRowKey(row []any, exact []bool) string {
	var key []byte
	for i, value := range row {
		if !exact[i] {
			continue
		}
		switch v := value.(type) {
		case nil:
			key = append(key, 0)
		case json.Number:
			n, _ := v.Int64()
			key = append(key, 1)
			key = binary.LittleEndian.AppendUint64(key, uint64(n))
		case string:
			key = append(key, 2)
			key = binary.AppendUvarint(key, uint64(len(v)))
			key = append(key, v...)
		}
	}
	return string(key)
}
func savedCellEqual(actual sql.Value, expected any) bool {
	if actual.Null {
		return expected == nil
	}
	if expected == nil {
		return false
	}
	if actual.Type == sql.ColBlob {
		encoded, ok := expected.(string)
		if !ok {
			return false
		}
		decoded, err := base64.StdEncoding.DecodeString(encoded)
		return err == nil && bytes.Equal(actual.Blob, decoded)
	}
	if actual.Type == sql.ColText {
		if text, ok := expected.(string); ok && text == actual.Text {
			return true
		}
	}
	if actual.Type == sql.ColInteger {
		if number, ok := expected.(json.Number); ok && !strings.ContainsAny(string(number), ".eE") {
			integer, err := number.Int64()
			return err == nil && integer == actual.Int
		}
	}
	var got float64
	switch actual.Type {
	case sql.ColInteger:
		got = float64(actual.Int)
	case sql.ColReal:
		got = actual.Real
	case sql.ColText:
		var err error
		got, err = strconv.ParseFloat(actual.Text, 64)
		if err != nil {
			return false
		}
	default:
		return false
	}
	var want float64
	switch value := expected.(type) {
	case json.Number:
		var err error
		want, err = value.Float64()
		if err != nil {
			return false
		}
	case string:
		var err error
		want, err = strconv.ParseFloat(value, 64)
		if err != nil {
			return false
		}
	default:
		return false
	}
	if math.IsNaN(got) || math.IsNaN(want) {
		return false
	}
	if got == want {
		return true
	}
	if math.IsInf(got, 0) || math.IsInf(want, 0) {
		return false
	}
	return math.Abs(got-want) <= math.Max(1e-6, 1e-8*math.Max(math.Abs(got), math.Abs(want)))
}
