package sql

import (
	"bytes"
	"fmt"
	"unicode/utf8"

	"slopdb/storage"
)

func validateMySQLCharsetText(charset, table, column, text string) error {
	if charset != "utf8mb4" && charset != "utf8mb3" && charset != "utf8" {
		return nil
	}
	if !utf8.ValidString(text) {
		return fmt.Errorf("invalid UTF-8 in %s.%s", table, column)
	}
	if charset != "utf8mb4" {
		for _, r := range text {
			if r > 0xffff {
				return fmt.Errorf("character outside %s repertoire in %s.%s", charset, table, column)
			}
		}
	}
	return nil
}

// Text is stored as UTF-8 independently of a table's declared MySQL charset.
// Supported Unicode conversions therefore validate the target repertoire and
// publish its metadata without rewriting identical row/index bytes. Binary
// columns retain their byte repertoire, as in MySQL CONVERT TO CHARACTER SET.
func (s *Schema) convertMySQLCharset(info *TableInfo, charset string) error {
	validate := func(column, text string) error {
		return validateMySQLCharsetText(charset, info.Name, column, text)
	}
	for _, column := range info.Columns {
		if column.ColType != ColText {
			continue
		}
		for _, value := range column.EnumValues {
			if err := validate(column.Name, value); err != nil {
				return err
			}
		}
		if literal, ok := column.Default.(*LiteralExpr); ok && !literal.Value.Null && literal.Value.Type == ColText {
			if err := validate(column.Name, literal.Value.Text); err != nil {
				return err
			}
		}
	}
	prefix := []byte(s.tableDataPrefix(info.Name))
	it := s.newRangeIterator(prefix, prefixSuccessor(prefix), false)
	closed := false
	defer func() {
		if !closed {
			it.Close()
		}
	}()
	for ; it.Valid() && bytes.HasPrefix(it.Key().UserKey, prefix); it.Next() {
		if err := s.checkInterrupt(); err != nil {
			return err
		}
		row := decodeRow(it.Value(), info)
		for _, column := range info.Columns {
			value := row[column.Name]
			if column.ColType == ColText && !value.Null {
				if err := validate(column.Name, value.Text); err != nil {
					return err
				}
			}
		}
	}
	if err := storage.IteratorError(it); err != nil {
		return err
	}
	// Close before catalog publication; iterators can block storage writes.
	it.Close()
	closed = true
	info.Charset = charset
	return s.saveSchema()
}
