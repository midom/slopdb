package sql

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"

	"slopdb/storage"
)

const fulltextIndexVersion = 1

func fulltextDocumentPrefix(idx *IndexInfo) []byte {
	return append([]byte(idx.storagePrefix()), 'd', ':')
}
func fulltextPostingPrefix(idx *IndexInfo, word string) []byte {
	prefix := append([]byte(idx.storagePrefix()), 'p', ':')
	prefix = append(prefix, hex.EncodeToString([]byte(word))...)
	return append(prefix, ':')
}

// Each term has a sorted posting range whose suffix is the complete clustered
// (or hidden) row key. Positions include short/stop words so phrase boundaries
// remain exact. Document markers make corpus statistics independent of rows.
func (s *Schema) fulltextIndexMutations(idx *IndexInfo, row map[string]Value, key []byte, remove bool) ([]storage.Mutation, error) {
	words := make([]string, 0)
	for _, name := range idx.columnNames() {
		value := row[name]
		if value.Null {
			words = append(words, "")
			continue
		}
		part, err := s.fulltextDocumentWords(mysqlStringValue(value), s.memoryBudget()-int64(len(words))*16)
		if err != nil {
			return nil, err
		}
		words = append(words, part...)
		words = append(words, "") // phrases never cross fields
	}
	positions := make(map[string][]uint64)
	query := fulltextQuery{minWord: 4}
	var used int64
	for position, word := range words {
		if !query.indexed(word) {
			continue
		}
		if _, ok := positions[word]; !ok {
			used += int64(len(word)) + 64
		}
		used += 8
		if used > s.memoryBudget() {
			return nil, fmt.Errorf("fulltext postings exceed query memory budget")
		}
		positions[word] = append(positions[word], uint64(position))
	}
	document := append(fulltextDocumentPrefix(idx), key...)
	result := make([]storage.Mutation, 0, len(positions)+1)
	result = append(result, storage.Mutation{Key: document, Value: binary.AppendUvarint(nil, uint64(len(words))), Delete: remove})
	terms := make([]string, 0, len(positions))
	for word := range positions {
		terms = append(terms, word)
	}
	sort.Strings(terms)
	for _, word := range terms {
		posting := append(fulltextPostingPrefix(idx, word), key...)
		var value []byte
		if !remove {
			value = binary.AppendUvarint(value, uint64(len(positions[word])))
			previous := uint64(0)
			for _, position := range positions[word] {
				value = binary.AppendUvarint(value, position-previous)
				previous = position
			}
		}
		result = append(result, storage.Mutation{Key: posting, Value: value, Delete: remove})
	}
	return result, nil
}
func (s *Schema) indexRowMutations(idx *IndexInfo, row map[string]Value, key []byte, remove bool) ([]storage.Mutation, error) {
	if idx.Fulltext {
		return s.fulltextIndexMutations(idx, row, key, remove)
	}
	return []storage.Mutation{{Key: encodeRowIndexKey(idx, row, key), Value: s.encodeIndexValue(idx, row), Delete: remove}}, nil
}
func (s *Schema) writeIndexRow(idx *IndexInfo, row map[string]Value, key []byte, remove bool) error {
	mutations, err := s.indexRowMutations(idx, row, key, remove)
	if err != nil {
		return err
	}
	for _, mutation := range mutations {
		if mutation.Delete {
			err = s.remove(mutation.Key)
		} else {
			err = s.write(mutation.Key, mutation.Value)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// Older FULLTEXT metadata described ordinary document-value indexes. Upgrade
// each such index in one atomic schema transaction before client admission.
func (s *Schema) upgradeFulltextIndexes() error {
	var names []string
	for name, idx := range s.indexes {
		if idx.Fulltext && idx.FulltextVersion < fulltextIndexVersion {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	for _, name := range names {
		if err := s.ExecuteStatement(func() error {
			s.ensureCatalogWritable()
			idx := s.indexes[name]
			info := s.tables[idx.Table]
			if info == nil {
				return fmt.Errorf("FULLTEXT index %s has no table", name)
			}
			rows, err := s.matchingDMLRows(info.Name, info, nil, -1, 0)
			if err != nil {
				return err
			}
			prefix := []byte(idx.storagePrefix())
			if err = s.removeRange(prefix, prefixSuccessor(prefix)); err != nil {
				return err
			}
			for _, entry := range rows {
				if err = s.writeIndexRow(idx, decodeRow(entry.Value, info), entry.Key.UserKey, false); err != nil {
					return err
				}
			}
			idx.FulltextVersion = fulltextIndexVersion
			return s.saveIndexes()
		}); err != nil {
			return err
		}
	}
	return nil
}

func (s *Schema) fulltextBindingForInfo(f *FuncCallExpr, info *TableInfo) (fulltextBinding, bool) {
	if binding, ok := s.fulltextBindings[f]; ok {
		return binding, binding.table.ID == info.ID
	}
	if len(f.Args) < 3 {
		return fulltextBinding{}, false
	}
	names := make(map[string]bool)
	for _, arg := range f.Args[:len(f.Args)-2] {
		column, ok := arg.(*ColumnRefExpr)
		if !ok {
			return fulltextBinding{}, false
		}
		name := column.Name
		if dot := strings.LastIndexByte(name, '.'); dot >= 0 {
			name = name[dot+1:]
		}
		names[name] = true
	}
	for _, idx := range s.tableIndexes(info.Name) {
		if !idx.Fulltext || len(idx.columnNames()) != len(names) {
			continue
		}
		same := true
		for _, name := range idx.columnNames() {
			same = same && names[name]
		}
		if same {
			return fulltextBinding{table: info, index: idx}, true
		}
	}
	return fulltextBinding{}, false
}

func hasFulltextCondition(e Expression) bool {
	if f, ok := e.(*FuncCallExpr); ok {
		return f.Name == fulltextFunction
	}
	if b, ok := e.(*BinaryExpr); ok {
		switch b.Op {
		case "AND", "OR":
			return hasFulltextCondition(b.Left) || hasFulltextCondition(b.Right)
		case ">":
			if l, ok := b.Right.(*LiteralExpr); ok && ValueCompare(l.Value, NewIntValue(0)) == 0 {
				return hasFulltextCondition(b.Left)
			}
		}
	}
	return false
}

// The candidates are a conservative necessary condition. Required terms and
// groups intersect ranges; optional terms union them; exclusions are evaluated
// against the fetched document. OR is bounded only when both sides are bounded.
func (s *Schema) fulltextCandidates(e Expression, info *TableInfo) (map[string]bool, bool, error) {
	if b, ok := e.(*BinaryExpr); ok {
		if b.Op == ">" {
			if l, ok := b.Right.(*LiteralExpr); ok && ValueCompare(l.Value, NewIntValue(0)) == 0 {
				return s.fulltextCandidates(b.Left, info)
			}
		}
		if b.Op != "AND" && b.Op != "OR" {
			return nil, false, nil
		}
		a, ak, err := s.fulltextCandidates(b.Left, info)
		if err != nil {
			return nil, false, err
		}
		c, ck, err := s.fulltextCandidates(b.Right, info)
		if err != nil {
			return nil, false, err
		}
		if b.Op == "OR" {
			if !ak || !ck {
				return nil, false, nil
			}
			for key := range c {
				a[key] = true
			}
			return a, true, nil
		}
		if !ak {
			return c, ck, nil
		}
		if !ck {
			return a, ak, nil
		}
		for key := range a {
			if !c[key] {
				delete(a, key)
			}
		}
		return a, true, nil
	}
	f, ok := e.(*FuncCallExpr)
	if !ok || f.Name != fulltextFunction {
		return nil, false, nil
	}
	binding, ok := s.fulltextBindingForInfo(f, info)
	if !ok {
		return nil, false, nil
	}
	q, err := s.compiledFulltextQuery(f, binding, nil)
	if err != nil {
		return nil, false, err
	}
	if q == nil {
		return map[string]bool{}, true, nil
	}
	var gather func([]fulltextTerm) (map[string]bool, error)
	gather = func(terms []fulltextTerm) (map[string]bool, error) {
		optional := make(map[string]bool)
		var required map[string]bool
		hasRequired := false
		for _, term := range terms {
			if term.exclude {
				continue
			}
			var keys map[string]bool
			if len(term.children) > 0 {
				keys, err = gather(term.children)
			} else {
				keys = make(map[string]bool)
				anchor := ""
				for _, word := range term.words {
					if q.indexed(word) {
						anchor = word
						break
					}
				}
				if term.prefix && len(term.words) > 0 {
					anchor = term.words[len(term.words)-1]
				}
				if anchor != "" {
					keys, err = s.fulltextPostingKeys(binding, q, anchor, term.prefix)
				}
			}
			if err != nil {
				return nil, err
			}
			if term.require {
				if !hasRequired {
					required = keys
					hasRequired = true
				} else {
					for key := range required {
						if !keys[key] {
							delete(required, key)
						}
					}
				}
			} else {
				for key := range keys {
					optional[key] = true
				}
			}
			if int64(len(optional)+len(required))*96 > s.memoryBudget() {
				return nil, fmt.Errorf("fulltext candidates exceed query memory budget")
			}
		}
		if hasRequired {
			return required, nil
		}
		return optional, nil
	}
	result, err := gather(q.terms)
	return result, true, err
}

// Posting scans do not fetch document bodies. The complete predicate is still
// evaluated on each candidate at the statement snapshot plus private writes.
func (s *Schema) fulltextPostingKeys(binding fulltextBinding, q *fulltextQuery, word string, prefixMatch bool) (map[string]bool, error) {
	lower := fulltextPostingPrefix(binding.index, word)
	upper := prefixSuccessor(lower)
	if prefixMatch {
		lower = lower[:len(lower)-1]
		upper = prefixSuccessor(lower)
	}
	it := s.newRangeIterator(lower, upper, false)
	defer it.Close()
	result := make(map[string]bool)
	var used int64
	for ; it.Valid(); it.Next() {
		if err := s.checkInterrupt(); err != nil {
			return nil, err
		}
		if err := validateFulltextPositions(it.Value()); err != nil {
			return nil, err
		}
		key := it.Key().UserKey
		suffix := key[len(lower):]
		if prefixMatch {
			colon := bytes.IndexByte(suffix, ':')
			if colon < 0 {
				return nil, fmt.Errorf("malformed FULLTEXT posting key")
			}
			suffix = suffix[colon+1:]
		}
		if !result[string(suffix)] {
			used += int64(len(suffix)) + 64
			result[string(suffix)] = true
		}
		if used > s.memoryBudget()/2 {
			return nil, fmt.Errorf("fulltext candidates exceed query memory budget")
		}
	}
	return result, storage.IteratorError(it)
}

func (s *Schema) walkFilteredAccess(info *TableInfo, needed map[string]bool, where Expression, paths []*accessPath, visit func(map[string]Value) error) (resultErr error) {
	keys, used, err := s.fulltextCandidates(where, info)
	if err != nil {
		return err
	}
	if !used {
		return s.walkAccess(info, needed, paths, visit)
	}
	ordered := make([]string, 0, len(keys))
	for key := range keys {
		ordered = append(ordered, key)
	}
	sort.Strings(ordered)
	finish := s.ProfileOperator("FulltextIndex", info.Name)
	var count int64
	defer func() {
		if finish != nil {
			finish(int64(len(keys)), count, resultErr)
		}
	}()
	for offset := 0; offset < len(ordered); offset += 256 {
		end := offset + 256
		if end > len(ordered) {
			end = len(ordered)
		}
		batch := make([][]byte, end-offset)
		for i, key := range ordered[offset:end] {
			batch[i] = []byte(key)
		}
		values, e := s.schemaMultiGet(batch)
		if e != nil {
			return e
		}
		for _, value := range values {
			if !value.Found {
				continue
			}
			if err = s.checkInterrupt(); err != nil {
				return err
			}
			if err = visit(decodeRow(value.Value, info)); err != nil {
				return err
			}
			count++
		}
	}
	return nil
}

func validateFulltextPositions(value []byte) error {
	count, n := binary.Uvarint(value)
	if n <= 0 || count == 0 {
		return fmt.Errorf("malformed FULLTEXT posting count")
	}
	value = value[n:]
	if count > uint64(len(value)) {
		return fmt.Errorf("truncated FULLTEXT positions")
	}
	position := uint64(0)
	for i := uint64(0); i < count; i++ {
		delta, n := binary.Uvarint(value)
		if n <= 0 || i > 0 && delta == 0 {
			return fmt.Errorf("malformed FULLTEXT position")
		}
		value = value[n:]
		next := position + delta
		if next < position {
			return fmt.Errorf("FULLTEXT position overflow")
		}
		position = next
	}
	if len(value) != 0 {
		return fmt.Errorf("trailing FULLTEXT position data")
	}
	return nil
}

// Metadata-only counterpart to candidate selection; EXPLAIN must not read or
// evaluate search expressions merely to report an available inverted path.
func (s *Schema) fulltextPlanIndex(e Expression, info *TableInfo) *IndexInfo {
	if info == nil {
		return nil
	}
	if f, ok := e.(*FuncCallExpr); ok && f.Name == fulltextFunction {
		if binding, ok := s.fulltextBindingForInfo(f, info); ok {
			return binding.index
		}
		return nil
	}
	if b, ok := e.(*BinaryExpr); ok {
		if b.Op == ">" {
			if value, ok := b.Right.(*LiteralExpr); ok && ValueCompare(value.Value, NewIntValue(0)) == 0 {
				return s.fulltextPlanIndex(b.Left, info)
			}
		}
		a, c := s.fulltextPlanIndex(b.Left, info), s.fulltextPlanIndex(b.Right, info)
		if b.Op == "AND" {
			if a != nil {
				return a
			}
			return c
		}
		if b.Op == "OR" && a != nil && c != nil {
			return a
		}
	}
	return nil
}
