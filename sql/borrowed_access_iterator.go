package sql

import "slopdb/storage"

const borrowedAccessScratchLimit = 64 << 10

func (it *transactionIterator) resetBorrowedCurrent() {
	it.current = storage.KeyValue{}
	if cap(it.keyScratch)+cap(it.valueScratch)+cap(it.seekScratch) > borrowedAccessScratchLimit {
		it.keyScratch, it.valueScratch, it.seekScratch = nil, nil, nil
	}
}

func (it *transactionIterator) copyBorrowedBase() {
	key := it.base.Key()
	it.keyScratch = append(it.keyScratch[:0], key.UserKey...)
	key.UserKey = it.keyScratch
	it.valueScratch = storage.IteratorValueInto(it.base, it.valueScratch)
	it.current = storage.KeyValue{Key: key, Value: it.valueScratch}
}
