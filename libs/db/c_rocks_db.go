// +build gcc

package db

import (
	"bytes"
	"fmt"
	"github.com/tecbot/gorocksdb"
	"path/filepath"
)

var _ DB = (*CRocksDB)(nil)

type CRocksDB struct {
	db     *gorocksdb.DB
	ro     *gorocksdb.ReadOptions
	wo     *gorocksdb.WriteOptions
	woSync *gorocksdb.WriteOptions
}

func NewCRocksDB(name string, dir string) (*CRocksDB, error) {
	dbPath := filepath.Join(dir, name+".db")

	bbto := gorocksdb.NewDefaultBlockBasedTableOptions()
	bbto.SetBlockCache(gorocksdb.NewLRUCache(1 << 30))
	opts := gorocksdb.NewDefaultOptions()
	opts.SetBlockBasedTableFactory(bbto)
	opts.SetCreateIfMissing(true)
	db, err := gorocksdb.OpenDb(opts, dbPath)
	if err != nil {
		return nil, err
	}
	ro := gorocksdb.NewDefaultReadOptions()
	wo := gorocksdb.NewDefaultWriteOptions()
	woSync := gorocksdb.NewDefaultWriteOptions()
	woSync.SetSync(true)
	database := &CRocksDB{
		db:     db,
		ro:     ro,
		wo:     wo,
		woSync: woSync,
	}
	return database, nil
}

// Implements DB.
func (db *CRocksDB) Get(key []byte) []byte {
	key = nonNilBytes(key)
	res, err := db.db.GetBytes(db.ro, key)
	if err != nil {
		panic(err)
	}
	return res
}

// Implements DB.
func (db *CRocksDB) Has(key []byte) bool {
	return db.Get(key) != nil
}

// Implements DB.
func (db *CRocksDB) Set(key []byte, value []byte) {
	key = nonNilBytes(key)
	value = nonNilBytes(value)
	err := db.db.Put(db.wo, key, value)
	if err != nil {
		panic(err)
	}
}

// Implements DB.
func (db *CRocksDB) SetSync(key []byte, value []byte) {
	key = nonNilBytes(key)
	value = nonNilBytes(value)
	err := db.db.Put(db.woSync, key, value)
	if err != nil {
		panic(err)
	}
}

// Implements DB.
func (db *CRocksDB) Delete(key []byte) {
	key = nonNilBytes(key)
	err := db.db.Delete(db.wo, key)
	if err != nil {
		panic(err)
	}
}

// Implements DB.
func (db *CRocksDB) DeleteSync(key []byte) {
	key = nonNilBytes(key)
	err := db.db.Delete(db.woSync, key)
	if err != nil {
		panic(err)
	}
}

func (db *CRocksDB) DB() *gorocksdb.DB {
	return db.db
}

// Implements DB.
func (db *CRocksDB) Close() {
	db.db.Close()
	db.ro.Destroy()
	db.wo.Destroy()
	db.woSync.Destroy()
}

// Implements DB.
func (db *CRocksDB) Print() {
	itr := db.Iterator(nil, nil)
	defer itr.Close()
	for ; itr.Valid(); itr.Next() {
		key := itr.Key()
		value := itr.Value()
		fmt.Printf("[%X]:\t[%X]\n", key, value)
	}
}

// Implements DB.
func (db *CRocksDB) Stats() map[string]string {
	// TODO: Find the available properties for the C LevelDB implementation
	keys := []string{}

	stats := make(map[string]string)
	for _, key := range keys {
		str := db.db.GetProperty(key)
		stats[key] = str
	}
	return stats
}

//----------------------------------------
// Batch

// Implements DB.
func (db *CRocksDB) NewBatch() Batch {
	batch := gorocksdb.NewWriteBatch()
	return &cRocksDBBatch{db, batch}
}

type cRocksDBBatch struct {
	db    *CRocksDB
	batch *gorocksdb.WriteBatch
}

// Implements Batch.
func (mBatch *cRocksDBBatch) Set(key, value []byte) {
	mBatch.batch.Put(key, value)
}

// Implements Batch.
func (mBatch *cRocksDBBatch) Delete(key []byte) {
	mBatch.batch.Delete(key)
}

// Implements Batch.
func (mBatch *cRocksDBBatch) Write() {
	err := mBatch.db.db.Write(mBatch.db.wo, mBatch.batch)
	if err != nil {
		panic(err)
	}
}

// Implements Batch.
func (mBatch *cRocksDBBatch) WriteSync() {
	err := mBatch.db.db.Write(mBatch.db.woSync, mBatch.batch)
	if err != nil {
		panic(err)
	}
}

//----------------------------------------
// Iterator
// NOTE This is almost identical to db/go_level_db.Iterator
// Before creating a third version, refactor.

func (db *CRocksDB) Iterator(start, end []byte) Iterator {
	itr := db.db.NewIterator(db.ro)
	return newCRocksDBIterator(itr, start, end, false)
}

func (db *CRocksDB) ReverseIterator(start, end []byte) Iterator {
	itr := db.db.NewIterator(db.ro)
	return newCRocksDBIterator(itr, start, end, true)
}

var _ Iterator = (*cRocksDBIterator)(nil)

type cRocksDBIterator struct {
	source     *gorocksdb.Iterator
	start, end []byte
	isReverse  bool
	isInvalid  bool
}

func newCRocksDBIterator(source *gorocksdb.Iterator, start, end []byte, isReverse bool) *cRocksDBIterator {
	if isReverse {
		if start == nil {
			source.SeekToLast()
		} else {
			source.Seek(start)
			if source.Valid() {
				soakey := source.KeyBytes() // start or after key
				if bytes.Compare(start, soakey) < 0 {
					source.Prev()
				}
			} else {
				source.SeekToLast()
			}
		}
	} else {
		if start == nil {
			source.SeekToFirst()
		} else {
			source.Seek(start)
		}
	}
	return &cRocksDBIterator{
		source:    source,
		start:     start,
		end:       end,
		isReverse: isReverse,
		isInvalid: false,
	}
}

func (itr cRocksDBIterator) Domain() ([]byte, []byte) {
	return itr.start, itr.end
}

func (itr cRocksDBIterator) Valid() bool {

	// Once invalid, forever invalid.
	if itr.isInvalid {
		return false
	}

	// Panic on DB error.  No way to recover.
	itr.assertNoError()

	// If source is invalid, invalid.
	if !itr.source.Valid() {
		itr.isInvalid = true
		return false
	}

	// If key is end or past it, invalid.
	var end = itr.end
	var key = itr.source.KeyBytes()
	if itr.isReverse {
		if end != nil && bytes.Compare(key, end) <= 0 {
			itr.isInvalid = true
			return false
		}
	} else {
		if end != nil && bytes.Compare(end, key) <= 0 {
			itr.isInvalid = true
			return false
		}
	}

	// It's valid.
	return true
}

func (itr cRocksDBIterator) Key() []byte {
	itr.assertNoError()
	itr.assertIsValid()
	return itr.source.Key().Data()
}

func (itr cRocksDBIterator) Value() []byte {
	itr.assertNoError()
	itr.assertIsValid()
	return itr.source.Value().Data()
}

func (itr cRocksDBIterator) Next() {
	itr.assertNoError()
	itr.assertIsValid()
	if itr.isReverse {
		itr.source.Prev()
	} else {
		itr.source.Next()
	}
}

func (itr cRocksDBIterator) Close() {
	itr.source.Close()
}

func (itr cRocksDBIterator) assertNoError() {
	if err := itr.source.Err(); err != nil {
		panic(err)
	}
}

func (itr cRocksDBIterator) assertIsValid() {
	if !itr.Valid() {
		panic("cRocksDBIterator is invalid")
	}
}