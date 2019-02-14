package db

import (
	"bytes"
	"fmt"
	"github.com/tecbot/gorocksdb"
	"path/filepath"
)

var _ DB = (*CRocksDB)(nil)

type CRocksDB struct {
	db                  *gorocksdb.DB
	ro                  *gorocksdb.ReadOptions
	wo                  *gorocksdb.WriteOptions
	columnFamilyHandles gorocksdb.ColumnFamilyHandles
}

func NewCRocksDB(name, dir string) (*CRocksDB, error) {
	dbPath := filepath.Join(dir, name+".db")
	columnFamilyNames := []string{"default", "metadata", "realdata"}

	bbto := gorocksdb.NewDefaultBlockBasedTableOptions()
	bbto.SetBlockCache(gorocksdb.NewLRUCache(1 << 30))
	defaultOpts := gorocksdb.NewDefaultOptions()
	defaultOpts.SetBlockBasedTableFactory(bbto)
	defaultOpts.SetCreateIfMissing(true)
	defaultOpts.SetCreateIfMissingColumnFamilies(true)

	opts := gorocksdb.NewDefaultOptions()
	db, columnFamilyHandles, err := gorocksdb.OpenDbColumnFamilies(defaultOpts, dbPath, columnFamilyNames, []*gorocksdb.Options{opts, opts, opts})

	if err != nil {
		fmt.Println("DB open error", err)
		return nil, err
	}

	ro := gorocksdb.NewDefaultReadOptions()
	wo := gorocksdb.NewDefaultWriteOptions()

	database := &CRocksDB{
		db:                  db,
		ro:                  ro,
		wo:                  wo,
		columnFamilyHandles: columnFamilyHandles,
	}
	return database, nil
}

// Implements DB.
func (db CRocksDB) GetDataFromColumnFamily(index int, key []byte) (*gorocksdb.Slice, error) {
	return db.db.GetCF(db.ro, db.ColumnFamilyHandles()[index], key)
}

// Implements DB.
func (db *CRocksDB) SetDataInColumnFamily(index int, key, value []byte) error {
	return db.db.PutCF(db.wo, db.ColumnFamilyHandles()[index], key, value)
}

// Implements DB.
func (db CRocksDB) IteratorColumnFamily(start, end []byte, cf *gorocksdb.ColumnFamilyHandle) Iterator {
	itr := db.db.NewIteratorCF(db.ro, cf)
	return newCRocksDBIterator(itr, start, end)
}

// Implements DB.
func (db CRocksDB) NewBatch() Batch {
	batch := gorocksdb.NewWriteBatch()
	return &cRocksDBBatch{&db, batch}
}

/*
	Below DB methods are for test
*/

// Implements DB.
func (db CRocksDB) Iterator(start, end []byte) Iterator {
	itr := db.db.NewIterator(db.ro)
	return newCRocksDBIterator(itr, start, end)
}

// Implements DB.
func (db *CRocksDB) Close() {
	db.db.Close()
	db.ro.Destroy()
	db.wo.Destroy()
}

// Implements DB.
func (db CRocksDB) ColumnFamilyHandles() gorocksdb.ColumnFamilyHandles {
	return db.columnFamilyHandles
}

//----------------------------------------
// Batch
var _ Batch = (*cRocksDBBatch)(nil)

type cRocksDBBatch struct {
	db    *CRocksDB
	batch *gorocksdb.WriteBatch
}

// Implements Batch.
func (mBatch *cRocksDBBatch) SetColumnFamily(cf *gorocksdb.ColumnFamilyHandle, key, value []byte) {
	mBatch.batch.PutCF(cf, key, value)
}

// Implements Batch.
func (mBatch *cRocksDBBatch) Write() (int, error) {
	if err := mBatch.db.db.Write(mBatch.db.wo, mBatch.batch); err != nil {
		return 0, err
	}
	return mBatch.batch.Count(), nil
}

//----------------------------------------
// Iterator
var _ Iterator = (*cRocksDBIterator)(nil)

type cRocksDBIterator struct {
	source     *gorocksdb.Iterator
	start, end []byte
	isInvalid  bool
}

func newCRocksDBIterator(source *gorocksdb.Iterator, start, end []byte) *cRocksDBIterator {
	if start == nil {
		source.SeekToFirst()
	} else {
		source.Seek(start)
	}

	return &cRocksDBIterator{
		source:    source,
		start:     start,
		end:       end,
		isInvalid: false,
	}
}

func (itr cRocksDBIterator) Valid() bool {

	// Once invalid, forever invalid.
	if itr.isInvalid {
		return false
	}

	// Panic on DB error.  No way to recover.
	itr.assertNoError()

	if !itr.source.Valid() {
		itr.isInvalid = true
		return false
	}

	end := itr.end
	key := itr.source.Key().Data()

	if end != nil && bytes.Compare(end, key) <= 0 {
		itr.isInvalid = true
		return false
	}

	return true
}

func (itr cRocksDBIterator) Next() {
	itr.assertNoError()
	itr.assertIsValid()
	itr.source.Next()
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

func (itr cRocksDBIterator) Close() {
	itr.source.Close()
}

func (itr cRocksDBIterator) Seek(key []byte) {
	itr.source.Seek(key)
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
