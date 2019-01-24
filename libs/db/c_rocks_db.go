package db

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/tecbot/gorocksdb"
	"path/filepath"
)

var _ DB = (*CRocksDB)(nil)

type CRocksDB struct {
	db                  *gorocksdb.DB
	ro                  *gorocksdb.ReadOptions
	wo                  *gorocksdb.WriteOptions
	woSync              *gorocksdb.WriteOptions
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
	woSync := gorocksdb.NewDefaultWriteOptions()
	woSync.SetSync(true)

	database := &CRocksDB{
		db:                  db,
		ro:                  ro,
		wo:                  wo,
		woSync:              woSync,
		columnFamilyHandles: columnFamilyHandles,
	}
	return database, nil
}

// Implements DB.
func (db CRocksDB) GetDataFromColumnFamily(index int, key []byte) (*gorocksdb.Slice, error) {
	return db.db.GetCF(db.ro, db.ColumnFamilyHandle(index), key)
}

// Implements DB.
func (db *CRocksDB) SetDataInColumnFamily(index int, key, value []byte) error {
	return db.db.PutCF(db.wo, db.ColumnFamilyHandle(index), key, value)
}

// Implements DB.
func (db CRocksDB) IteratorColumnFamily(start, end []byte, cf *gorocksdb.ColumnFamilyHandle) Iterator {
	itr := db.db.NewIteratorCF(db.ro, cf)
	return newCRocksDBIterator(itr, start, end, false)
}

// Implements DB.
func (db CRocksDB) NewBatch() Batch {
	batch := gorocksdb.NewWriteBatch()
	return &cRocksDBBatch{&db, batch}
}

// Implements DB.
func (db *CRocksDB) DB() *gorocksdb.DB {
	return db.db
}

// Implements DB.
func (db *CRocksDB) WriteOption() *gorocksdb.WriteOptions {
	return db.wo
}

// Implements DB.
func (db *CRocksDB) ReadOption() *gorocksdb.ReadOptions {
	return db.ro
}

// Implements DB.
func (db CRocksDB) ColumnFamilyHandle(i int) *gorocksdb.ColumnFamilyHandle {
	return db.columnFamilyHandles[i]
}

/*
	Below DB methods are for test
*/

// Implements DB.
func (db CRocksDB) Get(key []byte) []byte {
	key = nonNilBytes(key)
	res, err := db.db.GetBytes(db.ro, key)
	if err != nil {
		panic(err)
	}
	return res
}

// Implements DB.
func (db CRocksDB) Has(key []byte) bool {
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

// Implements DB.
func (db *CRocksDB) DeleteInColumnFamily(index int, key []byte) error {
	return db.db.DeleteCF(db.wo, db.ColumnFamilyHandle(index), key)
}

// Implements DB.
func (db CRocksDB) Iterator(start, end []byte) Iterator {
	itr := db.db.NewIterator(db.ro)
	return newCRocksDBIterator(itr, start, end, false)
}

// Implements DB.
func (db CRocksDB) ReverseIterator(start, end []byte) Iterator {
	itr := db.db.NewIterator(db.ro)
	return newCRocksDBIterator(itr, start, end, true)
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
	var metaValueObj struct {
		OwnerKey  []byte `json:"ownerKey"`
		Qualifier []byte `json:"qualifier"`
	}

	defaultItr := db.IteratorColumnFamily(nil, nil, db.ColumnFamilyHandle(0))
	defer defaultItr.Close()

	metaItr := db.IteratorColumnFamily(nil, nil, db.ColumnFamilyHandle(1))
	defer metaItr.Close()
	realItr := db.IteratorColumnFamily(nil, nil, db.ColumnFamilyHandle(2))
	defer realItr.Close()

	fmt.Println("--------------Default Column Family--------------")
	for defaultItr.SeekToFirst(); defaultItr.Valid(); defaultItr.Next() {
		fmt.Println("key : ", defaultItr.Key())
		fmt.Println("value : ", defaultItr.Value())
	}

	fmt.Println("--------------Metadata Column Family--------------")
	for metaItr.SeekToFirst(); metaItr.Valid(); metaItr.Next() {
		json.Unmarshal(metaItr.Value(), &metaValueObj)
		fmt.Println("key : ", metaItr.Key())
		fmt.Println("value : ", metaValueObj)
	}

	fmt.Println("--------------Realdata Column Family--------------")

	for realItr.SeekToFirst(); realItr.Valid(); realItr.Next() {
		fmt.Println("key : ", realItr.Key())
		fmt.Println("value: ", realItr.Value())
	}

}

// Implements DB.
func (db CRocksDB) Stats() map[string]string {
	keys := []string{}

	stats := make(map[string]string)
	for _, key := range keys {
		str := db.db.GetProperty(key)
		stats[key] = str
	}
	return stats
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
func (mBatch *cRocksDBBatch) DeleteColumnFamily(cf *gorocksdb.ColumnFamilyHandle, key []byte) {
	mBatch.batch.DeleteCF(cf, key)
}

// Implements Batch.
func (mBatch *cRocksDBBatch) Write() error {
	if err := mBatch.db.db.Write(mBatch.db.wo, mBatch.batch); err != nil {
		return err
	}
	return nil
}

/*
	Below Batch methods are for test
*/

// Implements Batch.
func (mBatch *cRocksDBBatch) WriteSync() error {
	if err := mBatch.db.db.Write(mBatch.db.woSync, mBatch.batch); err != nil {
		return err
	}
	return nil
}

// Implements Batch.
func (mBatch *cRocksDBBatch) Set(key, value []byte) {
	mBatch.batch.Put(key, value)
}

// Implements Batch.
func (mBatch *cRocksDBBatch) Delete(key []byte) {
	mBatch.batch.Delete(key)
}

//----------------------------------------
// Iterator
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
				soakey := source.Key().Data() // start or after key
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
	var key = itr.source.Key().Data()
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

func (itr cRocksDBIterator) Next() {
	itr.assertNoError()
	itr.assertIsValid()
	if itr.isReverse {
		itr.source.Prev()
	} else {
		itr.source.Next()
	}
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

func (itr cRocksDBIterator) SeekToFirst() {
	itr.source.SeekToFirst()
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
