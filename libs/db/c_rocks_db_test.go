package db

import (
	"github.com/stretchr/testify/assert"
	"github.com/tecbot/gorocksdb"
	"os"
	"testing"
)

const (
	dbName = "paustdbtest"
	dir    = "/Users/Andrew/dbtest"
)

func TestNewCRocksDB(t *testing.T) {
	dirSetting()
	db, err := NewCRocksDB(dbName, dir)
	defer db.Close()
	if db == nil || err != nil {
		t.Errorf("NewCRocksDB() error =%v", err)
	}
	os.RemoveAll(dir)
}

func TestDBCRUD(t *testing.T) {
	dirSetting()
	db, err := NewCRocksDB(dbName, dir)
	defer db.Close()
	if db == nil || err != nil {
		t.Errorf("NewCRocksDB() error =%v", err)
	}

	var (
		givenKey  = []byte("hello")
		givenVal1 = []byte("world1")
		givenVal2 = []byte("world2")
		wo        = gorocksdb.NewDefaultWriteOptions()
		ro        = gorocksdb.NewDefaultReadOptions()
	)

	//create
	assert.Nil(t, db.SetInColumnFamily(wo, db.ColumnFamilyHandle(0), givenKey, givenVal1))

	//retrieve
	value1, err := db.GetInColumnFamily(ro, db.ColumnFamilyHandle(0), givenKey)
	defer value1.Free()
	assert.Nil(t, err)
	assert.Equal(t, givenVal1, value1.Data())

	//update
	assert.Nil(t, db.SetInColumnFamily(wo, db.ColumnFamilyHandle(0), givenKey, givenVal2))
	value2, err := db.GetInColumnFamily(ro, db.ColumnFamilyHandle(0), givenKey)
	defer value2.Free()
	assert.Nil(t, err)
	assert.Equal(t, givenVal2, value2.Data())

	//delete
	assert.Nil(t, db.DeleteInColumnFamily(wo, db.ColumnFamilyHandle(0), givenKey))
	value3, err := db.GetInColumnFamily(ro, db.ColumnFamilyHandle(0), givenKey)

	assert.Nil(t, err)
	assert.Nil(t, value3.Data())

	os.RemoveAll(dir)
}

func TestColumnFamilyBatchPutGet(t *testing.T) {
	dirSetting()
	db, err := NewCRocksDB(dbName, dir)
	defer db.Close()
	if db == nil || err != nil {
		t.Errorf("NewCRocksDB() error =%v", err)
	}

	assert.Equal(t, 3, len(db.columnFamilyHandles), "The number of ColumnFamilyHandles should be 3")
	defer db.columnFamilyHandles[0].Destroy()
	defer db.columnFamilyHandles[1].Destroy()
	defer db.columnFamilyHandles[2].Destroy()

	wo := gorocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()
	ro := gorocksdb.NewDefaultReadOptions()
	defer ro.Destroy()

	metaGivenKey := []byte("metaKey")
	metaGivenValue := []byte("metaValue")
	realGivenKey := []byte("realKey")
	realGivenValue := []byte("realValue")

	metaBatch := db.NewBatch()
	metaBatch.SetColumnFamily(db.columnFamilyHandles[1], metaGivenKey, metaGivenValue)
	assert.Nil(t, metaBatch.Write())

	metaActualValue, err := db.GetInColumnFamily(ro, db.ColumnFamilyHandle(1), metaGivenKey)
	defer metaActualValue.Free()
	assert.Nil(t, err)
	assert.Equal(t, metaGivenValue, metaActualValue.Data())

	realBatch := db.NewBatch()
	realBatch.SetColumnFamily(db.columnFamilyHandles[2], realGivenKey, realGivenValue)
	assert.Nil(t, realBatch.Write())

	realActualValue, err := db.GetInColumnFamily(ro, db.ColumnFamilyHandle(2), realGivenKey)
	defer realActualValue.Free()
	assert.Nil(t, err)
	assert.Equal(t, realGivenValue, realActualValue.Data())

	os.RemoveAll(dir)
}

func TestPrint(t *testing.T) {
	dirSetting()
	db, err := NewCRocksDB(dbName, dir)
	defer db.Close()
	if db == nil || err != nil {
		t.Errorf("NewCRocksDB() error =%v", err)
	}

	db.Print()

	os.RemoveAll(dir)
}

func TestDBIterator(t *testing.T) {
	dirSetting()
	db, err := NewCRocksDB(dbName, dir)
	defer db.Close()
	if db == nil || err != nil {
		t.Errorf("NewCRocksDB() error =%v", err)
	}

	// insert Keys
	givenKeys1 := [][]byte{[]byte("default1"), []byte("default2"), []byte("default3")}
	givenKeys2 := [][]byte{[]byte("meta1"), []byte("meta2"), []byte("meta3")}
	givenKeys3 := [][]byte{[]byte("real1"), []byte("real2"), []byte("real3")}

	wo := gorocksdb.NewDefaultWriteOptions()
	for _, k := range givenKeys1 {
		assert.Nil(t, db.SetInColumnFamily(wo, db.ColumnFamilyHandle(0), k, []byte("defaultVal")))
	}
	for _, k := range givenKeys2 {
		assert.Nil(t, db.SetInColumnFamily(wo, db.ColumnFamilyHandle(1), k, []byte("metaVal")))
	}
	for _, k := range givenKeys3 {
		assert.Nil(t, db.SetInColumnFamily(wo, db.ColumnFamilyHandle(2), k, []byte("realVal")))
	}

	iter1 := db.IteratorColumnFamily(nil, nil, db.ColumnFamilyHandle(0))
	defer iter1.Close()
	iter2 := db.IteratorColumnFamily(nil, nil, db.ColumnFamilyHandle(1))
	defer iter2.Close()
	iter3 := db.IteratorColumnFamily(nil, nil, db.ColumnFamilyHandle(2))
	defer iter3.Close()

	var actualKeys1 [][]byte
	var actualKeys2 [][]byte
	var actualKeys3 [][]byte
	for iter1.SeekToFirst(); iter1.Valid(); iter1.Next() {
		key := make([]byte, 8)
		copy(key, iter1.Key())
		actualKeys1 = append(actualKeys1, key)
	}
	assert.Equal(t, givenKeys1, actualKeys1)

	for iter2.SeekToFirst(); iter2.Valid(); iter2.Next() {
		key := make([]byte, 5)
		copy(key, iter2.Key())
		actualKeys2 = append(actualKeys2, key)
	}
	assert.Equal(t, givenKeys2, actualKeys2)

	for iter3.SeekToFirst(); iter3.Valid(); iter3.Next() {
		key := make([]byte, 5)
		copy(key, iter3.Key())
		actualKeys3 = append(actualKeys3, key)
	}
	assert.Equal(t, givenKeys3, actualKeys3)

	os.RemoveAll(dir)
}

func dirSetting() {
	os.RemoveAll(dir)
	os.Mkdir(dir, 0777)
}
