package db

import (
	"github.com/stretchr/testify/suite"
	"github.com/tecbot/gorocksdb"
	"os"
	"testing"
)

const (
	dbName = "paustdbtest"
	dir    = "/tmp/paustdbtest"
	perm   = 0777
)

type DBSuite struct {
	suite.Suite
	DB	*CRocksDB
}

func (suite *DBSuite) SetupTest() {
	var err error
	SetDir()
	suite.DB, err = NewCRocksDB(dbName, dir)

	suite.NotNil(suite.DB, "db open error %v", err)
	suite.Nil(err, "db open error %v", err)

}

func (suite *DBSuite) TearDownTest() {
	suite.DB.Close()
	os.RemoveAll(dir)
}

func (suite *DBSuite) TestDBCreateRetrieve() {

	var (
		givenKey  = []byte("hello")
		givenVal = []byte("world1")
		wo        = gorocksdb.NewDefaultWriteOptions()
		ro        = gorocksdb.NewDefaultReadOptions()
	)
	defer wo.Destroy()
	defer ro.Destroy()

	//create
	err := suite.DB.SetInColumnFamily(wo, suite.DB.ColumnFamilyHandle(0), givenKey, givenVal)
	suite.Nil(err, "Default columnfamily Set error : %v", err)

	//retrieve
	value1, err1 := suite.DB.GetInColumnFamily(ro, suite.DB.ColumnFamilyHandle(0), givenKey)
	defer value1.Free()
	suite.Nil(err1, "Default columnfamily Get error : %v", err1)
	suite.Equal(givenVal, value1.Data())
}

func (suite *DBSuite) TestDBUpdate() {
	var (
		givenKey  = []byte("hello")
		givenVal1 = []byte("world1")
		givenVal2 = []byte("world2")
		wo        = gorocksdb.NewDefaultWriteOptions()
		ro        = gorocksdb.NewDefaultReadOptions()
	)
	defer wo.Destroy()
	defer ro.Destroy()

	//create
	err := suite.DB.SetInColumnFamily(wo, suite.DB.ColumnFamilyHandle(0), givenKey, givenVal1)
	suite.Nil(err, "Default columnfamily Set error : %v", err)

	//update
	suite.Nil(suite.DB.SetInColumnFamily(wo, suite.DB.ColumnFamilyHandle(0), givenKey, givenVal2))
	value2, err2 := suite.DB.GetInColumnFamily(ro, suite.DB.ColumnFamilyHandle(0), givenKey)
	defer value2.Free()
	suite.Nil(err2, "Default columnfamily Update error : %v", err2)
	suite.Equal(givenVal2, value2.Data())

}

func (suite *DBSuite) TestDBDelete() {
	var (
		givenKey  = []byte("hello")
		givenVal = []byte("world1")
		wo        = gorocksdb.NewDefaultWriteOptions()
		ro        = gorocksdb.NewDefaultReadOptions()
	)
	defer wo.Destroy()
	defer ro.Destroy()

	//create
	err := suite.DB.SetInColumnFamily(wo, suite.DB.ColumnFamilyHandle(0), givenKey, givenVal)
	suite.Nil(err, "Default columnfamily Set error : %v", err)

	//delete
	err3 := suite.DB.DeleteInColumnFamily(wo, suite.DB.ColumnFamilyHandle(0), givenKey)
	suite.Nil(err3, "Default columnfamily Delete error : %v", err3)

	value3, err4 := suite.DB.GetInColumnFamily(ro, suite.DB.ColumnFamilyHandle(0), givenKey)
	defer value3.Free()
	suite.Nil(err4, "Default columnfamily Get error : %v", err4)
	suite.Nil(value3.Data())
}

func (suite *DBSuite) TestColumnFamilyLength() {
	suite.Equal(3, len(suite.DB.columnFamilyHandles), "The number of ColumnFamilys should be 3")
}

func (suite *DBSuite) TestColumnFamilyBatchPutGet() {
	wo := gorocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()

	givenKey := []byte("Key")
	givenValue := []byte("Value")

	batch := suite.DB.NewBatch()
	batch.SetColumnFamily(suite.DB.ColumnFamilyHandle(0), givenKey, givenValue)
	batchWriteErr := batch.Write()
	suite.Nil(batchWriteErr, "Batch MetaColumnFamily Write Error : %v", batchWriteErr)

	ro := gorocksdb.NewDefaultReadOptions()
	defer ro.Destroy()

	actualValue, err1 := suite.DB.GetInColumnFamily(ro, suite.DB.ColumnFamilyHandle(0), givenKey)
	defer actualValue.Free()
	suite.Nil(err1, "MetaColumnFamily Get Error : %v", err1)
	suite.Equal(givenValue, actualValue.Data())
}

func (suite *DBSuite) TestDBIteratorDefault() {
	// insert Keys
	givenKeys1 := [][]byte{[]byte("default1"), []byte("default2"), []byte("default3")}

	wo := gorocksdb.NewDefaultWriteOptions()
	for _, k := range givenKeys1 {
		suite.Nil(suite.DB.SetInColumnFamily(wo, suite.DB.ColumnFamilyHandle(0), k, []byte("defaultVal")))
	}

	iter1 := suite.DB.IteratorColumnFamily(nil, nil, suite.DB.ColumnFamilyHandle(0))
	defer iter1.Close()

	var actualKeys1 [][]byte

	for iter1.SeekToFirst(); iter1.Valid(); iter1.Next() {
		key := make([]byte, 8)
		copy(key, iter1.Key())
		actualKeys1 = append(actualKeys1, key)
	}
	suite.Equal(givenKeys1, actualKeys1)
}

func (suite *DBSuite) TestDBIteratorMetaColumnFamily() {
	// insert Keys
	givenKeys := [][]byte{[]byte("meta1"), []byte("meta2"), []byte("meta3")}

	wo := gorocksdb.NewDefaultWriteOptions()
	for _, k := range givenKeys {
		suite.Nil(suite.DB.SetInColumnFamily(wo, suite.DB.ColumnFamilyHandle(1), k, []byte("metaVal")))
	}

	iter := suite.DB.IteratorColumnFamily(nil, nil, suite.DB.ColumnFamilyHandle(1))
	defer iter.Close()

	var actualKeys [][]byte

	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		key := make([]byte, 5)
		copy(key, iter.Key())
		actualKeys = append(actualKeys, key)
	}

	suite.Equal(givenKeys, actualKeys)

}

func (suite *DBSuite) TestDBIteratorRealColumnFamily() {
	// insert Keys
	givenKeys := [][]byte{[]byte("real1"), []byte("real2"), []byte("real3")}

	wo := gorocksdb.NewDefaultWriteOptions()
	for _, k := range givenKeys {
		suite.Nil(suite.DB.SetInColumnFamily(wo, suite.DB.ColumnFamilyHandle(2), k, []byte("realVal")))
	}

	iter := suite.DB.IteratorColumnFamily(nil, nil, suite.DB.ColumnFamilyHandle(2))
	defer iter.Close()

	var actualKeys [][]byte

	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		key := make([]byte, 5)
		copy(key, iter.Key())
		actualKeys = append(actualKeys, key)
	}
	suite.Equal(givenKeys, actualKeys)
}

func (suite *DBSuite) TestPrint() {
	// insert Keys
	key := []byte{0x15, 0x74, 0x6f, 0x3d, 0x98, 0x65, 0x1f, 0x98, 0xa2, 0x29, 0x9d, 0xf1, 0x97, 0x73, 0x81,
		0x41, 0xf3, 0x17, 0xd0, 0x8f, 0xa, 0x12, 0x54, 0xf2, 0x6, 0xfc, 0xf5, 0x56, 0x8c, 0x62, 0xf, 0xb5, 0x4a, 0x95,
		0xfa, 0x59, 0x3f, 0x27, 0x40, 0x71, 0x74, 0x65, 0x73, 0x74, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0,
		0x0, 0x0, 0x0, 0x0, 0x0, 0x0}
	value := []byte("test1")
	wo := gorocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()

	suite.DB.SetInColumnFamily(wo, suite.DB.ColumnFamilyHandle(0), key, value)
	suite.DB.SetInColumnFamily(wo, suite.DB.ColumnFamilyHandle(1), key, value)
	suite.DB.SetInColumnFamily(wo, suite.DB.ColumnFamilyHandle(2), key, value)

	suite.DB.Print()
}

func TestSuite(t *testing.T) {
	suite.Run(t, new(DBSuite))
}

func SetDir() {
	os.RemoveAll(dir)
	os.Mkdir(dir, perm)
}