package db

import (
	"github.com/stretchr/testify/suite"
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
	DB *CRocksDB
}

func (suite *DBSuite) SetupTest() {
	var err error
	SetDir()
	suite.DB, err = NewCRocksDB(dbName, dir)

	suite.Require().NotNil(suite.DB, "db open error %v", err)
	suite.Require().Nil(err, "db open error %v", err)

}

func (suite *DBSuite) TearDownTest() {
	suite.DB.Close()
	os.RemoveAll(dir)
}

func (suite *DBSuite) TestDBDefaultCreateRetrieve() {
	var (
		givenKey = []byte("hello")
		givenVal = []byte("world1")
	)

	//create
	suite.DB.Set(givenKey, givenVal)

	//retrieve
	value := suite.DB.Get(givenKey)
	suite.Equal(givenVal, value)
}

func (suite *DBSuite) TestDBDefaultUpdate() {
	var (
		givenKey  = []byte("hello")
		givenVal1 = []byte("world1")
		givenVal2 = []byte("world2")
	)

	//create
	suite.DB.Set(givenKey, givenVal1)

	//update
	suite.DB.Set(givenKey, givenVal2)
	value := suite.DB.Get(givenKey)
	suite.Equal(givenVal2, value)
}

func (suite *DBSuite) TestDBDefaultDelete() {
	var (
		givenKey = []byte("hello")
		givenVal = []byte("world1")
	)

	//create
	suite.DB.Set(givenKey, givenVal)

	//delete
	suite.DB.Delete(givenKey)
	value := suite.DB.Get(givenKey)
	suite.Nil(value)
}

func (suite *DBSuite) TestDBHasMethod() {
	var (
		givenKey = []byte("test")
		givenVal = []byte("val1")
	)

	suite.False(suite.DB.Has(givenKey), "DB should not have givenKey and value pair before Set")

	//create
	suite.DB.Set(givenKey, givenVal)

	suite.True(suite.DB.Has(givenKey), "DB should have givenKey and value pair after Set")
}

func (suite *DBSuite) TestDBCreateRetrieveInColumnFamily() {

	var (
		givenKey = []byte("hello")
		givenVal = []byte("world1")
	)

	//create
	err := suite.DB.SetInColumnFamily(0, givenKey, givenVal)
	suite.Nil(err, "Default columnfamily Set error : %v", err)

	//retrieve
	value, err := suite.DB.GetInColumnFamily(0, givenKey)
	defer value.Free()
	suite.Nil(err, "Default columnfamily Get error : %v", err)
	suite.Equal(givenVal, value.Data())
}

func (suite *DBSuite) TestDBUpdateInColumnFamily() {
	var (
		givenKey  = []byte("hello")
		givenVal1 = []byte("world1")
		givenVal2 = []byte("world2")
	)

	//create
	err := suite.DB.SetInColumnFamily(0, givenKey, givenVal1)
	suite.Nil(err, "Default columnfamily Set error : %v", err)

	//update
	suite.Nil(suite.DB.SetInColumnFamily(0, givenKey, givenVal2))
	value, err := suite.DB.GetInColumnFamily(0, givenKey)
	defer value.Free()
	suite.Nil(err, "Default columnfamily Update error : %v", err)
	suite.Equal(givenVal2, value.Data())

}

func (suite *DBSuite) TestDBDeleteInColumnFamily() {
	var (
		givenKey = []byte("hello")
		givenVal = []byte("world1")
	)

	//create
	err := suite.DB.SetInColumnFamily(0, givenKey, givenVal)
	suite.Nil(err, "Default columnfamily Set error : %v", err)

	//delete
	err = suite.DB.DeleteInColumnFamily(0, givenKey)
	suite.Nil(err, "Default columnfamily Delete error : %v", err)

	value, err := suite.DB.GetInColumnFamily(0, givenKey)
	defer value.Free()
	suite.Nil(err, "Default columnfamily Get error : %v", err)
	suite.Nil(value.Data())
}

func (suite *DBSuite) TestColumnFamilyLength() {
	suite.Equal(3, len(suite.DB.columnFamilyHandles), "The number of ColumnFamilys should be 3")
}

func (suite *DBSuite) TestColumnFamilyBatchPutGet() {

	givenKey := []byte("Key")
	givenValue := []byte("Value")

	batch := suite.DB.NewBatch()
	batch.SetColumnFamily(suite.DB.ColumnFamilyHandle(0), givenKey, givenValue)
	batchWriteErr := batch.Write()
	suite.Nil(batchWriteErr, "Batch MetaColumnFamily Write Error : %v", batchWriteErr)

	actualValue, err1 := suite.DB.GetInColumnFamily(0, givenKey)
	defer actualValue.Free()
	suite.Nil(err1, "MetaColumnFamily Get Error : %v", err1)
	suite.Equal(givenValue, actualValue.Data())
}

func (suite *DBSuite) TestDBIteratorDefault() {
	// insert Keys
	givenKeys := [][]byte{[]byte("default1"), []byte("default2"), []byte("default3")}

	for _, k := range givenKeys {
		suite.Nil(suite.DB.SetInColumnFamily(0, k, []byte("defaultVal")))
	}

	iter := suite.DB.IteratorColumnFamily(nil, nil, suite.DB.ColumnFamilyHandle(0))
	defer iter.Close()

	var actualKeys [][]byte

	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		key := make([]byte, 8)
		copy(key, iter.Key())
		actualKeys = append(actualKeys, key)
	}
	suite.Equal(givenKeys, actualKeys)
}

func (suite *DBSuite) TestDBIteratorMetaColumnFamily() {
	// insert Keys
	givenKeys := [][]byte{[]byte("meta1"), []byte("meta2"), []byte("meta3")}

	for _, k := range givenKeys {
		suite.Nil(suite.DB.SetInColumnFamily(1, k, []byte("metaVal")))
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

	for _, k := range givenKeys {
		suite.Nil(suite.DB.SetInColumnFamily(2, k, []byte("realVal")))
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

func TestSuite(t *testing.T) {
	suite.Run(t, new(DBSuite))
}

func SetDir() {
	os.RemoveAll(dir)
	os.Mkdir(dir, perm)
}
