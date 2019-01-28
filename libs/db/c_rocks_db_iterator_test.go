package db_test

import (
	"github.com/paust-team/paust-db/consts"
)

func (suite *DBSuite) TestDBIteratorDefault() {
	// insert Keys
	givenKeys := [][]byte{[]byte("default1"), []byte("default2"), []byte("default3")}

	for _, k := range givenKeys {
		suite.Nil(suite.DB.SetDataInColumnFamily(consts.DefaultCFNum, k, []byte("defaultVal")))
	}

	itr := suite.DB.IteratorColumnFamily(nil, nil, suite.DB.ColumnFamilyHandle(consts.DefaultCFNum))
	defer itr.Close()

	var actualKeys [][]byte

	for itr.SeekToFirst(); itr.Valid(); itr.Next() {
		key := make([]byte, 8)
		copy(key, itr.Key())
		actualKeys = append(actualKeys, key)
	}
	suite.Equal(givenKeys, actualKeys)
}

func (suite *DBSuite) TestDBIteratorMetaColumnFamily() {
	// insert Keys
	givenKeys := [][]byte{[]byte("meta1"), []byte("meta2"), []byte("meta3")}

	for _, k := range givenKeys {
		suite.Nil(suite.DB.SetDataInColumnFamily(consts.MetaCFNum, k, []byte("metaVal")))
	}

	itr := suite.DB.IteratorColumnFamily(nil, nil, suite.DB.ColumnFamilyHandle(consts.MetaCFNum))
	defer itr.Close()

	var actualKeys [][]byte

	for itr.SeekToFirst(); itr.Valid(); itr.Next() {
		key := make([]byte, 5)
		copy(key, itr.Key())
		actualKeys = append(actualKeys, key)
	}

	suite.Equal(givenKeys, actualKeys)

}

func (suite *DBSuite) TestDBIteratorRealColumnFamily() {
	// insert Keys
	givenKeys := [][]byte{[]byte("real1"), []byte("real2"), []byte("real3")}

	for _, k := range givenKeys {
		suite.Nil(suite.DB.SetDataInColumnFamily(consts.RealCFNum, k, []byte("realVal")))
	}

	itr := suite.DB.IteratorColumnFamily(nil, nil, suite.DB.ColumnFamilyHandle(consts.RealCFNum))
	defer itr.Close()

	var actualKeys [][]byte

	for itr.SeekToFirst(); itr.Valid(); itr.Next() {
		key := make([]byte, 5)
		copy(key, itr.Key())
		actualKeys = append(actualKeys, key)
	}
	suite.Equal(givenKeys, actualKeys)
}
