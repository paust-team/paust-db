package db_test

func (suite *DBSuite) TestDBIteratorDefault() {
	// insert Keys
	givenKeys := [][]byte{[]byte("default1"), []byte("default2"), []byte("default3")}

	for _, k := range givenKeys {
		suite.Nil(suite.DB.SetDataInColumnFamily(0, k, []byte("defaultVal")))
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
		suite.Nil(suite.DB.SetDataInColumnFamily(1, k, []byte("metaVal")))
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
		suite.Nil(suite.DB.SetDataInColumnFamily(2, k, []byte("realVal")))
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
