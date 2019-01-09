package db_test

func (suite *DBSuite) TestColumnFamilyBatchPutGet() {

	givenKey := []byte("Key")
	givenValue := []byte("Value")

	batch := suite.DB.NewBatch()
	batch.SetColumnFamily(suite.DB.ColumnFamilyHandle(0), givenKey, givenValue)
	batchWriteErr := batch.Write()
	suite.Nil(batchWriteErr, "Batch MetaColumnFamily Write Error : %v", batchWriteErr)

	actualValue, err1 := suite.DB.GetDataFromColumnFamily(0, givenKey)
	defer actualValue.Free()
	suite.Nil(err1, "MetaColumnFamily Get Error : %v", err1)
	suite.Equal(givenValue, actualValue.Data())
}
