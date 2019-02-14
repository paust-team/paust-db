package db_test

import (
	"github.com/paust-team/paust-db/consts"
)

func (suite *DBSuite) TestColumnFamilyBatchPutGet() {
	require := suite.Require()

	givenKey := []byte("Key")
	givenValue := []byte("Value")

	batch := suite.DB.NewBatch()
	batch.SetColumnFamily(suite.DB.ColumnFamilyHandles()[consts.DefaultCFNum], givenKey, givenValue)
	size, err := batch.Write()
	require.Equal(1, size)
	require.Nil(err, "Batch MetaColumnFamily Write Error : %v", err)

	actualValue, err := suite.DB.GetDataFromColumnFamily(consts.DefaultCFNum, givenKey)
	defer actualValue.Free()
	require.Nil(err, "MetaColumnFamily Get Error : %v", err)
	suite.Equal(givenValue, actualValue.Data())
}
