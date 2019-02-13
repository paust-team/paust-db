package db_test

import (
	"github.com/paust-team/paust-db/consts"
)

func (suite *DBSuite) TestDBCreateRetrieveInColumnFamily() {
	require := suite.Require()

	var (
		givenKey = []byte("hello")
		givenVal = []byte("world1")
	)

	//create
	err := suite.DB.SetDataInColumnFamily(consts.DefaultCFNum, givenKey, givenVal)
	require.Nil(err, "Default columnfamily Set error : %v", err)

	//retrieve
	value, err := suite.DB.GetDataFromColumnFamily(consts.DefaultCFNum, givenKey)
	defer value.Free()
	require.Nil(err, "Default columnfamily Get error : %v", err)
	suite.Equal(givenVal, value.Data())
}

func (suite *DBSuite) TestColumnFamilyLength() {
	suite.Equal(consts.TotalCFNum, len(suite.DB.ColumnFamilyHandles()), "The number of ColumnFamilies should be 3")
}
