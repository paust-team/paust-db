package db_test

import (
	"github.com/paust-team/paust-db/consts"
)

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
	err := suite.DB.SetDataInColumnFamily(consts.DefaultCFNum, givenKey, givenVal)
	suite.Nil(err, "Default columnfamily Set error : %v", err)

	//retrieve
	value, err := suite.DB.GetDataFromColumnFamily(consts.DefaultCFNum, givenKey)
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
	err := suite.DB.SetDataInColumnFamily(consts.DefaultCFNum, givenKey, givenVal1)
	suite.Nil(err, "Default columnfamily Set error : %v", err)

	//update
	suite.Nil(suite.DB.SetDataInColumnFamily(consts.DefaultCFNum, givenKey, givenVal2))
	value, err := suite.DB.GetDataFromColumnFamily(consts.DefaultCFNum, givenKey)
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
	err := suite.DB.SetDataInColumnFamily(consts.DefaultCFNum, givenKey, givenVal)
	suite.Nil(err, "Default columnfamily Set error : %v", err)

	//delete
	err = suite.DB.DeleteInColumnFamily(consts.DefaultCFNum, givenKey)
	suite.Nil(err, "Default columnfamily Delete error : %v", err)

	value, err := suite.DB.GetDataFromColumnFamily(consts.DefaultCFNum, givenKey)
	defer value.Free()
	suite.Nil(err, "Default columnfamily Get error : %v", err)
	suite.Nil(value.Data())
}

func (suite *DBSuite) TestColumnFamilyLength() {
	suite.Equal(consts.TotalCFNum, len(suite.DB.ColumnFamilyHandles()), "The number of ColumnFamilies should be 3")
}
