package master_test

import (
	"encoding/binary"
	"encoding/json"
	"github.com/paust-team/paust-db/consts"
	"github.com/paust-team/paust-db/types"
	"github.com/stretchr/testify/require"
	"github.com/tendermint/tendermint/abci/example/code"
	abciTypes "github.com/tendermint/tendermint/abci/types"
)

func (suite *MasterSuite) TestMasterApplication_Info() {
	//given
	givenReq := abciTypes.RequestInfo{}

	//when
	actualRes := suite.app.Info(givenReq)

	//then
	expectRes := abciTypes.ResponseInfo{Data: "---- Info"}
	suite.Equal(actualRes, expectRes)
}

func (suite *MasterSuite) TestMasterApplication_CheckTx() {
	require := suite.Require()

	/*
		RightCase
	*/
	givenTx, err := json.Marshal(givenBaseDataObjs)
	require.Nil(err)

	//when
	actualRes := suite.app.CheckTx(givenTx)

	//then
	expectRes := abciTypes.ResponseCheckTx{Code: code.CodeTypeOK, Log: ""}
	suite.Equal(expectRes, actualRes)

	/*
		WrongCase
	*/

	//given
	givenWrongTx := []byte("wrongtx")

	//when
	actualRes2 := suite.app.CheckTx(givenWrongTx)

	//then
	suite.Equal(code.CodeTypeEncodingError, actualRes2.Code)
	suite.NotEqual("", actualRes2.Log)

}

func (suite *MasterSuite) TestMasterApplication_InitChain() {
	require := suite.Require()

	//given
	givenReq := abciTypes.RequestInitChain{}

	//when
	actualRes := suite.app.InitChain(givenReq)

	//then
	expectRes := abciTypes.ResponseInitChain{}
	require.Equal(expectRes, actualRes)
}

func (suite *MasterSuite) TestMasterApplication_BeginBlock() {
	//given
	givenReq := abciTypes.RequestBeginBlock{}

	//when
	actualRes := suite.app.BeginBlock(givenReq)

	//then
	expectRes := abciTypes.ResponseBeginBlock{}
	suite.Equal(expectRes, actualRes)
}

func (suite *MasterSuite) TestMasterApplication_DeliverTx() {
	require := require.New(suite.T())

	//given
	suite.TestMasterApplication_InitChain()
	givenTx, err := json.Marshal(givenBaseDataObjs)
	require.Nil(err)

	//when
	actualRes := suite.app.DeliverTx(givenTx)

	//then
	suite.Equal(code.CodeTypeOK, actualRes.Code)

}

func (suite *MasterSuite) TestMasterApplication_EndBlock() {
	//given
	givenReq := abciTypes.RequestEndBlock{}

	//when
	actualRes := suite.app.EndBlock(givenReq)

	//then
	expectRes := abciTypes.ResponseEndBlock{}
	suite.Equal(expectRes, actualRes)
}

func (suite *MasterSuite) TestMasterApplication_Commit() {
	//given
	suite.TestMasterApplication_DeliverTx()

	//when
	actualRes := suite.app.Commit()

	//then
	//TODO app hash test
	expectRes := abciTypes.ResponseCommit{}
	suite.Equal(expectRes, actualRes)
}

// Query는 OwnerKey와 Qualifier에 따라 4가지 경우가 존재한다.

/*
	case query.OwnerKey == nil && query.Qualifier == nil:
*/
func (suite *MasterSuite) TestMasterApplication_time_only_Query() {
	require := suite.Require()
	//given
	suite.TestMasterApplication_Commit()

	/*
		Meta Query
	*/

	//when
	emptySlice := make([]byte, 0)
	start := make([]byte, 8)
	end := make([]byte, 8)
	binary.BigEndian.PutUint64(start, 1545982882435375000)
	binary.BigEndian.PutUint64(end, 1545982882435375002)
	metaQueryObj := types.QueryObj{Start: start, End: end, OwnerKey: emptySlice, Qualifier: emptySlice}
	metaQueryByteArr, err := json.Marshal(metaQueryObj)
	require.Nil(err)
	metaQuery := abciTypes.RequestQuery{Data: metaQueryByteArr, Path: consts.QueryPath}
	actualMetaRes := suite.app.Query(metaQuery)

	//then
	var expectMetaDataObjs []types.MetaDataObj
	expectMetaDataObjs = append(expectMetaDataObjs, givenMetaDataObj1, givenMetaDataObj2)

	expectMetaRes := abciTypes.ResponseQuery{}
	expectMetaRes.Value, err = json.Marshal(expectMetaDataObjs)
	require.Nil(err)

	require.Equal(expectMetaRes, actualMetaRes)

	/*
		Real Query
	*/
	//given
	var givenMetaDataObjs []types.MetaDataObj
	err = json.Unmarshal(actualMetaRes.Value, &givenMetaDataObjs)
	require.Nil(err)

	var givenRowKeys [][]byte
	for i := 0; i < len(givenMetaDataObjs); i++ {
		givenRowKeys = append(givenRowKeys, givenMetaDataObjs[i].RowKey)
	}

	//when
	realDataFetchObj := types.FetchObj{RowKeys: givenRowKeys}
	realDataFetchObjByte, err := json.Marshal(realDataFetchObj)
	realQuery := abciTypes.RequestQuery{Data: realDataFetchObjByte, Path: consts.FetchPath}
	actualRealRes := suite.app.Query(realQuery)

	//then
	var expectRealDataObjs []types.RealDataObj
	expectRealDataObjs = append(expectRealDataObjs, givenRealDataObj1, givenRealDataObj2)

	expectRealRes := abciTypes.ResponseQuery{}
	expectRealRes.Value, err = json.Marshal(expectRealDataObjs)
	require.Nil(err)

	suite.Equal(expectRealRes, actualRealRes)

}

/*
	case query.OwnerKey == nil:
*/
func (suite *MasterSuite) TestMasterApplication_qualifier_Query() {
	require := suite.Require()
	//given
	suite.TestMasterApplication_Commit()
	/*
		Meta Query
	*/

	//when
	emptySlice := make([]byte, 0)
	start := make([]byte, 8)
	end := make([]byte, 8)
	binary.BigEndian.PutUint64(start, 1545982882435375000)
	binary.BigEndian.PutUint64(end, 1545982882435375002)
	metaQueryObj := types.QueryObj{Start: start, End: end, OwnerKey: emptySlice, Qualifier: []byte("Memory")}
	metaQueryByteArr, err := json.Marshal(metaQueryObj)
	require.Nil(err)
	metaQuery := abciTypes.RequestQuery{Data: metaQueryByteArr, Path: consts.QueryPath}
	actualMetaRes := suite.app.Query(metaQuery)

	//then
	var expectMetaDataObjs []types.MetaDataObj
	expectMetaDataObjs = append(expectMetaDataObjs, givenMetaDataObj1)

	expectMetaRes := abciTypes.ResponseQuery{}
	expectMetaRes.Value, err = json.Marshal(expectMetaDataObjs)
	require.Nil(err)

	suite.Equal(expectMetaRes, actualMetaRes)

	/*
		Real Query
	*/
	//given
	var givenMetaDataObjs []types.MetaDataObj
	err = json.Unmarshal(actualMetaRes.Value, &givenMetaDataObjs)
	require.Nil(err)

	var givenRowKeys [][]byte
	for i := 0; i < len(givenMetaDataObjs); i++ {
		givenRowKeys = append(givenRowKeys, givenMetaDataObjs[i].RowKey)
	}

	//when
	realDataFetchObj := types.FetchObj{RowKeys: givenRowKeys}
	realDataFetchObjByte, err := json.Marshal(realDataFetchObj)
	require.Nil(err)
	realQuery := abciTypes.RequestQuery{Data: realDataFetchObjByte, Path: consts.FetchPath}
	actualRealRes := suite.app.Query(realQuery)

	//then
	var expectRealDataObjs []types.RealDataObj
	expectRealDataObjs = append(expectRealDataObjs, givenRealDataObj1)

	expectRealRes := abciTypes.ResponseQuery{}
	expectRealRes.Value, err = json.Marshal(expectRealDataObjs)
	suite.Nil(err)

	suite.Equal(expectRealRes, actualRealRes)

}

/*
	case query.Qualifier == nil:
*/
func (suite *MasterSuite) TestMasterApplication_ownerKey_Query() {
	require := suite.Require()
	//given
	suite.TestMasterApplication_Commit()

	/*
		Meta Query
	*/

	//when
	emptySlice := make([]byte, 0)
	start := make([]byte, 8)
	end := make([]byte, 8)
	binary.BigEndian.PutUint64(start, 1545982882435375000)
	binary.BigEndian.PutUint64(end, 1545982882435375002)
	metaQueryObj := types.QueryObj{Start: start, End: end, OwnerKey: givenOwnerKey2, Qualifier: emptySlice}
	metaQueryByteArr, err := json.Marshal(metaQueryObj)
	require.Nil(err)
	metaQuery := abciTypes.RequestQuery{Data: metaQueryByteArr, Path: consts.QueryPath}
	actualMetaRes := suite.app.Query(metaQuery)

	//then
	var expectMetaDataObjs []types.MetaDataObj
	expectMetaDataObjs = append(expectMetaDataObjs, givenMetaDataObj2)

	expectMetaRes := abciTypes.ResponseQuery{}
	expectMetaRes.Value, err = json.Marshal(expectMetaDataObjs)
	require.Nil(err)

	require.Equal(expectMetaRes, actualMetaRes)

	/*
		Real Query
	*/
	//given
	var givenMetaDataObjs []types.MetaDataObj
	err = json.Unmarshal(actualMetaRes.Value, &givenMetaDataObjs)
	suite.Nil(err)

	var givenRowKeys [][]byte
	for i := 0; i < len(givenMetaDataObjs); i++ {
		givenRowKeys = append(givenRowKeys, givenMetaDataObjs[i].RowKey)
	}

	//when
	realDataFetchObj := types.FetchObj{RowKeys: givenRowKeys}
	realDataFetchObjByte, err := json.Marshal(realDataFetchObj)
	realQuery := abciTypes.RequestQuery{Data: realDataFetchObjByte, Path: consts.FetchPath}
	actualRealRes := suite.app.Query(realQuery)

	//then
	var expectRealDataObjs []types.RealDataObj
	expectRealDataObjs = append(expectRealDataObjs, givenRealDataObj2)

	expectRealRes := abciTypes.ResponseQuery{}
	expectRealRes.Value, err = json.Marshal(expectRealDataObjs)
	suite.Nil(err)

	suite.Equal(expectRealRes, actualRealRes)

}

/*
	default:
*/
func (suite *MasterSuite) TestMasterApplication_both_Query() {
	require := suite.Require()
	//given
	suite.TestMasterApplication_Commit()

	/*
		Meta Query
	*/

	//when
	start := make([]byte, 8)
	end := make([]byte, 8)
	binary.BigEndian.PutUint64(start, 1545982882435375000)
	binary.BigEndian.PutUint64(end, 1545982882435375002)
	metaQueryObj := types.QueryObj{Start: start, End: end, OwnerKey: givenOwnerKey, Qualifier: []byte("Memory")}
	metaQueryByteArr, err := json.Marshal(metaQueryObj)
	require.Nil(err)
	metaQuery := abciTypes.RequestQuery{Data: metaQueryByteArr, Path: consts.QueryPath}
	actualMetaRes := suite.app.Query(metaQuery)

	//then
	var expectMetaDataObjs []types.MetaDataObj
	expectMetaDataObjs = append(expectMetaDataObjs, givenMetaDataObj1)

	expectMetaRes := abciTypes.ResponseQuery{}
	expectMetaRes.Value, err = json.Marshal(expectMetaDataObjs)
	suite.Nil(err)

	suite.Equal(expectMetaRes, actualMetaRes)

	/*
		Real Query
	*/
	//given
	var givenMetaDataObjs []types.MetaDataObj
	err = json.Unmarshal(actualMetaRes.Value, &givenMetaDataObjs)
	suite.Nil(err)

	var givenRowKeys [][]byte
	for i := 0; i < len(givenMetaDataObjs); i++ {
		givenRowKeys = append(givenRowKeys, givenMetaDataObjs[i].RowKey)
	}

	//when
	realDataFetchObj := types.FetchObj{RowKeys: givenRowKeys}
	realDataFetchObjByte, err := json.Marshal(realDataFetchObj)
	realQuery := abciTypes.RequestQuery{Data: realDataFetchObjByte, Path: consts.FetchPath}
	actualRealRes := suite.app.Query(realQuery)

	//then
	var expectRealDataObjs []types.RealDataObj
	expectRealDataObjs = append(expectRealDataObjs, givenRealDataObj1)

	expectRealRes := abciTypes.ResponseQuery{}
	expectRealRes.Value, err = json.Marshal(expectRealDataObjs)
	suite.Nil(err)

	suite.Equal(expectRealRes, actualRealRes)

}
