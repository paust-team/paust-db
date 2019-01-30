package master_test

import (
	"encoding/base64"
	"encoding/json"
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
	//given
	givenKeyObj1 := types.KeyObj{Timestamp: 1545982882435375000, Salt: 0}
	givenKeyObj2 := types.KeyObj{Timestamp: 1545982882435375001, Salt: 0}

	givenRowKey1, err := json.Marshal(givenKeyObj1)
	require.Nil(err)

	givenRowKey2, err := json.Marshal(givenKeyObj2)
	require.Nil(err)

	givenOwnerKey, err := base64.StdEncoding.DecodeString(TestPubKey)
	require.Nil(err)

	givenOwnerKey2, err := base64.StdEncoding.DecodeString(TestPubKey2)
	require.Nil(err)

	givenMetaDataObj1 := types.MetaDataObj{RowKey: givenRowKey1, OwnerKey: givenOwnerKey, Qualifier: []byte("Memory")}
	givenMetaDataObj2 := types.MetaDataObj{RowKey: givenRowKey2, OwnerKey: givenOwnerKey2, Qualifier: []byte("Stt")}

	givenRealDataObj1 := types.RealDataObj{RowKey: givenRowKey1, Data: []byte("aw")}
	givenRealDataObj2 := types.RealDataObj{RowKey: givenRowKey2, Data: []byte("good")}

	givenBaseDataObj1 := types.BaseDataObj{MetaData: givenMetaDataObj1, RealData: givenRealDataObj1}
	givenBaseDataObj2 := types.BaseDataObj{MetaData: givenMetaDataObj2, RealData: givenRealDataObj2}

	var givenBaseDataObjs []types.BaseDataObj
	givenBaseDataObjs = append(givenBaseDataObjs, givenBaseDataObj1, givenBaseDataObj2)

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
	require.NotNil(suite.app.WB(), "WriteBatch should not be nil after Initchain")
	require.NotNil(suite.app.MWB(), "MetaWriteBatch should not be nil after Initchain")
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

	givenKeyObj1 := types.KeyObj{Timestamp: 1545982882435375000, Salt: 0}
	givenKeyObj2 := types.KeyObj{Timestamp: 1545982882435375001, Salt: 0}

	givenRowKey1, err := json.Marshal(givenKeyObj1)
	require.Nil(err)

	givenRowKey2, err := json.Marshal(givenKeyObj2)
	require.Nil(err)

	givenOwnerKey, err := base64.StdEncoding.DecodeString(TestPubKey)
	require.Nil(err)

	givenOwnerKey2, err := base64.StdEncoding.DecodeString(TestPubKey2)
	require.Nil(err)

	givenMetaDataObj1 := types.MetaDataObj{RowKey: givenRowKey1, OwnerKey: givenOwnerKey, Qualifier: []byte("Memory")}
	givenMetaDataObj2 := types.MetaDataObj{RowKey: givenRowKey2, OwnerKey: givenOwnerKey2, Qualifier: []byte("Stt")}

	givenRealDataObj1 := types.RealDataObj{RowKey: givenRowKey1, Data: []byte("data1")}
	givenRealDataObj2 := types.RealDataObj{RowKey: givenRowKey2, Data: []byte("data2")}

	givenBaseDataObj1 := types.BaseDataObj{MetaData: givenMetaDataObj1, RealData: givenRealDataObj1}
	givenBaseDataObj2 := types.BaseDataObj{MetaData: givenMetaDataObj2, RealData: givenRealDataObj2}

	var givenBaseDataObjs []types.BaseDataObj
	givenBaseDataObjs = append(givenBaseDataObjs, givenBaseDataObj1, givenBaseDataObj2)

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
	expectRes := abciTypes.ResponseCommit{Data: suite.app.Hash()}
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

	givenKeyObj1 := types.KeyObj{Timestamp: 1545982882435375000, Salt: 0}
	givenKeyObj2 := types.KeyObj{Timestamp: 1545982882435375001, Salt: 0}

	givenRowKey1, err := json.Marshal(givenKeyObj1)
	require.Nil(err)

	givenRowKey2, err := json.Marshal(givenKeyObj2)
	require.Nil(err)

	givenOwnerKey, err := base64.StdEncoding.DecodeString(TestPubKey)
	require.Nil(err)

	givenOwnerKey2, err := base64.StdEncoding.DecodeString(TestPubKey2)
	require.Nil(err)

	givenMetaDataObj1 := types.MetaDataObj{RowKey: givenRowKey1, OwnerKey: givenOwnerKey, Qualifier: []byte("Memory")}
	givenMetaDataObj2 := types.MetaDataObj{RowKey: givenRowKey2, OwnerKey: givenOwnerKey2, Qualifier: []byte("Stt")}

	givenRealDataObj1 := types.RealDataObj{RowKey: givenRowKey1, Data: []byte("data1")}
	givenRealDataObj2 := types.RealDataObj{RowKey: givenRowKey2, Data: []byte("data2")}

	/*
		Meta Query
	*/

	//when
	metaQueryObj := types.MetaDataQueryObj{Start: 1545982882435375000, End: 1545982882435375002}
	metaQueryByteArr, err := json.Marshal(metaQueryObj)
	require.Nil(err)
	metaQuery := abciTypes.RequestQuery{Data: metaQueryByteArr, Path: "/metadata"}
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
	realDataQueryObj := types.RealDataQueryObj{RowKeys: givenRowKeys}
	realDataQueryObjByte, err := json.Marshal(realDataQueryObj)
	realQuery := abciTypes.RequestQuery{Data: realDataQueryObjByte, Path: "/realdata"}
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

	givenKeyObj1 := types.KeyObj{Timestamp: 1545982882435375000, Salt: 0}
	//givenKeyObj2 := types.KeyObj{Timestamp: 1545982882435375001, Salt: 0}

	givenRowKey1, err := json.Marshal(givenKeyObj1)
	require.Nil(err)

	//givenRowKey2, err := json.Marshal(givenKeyObj2)
	require.Nil(err)

	givenOwnerKey, err := base64.StdEncoding.DecodeString(TestPubKey)
	require.Nil(err)

	//givenOwnerKey2, err := base64.StdEncoding.DecodeString(TestPubKey2)
	require.Nil(err)

	givenMetaDataObj1 := types.MetaDataObj{RowKey: givenRowKey1, OwnerKey: givenOwnerKey, Qualifier: []byte("Memory")}
	//givenMetaDataObj2 := types.MetaDataObj{RowKey: givenRowKey2, OwnerKey: givenOwnerKey2, Qualifier: []byte("Stt")}

	givenRealDataObj1 := types.RealDataObj{RowKey: givenRowKey1, Data: []byte("data1")}
	//givenRealDataObj2 := types.RealDataObj{RowKey: givenRowKey2, Data: []byte("data2")}

	/*
		Meta Query
	*/

	//when
	metaQueryObj := types.MetaDataQueryObj{Start: 1545982882435375000, End: 1545982882435375002, Qualifier: []byte("Memory")}
	metaQueryByteArr, err := json.Marshal(metaQueryObj)
	require.Nil(err)
	metaQuery := abciTypes.RequestQuery{Data: metaQueryByteArr, Path: "/metadata"}
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
	realDataQueryObj := types.RealDataQueryObj{RowKeys: givenRowKeys}
	realDataQueryObjByte, err := json.Marshal(realDataQueryObj)
	require.Nil(err)
	realQuery := abciTypes.RequestQuery{Data: realDataQueryObjByte, Path: "/realdata"}
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

	//givenKeyObj1 := types.KeyObj{Timestamp: 1545982882435375000, Salt: 0}
	givenKeyObj2 := types.KeyObj{Timestamp: 1545982882435375001, Salt: 0}

	//givenRowKey1, err := json.Marshal(givenKeyObj1)
	//require.Nil(err)

	givenRowKey2, err := json.Marshal(givenKeyObj2)
	require.Nil(err)

	//givenOwnerKey, err := base64.StdEncoding.DecodeString(TestPubKey)
	require.Nil(err)

	givenOwnerKey2, err := base64.StdEncoding.DecodeString(TestPubKey2)
	require.Nil(err)

	//givenMetaDataObj1 := types.MetaDataObj{RowKey: givenRowKey1, OwnerKey: givenOwnerKey, Qualifier: []byte("Memory")}
	givenMetaDataObj2 := types.MetaDataObj{RowKey: givenRowKey2, OwnerKey: givenOwnerKey2, Qualifier: []byte("Stt")}

	//givenRealDataObj1 := types.RealDataObj{RowKey: givenRowKey1, Data: []byte("data1")}
	givenRealDataObj2 := types.RealDataObj{RowKey: givenRowKey2, Data: []byte("data2")}

	/*
		Meta Query
	*/

	//when
	metaQueryObj := types.MetaDataQueryObj{Start: 1545982882435375000, End: 1545982882435375002, OwnerKey: givenOwnerKey2}
	metaQueryByteArr, err := json.Marshal(metaQueryObj)
	require.Nil(err)
	metaQuery := abciTypes.RequestQuery{Data: metaQueryByteArr, Path: "/metadata"}
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
	realDataQueryObj := types.RealDataQueryObj{RowKeys: givenRowKeys}
	realDataQueryObjByte, err := json.Marshal(realDataQueryObj)
	realQuery := abciTypes.RequestQuery{Data: realDataQueryObjByte, Path: "/realdata"}
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

	givenKeyObj1 := types.KeyObj{Timestamp: 1545982882435375000, Salt: 0}
	//givenKeyObj2 := types.KeyObj{Timestamp: 1545982882435375001, Salt: 0}

	givenRowKey1, err := json.Marshal(givenKeyObj1)
	require.Nil(err)

	//givenRowKey2, err := json.Marshal(givenKeyObj2)
	//require.Nil(err)

	givenOwnerKey, err := base64.StdEncoding.DecodeString(TestPubKey)
	require.Nil(err)

	//givenOwnerKey2, err := base64.StdEncoding.DecodeString(TestPubKey2)
	//require.Nil(err)

	givenMetaDataObj1 := types.MetaDataObj{RowKey: givenRowKey1, OwnerKey: givenOwnerKey, Qualifier: []byte("Memory")}
	//givenMetaDataObj2 := types.MetaDataObj{RowKey: givenRowKey2, OwnerKey: givenOwnerKey2, Qualifier: []byte("Stt")}

	givenRealDataObj1 := types.RealDataObj{RowKey: givenRowKey1, Data: []byte("data1")}
	//givenRealDataObj2 := types.RealDataObj{RowKey: givenRowKey2, Data: []byte("data2")}

	/*
		Meta Query
	*/

	//when
	metaQueryObj := types.MetaDataQueryObj{Start: 1545982882435375000, End: 1545982882435375002, OwnerKey: givenOwnerKey, Qualifier: []byte("Memory")}
	metaQueryByteArr, err := json.Marshal(metaQueryObj)
	require.Nil(err)
	metaQuery := abciTypes.RequestQuery{Data: metaQueryByteArr, Path: "/metadata"}
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
	realDataQueryObj := types.RealDataQueryObj{RowKeys: givenRowKeys}
	realDataQueryObjByte, err := json.Marshal(realDataQueryObj)
	realQuery := abciTypes.RequestQuery{Data: realDataQueryObjByte, Path: "/realdata"}
	actualRealRes := suite.app.Query(realQuery)

	//then
	var expectRealDataObjs []types.RealDataObj
	expectRealDataObjs = append(expectRealDataObjs, givenRealDataObj1)

	expectRealRes := abciTypes.ResponseQuery{}
	expectRealRes.Value, err = json.Marshal(expectRealDataObjs)
	suite.Nil(err)

	suite.Equal(expectRealRes, actualRealRes)

}
