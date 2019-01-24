package master_test

import (
	"encoding/base64"
	"encoding/json"
	"github.com/paust-team/paust-db/types"
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
	/*
		RightCase
	*/
	//given

	givenOwnerKey, _ := base64.StdEncoding.DecodeString("oimd8ZdzgUHzF9CPChJU8gb89VaMYg+1SpX6WT8nQHE=")
	givenKeyObj1 := types.KeyObj{Timestamp:1545982882435375000, Salt: 0}
	givenRowKey1, err := json.Marshal(givenKeyObj1)
	suite.Nil(err)
	givenKeyObj2 := types.KeyObj{Timestamp:1545982882435375001, Salt: 0}
	givenRowKey2, err := json.Marshal(givenKeyObj2)
	suite.Nil(err)
	givenMetaDataObj1 := types.MetaDataObj{RowKey:givenRowKey1, OwnerKey: givenOwnerKey, Qualifier: []byte("Memory")}
	givenMetaDataObj2 := types.MetaDataObj{RowKey:givenRowKey2, OwnerKey: givenOwnerKey, Qualifier: []byte("Stt")}

	givenRealDataObj1 := types.RealDataObj{RowKey:givenRowKey1, Data: []byte("aw")}
	givenRealDataObj2 := types.RealDataObj{RowKey:givenRowKey1, Data: []byte("good")}

	givenBaseDataObj1 := types.BaseDataObj{MetaData:givenMetaDataObj1, RealData:givenRealDataObj1}
	givenBaseDataObj2 := types.BaseDataObj{MetaData:givenMetaDataObj2, RealData:givenRealDataObj2}

	var givenBaseDataObjs []types.BaseDataObj
	givenBaseDataObjs = append(givenBaseDataObjs, givenBaseDataObj1, givenBaseDataObj2)
	givenTx, _ := json.Marshal(givenBaseDataObjs)

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
	//given
	givenReq := abciTypes.RequestInitChain{}

	//when
	actualRes := suite.app.InitChain(givenReq)

	//then
	suite.NotNil(suite.app.WB(), "WriteBatch should not be nil after Initchain")
	suite.NotNil(suite.app.MWB(), "MetaWriteBatch should not be nil after Initchain")
	expectRes := abciTypes.ResponseInitChain{}
	suite.Equal(expectRes, actualRes)
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
	//given
	suite.TestMasterApplication_InitChain()

	givenOwnerKey, _ := base64.StdEncoding.DecodeString("oimd8ZdzgUHzF9CPChJU8gb89VaMYg+1SpX6WT8nQHE=")
	givenKeyObj1 := types.KeyObj{Timestamp:1545982882435375000, Salt: 0}
	givenRowKey1, err := json.Marshal(givenKeyObj1)
	suite.Nil(err)
	givenKeyObj2 := types.KeyObj{Timestamp:1545982882435375001, Salt: 0}
	givenRowKey2, err := json.Marshal(givenKeyObj2)
	suite.Nil(err)
	givenMetaDataObj1 := types.MetaDataObj{RowKey:givenRowKey1, OwnerKey: givenOwnerKey, Qualifier: []byte("Memory")}
	givenMetaDataObj2 := types.MetaDataObj{RowKey:givenRowKey2, OwnerKey: givenOwnerKey, Qualifier: []byte("Stt")}

	givenRealDataObj1 := types.RealDataObj{RowKey:givenRowKey1, Data: []byte("data1")}
	givenRealDataObj2 := types.RealDataObj{RowKey:givenRowKey2, Data: []byte("data2")}

	givenBaseDataObj1 := types.BaseDataObj{MetaData:givenMetaDataObj1, RealData:givenRealDataObj1}
	givenBaseDataObj2 := types.BaseDataObj{MetaData:givenMetaDataObj2, RealData:givenRealDataObj2}

	var givenBaseDataObjs []types.BaseDataObj
	givenBaseDataObjs = append(givenBaseDataObjs, givenBaseDataObj1, givenBaseDataObj2)
	givenTx, err := json.Marshal(givenBaseDataObjs)
	suite.Nil(err)

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

//path test
func (suite *MasterSuite) TestMasterApplication_Query() {
	//given
	suite.TestMasterApplication_Commit()

	givenOwnerKey, err := base64.StdEncoding.DecodeString("oimd8ZdzgUHzF9CPChJU8gb89VaMYg+1SpX6WT8nQHE=")
	suite.Nil(err)
	givenKeyObj1 := types.KeyObj{Timestamp:1545982882435375000, Salt: 0}
	givenRowKey1, err := json.Marshal(givenKeyObj1)
	suite.Nil(err)
	givenKeyObj2 := types.KeyObj{Timestamp:1545982882435375001, Salt: 0}
	givenRowKey2, err := json.Marshal(givenKeyObj2)
	suite.Nil(err)
	givenMetaDataObj1 := types.MetaDataObj{RowKey:givenRowKey1, OwnerKey: givenOwnerKey, Qualifier: []byte("Memory")}
	givenMetaDataObj2 := types.MetaDataObj{RowKey:givenRowKey2, OwnerKey: givenOwnerKey, Qualifier: []byte("Stt")}

	givenRealDataObj1 := types.RealDataObj{RowKey:givenRowKey1, Data: []byte("data1")}
	givenRealDataObj2 := types.RealDataObj{RowKey:givenRowKey2, Data: []byte("data2")}

	/*
		Meta Query
	*/

	//when
	metaQueryObj := types.MetaDataQueryObj{Start: 1545982882435375000, End: 1545982882435375002}
	metaQueryByteArr, err := json.Marshal(metaQueryObj)
	suite.Nil(err)
	metaQuery := abciTypes.RequestQuery{Data: metaQueryByteArr, Path: "/metadata"}
	actualMetaRes := suite.app.Query(metaQuery)

	//then
	var expectMetaDataObjs []types.MetaDataObj
	expectMetaDataObjs = append(expectMetaDataObjs, givenMetaDataObj1, givenMetaDataObj2)

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
	expectRealDataObjs = append(expectRealDataObjs, givenRealDataObj1, givenRealDataObj2)

	expectRealRes := abciTypes.ResponseQuery{}
	expectRealRes.Value, err = json.Marshal(expectRealDataObjs)
	suite.Nil(err)

	suite.Equal(expectRealRes, actualRealRes)

}
