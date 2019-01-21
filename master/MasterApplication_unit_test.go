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
	givenWRealDataObj1 := types.WRealDataObj{Timestamp: 1545982882435375000, OwnerKey: givenOwnerKey, Qualifier: []byte("Memory"), Data: []byte("aw")}
	givenWRealDataObj2 := types.WRealDataObj{Timestamp: 1545982882435375001, OwnerKey: givenOwnerKey, Qualifier: []byte("Stt"), Data: []byte("goog")}

	givenWRealDataObjs := types.WRealDataObjs{}
	givenWRealDataObjs = append(givenWRealDataObjs, givenWRealDataObj1, givenWRealDataObj2)
	givenMarshaledObjs, _ := json.Marshal(givenWRealDataObjs)

	//when
	actualRes := suite.app.CheckTx(givenMarshaledObjs)

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

	givenOwnerKey, err := base64.StdEncoding.DecodeString("oimd8ZdzgUHzF9CPChJU8gb89VaMYg+1SpX6WT8nQHE=")
	suite.Nil(err)

	givenWRealDataObj1 := types.WRealDataObj{Timestamp: 1545982882435375000, OwnerKey: givenOwnerKey, Qualifier: []byte("Memory"), Data: []byte("data1")}
	givenWRealDataObj2 := types.WRealDataObj{Timestamp: 1545982882435375001, OwnerKey: givenOwnerKey, Qualifier: []byte("Stt"), Data: []byte("data2")}

	givenWRealDataObjs := types.WRealDataObjs{}
	givenWRealDataObjs = append(givenWRealDataObjs, givenWRealDataObj1, givenWRealDataObj2)
	givenTx, err := json.Marshal(givenWRealDataObjs)
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
	givenWRealDataObj1 := types.WRealDataObj{Timestamp: 1545982882435375000, OwnerKey: givenOwnerKey, Qualifier: []byte("Memory"), Data: []byte("data1")}
	givenWRealDataObj2 := types.WRealDataObj{Timestamp: 1545982882435375001, OwnerKey: givenOwnerKey, Qualifier: []byte("Stt"), Data: []byte("data2")}

	/*
		Meta Query
	*/

	//when
	metaQueryObj := types.RMetaDataQueryObj{Start: 1545982882435375000, End: 1545982882435375002}
	metaQueryByteArr, err := json.Marshal(metaQueryObj)
	suite.Nil(err)
	metaQuery := abciTypes.RequestQuery{Data: metaQueryByteArr, Path: "/metadata"}
	actualMetaRes := suite.app.Query(metaQuery)

	//then
	expectRowKey1 := types.WRealDataObjToRowKey(givenWRealDataObj1)
	expectRowKey2 := types.WRealDataObjToRowKey(givenWRealDataObj2)
	expectRMetaResObj1 := types.RMetaDataResObj{RowKey: expectRowKey1, OwnerKey: givenOwnerKey, Qualifier: []byte("Memory")}
	expectRMetaResObj2 := types.RMetaDataResObj{RowKey: expectRowKey2, OwnerKey: givenOwnerKey, Qualifier: []byte("Stt")}
	expectRMetaResObjs := types.RMetaDataResObjs{}
	expectRMetaResObjs = append(expectRMetaResObjs, expectRMetaResObj1, expectRMetaResObj2)

	marshaledExpectMetaResObjs, err := json.Marshal(expectRMetaResObjs)
	suite.Nil(err)

	expectMetaRes := abciTypes.ResponseQuery{Value: marshaledExpectMetaResObjs}
	suite.Equal(expectMetaRes, actualMetaRes)

	/*
		Real Query
	*/

	//given
	givenMetaResObjs := types.RMetaDataResObjs{}
	err = json.Unmarshal(actualMetaRes.Value, &givenMetaResObjs)
	suite.Nil(err)

	var givenRowKeys types.RowKeys
	for i := 0; i < len(givenMetaResObjs); i++ {
		givenRowKeys = append(givenRowKeys, givenMetaResObjs[i].RowKey)
	}

	//when
	realQueryObj := types.RRealDataQueryObj{Keys: givenRowKeys}
	realQueryByteArr, err := json.Marshal(realQueryObj)
	reqQuery := abciTypes.RequestQuery{Data: realQueryByteArr, Path: "/realdata"}
	actualRealRes := suite.app.Query(reqQuery)

	//then
	expectRealResObj1 := types.RRealDataResObj{RowKey: givenRowKeys[0], Data: []byte("data1")}
	expectRealResObj2 := types.RRealDataResObj{RowKey: givenRowKeys[1], Data: []byte("data2")}
	expectRealResObjs := types.RRealDataResObjs{}
	expectRealResObjs = append(expectRealResObjs, expectRealResObj1, expectRealResObj2)

	expectRealResValue, err := json.Marshal(expectRealResObjs)
	suite.Nil(err)

	expectRealRes := abciTypes.ResponseQuery{Value: expectRealResValue}
	suite.Equal(expectRealRes, actualRealRes)

}

func (suite *MasterSuite) TestMasterApplication_metaDataQuery() {
	//given
	suite.TestMasterApplication_Commit()

	givenOwnerKey, err := base64.StdEncoding.DecodeString("oimd8ZdzgUHzF9CPChJU8gb89VaMYg+1SpX6WT8nQHE=")
	suite.Nil(err)

	givenWRealDataObj1 := types.WRealDataObj{Timestamp: 1545982882435375000, OwnerKey: givenOwnerKey, Qualifier: []byte("Memory"), Data: []byte("data1")}
	givenWRealDataObj2 := types.WRealDataObj{Timestamp: 1545982882435375001, OwnerKey: givenOwnerKey, Qualifier: []byte("Stt"), Data: []byte("data2")}

	givenRowKey1 := types.WRealDataObjToRowKey(givenWRealDataObj1)
	givenRowKey2 := types.WRealDataObjToRowKey(givenWRealDataObj2)

	//when
	metaQuery := types.RMetaDataQueryObj{Start: 1545982882435375000, End: 1555982882435375002}
	actualRes, err := suite.app.MetaDataQuery(metaQuery)
	suite.Nil(err)

	//then
	expectMetaRes1 := types.RMetaDataResObj{RowKey: givenRowKey1, OwnerKey: givenOwnerKey, Qualifier: []byte("Memory")}
	expectMetaRes2 := types.RMetaDataResObj{RowKey: givenRowKey2, OwnerKey: givenOwnerKey, Qualifier: []byte("Stt")}
	expectRes := types.RMetaDataResObjs{}
	expectRes = append(expectRes, expectMetaRes1, expectMetaRes2)

	suite.Equal(expectRes, actualRes)
}

func (suite *MasterSuite) TestMasterApplication_realDataQuery() {
	//given
	suite.TestMasterApplication_Commit()

	givenOwnerKey, err := base64.StdEncoding.DecodeString("oimd8ZdzgUHzF9CPChJU8gb89VaMYg+1SpX6WT8nQHE=")
	suite.Nil(err)

	givenWRealDataObj1 := types.WRealDataObj{Timestamp: 1545982882435375000, OwnerKey: givenOwnerKey, Qualifier: []byte("Memory"), Data: []byte("data1")}
	givenWRealDataObj2 := types.WRealDataObj{Timestamp: 1545982882435375001, OwnerKey: givenOwnerKey, Qualifier: []byte("Stt"), Data: []byte("data2")}

	givenRowKey1 := types.WRealDataObjToRowKey(givenWRealDataObj1)
	givenRowKey2 := types.WRealDataObjToRowKey(givenWRealDataObj2)
	givenRowKeys := types.RowKeys{}
	givenRowKeys = append(givenRowKeys, givenRowKey1, givenRowKey2)

	//when
	realQuery := types.RRealDataQueryObj{Keys: givenRowKeys}
	actualRRealDataResObjs, err := suite.app.RealDataQuery(realQuery)
	suite.Nil(err)

	//then
	expectRRealDataResObj1 := types.RRealDataResObj{RowKey: givenRowKey1, Data: givenWRealDataObj1.Data}
	expectRRealDataResObj2 := types.RRealDataResObj{RowKey: givenRowKey2, Data: givenWRealDataObj2.Data}
	expectRRealDataResObjs := types.RRealDataResObjs{}
	expectRRealDataResObjs = append(expectRRealDataResObjs, expectRRealDataResObj1, expectRRealDataResObj2)
	suite.Equal(expectRRealDataResObjs, actualRRealDataResObjs)
}
