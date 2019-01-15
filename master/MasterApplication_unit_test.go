package master_test

import (
	"encoding/base64"
	"encoding/json"
	"github.com/paust-team/paust-db/types"
	"github.com/tendermint/tendermint/abci/example/code"
	abciTypes "github.com/tendermint/tendermint/abci/types"
	"github.com/thoas/go-funk"
)

func (suite *MasterSuite) TestMasterApplication_Info() {
	//given
	req := abciTypes.RequestInfo{}

	//when
	res := suite.app.Info(req)

	//then
	suite.Equal(res.Data, "---- Info")
}

func (suite *MasterSuite) TestMasterApplication_CheckTx() {
	/*
		RightCase
	*/
	//given

	pubKeyBytes, _ := base64.StdEncoding.DecodeString("oimd8ZdzgUHzF9CPChJU8gb89VaMYg+1SpX6WT8nQHE=")
	realData1 := types.WRealDataObj{Timestamp: 1545982882435375000, UserKey: pubKeyBytes, Qualifier: "Memory", Data: []byte("aw")}
	realData2 := types.WRealDataObj{Timestamp: 1545982882435375001, UserKey: pubKeyBytes, Qualifier: "Stt", Data: []byte("goog")}

	realDataSlice := funk.FlattenDeep([]types.WRealDataObj{realData1, realData2}).([]types.WRealDataObj)
	jsonString, _ := json.Marshal(realDataSlice)

	//when
	res := suite.app.CheckTx(jsonString)

	//then
	expectRes := abciTypes.ResponseCheckTx{Code: code.CodeTypeOK, Log: ""}
	suite.Equal(expectRes, res)

	/*
		WrongCase
	*/

	//given
	wrongTx := []byte("wrongtx")

	//when
	res2 := suite.app.CheckTx(wrongTx)

	//then
	suite.Equal(code.CodeTypeEncodingError, res2.Code)
	suite.NotEqual("", res2.Log)

}

func (suite *MasterSuite) TestMasterApplication_InitChain() {
	//given
	req := abciTypes.RequestInitChain{}

	//when
	res := suite.app.InitChain(req)

	//then
	suite.NotNil(suite.app.WB(), "WriteBatch should not be nil after Initchain")
	suite.NotNil(suite.app.MWB(), "MetaWriteBatch should not be nil after Initchain")
	expectRes := abciTypes.ResponseInitChain{}
	suite.Equal(expectRes, res)
}

func (suite *MasterSuite) TestMasterApplication_BeginBlock() {
	//given
	req := abciTypes.RequestBeginBlock{}

	//when
	res := suite.app.BeginBlock(req)

	//then
	expectRes := abciTypes.ResponseBeginBlock{}
	suite.Equal(expectRes, res)
}

func (suite *MasterSuite) TestMasterApplication_DeliverTx() {
	//given
	suite.TestMasterApplication_InitChain()

	pubKeyBytes, err := base64.StdEncoding.DecodeString("oimd8ZdzgUHzF9CPChJU8gb89VaMYg+1SpX6WT8nQHE=")
	suite.Nil(err)
	pubKeyBytes2, err2 := base64.StdEncoding.DecodeString("azbYS7sLOQG0XphoneMrVEQUvZpVSflsDgbLWH0vZVE=")
	suite.Nil(err2)

	realData1 := types.WRealDataObj{Timestamp: 1545982882435375000, UserKey: pubKeyBytes, Qualifier: "Memory", Data: []byte("data1")}
	realData2 := types.WRealDataObj{Timestamp: 1545982882435375001, UserKey: pubKeyBytes, Qualifier: "Stt", Data: []byte("data2")}
	realData3 := types.WRealDataObj{Timestamp: 1555982882435375000, UserKey: pubKeyBytes2, Qualifier: "Stt", Data: []byte("data3")}

	realDataSlice := funk.FlattenDeep([]types.WRealDataObj{realData1, realData2, realData3})
	tx, err3 := json.Marshal(realDataSlice)
	suite.Nil(err3)

	//when
	res := suite.app.DeliverTx(tx)

	//then
	suite.Equal(code.CodeTypeOK, res.Code)

}

func (suite *MasterSuite) TestMasterApplication_EndBlock() {
	//given
	req := abciTypes.RequestEndBlock{}

	//when
	res := suite.app.EndBlock(req)

	//then
	expectRes := abciTypes.ResponseEndBlock{}
	suite.Equal(expectRes, res)
}

func (suite *MasterSuite) TestMasterApplication_Commit() {
	//given
	suite.TestMasterApplication_DeliverTx()

	//when
	res := suite.app.Commit()

	//then
	expectRes := abciTypes.ResponseCommit{Data: suite.app.Hash()}
	suite.Equal(expectRes, res)
}

//path test
func (suite *MasterSuite) TestMasterApplication_Query() {
	//given
	suite.TestMasterApplication_DeliverTx()
	suite.TestMasterApplication_Commit()

	pubKeyBytes, err := base64.StdEncoding.DecodeString("oimd8ZdzgUHzF9CPChJU8gb89VaMYg+1SpX6WT8nQHE=")
	pubKeyBytes2, err := base64.StdEncoding.DecodeString("azbYS7sLOQG0XphoneMrVEQUvZpVSflsDgbLWH0vZVE=")
	suite.Nil(err)
	realData1 := types.WRealDataObj{Timestamp: 1545982882435375000, UserKey: pubKeyBytes, Qualifier: "Memory", Data: []byte("data1")}
	realData2 := types.WRealDataObj{Timestamp: 1545982882435375001, UserKey: pubKeyBytes, Qualifier: "Stt", Data: []byte("data2")}

	metaRes1 := types.RMetaResObj{Timestamp: 1545982882435375000, UserKey: pubKeyBytes, Qualifier: "Memory"}
	metaRes2 := types.RMetaResObj{Timestamp: 1545982882435375001, UserKey: pubKeyBytes, Qualifier: "Stt"}
	metaRes3 := types.RMetaResObj{Timestamp: 1555982882435375000, UserKey: pubKeyBytes2, Qualifier: "Stt"}

	/**
	WRealDataObj query
	*/

	//when
	realQuery := types.RDataQueryObj{Start: 1545982882435375000, End: 1545982882435375002, UserKey: pubKeyBytes, Qualifier: ""}
	realQueryByteArr, _ := json.Marshal(realQuery)
	req := abciTypes.RequestQuery{Data: realQueryByteArr, Path: "/realdata"}
	res := suite.app.Query(req)

	//then
	realDataSlice := funk.FlattenDeep([]types.WRealDataObj{realData1, realData2})
	value, err := json.Marshal(realDataSlice)
	suite.Nil(err)

	expectRes := abciTypes.ResponseQuery{Value: value}
	suite.Equal(expectRes, res)

	/**
	MetaDataObj query
	*/

	//when
	metaQuery := types.RDataQueryObj{Start: 1545982882435375000, End: 1555982882435375001, UserKey: nil, Qualifier: ""}
	metaQueryByteArr, _ := json.Marshal(metaQuery)
	req2 := abciTypes.RequestQuery{Data: metaQueryByteArr, Path: "/metadata"}
	res2 := suite.app.Query(req2)

	//then
	metaResSlice := funk.FlattenDeep([]types.RMetaResObj{metaRes1, metaRes2, metaRes3})
	value2, err := json.Marshal(metaResSlice)
	suite.Nil(err)

	expectRes2 := abciTypes.ResponseQuery{Value: value2}
	suite.Equal(expectRes2, res2)

}

//4가지 case 존재
func (suite *MasterSuite) TestMasterApplication_metaDataQuery() {

	//given
	suite.TestMasterApplication_DeliverTx()
	suite.TestMasterApplication_Commit()

	pubKeyBytes, err := base64.StdEncoding.DecodeString("oimd8ZdzgUHzF9CPChJU8gb89VaMYg+1SpX6WT8nQHE=")
	suite.Nil(err)

	pubKeyBytes2, err := base64.StdEncoding.DecodeString("azbYS7sLOQG0XphoneMrVEQUvZpVSflsDgbLWH0vZVE=")
	suite.Nil(err)

	metaRes1 := types.RMetaResObj{Timestamp: 1545982882435375000, UserKey: pubKeyBytes, Qualifier: "Memory"}
	metaRes2 := types.RMetaResObj{Timestamp: 1545982882435375001, UserKey: pubKeyBytes, Qualifier: "Stt"}
	metaRes3 := types.RMetaResObj{Timestamp: 1555982882435375000, UserKey: pubKeyBytes2, Qualifier: "Stt"}

	/*
		case: query.UserKey == nil && query.Qualifier == ""
	*/

	//when
	metaQuery := types.RDataQueryObj{Start: 1545982882435375000, End: 1555982882435375001, UserKey: nil, Qualifier: ""}
	res, err := suite.app.MetaDataQuery(metaQuery)
	suite.Nil(err)

	//then
	expectRes := types.RMetaResObjs{}
	expectRes = append(expectRes, metaRes1, metaRes2, metaRes3)
	suite.Equal(expectRes, res)

	/*
		case: query.Qualifier == ""
	*/

	//when
	metaQuery2 := types.RDataQueryObj{Start: 1545982882435375000, End: 1555982882435375001, UserKey: pubKeyBytes, Qualifier: ""}
	res2, err := suite.app.MetaDataQuery(metaQuery2)
	suite.Nil(err)

	//then
	expectRes2 := types.RMetaResObjs{}
	expectRes2 = append(expectRes2, metaRes1, metaRes2)
	suite.Equal(expectRes2, res2)

	/*
		case: query.UserKey == nil
	*/

	//when
	metaQuery3 := types.RDataQueryObj{Start: 1545982882435375000, End: 1555982882435375001, UserKey: nil, Qualifier: "Stt"}
	res3, err := suite.app.MetaDataQuery(metaQuery3)
	suite.Nil(err)

	//then
	expectRes3 := types.RMetaResObjs{}
	expectRes3 = append(expectRes3, metaRes2, metaRes3)
	suite.Equal(expectRes3, res3)

	/*
		default
	*/

	//when
	metaQuery4 := types.RDataQueryObj{Start: 1545982882435375000, End: 1555982882435375001, UserKey: pubKeyBytes, Qualifier: "Memory"}
	res4, err := suite.app.MetaDataQuery(metaQuery4)
	suite.Nil(err)

	//then
	expectRes4 := types.RMetaResObjs{}
	expectRes4 = append(expectRes4, metaRes1)
	suite.Equal(expectRes4, res4)

}

//4가지 case 존재
func (suite *MasterSuite) TestMasterApplication_realDataQuery() {
	//given
	suite.TestMasterApplication_DeliverTx()
	suite.TestMasterApplication_Commit()

	pubKeyBytes, err := base64.StdEncoding.DecodeString("oimd8ZdzgUHzF9CPChJU8gb89VaMYg+1SpX6WT8nQHE=")
	suite.Nil(err)

	pubKeyBytes2, err := base64.StdEncoding.DecodeString("azbYS7sLOQG0XphoneMrVEQUvZpVSflsDgbLWH0vZVE=")
	suite.Nil(err)

	realData1 := types.WRealDataObj{Timestamp: 1545982882435375000, UserKey: pubKeyBytes, Qualifier: "Memory", Data: []byte("data1")}
	realData2 := types.WRealDataObj{Timestamp: 1545982882435375001, UserKey: pubKeyBytes, Qualifier: "Stt", Data: []byte("data2")}
	realData3 := types.WRealDataObj{Timestamp: 1555982882435375000, UserKey: pubKeyBytes2, Qualifier: "Stt", Data: []byte("data3")}

	/*
		case: query.UserKey == nil && query.Qualifier == ""
	*/

	//when
	realQuery := types.RDataQueryObj{Start: 1545982882435375000, End: 1555982882435375001, UserKey: nil, Qualifier: ""}
	res, err := suite.app.RealDataQuery(realQuery)
	suite.Nil(err)

	//then
	expectRes := types.WRealDataObjs{}
	expectRes = append(expectRes, realData1, realData2, realData3)
	suite.Equal(expectRes, res)

	/*
		case: query.Qualifier == ""
	*/

	//when
	realQuery2 := types.RDataQueryObj{Start: 1545982882435375000, End: 1555982882435375001, UserKey: pubKeyBytes, Qualifier: ""}
	res2, err := suite.app.RealDataQuery(realQuery2)
	suite.Nil(err)

	//then
	expectRes2 := types.WRealDataObjs{}
	expectRes2 = append(expectRes2, realData1, realData2)
	suite.Equal(expectRes2, res2)

	/*
		case: query.UserKey == nil
	*/

	//when
	realQuery3 := types.RDataQueryObj{Start: 1545982882435375000, End: 1555982882435375001, UserKey: nil, Qualifier: "Stt"}
	res3, err := suite.app.RealDataQuery(realQuery3)
	suite.Nil(err)

	//then
	expectRes3 := types.WRealDataObjs{}
	expectRes3 = append(expectRes3, realData2, realData3)
	suite.Equal(expectRes3, res3)

	/*
		default
	*/

	//when
	realQuery4 := types.RDataQueryObj{Start: 1545982882435375000, End: 1555982882435375001, UserKey: pubKeyBytes, Qualifier: "Memory"}
	res4, err := suite.app.RealDataQuery(realQuery4)
	suite.Nil(err)

	//then
	expectRes4 := types.WRealDataObjs{}
	expectRes4 = append(expectRes4, realData1)
	suite.Equal(expectRes4, res4)
}
