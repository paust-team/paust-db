package client_test

import (
	"encoding/base64"
	"encoding/json"
	"github.com/paust-team/paust-db/client"
	"github.com/paust-team/paust-db/types"
	"github.com/stretchr/testify/require"
	cmn "github.com/tendermint/tendermint/libs/common"
	rpcClient "github.com/tendermint/tendermint/rpc/client"
	"time"
)

const (
	TestReadFile = "../test/read_file.json"
)

type RClientMetaDataResObj struct {
	RowKey    types.KeyObj `json:"rowKey"`
	OwnerKey  []byte       `json:"ownerKey"`
	Qualifier []byte       `json:"qualifier"`
}

type RClientMetaDataResObjs []RClientMetaDataResObj

type RClientRealDataResObj struct {
	RowKey types.KeyObj `json:"rowKey"`
	Data   []byte       `json:"data"`
}

type RClientRealDataResObjs []RClientRealDataResObj

func (suite *ClientTestSuite) TestClient_ReadData() {
	require := require.New(suite.T())

	mempool := node.MempoolReactor().Mempool
	time := time.Now()
	data := []byte(cmn.RandStr(8))
	pubKeyBytes, err := base64.StdEncoding.DecodeString(TestPubKey)
	require.Nil(err, "base64 decode err: %+v", err)
	tx, err := json.Marshal(types.WRealDataObjs{types.WRealDataObj{Timestamp: uint64(time.UnixNano()), OwnerKey: pubKeyBytes, Qualifier: []byte(TestQualifier), Data: data}})
	require.Nil(err, "json marshal err: %+v", err)
	keyObj, err := json.Marshal(types.KeyObj{Timestamp: uint64(time.UnixNano())})
	require.Nil(err, "json marshal err: %+v", err)
	expectedValue, err := json.Marshal(types.RRealDataResObjs{types.RRealDataResObj{RowKey: keyObj, Data: data}})
	require.Nil(err, "json marshal err: %+v", err)

	c := rpcClient.NewLocal(node)
	bres, err := c.BroadcastTxCommit(tx)

	require.Nil(err, "err: %+v", err)
	require.True(bres.CheckTx.IsOK())
	require.True(bres.DeliverTx.IsOK())

	require.Equal(0, mempool.Size())

	res, err := suite.dbClient.ReadData([]string{string(keyObj)})
	qres := res.Response
	if suite.Nil(err) && suite.True(qres.IsOK()) {
		suite.EqualValues(expectedValue, qres.Value)
	}
}

func (suite *ClientTestSuite) TestClient_ReadDataOfFile() {
	require := require.New(suite.T())

	mempool := node.MempoolReactor().Mempool

	pubKeyBytes, err := base64.StdEncoding.DecodeString(TestPubKey)
	require.Nil(err, "base64 decode err: %+v", err)
	tx, err := json.Marshal(types.WRealDataObjs{
		types.WRealDataObj{Timestamp: 1547772882435375000, OwnerKey: pubKeyBytes, Qualifier: []byte(TestQualifier), Data: []byte("testData1")},
		types.WRealDataObj{Timestamp: 1547772960049177000, OwnerKey: pubKeyBytes, Qualifier: []byte(TestQualifier), Data: []byte("testData2")},
		types.WRealDataObj{Timestamp: 1547772967331458000, OwnerKey: pubKeyBytes, Qualifier: []byte(TestQualifier), Data: []byte("testData3")},
	})
	require.Nil(err, "json marshal err: %+v", err)
	rowKey1, err := json.Marshal(types.KeyObj{Timestamp: 1547772882435375000})
	require.Nil(err, "json marshal err: %+v", err)
	rowKey2, err := json.Marshal(types.KeyObj{Timestamp: 1547772960049177000})
	require.Nil(err, "json marshal err: %+v", err)
	rowKey3, err := json.Marshal(types.KeyObj{Timestamp: 1547772967331458000})
	require.Nil(err, "json marshal err: %+v", err)
	expectedValue, err := json.Marshal(types.RRealDataResObjs{
		types.RRealDataResObj{RowKey: rowKey1, Data: []byte("testData1")},
		types.RRealDataResObj{RowKey: rowKey2, Data: []byte("testData2")},
		types.RRealDataResObj{RowKey: rowKey3, Data: []byte("testData3")},
	})

	c := rpcClient.NewLocal(node)
	bres, err := c.BroadcastTxCommit(tx)

	require.Nil(err, "err: %+v", err)
	require.True(bres.CheckTx.IsOK())
	require.True(bres.DeliverTx.IsOK())

	require.Equal(0, mempool.Size())

	res, err := suite.dbClient.ReadDataOfFile(TestReadFile)
	qres := res.Response
	if suite.Nil(err) && suite.True(qres.IsOK()) {
		suite.EqualValues(expectedValue, qres.Value)
	}
}

func (suite *ClientTestSuite) TestClient_ReadMetaData() {
	require := require.New(suite.T())

	mempool := node.MempoolReactor().Mempool
	time := time.Now()
	data := []byte(cmn.RandStr(8))
	pubKeyBytes, err := base64.StdEncoding.DecodeString(TestPubKey)
	require.Nil(err, "base64 decode err: %+v", err)
	tx, err := json.Marshal(types.WRealDataObjs{types.WRealDataObj{Timestamp: uint64(time.UnixNano()), OwnerKey: pubKeyBytes, Qualifier: []byte(TestQualifier), Data: data}})
	require.Nil(err, "json marshal err: %+v", err)
	keyObj, err := json.Marshal(types.KeyObj{Timestamp: uint64(time.UnixNano())})
	require.Nil(err, "json marshal err: %+v", err)
	expectedValue, err := json.Marshal(types.RMetaDataResObjs{types.RMetaDataResObj{RowKey: keyObj, OwnerKey: pubKeyBytes, Qualifier: []byte(TestQualifier)}})
	require.Nil(err, "json marshal err: %+v", err)

	c := rpcClient.NewLocal(node)
	bres, err := c.BroadcastTxCommit(tx)

	require.Nil(err, "err: %+v", err)
	require.True(bres.CheckTx.IsOK())
	require.True(bres.DeliverTx.IsOK())

	require.Equal(0, mempool.Size())

	res, err := suite.dbClient.ReadMetaData(uint64(time.UnixNano()), uint64(time.UnixNano())+1)
	qres := res.Response
	if suite.Nil(err) && suite.True(qres.IsOK()) {
		suite.EqualValues(expectedValue, qres.Value)
	}
}

func (suite *ClientTestSuite) TestSerializeKeyObj() {
	require := require.New(suite.T())

	var timestamp1 uint64 = 1547772882435375000
	var timestamp2 uint64 = 1547772960049177000
	rowKey1, err := json.Marshal(types.KeyObj{Timestamp: timestamp1})
	require.Nil(err, "json marshal err: %+v", err)
	rowKey2, err := json.Marshal(types.KeyObj{Timestamp: timestamp2})
	require.Nil(err, "json marshal err: %+v", err)

	// RealDataQueryObj serialize
	rClientRealDataQueryObj, err := json.Marshal(types.RClientRealDataQueryObj{Keys: []types.KeyObj{{Timestamp: timestamp1}, {Timestamp: timestamp2}}})
	require.Nil(err, "json marshal err: %+v", err)
	rRealDataQueryObj, err := json.Marshal(types.RRealDataQueryObj{Keys: types.RowKeys{rowKey1, rowKey2}})
	require.Nil(err, "json marshal err: %+v", err)

	serializedBytes, err := client.SerializeKeyObj(rClientRealDataQueryObj)
	require.Nil(err, "SerializeKeyObj err: %+v", err)

	require.EqualValues(rRealDataQueryObj, serializedBytes)
}

func (suite *ClientTestSuite) TestDeSerializeKeyObj() {
	require := require.New(suite.T())

	var timestamp1 uint64 = 1547772882435375000
	var timestamp2 uint64 = 1547772960049177000
	rowKey1, err := json.Marshal(types.KeyObj{Timestamp: timestamp1})
	require.Nil(err, "json marshal err: %+v", err)
	rowKey2, err := json.Marshal(types.KeyObj{Timestamp: timestamp2})
	require.Nil(err, "json marshal err: %+v", err)

	// MetaDataResObj deserialize
	pubKeyBytes, err := base64.StdEncoding.DecodeString(TestPubKey)
	require.Nil(err, "base64 decode err: %+v", err)
	rMetaDataResObjs, err := json.Marshal(types.RMetaDataResObjs{types.RMetaDataResObj{RowKey: rowKey1, OwnerKey: pubKeyBytes, Qualifier: []byte(TestQualifier)}, types.RMetaDataResObj{RowKey: rowKey2, OwnerKey: pubKeyBytes, Qualifier: []byte(TestQualifier)}})
	require.Nil(err, "json marshal err: %+v", err)
	rClientMetaDataResObjs, err := json.Marshal(RClientMetaDataResObjs{RClientMetaDataResObj{RowKey: types.KeyObj{Timestamp: timestamp1}, OwnerKey: pubKeyBytes, Qualifier: []byte(TestQualifier)}, RClientMetaDataResObj{RowKey: types.KeyObj{Timestamp: timestamp2}, OwnerKey: pubKeyBytes, Qualifier: []byte(TestQualifier)}})
	require.Nil(err, "json marshal err: %+v", err)

	deserializedBytes, err := client.DeSerializeKeyObj(rMetaDataResObjs, true)
	require.Nil(err, "SerializeKeyObj err: %+v", err)

	require.EqualValues(rClientMetaDataResObjs, deserializedBytes)

	// RealDataResObj deserialize
	rRealDataResObjs, err := json.Marshal(types.RRealDataResObjs{types.RRealDataResObj{RowKey: rowKey1, Data: []byte("testData1")}, types.RRealDataResObj{RowKey: rowKey2, Data: []byte("testData2")}})
	require.Nil(err, "json marshal err: %+v", err)
	rClientRealDataResObjs, err := json.Marshal(RClientRealDataResObjs{RClientRealDataResObj{RowKey: types.KeyObj{Timestamp: timestamp1}, Data: []byte("testData1")}, RClientRealDataResObj{RowKey: types.KeyObj{Timestamp: timestamp2}, Data: []byte("testData2")}})
	require.Nil(err, "json marshal err: %+v", err)

	deserializedBytes, err = client.DeSerializeKeyObj(rRealDataResObjs, false)
	require.Nil(err, "SerializeKeyObj err: %+v", err)

	require.EqualValues(rClientRealDataResObjs, deserializedBytes)
}
