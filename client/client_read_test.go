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
	rowKey, err := json.Marshal(types.KeyObj{Timestamp: uint64(time.UnixNano()), Salt: 0})
	require.Nil(err, "json marshal err: %+v", err)
	tx, err := json.Marshal([]types.BaseDataObj{{MetaData: types.MetaDataObj{RowKey: rowKey, OwnerKey: pubKeyBytes, Qualifier: []byte(TestQualifier)}, RealData: types.RealDataObj{RowKey: rowKey, Data: data}}})
	require.Nil(err, "json marshal err: %+v", err)
	expectedValue, err := json.Marshal([]types.RealDataObj{{RowKey: rowKey, Data: data}})
	require.Nil(err, "json marshal err: %+v", err)

	c := rpcClient.NewLocal(node)
	bres, err := c.BroadcastTxCommit(tx)

	require.Nil(err, "err: %+v", err)
	require.True(bres.CheckTx.IsOK())
	require.True(bres.DeliverTx.IsOK())

	require.Equal(0, mempool.Size())

	res, err := suite.dbClient.ReadData([]string{base64.StdEncoding.EncodeToString(rowKey)})
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
	rowKey1, err := json.Marshal(types.KeyObj{Timestamp: 1547772882435375000, Salt: 0})
	require.Nil(err, "json marshal err: %+v", err)
	rowKey2, err := json.Marshal(types.KeyObj{Timestamp: 1547772960049177000, Salt: 0})
	require.Nil(err, "json marshal err: %+v", err)
	rowKey3, err := json.Marshal(types.KeyObj{Timestamp: 1547772967331458000, Salt: 0})
	require.Nil(err, "json marshal err: %+v", err)
	tx, err := json.Marshal([]types.BaseDataObj{
		{MetaData: types.MetaDataObj{RowKey: rowKey1, OwnerKey: pubKeyBytes, Qualifier: []byte(TestQualifier)}, RealData: types.RealDataObj{RowKey: rowKey1, Data: []byte("testData1")}},
		{MetaData: types.MetaDataObj{RowKey: rowKey2, OwnerKey: pubKeyBytes, Qualifier: []byte(TestQualifier)}, RealData: types.RealDataObj{RowKey: rowKey2, Data: []byte("testData2")}},
		{MetaData: types.MetaDataObj{RowKey: rowKey3, OwnerKey: pubKeyBytes, Qualifier: []byte(TestQualifier)}, RealData: types.RealDataObj{RowKey: rowKey3, Data: []byte("testData3")}},
	})
	require.Nil(err, "json marshal err: %+v", err)
	expectedValue, err := json.Marshal([]types.RealDataObj{
		{RowKey: rowKey1, Data: []byte("testData1")},
		{RowKey: rowKey2, Data: []byte("testData2")},
		{RowKey: rowKey3, Data: []byte("testData3")},
	})
	require.Nil(err, "json marshal err: %+v", err)

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
	rowKey, err := json.Marshal(types.KeyObj{Timestamp: uint64(time.UnixNano()), Salt: 0})
	require.Nil(err, "json marshal err: %+v", err)
	tx, err := json.Marshal([]types.BaseDataObj{{MetaData: types.MetaDataObj{RowKey: rowKey, OwnerKey: pubKeyBytes, Qualifier: []byte(TestQualifier)}, RealData: types.RealDataObj{RowKey: rowKey, Data: data}}})
	require.Nil(err, "json marshal err: %+v", err)
	expectedValue, err := json.Marshal([]types.MetaDataObj{{RowKey: rowKey, OwnerKey: pubKeyBytes, Qualifier: []byte(TestQualifier)}})
	require.Nil(err, "json marshal err: %+v", err)

	c := rpcClient.NewLocal(node)
	bres, err := c.BroadcastTxCommit(tx)

	require.Nil(err, "err: %+v", err)
	require.True(bres.CheckTx.IsOK())
	require.True(bres.DeliverTx.IsOK())

	require.Equal(0, mempool.Size())

	res, err := suite.dbClient.ReadMetaData(uint64(time.UnixNano()), uint64(time.UnixNano())+1, "", "")
	qres := res.Response
	if suite.Nil(err) && suite.True(qres.IsOK()) {
		suite.EqualValues(expectedValue, qres.Value)
	}
}

func (suite *ClientTestSuite) TestDeSerializeKeyObj() {
	require := require.New(suite.T())

	var timestamp1 uint64 = 1547772882435375000
	var timestamp2 uint64 = 1547772960049177000
	rowKey1, err := json.Marshal(types.KeyObj{Timestamp: timestamp1, Salt: 0})
	require.Nil(err, "json marshal err: %+v", err)
	rowKey2, err := json.Marshal(types.KeyObj{Timestamp: timestamp2, Salt: 0})
	require.Nil(err, "json marshal err: %+v", err)

	// MetaDataResObj deserialize
	pubKeyBytes, err := base64.StdEncoding.DecodeString(TestPubKey)
	require.Nil(err, "base64 decode err: %+v", err)
	metaDataObjs, err := json.Marshal([]types.MetaDataObj{{RowKey: rowKey1, OwnerKey: pubKeyBytes, Qualifier: []byte(TestQualifier)}, {RowKey: rowKey2, OwnerKey: pubKeyBytes, Qualifier: []byte(TestQualifier)}})
	require.Nil(err, "json marshal err: %+v", err)
	clientMetaDataObjs, err := json.Marshal([]client.MetaDataObj{{Id: rowKey1, Timestamp: timestamp1, OwnerKey: pubKeyBytes, Qualifier: []byte(TestQualifier)}, {Id: rowKey2, Timestamp: timestamp2, OwnerKey: pubKeyBytes, Qualifier: []byte(TestQualifier)}})
	require.Nil(err, "json marshal err: %+v", err)

	deserializedBytes, err := client.DeSerializeKeyObj(metaDataObjs, true)
	require.Nil(err, "SerializeKeyObj err: %+v", err)

	require.EqualValues(clientMetaDataObjs, deserializedBytes)

	// RealDataResObj deserialize
	realDataObjs, err := json.Marshal([]types.RealDataObj{{RowKey: rowKey1, Data: []byte("testData1")}, {RowKey: rowKey2, Data: []byte("testData2")}})
	require.Nil(err, "json marshal err: %+v", err)
	clientRealDataObjs, err := json.Marshal([]client.RealDataObj{{Id: rowKey1, Timestamp: timestamp1, Data: []byte("testData1")}, {Id: rowKey2, Timestamp: timestamp2, Data: []byte("testData2")}})
	require.Nil(err, "json marshal err: %+v", err)

	deserializedBytes, err = client.DeSerializeKeyObj(realDataObjs, false)
	require.Nil(err, "SerializeKeyObj err: %+v", err)

	require.EqualValues(clientRealDataObjs, deserializedBytes)
}
