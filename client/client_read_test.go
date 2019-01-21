package client_test

import (
	"encoding/base64"
	"encoding/json"
	"github.com/paust-team/paust-db/types"
	"github.com/stretchr/testify/require"
	cmn "github.com/tendermint/tendermint/libs/common"
	rpcClient "github.com/tendermint/tendermint/rpc/client"
	"time"
)

const (
	TestReadFile = "../test/read_file.json"
)

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
