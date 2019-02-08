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

func (suite *ClientTestSuite) TestClient_Query() {
	require := require.New(suite.T())

	mempool := node.MempoolReactor().Mempool
	timestamp := uint64(time.Now().UnixNano())
	data := []byte(cmn.RandStr(8))
	pubKeyBytes, err := base64.StdEncoding.DecodeString(TestPubKey)
	require.Nil(err, "base64 decode err: %+v", err)
	rowKey, err := json.Marshal(types.KeyObj{Timestamp: timestamp, Salt: 0})
	require.Nil(err, "json marshal err: %+v", err)
	tx, err := json.Marshal([]types.BaseDataObj{{MetaData: types.MetaDataObj{RowKey: rowKey, OwnerKey: pubKeyBytes, Qualifier: []byte(TestQualifier)}, RealData: types.RealDataObj{RowKey: rowKey, Data: data}}})
	require.Nil(err, "json marshal err: %+v", err)
	expectedValue, err := json.Marshal([]client.OutputMetaDataObj{{Id: rowKey, Timestamp: timestamp, OwnerKey: pubKeyBytes, Qualifier: []byte(TestQualifier)}})
	require.Nil(err, "json marshal err: %+v", err)

	c := rpcClient.NewLocal(node)
	bres, err := c.BroadcastTxCommit(tx)

	require.Nil(err, "err: %+v", err)
	require.True(bres.CheckTx.IsOK())
	require.True(bres.DeliverTx.IsOK())

	require.Equal(0, mempool.Size())

	res, err := suite.dbClient.Query(timestamp, timestamp+1, pubKeyBytes, []byte(TestQualifier))
	qres := res.Response
	if suite.Nil(err) && suite.True(qres.IsOK()) {
		suite.EqualValues(expectedValue, qres.Value)
	}
}

func (suite *ClientTestSuite) TestClient_Fetch() {
	require := require.New(suite.T())

	mempool := node.MempoolReactor().Mempool
	timestamp := uint64(time.Now().UnixNano())
	data := []byte(cmn.RandStr(8))
	pubKeyBytes, err := base64.StdEncoding.DecodeString(TestPubKey)
	require.Nil(err, "base64 decode err: %+v", err)
	rowKey, err := json.Marshal(types.KeyObj{Timestamp: timestamp, Salt: 0})
	require.Nil(err, "json marshal err: %+v", err)
	tx, err := json.Marshal([]types.BaseDataObj{{MetaData: types.MetaDataObj{RowKey: rowKey, OwnerKey: pubKeyBytes, Qualifier: []byte(TestQualifier)}, RealData: types.RealDataObj{RowKey: rowKey, Data: data}}})
	require.Nil(err, "json marshal err: %+v", err)
	expectedValue, err := json.Marshal([]client.OutputRealDataObj{{Id: rowKey, Timestamp: timestamp, Data: data}})
	require.Nil(err, "json marshal err: %+v", err)

	c := rpcClient.NewLocal(node)
	bres, err := c.BroadcastTxCommit(tx)

	require.Nil(err, "err: %+v", err)
	require.True(bres.CheckTx.IsOK())
	require.True(bres.DeliverTx.IsOK())

	require.Equal(0, mempool.Size())

	queryObj := client.InputQueryObj{Ids: [][]byte{rowKey}}
	res, err := suite.dbClient.Fetch(queryObj)
	qres := res.Response
	if suite.Nil(err) && suite.True(qres.IsOK()) {
		suite.EqualValues(expectedValue, qres.Value)
	}
}
