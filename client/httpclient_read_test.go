package client_test

import (
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

	rowKey := types.GetRowKey(timestamp, 0)
	tx, err := json.Marshal([]types.BaseDataObj{{MetaData: types.MetaDataObj{RowKey: rowKey, OwnerId: TestOwnerId, Qualifier: []byte(TestQualifier)}, RealData: types.RealDataObj{RowKey: rowKey, Data: data}}})
	require.Nil(err, "json marshal err: %+v", err)
	expectedValue, err := json.MarshalIndent([]client.OutputQueryObj{{Id: rowKey, Timestamp: timestamp, OwnerId: TestOwnerId, Qualifier: TestQualifier}}, "", "    ")
	require.Nil(err, "json marshal err: %+v", err)

	c := rpcClient.NewLocal(node)
	bres, err := c.BroadcastTxCommit(tx)

	require.Nil(err, "err: %+v", err)
	require.True(bres.CheckTx.IsOK())
	require.True(bres.DeliverTx.IsOK())

	require.Equal(0, mempool.Size())

	res, err := suite.dbClient.Query(client.InputQueryObj{Start: timestamp, End: timestamp + 1, OwnerId: TestOwnerId, Qualifier: TestQualifier})
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
	rowKey := types.GetRowKey(timestamp, 0)
	tx, err := json.Marshal([]types.BaseDataObj{{MetaData: types.MetaDataObj{RowKey: rowKey, OwnerId: TestOwnerId, Qualifier: []byte(TestQualifier)}, RealData: types.RealDataObj{RowKey: rowKey, Data: data}}})
	require.Nil(err, "json marshal err: %+v", err)
	expectedValue, err := json.MarshalIndent([]client.OutputFetchObj{{Id: rowKey, Timestamp: timestamp, Data: data}}, "", "    ")
	require.Nil(err, "json marshal err: %+v", err)

	c := rpcClient.NewLocal(node)
	bres, err := c.BroadcastTxCommit(tx)

	require.Nil(err, "err: %+v", err)
	require.True(bres.CheckTx.IsOK())
	require.True(bres.DeliverTx.IsOK())

	require.Equal(0, mempool.Size())

	fetchObj := client.InputFetchObj{Ids: [][]byte{rowKey}}
	res, err := suite.dbClient.Fetch(fetchObj)
	qres := res.Response
	if suite.Nil(err) && suite.True(qres.IsOK()) {
		suite.EqualValues(expectedValue, qres.Value)
	}
}
