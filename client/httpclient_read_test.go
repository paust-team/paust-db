package client_test

import (
	"encoding/base64"
	"encoding/binary"
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
	timestampBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(timestampBytes, timestamp)
	data := []byte(cmn.RandStr(8))
	pubKeyBytes, err := base64.StdEncoding.DecodeString(TestPubKey)
	require.Nil(err, "base64 decode err: %+v", err)
	salt := make([]byte, 2)
	binary.BigEndian.PutUint16(salt, 0)
	rowKey, err := json.Marshal(types.KeyObj{Timestamp: timestampBytes, Salt: salt})
	require.Nil(err, "json marshal err: %+v", err)
	tx, err := json.Marshal([]types.BaseDataObj{{MetaData: types.MetaDataObj{RowKey: rowKey, OwnerKey: pubKeyBytes, Qualifier: []byte(TestQualifier)}, RealData: types.RealDataObj{RowKey: rowKey, Data: data}}})
	require.Nil(err, "json marshal err: %+v", err)
	expectedValue, err := json.Marshal([]client.OutputQueryObj{{Id: rowKey, Timestamp: timestamp, OwnerKey: pubKeyBytes, Qualifier: TestQualifier}})
	require.Nil(err, "json marshal err: %+v", err)

	c := rpcClient.NewLocal(node)
	bres, err := c.BroadcastTxCommit(tx)

	require.Nil(err, "err: %+v", err)
	require.True(bres.CheckTx.IsOK())
	require.True(bres.DeliverTx.IsOK())

	require.Equal(0, mempool.Size())

	res, err := suite.dbClient.Query(timestamp, timestamp+1, pubKeyBytes, TestQualifier)
	qres := res.Response
	if suite.Nil(err) && suite.True(qres.IsOK()) {
		suite.EqualValues(expectedValue, qres.Value)
	}
}

func (suite *ClientTestSuite) TestClient_Fetch() {
	require := require.New(suite.T())

	mempool := node.MempoolReactor().Mempool
	timestamp := uint64(time.Now().UnixNano())
	timestampBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(timestampBytes, timestamp)
	data := []byte(cmn.RandStr(8))
	pubKeyBytes, err := base64.StdEncoding.DecodeString(TestPubKey)
	require.Nil(err, "base64 decode err: %+v", err)
	salt := make([]byte, 2)
	binary.BigEndian.PutUint16(salt, 0)
	rowKey, err := json.Marshal(types.KeyObj{Timestamp: timestampBytes, Salt: salt})
	require.Nil(err, "json marshal err: %+v", err)
	tx, err := json.Marshal([]types.BaseDataObj{{MetaData: types.MetaDataObj{RowKey: rowKey, OwnerKey: pubKeyBytes, Qualifier: []byte(TestQualifier)}, RealData: types.RealDataObj{RowKey: rowKey, Data: data}}})
	require.Nil(err, "json marshal err: %+v", err)
	expectedValue, err := json.Marshal([]client.OutputFetchObj{{Id: rowKey, Timestamp: timestamp, Data: data}})
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
