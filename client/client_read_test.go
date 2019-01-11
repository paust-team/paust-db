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

func (suite *ClientTestSuite) TestClient_ReadData() {
	require := require.New(suite.T())

	mempool := node.MempoolReactor().Mempool
	time := time.Now()
	data := []byte(cmn.RandStr(8))
	pubKeyBytes, err := base64.StdEncoding.DecodeString(TestPubKey)
	require.Nil(err, "base64 decode err: %+v", err)
	tx, err := json.Marshal(types.RealDataSlice{types.RealData{Timestamp: time.UnixNano(), UserKey: pubKeyBytes, Qualifier: TestQualifier, Data: data}})
	require.Nil(err, "json marshal err: %+v", err)

	c := rpcClient.NewLocal(node)
	bres, err := c.BroadcastTxCommit(tx)

	require.Nil(err, "err: %+v", err)
	require.True(bres.CheckTx.IsOK())
	require.True(bres.DeliverTx.IsOK())

	require.Equal(0, mempool.Size())

	res, err := suite.dbClient.ReadData(time.UnixNano(), time.UnixNano()+1, TestPubKey, TestQualifier)
	qres := res.Response
	if suite.Nil(err) && suite.True(qres.IsOK()) {
		suite.EqualValues(tx, qres.Value)
	}
}

func (suite *ClientTestSuite) TestClient_ReadMetaData() {
	require := require.New(suite.T())

	mempool := node.MempoolReactor().Mempool
	time := time.Now()
	data := []byte(cmn.RandStr(8))
	pubKeyBytes, err := base64.StdEncoding.DecodeString(TestPubKey)
	require.Nil(err, "base64 decode err: %+v", err)
	tx, err := json.Marshal(types.RealDataSlice{types.RealData{Timestamp: time.UnixNano(), UserKey: pubKeyBytes, Qualifier: TestQualifier, Data: data}})
	require.Nil(err, "json marshal err: %+v", err)
	expectedValue, err := json.Marshal(types.MetaResponseSlice{types.MetaResponse{Timestamp: time.UnixNano(), UserKey: pubKeyBytes, Qualifier: TestQualifier}})
	require.Nil(err, "json marshal err: %+v", err)

	c := rpcClient.NewLocal(node)
	bres, err := c.BroadcastTxCommit(tx)

	require.Nil(err, "err: %+v", err)
	require.True(bres.CheckTx.IsOK())
	require.True(bres.DeliverTx.IsOK())

	require.Equal(0, mempool.Size())

	res, err := suite.dbClient.ReadMetaData(time.UnixNano(), time.UnixNano()+1, TestPubKey, TestQualifier)
	qres := res.Response
	if suite.Nil(err) && suite.True(qres.IsOK()) {
		suite.EqualValues(expectedValue, qres.Value)
	}
}
