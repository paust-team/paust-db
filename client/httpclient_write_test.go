package client_test

import (
	"github.com/paust-team/paust-db/client"
	"github.com/stretchr/testify/require"
	cmn "github.com/tendermint/tendermint/libs/common"
	"time"
)

func (suite *ClientTestSuite) TestClient_Put() {
	require := require.New(suite.T())

	mempool := node.MempoolReactor().Mempool

	timestamp := uint64(time.Now().UnixNano())
	data := []byte(cmn.RandStr(8))
	dataObjs := []client.InputDataObj{{Timestamp: timestamp, OwnerId: TestOwnerId, Qualifier: TestQualifier, Data: data}}
	bres, err := suite.dbClient.Put(dataObjs)

	require.Nil(err, "err: %+v", err)
	require.True(bres.CheckTx.IsOK())
	require.True(bres.DeliverTx.IsOK())

	require.Equal(0, mempool.Size())
}
