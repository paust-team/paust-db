package client_test

import (
	"encoding/base64"
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
	pubKeyBytes, err := base64.StdEncoding.DecodeString(TestPubKey)
	require.Nil(err, "base64 decode err: %+v", err)
	dataObjs := []client.InputDataObj{{Timestamp: timestamp, OwnerKey: pubKeyBytes, Qualifier: TestQualifier, Data: data}}
	bres, err := suite.dbClient.Put(dataObjs)

	require.Nil(err, "err: %+v", err)
	require.True(bres.CheckTx.IsOK())
	require.True(bres.DeliverTx.IsOK())

	require.Equal(0, mempool.Size())
}
