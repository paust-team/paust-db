package client_test

import (
	"encoding/base64"
	"encoding/json"
	"github.com/paust-team/paust-db/client"
	"github.com/paust-team/paust-db/types"
	"github.com/stretchr/testify/require"
	abci "github.com/tendermint/tendermint/abci/types"
	cmn "github.com/tendermint/tendermint/libs/common"
	tendermint "github.com/tendermint/tendermint/types"
	"time"
)

func (suite *ClientTestSuite) TestClient_WriteData() {
	require := require.New(suite.T())

	mempool := node.MempoolReactor().Mempool
	initMempoolSize := mempool.Size()

	timestamp := uint64(time.Now().UnixNano())
	data := []byte(cmn.RandStr(8))
	pubKeyBytes, err := base64.StdEncoding.DecodeString(TestPubKey)
	require.Nil(err, "base64 decode err: %+v", err)
	rowKey, err := json.Marshal(types.KeyObj{Timestamp: timestamp, Salt: suite.salt[0]})
	require.Nil(err, "json marshal err: %+v", err)
	tx, err := json.Marshal([]types.BaseDataObj{{MetaData: types.MetaDataObj{RowKey: rowKey, OwnerKey: pubKeyBytes, Qualifier: []byte(TestQualifier)}, RealData: types.RealDataObj{RowKey: rowKey, Data: data}}})
	require.Nil(err, "json marshal err: %+v", err)

	dataObjs := []client.InputDataObj{{Timestamp: timestamp, OwnerKey: pubKeyBytes, Qualifier: []byte(TestQualifier), Data: data}}
	bres, err := suite.dbClient.WriteData(dataObjs)

	require.Nil(err, "err: %+v", err)
	require.Equal(abci.CodeTypeOK, bres.Code)

	require.Equal(initMempoolSize+1, mempool.Size())

	txs := mempool.ReapMaxTxs(-1)
	require.EqualValues(tendermint.Tx(tx), txs[0])

	mempool.Flush()
}
