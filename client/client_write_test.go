package client_test

import (
	"encoding/base64"
	"encoding/json"
	"github.com/paust-team/paust-db/types"
	"github.com/stretchr/testify/require"
	abci "github.com/tendermint/tendermint/abci/types"
	cmn "github.com/tendermint/tendermint/libs/common"
	tendermint "github.com/tendermint/tendermint/types"
	"io/ioutil"
	"time"
)

func (suite *ClientTestSuite) TestClient_WriteData() {
	require := require.New(suite.T())

	mempool := node.MempoolReactor().Mempool
	initMempoolSize := mempool.Size()

	time := time.Now()
	data := []byte(cmn.RandStr(8))
	pubKeyBytes, err := base64.StdEncoding.DecodeString(TestPubKey)
	require.Nil(err, "base64 decode err: %+v", err)
	tx, err := json.Marshal(types.DataSlice{types.Data{Timestamp: time.UnixNano(), UserKey: pubKeyBytes, Qualifier: TestQualifier, Data: data}})
	require.Nil(err, "json marshal err: %+v", err)

	bres, err := suite.dbClient.WriteData(time, TestPubKey, TestQualifier, data)

	require.Nil(err, "err: %+v", err)
	require.Equal(bres.Code, abci.CodeTypeOK)

	require.Equal(initMempoolSize+1, mempool.Size())

	txs := mempool.ReapMaxTxs(-1)
	require.EqualValues(tendermint.Tx(tx), txs[0])

	mempool.Flush()
}

func (suite *ClientTestSuite) TestClient_WriteFile() {
	require := require.New(suite.T())

	mempool := node.MempoolReactor().Mempool
	initMempoolSize := mempool.Size()

	bytes, err := ioutil.ReadFile("../test/writeFile.json")
	require.Nil(err, "file read err: %+v", err)

	bres, err := suite.dbClient.WriteFile("../test/writeFile.json")

	require.Nil(err, "err: %+v", err)
	require.Equal(bres.Code, abci.CodeTypeOK)

	require.Equal(initMempoolSize+1, mempool.Size())

	txs := mempool.ReapMaxTxs(-1)
	require.EqualValues(tendermint.Tx(bytes), txs[0])

	mempool.Flush()
}
