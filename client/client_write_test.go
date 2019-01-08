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
	"os"
	"path/filepath"
	"time"
)

const (
	TestFile      = "../test/write_file.json"
	TestDirectory = "../test/write_directory"
)

func (suite *ClientTestSuite) TestClient_WriteData() {
	require := require.New(suite.T())

	mempool := node.MempoolReactor().Mempool
	initMempoolSize := mempool.Size()

	time := time.Now()
	data := []byte(cmn.RandStr(8))
	pubKeyBytes, err := base64.StdEncoding.DecodeString(TestPubKey)
	require.Nil(err, "base64 decode err: %+v", err)
	tx, err := json.Marshal(types.DataSlice{types.RealData{Timestamp: time.UnixNano(), UserKey: pubKeyBytes, Qualifier: TestQualifier, Data: data}})
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

	bytes, err := ioutil.ReadFile(TestFile)
	require.Nil(err, "file read err: %+v", err)

	bres, err := suite.dbClient.WriteFile(TestFile)

	require.Nil(err, "err: %+v", err)
	require.Equal(bres.Code, abci.CodeTypeOK)

	require.Equal(initMempoolSize+1, mempool.Size())

	txs := mempool.ReapMaxTxs(-1)
	require.EqualValues(tendermint.Tx(bytes), txs[0])

	mempool.Flush()
}

func (suite *ClientTestSuite) TestClient_WriteDirectory() {
	require := require.New(suite.T())

	var fileBytes [][]byte

	mempool := node.MempoolReactor().Mempool
	initMempoolSize := mempool.Size()

	err := filepath.Walk(TestDirectory, func(path string, info os.FileInfo, err error) error {
		require.Nil(err, "directory traverse err: %v\n", err)
		switch {
		case info.IsDir() == true && path != TestDirectory:
			return filepath.SkipDir
		case ".json" == filepath.Ext(path) && info.IsDir() == false:
			bytes, err := ioutil.ReadFile(path)
			require.Nil(err, "file read err: %+v", err)
			fileBytes = append(fileBytes, bytes)
			return nil
		default:
			return nil
		}
	})
	require.Nil(err, "directory traverse err: %+v", err)

	suite.dbClient.WriteDirectory(TestDirectory, false)

	require.Equal(initMempoolSize+len(fileBytes), mempool.Size())

	txs := mempool.ReapMaxTxs(-1)
	for i, bytes := range fileBytes {
		require.EqualValues(tendermint.Tx(bytes), txs[i])
	}

	mempool.Flush()
}

func (suite *ClientTestSuite) TestClient_WriteDirectoryRecursive() {
	require := require.New(suite.T())

	var fileBytes [][]byte

	mempool := node.MempoolReactor().Mempool
	initMempoolSize := mempool.Size()

	err := filepath.Walk(TestDirectory, func(path string, info os.FileInfo, err error) error {
		require.Nil(err, "directory traverse err: %v\n", err)
		if ".json" == filepath.Ext(path) && info.IsDir() == false {
			bytes, err := ioutil.ReadFile(path)
			require.Nil(err, "file read err: %+v", err)
			fileBytes = append(fileBytes, bytes)
			return nil
		} else {
			return nil
		}
	})
	require.Nil(err, "directory traverse err: %+v", err)

	suite.dbClient.WriteDirectory(TestDirectory, true)

	require.Equal(initMempoolSize+len(fileBytes), mempool.Size())

	txs := mempool.ReapMaxTxs(-1)
	for i, bytes := range fileBytes {
		require.EqualValues(tendermint.Tx(bytes), txs[i])
	}

	mempool.Flush()
}
