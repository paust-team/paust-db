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
	"io/ioutil"
	"os"
	"path/filepath"
	"time"
)

const (
	TestWriteFile = "../test/write_file.json"
	TestDirectory = "../test/write_directory"
)

func (suite *ClientTestSuite) TestClient_WriteDataFixedSalt() {
	require := require.New(suite.T())

	mempool := node.MempoolReactor().Mempool
	initMempoolSize := mempool.Size()

	time := time.Now()
	data := []byte(cmn.RandStr(8))
	pubKeyBytes, err := base64.StdEncoding.DecodeString(TestPubKey)
	require.Nil(err, "base64 decode err: %+v", err)
	rowKey, err := json.Marshal(types.KeyObj{Timestamp: uint64(time.Unix()), Salt: 0})
	require.Nil(err, "json marshal err: %+v", err)
	tx, err := json.Marshal([]types.BaseDataObj{{MetaData: types.MetaDataObj{RowKey: rowKey, OwnerKey: pubKeyBytes, Qualifier: []byte(TestQualifier)}, RealData: types.RealDataObj{RowKey: rowKey, Data: data}}})
	require.Nil(err, "json marshal err: %+v", err)

	bres, err := suite.dbClient.WriteDataFixedSalt(time, TestPubKey, TestQualifier, data)

	require.Nil(err, "err: %+v", err)
	require.Equal(bres.Code, abci.CodeTypeOK)

	require.Equal(initMempoolSize+1, mempool.Size())

	txs := mempool.ReapMaxTxs(-1)
	require.EqualValues(tendermint.Tx(tx), txs[0])

	mempool.Flush()
}

func (suite *ClientTestSuite) TestClient_WriteFileFixedSalt() {
	require := require.New(suite.T())

	mempool := node.MempoolReactor().Mempool
	initMempoolSize := mempool.Size()

	bytes, err := ioutil.ReadFile(TestWriteFile)
	require.Nil(err, "file read err: %+v", err)

	var writeDataObjs []client.WriteDataObj

	err = json.Unmarshal(bytes, &writeDataObjs)
	require.Nil(err, "json unmarshal err: %+v", err)

	var baseDataObjs []types.BaseDataObj

	for _, writeDataObj := range writeDataObjs {
		rowKey, err := json.Marshal(types.KeyObj{Timestamp: writeDataObj.Timestamp, Salt: 0})
		require.Nil(err, "json marshal err: %+v", err)
		baseDataObjs = append(baseDataObjs, types.BaseDataObj{MetaData: types.MetaDataObj{RowKey: rowKey, OwnerKey: writeDataObj.OwnerKey, Qualifier: writeDataObj.Qualifier}, RealData: types.RealDataObj{RowKey: rowKey, Data: writeDataObj.Data}})
	}

	tx, err := json.Marshal(baseDataObjs)
	require.Nil(err, "json marshal err: %+v", err)

	bres, err := suite.dbClient.WriteFileFixedSalt(TestWriteFile)

	require.Nil(err, "err: %+v", err)
	require.Equal(bres.Code, abci.CodeTypeOK)

	require.Equal(initMempoolSize+1, mempool.Size())

	txs := mempool.ReapMaxTxs(-1)
	require.EqualValues(tendermint.Tx(tx), txs[0])

	mempool.Flush()
}

func (suite *ClientTestSuite) TestClient_WriteFilesInDirFixedSalt() {
	require := require.New(suite.T())

	var fileBytes [][]byte

	mempool := node.MempoolReactor().Mempool
	initMempoolSize := mempool.Size()

	err := filepath.Walk(TestDirectory, func(path string, info os.FileInfo, err error) error {
		require.Nil(err, "directory traverse err: %+v", err)
		switch {
		case info.IsDir() == true && path != TestDirectory:
			return filepath.SkipDir
		case info.IsDir() == false && ".json" == filepath.Ext(path):
			bytes, err := ioutil.ReadFile(path)
			require.Nil(err, "file read err: %+v", err)

			var writeDataObjs []client.WriteDataObj

			err = json.Unmarshal(bytes, &writeDataObjs)
			require.Nil(err, "json unmarshal err: %+v", err)

			var baseDataObjs []types.BaseDataObj

			for _, writeDataObj := range writeDataObjs {
				rowKey, err := json.Marshal(types.KeyObj{Timestamp: writeDataObj.Timestamp, Salt: 0})
				require.Nil(err, "json marshal err: %+v", err)
				baseDataObjs = append(baseDataObjs, types.BaseDataObj{MetaData: types.MetaDataObj{RowKey: rowKey, OwnerKey: writeDataObj.OwnerKey, Qualifier: writeDataObj.Qualifier}, RealData: types.RealDataObj{RowKey: rowKey, Data: writeDataObj.Data}})
			}

			tx, err := json.Marshal(baseDataObjs)
			require.Nil(err, "json marshal err: %+v", err)

			fileBytes = append(fileBytes, tx)
			return nil
		default:
			return nil
		}
	})
	require.Nil(err, "directory traverse err: %+v", err)

	suite.dbClient.WriteFilesInDirFixedSalt(TestDirectory, false)

	require.Equal(initMempoolSize+len(fileBytes), mempool.Size())

	txs := mempool.ReapMaxTxs(-1)
	for i, bytes := range fileBytes {
		require.EqualValues(tendermint.Tx(bytes), txs[i])
	}

	mempool.Flush()
}

func (suite *ClientTestSuite) TestClient_WriteFilesInDirFixedSaltRecursive() {
	require := require.New(suite.T())

	var fileBytes [][]byte

	mempool := node.MempoolReactor().Mempool
	initMempoolSize := mempool.Size()

	err := filepath.Walk(TestDirectory, func(path string, info os.FileInfo, err error) error {
		require.Nil(err, "directory traverse err: %+v", err)
		if info.IsDir() == false && ".json" == filepath.Ext(path) {
			bytes, err := ioutil.ReadFile(path)
			require.Nil(err, "file read err: %+v", err)

			var writeDataObjs []client.WriteDataObj

			err = json.Unmarshal(bytes, &writeDataObjs)
			require.Nil(err, "json unmarshal err: %+v", err)

			var baseDataObjs []types.BaseDataObj

			for _, writeDataObj := range writeDataObjs {
				rowKey, err := json.Marshal(types.KeyObj{Timestamp: writeDataObj.Timestamp, Salt: 0})
				require.Nil(err, "json marshal err: %+v", err)
				baseDataObjs = append(baseDataObjs, types.BaseDataObj{MetaData: types.MetaDataObj{RowKey: rowKey, OwnerKey: writeDataObj.OwnerKey, Qualifier: writeDataObj.Qualifier}, RealData: types.RealDataObj{RowKey: rowKey, Data: writeDataObj.Data}})
			}

			tx, err := json.Marshal(baseDataObjs)
			require.Nil(err, "json marshal err: %+v", err)

			fileBytes = append(fileBytes, tx)
			return nil
		} else {
			return nil
		}
	})
	require.Nil(err, "directory traverse err: %+v", err)

	suite.dbClient.WriteFilesInDirFixedSalt(TestDirectory, true)

	require.Equal(initMempoolSize+len(fileBytes), mempool.Size())

	txs := mempool.ReapMaxTxs(-1)
	for i, bytes := range fileBytes {
		require.EqualValues(tendermint.Tx(bytes), txs[i])
	}

	mempool.Flush()
}
