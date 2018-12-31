package client_test

import (
	"encoding/base64"
	"encoding/json"
	"github.com/paust-team/paust-db/client"
	"github.com/paust-team/paust-db/master"
	"github.com/paust-team/paust-db/types"
	"github.com/stretchr/testify/suite"
	abci "github.com/tendermint/tendermint/abci/types"
	cmn "github.com/tendermint/tendermint/libs/common"
	nm "github.com/tendermint/tendermint/node"
	rpcClient "github.com/tendermint/tendermint/rpc/client"
	"github.com/tendermint/tendermint/rpc/test"
	tendermint "github.com/tendermint/tendermint/types"
	"io/ioutil"
	"os"
	"testing"
	"time"
)

var node *nm.Node
var testDir string

const (
	TestPubKey   = "Pe8PPI4Mq7kJIjDJjffoTl6s5EezGQSyIcu5Y2KYDaE="
	TestDataType = "testType"
)

type ClientTestSuite struct {
	suite.Suite
}

func (suite *ClientTestSuite) SetupSuite() {
	testDir = "/tmp/" + cmn.RandStr(4)
	os.MkdirAll(testDir, os.ModePerm)
	app := master.NewMasterApplication(true, testDir)
	node = rpctest.StartTendermint(app)
}

func (suite *ClientTestSuite) TearDownSuite() {
	node.Stop()
	node.Wait()
	os.RemoveAll(testDir)
}

func (suite *ClientTestSuite) TestClient_WriteData() {
	mempool := node.MempoolReactor().Mempool
	initMempoolSize := mempool.Size()

	time := time.Now()
	data := []byte(cmn.RandStr(8))
	pubKeyBytes, _ := base64.StdEncoding.DecodeString(TestPubKey)
	tx, _ := json.Marshal(types.DataSlice{types.Data{Timestamp: time.UnixNano(), UserKey: pubKeyBytes, Type: TestDataType, Data: data}})

	dbClient := client.NewLocalClient(node)
	bres, err := dbClient.WriteData(time, TestPubKey, TestDataType, data)

	suite.Nil(err, "err: %+v", err)
	suite.Equal(bres.Code, abci.CodeTypeOK)

	suite.Equal(initMempoolSize+1, mempool.Size())

	txs := mempool.ReapMaxTxs(-1)
	suite.Equal(tendermint.Tx(tx), txs[0])

	mempool.Flush()
}

func (suite *ClientTestSuite) TestClient_ReadData() {
	time := time.Now()
	data := []byte(cmn.RandStr(8))
	pubKeyBytes, _ := base64.StdEncoding.DecodeString(TestPubKey)
	tx, _ := json.Marshal(types.DataSlice{types.Data{Timestamp: time.UnixNano(), UserKey: pubKeyBytes, Type: TestDataType, Data: data}})

	c := rpcClient.NewLocal(node)
	_, err := c.BroadcastTxCommit(tx)
	suite.Nil(err, "err: %+v", err)

	dbClient := client.NewLocalClient(node)
	res, err := dbClient.ReadData(time.UnixNano(), time.UnixNano()+1, TestPubKey, TestDataType)
	qres := res.Response
	if suite.Nil(err) && suite.True(qres.IsOK()) {
		suite.EqualValues(tx, qres.Value)
	}
}

func (suite *ClientTestSuite) TestClient_ReadMetaData() {
	time := time.Now()
	data := []byte(cmn.RandStr(8))
	pubKeyBytes, _ := base64.StdEncoding.DecodeString(TestPubKey)
	tx, _ := json.Marshal(types.DataSlice{types.Data{Timestamp: time.UnixNano(), UserKey: pubKeyBytes, Type: TestDataType, Data: data}})
	expectedValue, _ := json.Marshal(types.MetaResponseSlice{types.MetaResponse{Timestamp: time.UnixNano(), UserKey: pubKeyBytes, Type: TestDataType}})

	c := rpcClient.NewLocal(node)
	_, err := c.BroadcastTxCommit(tx)
	suite.Nil(err, "err: %+v", err)

	dbClient := client.NewLocalClient(node)
	res, err := dbClient.ReadMetaData(time.UnixNano(), time.UnixNano()+1, TestPubKey, TestDataType)
	qres := res.Response
	if suite.Nil(err) && suite.True(qres.IsOK()) {
		suite.EqualValues(expectedValue, qres.Value)
	}
}

func (suite *ClientTestSuite) TestClient_WriteFile() {
	mempool := node.MempoolReactor().Mempool
	initMempoolSize := mempool.Size()

	dbClient := client.NewLocalClient(node)
	bres, err := dbClient.WriteFile("../test/writeFile.json")
	bytes, err := ioutil.ReadFile("../test/writeFile.json")

	suite.Nil(err, "err: %+v", err)
	suite.Equal(bres.Code, abci.CodeTypeOK)

	suite.Equal(initMempoolSize+1, mempool.Size())

	txs := mempool.ReapMaxTxs(-1)
	suite.Equal(tendermint.Tx(bytes), txs[0])

	mempool.Flush()
}

func TestClient(t *testing.T) {
	suite.Run(t, new(ClientTestSuite))
}
