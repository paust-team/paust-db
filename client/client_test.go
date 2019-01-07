package client_test

import (
	"github.com/paust-team/paust-db/client"
	"github.com/paust-team/paust-db/master"
	"github.com/stretchr/testify/suite"
	cmn "github.com/tendermint/tendermint/libs/common"
	nm "github.com/tendermint/tendermint/node"
	"github.com/tendermint/tendermint/rpc/test"
	"os"
	"testing"
)

var node *nm.Node
var testDir string

const (
	TestPubKey   = "Pe8PPI4Mq7kJIjDJjffoTl6s5EezGQSyIcu5Y2KYDaE="
	TestDataType = "testType"
)

type ClientTestSuite struct {
	suite.Suite

	dbClient *client.Client
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

func (suite *ClientTestSuite) SetupTest() {
	suite.dbClient = client.NewLocalClient(node)
}

func TestClient(t *testing.T) {
	suite.Run(t, new(ClientTestSuite))
}
