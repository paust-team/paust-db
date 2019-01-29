package master_test

import (
	"github.com/paust-team/paust-db/master"
	"github.com/stretchr/testify/suite"
	"os"
	"testing"
)

// db folder관련 상수
const (
	testDir = "/tmp/mastertest"
	perm    = 0777
)

//db test 관련 상수
const (
	TestPubKey = "oimd8ZdzgUHzF9CPChJU8gb89VaMYg+1SpX6WT8nQHE="
	TestPubKey2    = "Pe8PPI4Mq7kJIjDJjffoTl6s5EezGQSyIcu5Y2KYDaE="

)

type MasterSuite struct {
	suite.Suite
	app *master.MasterApplication
}

func (suite *MasterSuite) SetupTest() {
	require := suite.Require()
	SetDir()
	suite.app = master.NewMasterApplication(true, testDir)
	require.NotNil(suite.app, "app should not be nil")
}

func (suite *MasterSuite) TearDownTest() {
	suite.app.DB().Close()
}

func TestSuite(t *testing.T) {
	suite.Run(t, new(MasterSuite))
}

func SetDir() {
	os.RemoveAll(testDir)
	os.Mkdir(testDir, perm)
}
