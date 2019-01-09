package master_test

import (
	"github.com/paust-team/paust-db/master"
	"github.com/stretchr/testify/suite"
	"os"
	"testing"
)

const (
	testDir = "/tmp/mastertest"
	perm    = 0777
)

type MasterSuite struct {
	suite.Suite
	app *master.MasterApplication
}

func (suite *MasterSuite) SetupTest() {
	SetDir()
	suite.app = master.NewMasterApplication(true, testDir)
	suite.Require().NotNil(suite.app, "app should not be nil")
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
