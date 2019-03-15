package master_test

import (
	"github.com/paust-team/paust-db/libs/log"
	"github.com/paust-team/paust-db/master"
	"github.com/paust-team/paust-db/types"
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
	TestOwnerId  = "owner1"
	TestOwnerId2 = "owner2"
)

//test data
var (
	givenRowKey1, givenRowKey2           []byte
	givenMetaDataObj1, givenMetaDataObj2 types.MetaDataObj
	givenRealDataObj1, givenRealDataObj2 types.RealDataObj
	givenBaseDataObj1, givenBaseDataObj2 types.BaseDataObj
	givenBaseDataObjs                    []types.BaseDataObj
)

type MasterSuite struct {
	suite.Suite
	app *master.MasterApplication
}

func (suite *MasterSuite) SetupSuite() {
	//test data 설정
	timestamp1 := uint64(1545982882435375000)
	timestamp2 := uint64(1545982882435375001)
	salt := uint16(0)

	givenRowKey1 = types.GetRowKey(timestamp1, salt)
	givenRowKey2 = types.GetRowKey(timestamp2, salt)

	givenMetaDataObj1 = types.MetaDataObj{RowKey: givenRowKey1, OwnerId: TestOwnerId, Qualifier: []byte("Memory")}
	givenMetaDataObj2 = types.MetaDataObj{RowKey: givenRowKey2, OwnerId: TestOwnerId2, Qualifier: []byte("Stt")}

	givenRealDataObj1 = types.RealDataObj{RowKey: givenRowKey1, Data: []byte("aw")}
	givenRealDataObj2 = types.RealDataObj{RowKey: givenRowKey2, Data: []byte("good")}

	givenBaseDataObj1 = types.BaseDataObj{MetaData: givenMetaDataObj1, RealData: givenRealDataObj1}
	givenBaseDataObj2 = types.BaseDataObj{MetaData: givenMetaDataObj2, RealData: givenRealDataObj2}

	givenBaseDataObjs = append(givenBaseDataObjs, givenBaseDataObj1, givenBaseDataObj2)
}

func (suite *MasterSuite) SetupTest() {
	require := suite.Require()

	var err error

	os.RemoveAll(testDir)
	os.Mkdir(testDir, perm)
	suite.app, err = master.NewMasterApplication(true, testDir, log.AllowDebug())
	require.NotNil(suite.app, "app should not be nil")
	require.Nil(err, "err: %+v", err)
}

func (suite *MasterSuite) TearDownTest() {
	suite.app.Destroy()
}

func TestSuite(t *testing.T) {
	suite.Run(t, new(MasterSuite))
}
