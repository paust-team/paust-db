package master_test

import (
	"encoding/base64"
	"encoding/json"
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
	TestPubKey  = "oimd8ZdzgUHzF9CPChJU8gb89VaMYg+1SpX6WT8nQHE="
	TestPubKey2 = "Pe8PPI4Mq7kJIjDJjffoTl6s5EezGQSyIcu5Y2KYDaE="
)

//test data
var (
	givenKeyObj1, givenKeyObj2           types.KeyObj
	givenRowKey1, givenRowKey2           []byte
	givenOwnerKey, givenOwnerKey2        []byte
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
	require := suite.Require()

	var err error

	//test data 설정
	givenKeyObj1 = types.KeyObj{Timestamp: 1545982882435375000, Salt: 0}
	givenKeyObj2 = types.KeyObj{Timestamp: 1545982882435375001, Salt: 0}

	givenRowKey1, err = json.Marshal(givenKeyObj1)
	require.Nil(err)

	givenRowKey2, err = json.Marshal(givenKeyObj2)
	require.Nil(err)

	givenOwnerKey, err = base64.StdEncoding.DecodeString(TestPubKey)
	require.Nil(err)

	givenOwnerKey2, err = base64.StdEncoding.DecodeString(TestPubKey2)
	require.Nil(err)

	givenMetaDataObj1 = types.MetaDataObj{RowKey: givenRowKey1, OwnerKey: givenOwnerKey, Qualifier: []byte("Memory")}
	givenMetaDataObj2 = types.MetaDataObj{RowKey: givenRowKey2, OwnerKey: givenOwnerKey2, Qualifier: []byte("Stt")}

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
