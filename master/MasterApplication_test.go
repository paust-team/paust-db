package master_test

import (
	"encoding/binary"
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
	TestOwnerId  = "owner1"
	TestOwnerId2 = "owner2"
)

//test data
var (
	givenKeyObj1, givenKeyObj2           types.KeyObj
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
	require := suite.Require()

	var err error

	//test data 설정
	timestamp1 := make([]byte, 8)
	timestamp2 := make([]byte, 8)
	binary.BigEndian.PutUint64(timestamp1, 1545982882435375000)
	binary.BigEndian.PutUint64(timestamp2, 1545982882435375001)
	salt := make([]byte, 2)
	binary.BigEndian.PutUint16(salt, 0)
	givenKeyObj1 = types.KeyObj{Timestamp: timestamp1, Salt: salt}
	givenKeyObj2 = types.KeyObj{Timestamp: timestamp2, Salt: salt}

	givenRowKey1, err = json.Marshal(givenKeyObj1)
	require.Nil(err)

	givenRowKey2, err = json.Marshal(givenKeyObj2)
	require.Nil(err)


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
