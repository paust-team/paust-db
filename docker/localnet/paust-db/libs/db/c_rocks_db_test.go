package db_test

import (
	"github.com/paust-team/paust-db/libs/db"
	"github.com/stretchr/testify/suite"
	"os"
	"testing"
)

const (
	dbName = "paustdbtest"
	dir    = "/tmp/paustdbtest"
	perm   = 0777
)

type DBSuite struct {
	suite.Suite
	DB *db.CRocksDB
}

func (suite *DBSuite) SetupTest() {
	var err error
	os.RemoveAll(dir)
	os.Mkdir(dir, perm)
	suite.DB, err = db.NewCRocksDB(dbName, dir)

	suite.Require().NotNil(suite.DB, "db open error %v", err)
	suite.Require().Nil(err, "db open error %v", err)
}

func (suite *DBSuite) TearDownTest() {
	suite.DB.Close()
}

func TestSuite(t *testing.T) {
	suite.Run(t, new(DBSuite))
}
