package master

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/paust-team/paust-db/libs/db"
	"github.com/paust-team/paust-db/types"
	"github.com/tendermint/tendermint/abci/example/code"
	abciTypes "github.com/tendermint/tendermint/abci/types"
	"math/rand"
)

type MasterApplication struct {
	abciTypes.BaseApplication

	hash   []byte
	serial bool
	db     *db.CRocksDB
	wb     db.Batch
	mwb    db.Batch
	cfs    db.ColumnFamily
}

func NewMasterApplication(serial bool, dir string) *MasterApplication {
	hash := make([]byte, 8)
	database, err := db.NewCRocksDB("paustdb", dir)

	if err != nil {
		println(err)
	}

	binary.BigEndian.PutUint64(hash, rand.Uint64())
	return &MasterApplication{
		serial: serial,
		hash:   hash,
		db:     database,
	}
}

func (app *MasterApplication) Info(req abciTypes.RequestInfo) abciTypes.ResponseInfo {
	fmt.Println("------------- Master - Info req: " + req.String())
	return abciTypes.ResponseInfo{
		Data: fmt.Sprintf("---- Info"),
	}
}

//gas확인
func (app *MasterApplication) CheckTx(tx []byte) abciTypes.ResponseCheckTx {
	return abciTypes.ResponseCheckTx{Code: code.CodeTypeOK}
}

func (app *MasterApplication) InitChain(req abciTypes.RequestInitChain) abciTypes.ResponseInitChain {
	//Create ColumnFamilyHandles
	app.cfs = app.db.NewCFHandles()
	//Create ColumnFamily in rocksdb
	app.cfs.CreateCF("Meta")
	app.cfs.CreateCF("Data")

	app.wb = app.db.NewBatch()
	app.mwb = app.db.NewBatch()

	return abciTypes.ResponseInitChain{}
}

//TODO commit이 안되었을 떄의 처리
func (app *MasterApplication) BeginBlock(req abciTypes.RequestBeginBlock) abciTypes.ResponseBeginBlock {
	//Commit이 일어나지 않았을 경우에 batch를 flush 한다.
	app.wb = app.db.NewBatch()
	app.mwb = app.db.NewBatch()

	return abciTypes.ResponseBeginBlock{}
}

//TODO json object array 처리. 지금은 단일 object
func (app *MasterApplication) DeliverTx(tx []byte) abciTypes.ResponseDeliverTx {

	var data = &types.Data{}
	err := json.Unmarshal(tx, data)
	if err != nil {
		fmt.Println(err)
	}

	var metaData = &types.MetaData{}
	metaData.UserKey = data.UserKey
	metaData.Type = data.Type
	metaByte, err := json.Marshal(metaData)
	if err != nil {
		fmt.Println("meta 변환 error : ", err)
	}

	rowKey := types.DataKeyToByteArr(*data)
	app.wb.SetCF(app.cfs.GetCFH(0), rowKey, metaByte)
	app.wb.SetCF(app.cfs.GetCFH(1), rowKey, data.Data)

	return abciTypes.ResponseDeliverTx{Code: code.CodeTypeOK}
}

func (app *MasterApplication) EndBlock(req abciTypes.RequestEndBlock) abciTypes.ResponseEndBlock {
	return abciTypes.ResponseEndBlock{}
}

func (app *MasterApplication) Commit() (resp abciTypes.ResponseCommit) {
	resp.Data = app.hash
	app.mwb.Write()
	app.wb.Write()

	return
}

func (app *MasterApplication) Query(reqQuery abciTypes.RequestQuery) (resp abciTypes.ResponseQuery) {
	if reqQuery.Path == "/between" {
		var query = &types.BetweenQuery{}
		json.Unmarshal(reqQuery.Data, query)

		fmt.Printf("---- Query - path: /between, query: %v\n", query)

		resp.Value = []byte("test")
		return
	}

	return
}
