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
		fmt.Println(err)
	}

	binary.BigEndian.PutUint64(hash, rand.Uint64())
	return &MasterApplication{
		serial: serial,
		hash:   hash,
		db:     database,
	}
}

func (app *MasterApplication) Info(req abciTypes.RequestInfo) abciTypes.ResponseInfo {
	return abciTypes.ResponseInfo{
		Data: fmt.Sprintf("---- Info"),
	}
}

func (app *MasterApplication) CheckTx(tx []byte) abciTypes.ResponseCheckTx {
	var dataSlice = types.DataSlice{}
	err := json.Unmarshal(tx, &dataSlice)
	if err != nil {
		return abciTypes.ResponseCheckTx{Code: code.CodeTypeEncodingError, Log: err.Error()}
	}

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

func (app *MasterApplication) BeginBlock(req abciTypes.RequestBeginBlock) abciTypes.ResponseBeginBlock {
	return abciTypes.ResponseBeginBlock{}
}

func (app *MasterApplication) DeliverTx(tx []byte) abciTypes.ResponseDeliverTx {
	var dataSlice = types.DataSlice{}
	err := json.Unmarshal(tx, &dataSlice)
	if err != nil {
		fmt.Println("dataSlice Unmarshal error", err)
	}

	for i := 0; i < len(dataSlice); i++ {
		var metaData = &types.MetaData{}
		metaData.UserKey = dataSlice[i].UserKey
		metaData.Qualifier = dataSlice[i].Qualifier
		metaByte, err := json.Marshal(metaData)
		if err != nil {
			fmt.Println("meta Marshal error : ", err)
		}

		rowKey := types.DataKeyToByteArr(dataSlice[i])
		app.wb.SetCF(app.cfs.GetCFH(0), rowKey, metaByte)
		app.wb.SetCF(app.cfs.GetCFH(1), rowKey, dataSlice[i].Data)
	}

	return abciTypes.ResponseDeliverTx{Code: code.CodeTypeOK}
}

func (app *MasterApplication) EndBlock(req abciTypes.RequestEndBlock) abciTypes.ResponseEndBlock {
	return abciTypes.ResponseEndBlock{}
}

func (app *MasterApplication) Commit() (resp abciTypes.ResponseCommit) {
	resp.Data = app.hash
	err := app.mwb.Write()
	if err != nil {
		fmt.Println(err)
	}
	err2 := app.wb.Write()
	if err2 != nil {
		fmt.Println(err2)
	}

	app.mwb = app.db.NewBatch()
	app.wb = app.db.NewBatch()

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
