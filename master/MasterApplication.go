package master

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/paust-team/paust-db/types"
	"github.com/tendermint/tendermint/abci/example/code"
	abciTypes "github.com/tendermint/tendermint/abci/types"
	"github.com/tendermint/tendermint/libs/db"
	"math/rand"
)

type MasterApplication struct {
	abciTypes.BaseApplication

	hash []byte
	serial bool
	db *db.GoLevelDB

	caches map[int64]types.Data
}

func NewMasterApplication(serial bool) *MasterApplication {
	hash := make([]byte, 8)
	database, err := db.NewGoLevelDB("paustdb", "/Users/dennis/tmp")
	if err != nil {
		println(err)
	}

	binary.BigEndian.PutUint64(hash, rand.Uint64())
	return &MasterApplication{
		serial: serial,
		hash: hash,
		db: database,
	}
}

func (app *MasterApplication) Info(req abciTypes.RequestInfo) abciTypes.ResponseInfo {
	fmt.Println("------------- Master - Info req: " + req.String())
	return abciTypes.ResponseInfo{
		Data: fmt.Sprintf("---- Info"),
	}
}

func (app *MasterApplication) CheckTx(tx []byte) abciTypes.ResponseCheckTx {
	return abciTypes.ResponseCheckTx{Code: code.CodeTypeOK}
}

func (app *MasterApplication) InitChain(req abciTypes.RequestInitChain) abciTypes.ResponseInitChain {
	return abciTypes.ResponseInitChain{}
}

func (app *MasterApplication) BeginBlock(req abciTypes.RequestBeginBlock) abciTypes.ResponseBeginBlock {
	return abciTypes.ResponseBeginBlock{}
}

func (app *MasterApplication) DeliverTx(tx []byte) abciTypes.ResponseDeliverTx {
	var data = &types.Data{}
	err := json.Unmarshal(tx, data)
	if err != nil {
		fmt.Println(err)
	}

	fmt.Printf("--------- DeliverTx - tx: %v, time: %d, data: %s\n", tx, data.Timestamp, string(data.Data))

	app.caches[data.Timestamp] = *data

	return abciTypes.ResponseDeliverTx{Code: code.CodeTypeOK}
}

func (app *MasterApplication) EndBlock(req abciTypes.RequestEndBlock) abciTypes.ResponseEndBlock {
	return abciTypes.ResponseEndBlock{}
}

func (app *MasterApplication) Commit() (resp abciTypes.ResponseCommit) {
	resp.Data = app.hash

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
