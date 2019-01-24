package master

import (
	"bytes"
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
	var wRealDataObjs = types.WRealDataObjs{}
	err := json.Unmarshal(tx, &wRealDataObjs)
	if err != nil {
		return abciTypes.ResponseCheckTx{Code: code.CodeTypeEncodingError, Log: err.Error()}
	}

	return abciTypes.ResponseCheckTx{Code: code.CodeTypeOK}
}

func (app *MasterApplication) InitChain(req abciTypes.RequestInitChain) abciTypes.ResponseInitChain {
	app.wb = app.db.NewBatch()
	app.mwb = app.db.NewBatch()

	return abciTypes.ResponseInitChain{}
}

func (app *MasterApplication) BeginBlock(req abciTypes.RequestBeginBlock) abciTypes.ResponseBeginBlock {
	return abciTypes.ResponseBeginBlock{}
}

func (app *MasterApplication) DeliverTx(tx []byte) abciTypes.ResponseDeliverTx {
	var wRealDataObjs = types.WRealDataObjs{}
	err := json.Unmarshal(tx, &wRealDataObjs)
	if err != nil {
		fmt.Println("wRealDataObjs unmarshal error", err)
	}

	for i := 0; i < len(wRealDataObjs); i++ {
		var wMetaDataObj = &types.WMetaDataObj{OwnerKey: wRealDataObjs[i].OwnerKey, Qualifier: wRealDataObjs[i].Qualifier}
		metaByte, err := json.Marshal(wMetaDataObj)
		if err != nil {
			fmt.Println("meta marshal error : ", err)
		}

		rowKey := types.WRealDataObjToRowKey(wRealDataObjs[i])
		app.mwb.SetColumnFamily(app.db.ColumnFamilyHandle(1), rowKey, metaByte)
		app.wb.SetColumnFamily(app.db.ColumnFamilyHandle(2), rowKey, wRealDataObjs[i].Data)
	}

	return abciTypes.ResponseDeliverTx{Code: code.CodeTypeOK}
}

func (app *MasterApplication) EndBlock(req abciTypes.RequestEndBlock) abciTypes.ResponseEndBlock {
	return abciTypes.ResponseEndBlock{}
}

func (app *MasterApplication) Commit() (resp abciTypes.ResponseCommit) {
	resp.Data = app.hash
	if err := app.mwb.Write(); err != nil {
		fmt.Println(err)
	}

	if err := app.wb.Write(); err != nil {
		fmt.Println(err)
	}

	app.mwb = app.db.NewBatch()
	app.wb = app.db.NewBatch()

	return
}

func (app *MasterApplication) Query(reqQuery abciTypes.RequestQuery) (resp abciTypes.ResponseQuery) {
	switch reqQuery.Path {
	case "/metadata":
		var query = types.RMetaDataQueryObj{}
		err := json.Unmarshal(reqQuery.Data, &query)
		if err != nil {
			fmt.Println("RMetaDataQueryObj struct unmarshal error", err)
		}

		metaSlice, _ := app.metaDataQuery(query)
		resp.Value, _ = json.Marshal(metaSlice)

	case "/realdata":
		var query = types.RRealDataQueryObj{}
		err := json.Unmarshal(reqQuery.Data, &query)
		if err != nil {
			fmt.Println(err)
		}

		realDataSlice, _ := app.realDataQuery(query)
		resp.Value, _ = json.Marshal(realDataSlice)

	}

	return
}

func (app *MasterApplication) metaDataQuery(query types.RMetaDataQueryObj) (types.RMetaDataResObjs, error) {
	var rMetaDataResObjs = types.RMetaDataResObjs{}

	startByte, endByte := types.CreateStartByteAndEndByte(query)
	itr := app.db.IteratorColumnFamily(startByte, endByte, app.db.ColumnFamilyHandle(1))
	//TODO unittest close test
	defer itr.Close()

	for itr.Seek(startByte); itr.Valid() && bytes.Compare(itr.Key(), endByte) == -1; itr.Next() {
		var metaObj = types.WMetaDataObj{}
		err := json.Unmarshal(itr.Value(), &metaObj)
		if err != nil {
			fmt.Println(err)
		}

		var rMetaDataResObj types.RMetaDataResObj
		rMetaDataResObj.RowKey = make([]byte, len(itr.Key()))
		copy(rMetaDataResObj.RowKey, itr.Key())
		rMetaDataResObj.OwnerKey = metaObj.OwnerKey
		rMetaDataResObj.Qualifier = metaObj.Qualifier

		rMetaDataResObjs = append(rMetaDataResObjs, rMetaDataResObj)

	}

	return rMetaDataResObjs, nil

}

func (app *MasterApplication) realDataQuery(query types.RRealDataQueryObj) (types.RRealDataResObjs, error) {
	rRealDataResObj := types.RRealDataResObj{}
	rRealDataResObjs := types.RRealDataResObjs{}

	for _, rowKey := range query.Keys {
		valueSlice, err := app.db.GetDataFromColumnFamily(2, rowKey)
		if err != nil {
			fmt.Print(err)
		}
		rRealDataResObj.RowKey = rowKey
		rRealDataResObj.Data = valueSlice.Data()

		rRealDataResObjs = append(rRealDataResObjs, rRealDataResObj)

	}

	return rRealDataResObjs, nil
}

// Below method ares all For Test
func (app MasterApplication) Hash() []byte {
	return app.hash
}

func (app MasterApplication) DB() *db.CRocksDB {
	return app.db
}

func (app MasterApplication) WB() db.Batch {
	return app.wb
}

func (app MasterApplication) MWB() db.Batch {
	return app.mwb
}

func (app MasterApplication) RealDataQuery(query types.RRealDataQueryObj) (types.RRealDataResObjs, error) {
	return app.realDataQuery(query)
}

func (app MasterApplication) MetaDataQuery(query types.RMetaDataQueryObj) (types.RMetaDataResObjs, error) {
	return app.metaDataQuery(query)
}
