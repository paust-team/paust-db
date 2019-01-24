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
	var baseDataObjs []types.BaseDataObj
	err := json.Unmarshal(tx, &baseDataObjs)
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
	//Unmarshal tx to baseDataObjs
	var baseDataObjs []types.BaseDataObj
	err := json.Unmarshal(tx, &baseDataObjs)
	if err != nil {
		fmt.Println("wRealDataObjs unmarshal error", err)
	}

	//meta와 real 나누어 batch에 담는다
	for i := 0; i < len(baseDataObjs); i++ {
		var metaValue struct {
			OwnerKey  []byte `json:"ownerKey"`
			Qualifier []byte `json:"qualifier"`
		}
		metaValue.OwnerKey = baseDataObjs[i].MetaData.OwnerKey
		metaValue.Qualifier = baseDataObjs[i].MetaData.Qualifier

		metaData, err := json.Marshal(metaValue)
		if err != nil {
			fmt.Println(err)
		}
		app.mwb.SetColumnFamily(app.db.ColumnFamilyHandle(1), baseDataObjs[i].MetaData.RowKey, metaData)
		app.wb.SetColumnFamily(app.db.ColumnFamilyHandle(2), baseDataObjs[i].RealData.RowKey, baseDataObjs[i].RealData.Data)
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
		var query = types.MetaDataQueryObj{}
		err := json.Unmarshal(reqQuery.Data, &query)
		if err != nil {
			fmt.Println("RMetaDataQueryObj struct unmarshal error", err)
		}

		metaDataObjs, _ := app.metaDataQuery(query)
		resp.Value, _ = json.Marshal(metaDataObjs)

	case "/realdata":
		var query = types.RealDataQueryObj{}
		err := json.Unmarshal(reqQuery.Data, &query)
		if err != nil {
			fmt.Println(err)
		}

		realDataObjs, _ := app.realDataQuery(query)
		resp.Value, _ = json.Marshal(realDataObjs)

	}

	return
}

func (app *MasterApplication) metaDataQuery(query types.MetaDataQueryObj) ([]types.MetaDataObj, error) {
	var metaDataObjs []types.MetaDataObj

	startByte, endByte := types.CreateStartByteAndEndByte(query)
	itr := app.db.IteratorColumnFamily(startByte, endByte, app.db.ColumnFamilyHandle(1))
	//TODO unittest close test
	defer itr.Close()

	for itr.Seek(startByte); itr.Valid() && bytes.Compare(itr.Key(), endByte) == -1; itr.Next() {
		var metaObj = types.MetaDataObj{}

		var metaValue struct {
			OwnerKey  []byte `json:"ownerKey"`
			Qualifier []byte `json:"qualifier"`
		}
		err := json.Unmarshal(itr.Value(), &metaValue)
		if err != nil {
			fmt.Println(err)
		}

		metaObj.RowKey = make([]byte, len(itr.Key()))
		copy(metaObj.RowKey, itr.Key())
		metaObj.OwnerKey = metaValue.OwnerKey
		metaObj.Qualifier = metaValue.Qualifier

		metaDataObjs = append(metaDataObjs, metaObj)

	}

	return metaDataObjs, nil

}

func (app *MasterApplication) realDataQuery(query types.RealDataQueryObj) ([]types.RealDataObj, error) {
	var realDataObjs []types.RealDataObj

	for _, rowKey := range query.RowKeys {
		var realDataObj types.RealDataObj

		realDataObj.RowKey = rowKey
		valueSlice, err := app.db.GetDataFromColumnFamily(2, rowKey)
		if err != nil {
			fmt.Print(err)
		}
		realDataObj.Data = make([]byte, valueSlice.Size())
		copy(realDataObj.Data, valueSlice.Data())
		realDataObjs = append(realDataObjs, realDataObj)

		valueSlice.Free()

	}

	return realDataObjs, nil
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
