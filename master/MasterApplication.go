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
		var metaDataObj = &types.MetaDataObj{}
		metaDataObj.UserKey = wRealDataObjs[i].UserKey
		metaDataObj.Qualifier = wRealDataObjs[i].Qualifier
		metaByte, err := json.Marshal(metaDataObj)
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
	var query = types.RDataQueryObj{}
	switch reqQuery.Path {
	case "/metadata":
		err := json.Unmarshal(reqQuery.Data, &query)
		if err != nil {
			fmt.Println("RDataQueryObj struct unmarshal error", err)
		}

		metaSlice, _ := app.metaDataQuery(query)
		resp.Value, _ = json.Marshal(metaSlice)

	case "/realdata":
		err := json.Unmarshal(reqQuery.Data, &query)
		if err != nil {
			fmt.Println("RDataQueryObj struct unmarshal error", err)
		}

		realDataSlice, _ := app.realDataQuery(query)
		resp.Value, _ = json.Marshal(realDataSlice)

	}

	return
}

func (app *MasterApplication) metaDataQuery(query types.RDataQueryObj) (types.RMetaResObjs, error) {
	var metaObjs = types.RMetaResObjs{}

	startByte, endByte := types.CreateStartByteAndEndByte(query)
	itr := app.db.IteratorColumnFamily(startByte, endByte, app.db.ColumnFamilyHandle(1))
	//TODO unittest close test
	defer itr.Close()

	switch {
	case query.UserKey == nil && query.Qualifier == "":
		metaObjs = searchInMetaColumnFamily(startByte, endByte, itr, func(meta types.MetaDataObj) *types.RMetaResObj {
			metaResp, err := types.RMetaDataObjAndKeyToMetaRes(itr.Key(), meta)
			if err != nil {
				fmt.Println(err)
			}
			return &metaResp
		})

	case query.Qualifier == "":
		metaObjs = searchInMetaColumnFamily(startByte, endByte, itr, func(meta types.MetaDataObj) *types.RMetaResObj {
			if string(query.UserKey) == string(meta.UserKey) {
				metaResp, err := types.RMetaDataObjAndKeyToMetaRes(itr.Key(), meta)
				if err != nil {
					fmt.Println(err)
				}
				return &metaResp
			}
			return nil
		})

	case query.UserKey == nil:
		metaObjs = searchInMetaColumnFamily(startByte, endByte, itr, func(meta types.MetaDataObj) *types.RMetaResObj {
			if string(query.Qualifier) == string(meta.Qualifier) {
				metaResp, err := types.RMetaDataObjAndKeyToMetaRes(itr.Key(), meta)
				if err != nil {
					fmt.Println(err)
				}
				return &metaResp
			}
			return nil
		})

	default:
		metaObjs = searchInMetaColumnFamily(startByte, endByte, itr, func(meta types.MetaDataObj) *types.RMetaResObj {
			if string(query.Qualifier) == string(meta.Qualifier) && string(query.UserKey) == string(meta.UserKey) {
				metaResp, err := types.RMetaDataObjAndKeyToMetaRes(itr.Key(), meta)
				if err != nil {
					fmt.Println(err)
				}
				return &metaResp
			}
			return nil
		})
	}

	return metaObjs, nil

}

func (app *MasterApplication) realDataQuery(query types.RDataQueryObj) (types.WRealDataObjs, error) {
	var wRealDataObjs = types.WRealDataObjs{}

	startByte, endByte := types.CreateStartByteAndEndByte(query)
	itr := app.db.IteratorColumnFamily(startByte, endByte, app.db.ColumnFamilyHandle(2))
	//TODO unittest close test
	defer itr.Close()

	switch {
	case query.UserKey == nil && query.Qualifier == "":
		wRealDataObjs = searchInRealColumnFamily(startByte, endByte, itr, func(realData types.WRealDataObj) *types.WRealDataObj {
			return &realData
		})
	case query.Qualifier == "":
		wRealDataObjs = searchInRealColumnFamily(startByte, endByte, itr, func(realData types.WRealDataObj) *types.WRealDataObj {
			if string(query.UserKey) == string(realData.UserKey) {
				return &realData
			}
			return nil
		})
	case query.UserKey == nil:
		wRealDataObjs = searchInRealColumnFamily(startByte, endByte, itr, func(realData types.WRealDataObj) *types.WRealDataObj {
			if string(query.Qualifier) == string(realData.Qualifier) {
				return &realData
			}
			return nil
		})
	default:
		wRealDataObjs = searchInRealColumnFamily(startByte, endByte, itr, func(realData types.WRealDataObj) *types.WRealDataObj {
			if string(query.Qualifier) == string(realData.Qualifier) && string(query.UserKey) == string(realData.UserKey) {
				return &realData
			}
			return nil
		})
	}

	return wRealDataObjs, nil
}

func searchInMetaColumnFamily(startByte, endByte []byte, itr db.Iterator, closureFunc func(meta types.MetaDataObj) *types.RMetaResObj) types.RMetaResObjs {
	var metaObj = types.MetaDataObj{}
	var metaObjs = &types.RMetaResObjs{}

	for itr.Seek(startByte); itr.Valid() && bytes.Compare(itr.Key(), endByte) < 1; itr.Next() {
		json.Unmarshal(itr.Value(), &metaObj)
		ret := closureFunc(metaObj)
		if ret != nil {
			*metaObjs = append(*metaObjs, *ret)
		}
	}

	return *metaObjs
}

func searchInRealColumnFamily(startByte, endByte []byte, itr db.Iterator, closureFunc func(realData types.WRealDataObj) *types.WRealDataObj) types.WRealDataObjs {
	var wRealDataObjs = types.WRealDataObj{}
	var realDataSlice = &types.WRealDataObjs{}
	for itr.Seek(startByte); itr.Valid() && bytes.Compare(itr.Key(), endByte) < 1; itr.Next() {
		wRealDataObjs = types.RowKeyAndValueToWRealDataObj(itr.Key(), itr.Value())
		ret := closureFunc(wRealDataObjs)
		if ret != nil {
			*realDataSlice = append(*realDataSlice, *ret)
		}
	}
	return *realDataSlice
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

func (app MasterApplication) RealDataQuery(query types.RDataQueryObj) (types.WRealDataObjs, error) {
	return app.realDataQuery(query)
}

func (app MasterApplication) MetaDataQuery(query types.RDataQueryObj) (types.RMetaResObjs, error) {
	return app.metaDataQuery(query)
}
