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
	var realDataSlice = types.RealDataSlice{}
	err := json.Unmarshal(tx, &realDataSlice)
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
	var realDataSlice = types.RealDataSlice{}
	err := json.Unmarshal(tx, &realDataSlice)
	if err != nil {
		fmt.Println("realDataSlice unmarshal error", err)
	}

	for i := 0; i < len(realDataSlice); i++ {
		var metaData = &types.MetaData{}
		metaData.UserKey = realDataSlice[i].UserKey
		metaData.Qualifier = realDataSlice[i].Qualifier
		metaByte, err := json.Marshal(metaData)
		if err != nil {
			fmt.Println("meta marshal error : ", err)
		}

		rowKey := types.DataToRowKey(realDataSlice[i])
		app.mwb.SetColumnFamily(app.db.ColumnFamilyHandle(1), rowKey, metaByte)
		app.wb.SetColumnFamily(app.db.ColumnFamilyHandle(2), rowKey, realDataSlice[i].Data)
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
	var query = types.DataQuery{}
	switch reqQuery.Path {
	case "/metadata":
		err := json.Unmarshal(reqQuery.Data, &query)
		if err != nil {
			fmt.Println("DataQuery struct unmarshal error", err)
		}

		metaSlice, _ := app.metaDataQuery(query)
		resp.Value, _ = json.Marshal(metaSlice)

	case "/realdata":
		err := json.Unmarshal(reqQuery.Data, &query)
		if err != nil {
			fmt.Println("DataQuery struct unmarshal error", err)
		}

		realDataSlice, _ := app.realDataQuery(query)
		resp.Value, _ = json.Marshal(realDataSlice)

	}

	return
}

func (app *MasterApplication) metaDataQuery(query types.DataQuery) (types.MetaResponseSlice, error) {
	var metaSlice = types.MetaResponseSlice{}

	startByte, endByte := types.CreateStartByteAndEndByte(query)
	itr := app.db.IteratorColumnFamily(startByte, endByte, app.db.ColumnFamilyHandle(1))
	//TODO unittest close test
	defer itr.Close()

	switch {
	case query.UserKey == nil && query.Qualifier == "":
		metaSlice = searchInMetaColumnFamily(startByte, endByte, itr, func(meta types.MetaData) *types.MetaResponse {
			metaResp, err := types.MetaDataAndKeyToMetaResponse(itr.Key(), meta)
			if err != nil {
				fmt.Println(err)
			}
			return &metaResp
		})

	case query.Qualifier == "":
		metaSlice = searchInMetaColumnFamily(startByte, endByte, itr, func(meta types.MetaData) *types.MetaResponse {
			if string(query.UserKey) == string(meta.UserKey) {
				metaResp, err := types.MetaDataAndKeyToMetaResponse(itr.Key(), meta)
				if err != nil {
					fmt.Println(err)
				}
				return &metaResp
			}
			return nil
		})

	case query.UserKey == nil:
		metaSlice = searchInMetaColumnFamily(startByte, endByte, itr, func(meta types.MetaData) *types.MetaResponse {
			if string(query.Qualifier) == string(meta.Qualifier) {
				metaResp, err := types.MetaDataAndKeyToMetaResponse(itr.Key(), meta)
				if err != nil {
					fmt.Println(err)
				}
				return &metaResp
			}
			return nil
		})

	default:
		metaSlice = searchInMetaColumnFamily(startByte, endByte, itr, func(meta types.MetaData) *types.MetaResponse {
			if string(query.Qualifier) == string(meta.Qualifier) && string(query.UserKey) == string(meta.UserKey) {
				metaResp, err := types.MetaDataAndKeyToMetaResponse(itr.Key(), meta)
				if err != nil {
					fmt.Println(err)
				}
				return &metaResp
			}
			return nil
		})
	}

	return metaSlice, nil

}

func (app *MasterApplication) realDataQuery(query types.DataQuery) (types.RealDataSlice, error) {
	var realDataSlice = types.RealDataSlice{}

	startByte, endByte := types.CreateStartByteAndEndByte(query)
	itr := app.db.IteratorColumnFamily(startByte, endByte, app.db.ColumnFamilyHandle(2))
	//TODO unittest close test
	defer itr.Close()

	switch {
	case query.UserKey == nil && query.Qualifier == "":
		realDataSlice = searchInRealColumnFamily(startByte, endByte, itr, func(realData types.RealData) *types.RealData {
			return &realData
		})
	case query.Qualifier == "":
		realDataSlice = searchInRealColumnFamily(startByte, endByte, itr, func(realData types.RealData) *types.RealData {
			if string(query.UserKey) == string(realData.UserKey) {
				return &realData
			}
			return nil
		})
	case query.UserKey == nil:
		realDataSlice = searchInRealColumnFamily(startByte, endByte, itr, func(realData types.RealData) *types.RealData {
			if string(query.Qualifier) == string(realData.Qualifier) {
				return &realData
			}
			return nil
		})
	default:
		realDataSlice = searchInRealColumnFamily(startByte, endByte, itr, func(realData types.RealData) *types.RealData {
			if string(query.Qualifier) == string(realData.Qualifier) && string(query.UserKey) == string(realData.UserKey) {
				return &realData
			}
			return nil
		})
	}

	return realDataSlice, nil
}

func searchInMetaColumnFamily(startByte, endByte []byte, itr db.Iterator, closureFunc func(meta types.MetaData) *types.MetaResponse) types.MetaResponseSlice {
	var meta = types.MetaData{}
	var metaSlice = &types.MetaResponseSlice{}

	for itr.Seek(startByte); itr.Valid() && bytes.Compare(itr.Key(), endByte) < 1; itr.Next() {
		json.Unmarshal(itr.Value(), &meta)
		ret := closureFunc(meta)
		if ret != nil {
			*metaSlice = append(*metaSlice, *ret)
		}
	}

	return *metaSlice
}

func searchInRealColumnFamily(startByte, endByte []byte, itr db.Iterator, closureFunc func(realData types.RealData) *types.RealData) types.RealDataSlice {
	var realData = types.RealData{}
	var realDataSlice = &types.RealDataSlice{}
	for itr.Seek(startByte); itr.Valid() && bytes.Compare(itr.Key(), endByte) < 1; itr.Next() {
		realData = types.RowKeyAndValueToRealData(itr.Key(), itr.Value())
		ret := closureFunc(realData)
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

func (app MasterApplication) RealDataQuery(query types.DataQuery) (types.RealDataSlice, error) {
	return app.realDataQuery(query)
}

func (app MasterApplication) MetaDataQuery(query types.DataQuery) (types.MetaResponseSlice, error) {
	return app.metaDataQuery(query)
}
