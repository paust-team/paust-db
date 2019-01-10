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

		metaSlice, _ := app.MetaDataQuery(query)
		resp.Value, _ = json.Marshal(metaSlice)

	case "/realdata":
		err := json.Unmarshal(reqQuery.Data, &query)
		if err != nil {
			fmt.Println("DataQuery struct unmarshal error", err)
		}

		realDataSlice, _ := app.RealDataQuery(query)
		resp.Value, _ = json.Marshal(realDataSlice)

	}

	return
}

func (app *MasterApplication) metaDataQuery(query types.DataQuery) (types.MetaResponseSlice, error) {
	var meta = types.MetaData{}
	var metaSlice = types.MetaResponseSlice{}

	startByte, endByte := types.CreateStartByteAndEndByte(query)
	itr := app.db.IteratorColumnFamily(startByte, endByte, app.db.ColumnFamilyHandle(1))
	//TODO unittest close test
	defer itr.Close()

	switch {
	case query.UserKey == nil && query.Qualifier == "":
		for itr.Seek(startByte); itr.Valid() && bytes.Compare(itr.Key(), endByte) < 1; itr.Next() {
			json.Unmarshal(itr.Value(), &meta)
			metaResp, err := types.MetaDataAndKeyToMetaResponse(itr.Key(), meta)
			if err != nil {
				fmt.Println(err)
			}
			metaSlice = append(metaSlice, metaResp)
		}
	case query.Qualifier == "":
		for itr.Seek(startByte); itr.Valid() && bytes.Compare(itr.Key(), endByte) < 1; itr.Next() {
			json.Unmarshal(itr.Value(), &meta)
			if string(query.UserKey) == string(meta.UserKey) {
				metaResp, err := types.MetaDataAndKeyToMetaResponse(itr.Key(), meta)
				if err != nil {
					fmt.Println(err)
				}
				metaSlice = append(metaSlice, metaResp)
			}
		}
	case query.UserKey == nil:
		for itr.Seek(startByte); itr.Valid() && bytes.Compare(itr.Key(), endByte) < 1; itr.Next() {
			json.Unmarshal(itr.Value(), &meta)
			if string(query.Qualifier) == string(meta.Qualifier) {
				metaResp, err := types.MetaDataAndKeyToMetaResponse(itr.Key(), meta)
				if err != nil {
					fmt.Println(err)
				}
				metaSlice = append(metaSlice, metaResp)
			}
		}
	default:
		for itr.Seek(startByte); itr.Valid() && bytes.Compare(itr.Key(), endByte) < 1; itr.Next() {
			json.Unmarshal(itr.Value(), &meta)
			if string(query.Qualifier) == string(meta.Qualifier) && string(query.UserKey) == string(meta.UserKey) {
				metaResp, err := types.MetaDataAndKeyToMetaResponse(itr.Key(), meta)
				if err != nil {
					fmt.Println(err)
				}
				metaSlice = append(metaSlice, metaResp)
			}
		}

	}

	return metaSlice, nil

}

func (app *MasterApplication) realDataQuery(query types.DataQuery) (types.RealDataSlice, error) {
	var realData = types.RealData{}
	var realDataSlice = types.RealDataSlice{}

	startByte, endByte := types.CreateStartByteAndEndByte(query)
	itr := app.db.IteratorColumnFamily(startByte, endByte, app.db.ColumnFamilyHandle(2))
	//TODO unittest close test
	defer itr.Close()

	switch {
	case query.UserKey == nil && query.Qualifier == "":
		for itr.Seek(startByte); itr.Valid() && bytes.Compare(itr.Key(), endByte) < 1; itr.Next() {
			realData = types.RowKeyAndValueToRealData(itr.Key(), itr.Value())
			realDataSlice = append(realDataSlice, realData)
		}
	case query.Qualifier == "":
		for itr.Seek(startByte); itr.Valid() && bytes.Compare(itr.Key(), endByte) < 1; itr.Next() {
			realData = types.RowKeyAndValueToRealData(itr.Key(), itr.Value())
			if string(query.UserKey) == string(realData.UserKey) {
				realDataSlice = append(realDataSlice, realData)
			}
		}
	case query.UserKey == nil:
		for itr.Seek(startByte); itr.Valid() && bytes.Compare(itr.Key(), endByte) < 1; itr.Next() {
			realData = types.RowKeyAndValueToRealData(itr.Key(), itr.Value())
			if string(query.Qualifier) == string(realData.Qualifier) {
				realDataSlice = append(realDataSlice, realData)
			}
		}
	default:
		for itr.Seek(startByte); itr.Valid() && bytes.Compare(itr.Key(), endByte) < 1; itr.Next() {
			realData = types.RowKeyAndValueToRealData(itr.Key(), itr.Value())
			if string(query.Qualifier) == string(realData.Qualifier) && string(query.UserKey) == string(realData.UserKey) {
				realDataSlice = append(realDataSlice, realData)
			}
		}
	}

	return realDataSlice, nil
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
