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
	app.cfs = app.db.NewColumnFamilyHandles()
	err := app.cfs.CreateColumnFamily("metadata")
	if err != nil {
		fmt.Println(err)
	}
	app.cfs.CreateColumnFamily("realdata")
	if err != nil {
		fmt.Println(err)
	}
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
		metaData.Type = dataSlice[i].Type
		metaByte, err := json.Marshal(metaData)
		if err != nil {
			fmt.Println("meta Marshal error : ", err)
		}

		rowKey := types.DataToRowKey(dataSlice[i])
		app.mwb.SetColumnFamily(app.cfs.ColumnFamilyHandle(0), rowKey, metaByte)
		app.wb.SetColumnFamily(app.cfs.ColumnFamilyHandle(1), rowKey, dataSlice[i].Data)
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
		json.Unmarshal(reqQuery.Data, &query)

		dataSlice, _ := app.RealDataQuery(query)
		resp.Value, _ = json.Marshal(dataSlice)

	}

	return
}

func (app *MasterApplication) MetaDataQuery(query types.DataQuery) (types.MetaResponseSlice, error) {
	var meta = types.MetaData{}
	var metaSlice = types.MetaResponseSlice{}

	startByte, endByte := types.CreateStartByteAndEndByte(query)
	itr := app.db.IteratorColumnFamily(startByte, endByte, app.cfs.ColumnFamilyHandle(0))
	//TODO unittest close test
	defer itr.Close()

	for itr.Seek(startByte); itr.Valid() && bytes.Compare(itr.Key(), endByte) < 1; itr.Next() {
		json.Unmarshal(itr.Value(), &meta)

		switch {
		case query.UserKey == nil && query.Type == "":
			metaResp, err := types.MetaDataToMetaResponse(itr.Key(), meta)
			if err != nil {
				fmt.Println(err)
			}
			metaSlice = append(metaSlice, metaResp)
		case query.Type == "":
			if string(query.UserKey) == string(meta.UserKey) {
				metaResp, err := types.MetaDataToMetaResponse(itr.Key(), meta)
				if err != nil {
					fmt.Println(err)
				}
				metaSlice = append(metaSlice, metaResp)
			}
		case query.UserKey == nil:
			if string(query.Type) == string(meta.Type) {
				metaResp, err := types.MetaDataToMetaResponse(itr.Key(), meta)
				if err != nil {
					fmt.Println(err)
				}
				metaSlice = append(metaSlice, metaResp)
			}
		default:
			if string(query.Type) == string(meta.Type) && string(query.UserKey) == string(meta.UserKey) {
				metaResp, err := types.MetaDataToMetaResponse(itr.Key(), meta)
				if err != nil {
					fmt.Println(err)
				}
				metaSlice = append(metaSlice, metaResp)
			}
		}

	}

	return metaSlice, nil

}

func (app *MasterApplication) RealDataQuery(query types.DataQuery) (types.DataSlice, error) {
	var data = types.Data{}
	var dataSlice = types.DataSlice{}

	startByte, endByte := types.CreateStartByteAndEndByte(query)
	itr := app.db.IteratorColumnFamily(startByte, endByte, app.cfs.ColumnFamilyHandle(1))
	//TODO unittest close test
	defer itr.Close()

	for itr.Seek(startByte); itr.Valid() && bytes.Compare(itr.Key(), endByte) < 1; itr.Next() {
		data = types.RowKeyToData(itr.Key(), itr.Value())
		switch {
		case query.UserKey == nil && query.Type == "":
			dataSlice = append(dataSlice, data)
		case query.Type == "":
			if string(query.UserKey) == string(data.UserKey) {
				dataSlice = append(dataSlice, data)
			}
		case query.UserKey == nil:
			if string(query.Type) == string(data.Type) {
				dataSlice = append(dataSlice, data)
			}
		default:
			if string(query.Type) == string(data.Type) && string(query.UserKey) == string(data.UserKey){
				dataSlice = append(dataSlice, data)
			}
		}
	}

	return dataSlice, nil
}

//TODO 단일 스탬프 조회추가?
