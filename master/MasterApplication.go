package master

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/paust-team/paust-db/consts"
	"github.com/paust-team/paust-db/libs/db"
	"github.com/paust-team/paust-db/types"
	"github.com/pkg/errors"
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
	database, err := db.NewCRocksDB(consts.DBName, dir)

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
		fmt.Println("BaseDataObj unmarshal error", err)
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
		app.mwb.SetColumnFamily(app.db.ColumnFamilyHandles()[consts.MetaCFNum], baseDataObjs[i].MetaData.RowKey, metaData)
		app.wb.SetColumnFamily(app.db.ColumnFamilyHandles()[consts.RealCFNum], baseDataObjs[i].RealData.RowKey, baseDataObjs[i].RealData.Data)
	}

	return abciTypes.ResponseDeliverTx{Code: code.CodeTypeOK}
}

func (app *MasterApplication) EndBlock(req abciTypes.RequestEndBlock) abciTypes.ResponseEndBlock {
	return abciTypes.ResponseEndBlock{}
}

func (app *MasterApplication) Commit() (resp abciTypes.ResponseCommit) {
	//resp.Data = app.hash
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
	case consts.QueryPath:
		var queryObj = types.QueryObj{}
		err := json.Unmarshal(reqQuery.Data, &queryObj)
		if err != nil {
			fmt.Println("QueryObj struct unmarshal error", err)
		}

		metaDataObjs, _ := app.metaDataQuery(queryObj)
		resp.Value, _ = json.Marshal(metaDataObjs)

	case consts.FetchPath:
		var fetchObj = types.FetchObj{}
		err := json.Unmarshal(reqQuery.Data, &fetchObj)
		if err != nil {
			fmt.Println(err)
		}

		realDataObjs, _ := app.realDataFetch(fetchObj)
		resp.Value, _ = json.Marshal(realDataObjs)

	}

	return
}

func (app *MasterApplication) metaDataQuery(queryObj types.QueryObj) ([]types.MetaDataObj, error) {
	var rawMetaDataObjs []types.MetaDataObj
	var metaDataObjs []types.MetaDataObj

	// query field nil error 처리
	if queryObj.Qualifier == nil || queryObj.OwnerKey == nil {
		return nil, errors.Errorf("ownerKey and Qualifier must not be nil")
	}

	startByte, endByte := types.CreateStartByteAndEndByte(queryObj)
	itr := app.db.IteratorColumnFamily(startByte, endByte, app.db.ColumnFamilyHandles()[consts.MetaCFNum])
	//TODO unittest close test
	defer itr.Close()

	// time range에 해당하는 모든 데이터를 가져온다
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

		rawMetaDataObjs = append(rawMetaDataObjs, metaObj)

	}

	// 가져온 데이터를 제한사항에 맞게 거른다
	switch {
	case len(queryObj.OwnerKey) == 0 && len(queryObj.Qualifier) == 0:
		metaDataObjs = rawMetaDataObjs
	case len(queryObj.OwnerKey) == 0:
		for i, metaObj := range rawMetaDataObjs {
			if bytes.Compare(metaObj.Qualifier, queryObj.Qualifier) == 0 {
				metaDataObjs = append(metaDataObjs, rawMetaDataObjs[i])
			}
		}
	case len(queryObj.Qualifier) == 0:
		for i, metaObj := range rawMetaDataObjs {
			if bytes.Compare(metaObj.OwnerKey, queryObj.OwnerKey) == 0 {
				metaDataObjs = append(metaDataObjs, rawMetaDataObjs[i])
			}
		}
	default:
		for i, metaObj := range rawMetaDataObjs {
			if bytes.Compare(metaObj.Qualifier, queryObj.Qualifier) == 0 && bytes.Compare(metaObj.OwnerKey, queryObj.OwnerKey) == 0 {
				metaDataObjs = append(metaDataObjs, rawMetaDataObjs[i])
			}
		}
	}
	return metaDataObjs, nil

}

func (app *MasterApplication) realDataFetch(fetchObj types.FetchObj) ([]types.RealDataObj, error) {
	var realDataObjs []types.RealDataObj

	for _, rowKey := range fetchObj.RowKeys {
		var realDataObj types.RealDataObj

		realDataObj.RowKey = rowKey
		valueSlice, err := app.db.GetDataFromColumnFamily(consts.RealCFNum, rowKey)
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

func (app *MasterApplication) Destroy() {
	app.db.Close()
}
