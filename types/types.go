package types

import (
	"encoding/json"
	"fmt"
)

type WRealDataObj struct {
	//Timestamp는 client에서 nano단위로 들어옴.
	Timestamp uint64 `json:"timestamp"`
	OwnerKey  []byte `json:"ownerKey"`
	Qualifier []byte `json:"qualifier"`
	Data      []byte `json:"data"`
}

type WRealDataObjs []WRealDataObj

type WMetaDataObj struct {
	OwnerKey  []byte `json:"ownerKey"`
	Qualifier []byte `json:"qualifier"`
}

type KeyObj struct {
	Timestamp uint64 `json:"timestamp"`
}

//RowKey is marshaled KeyObj
type RowKey []byte
type RowKeys []RowKey

type RMetaDataQueryObj struct {
	Start uint64 `json:"start"`
	End   uint64 `json:"end"`
}

type RRealDataQueryObj struct {
	Keys RowKeys `json:"rowKeys"`
}

type RMetaDataResObj struct {
	RowKey    RowKey `json:"rowKey"`
	OwnerKey  []byte `json:"ownerKey"`
	Qualifier []byte `json:"qualifier"`
}

type RMetaDataResObjs []RMetaDataResObj

type RRealDataResObj struct {
	RowKey RowKey `json:"rowKey"`
	Data   []byte `json:"data"`
}

type RRealDataResObjs []RRealDataResObj

const (
	OwnerKeyLen = 32
)

func WRealDataObjToRowKey(data WRealDataObj) []byte {
	keyObj := KeyObj{Timestamp: data.Timestamp}
	rowKey, _ := json.Marshal(keyObj)

	return rowKey
}

// 주어진 DataQuery로부터 시작할 지점(startByte)과 마지막 지점(endByte)을 구한다.
func CreateStartByteAndEndByte(query RMetaDataQueryObj) ([]byte, []byte) {
	startKeyObj := KeyObj{Timestamp: query.Start}
	endKeyObj := KeyObj{Timestamp: query.End}

	startByte, err := json.Marshal(startKeyObj)
	if err != nil {
		fmt.Println(err)
	}
	endByte, err := json.Marshal(endKeyObj)
	if err != nil {
		fmt.Println(err)
	}

	return startByte, endByte

}
