package types

import (
	"encoding/json"
	"fmt"
)

//TODO offset 추가
type KeyObj struct {
	Timestamp uint64 `json:"timestamp"`
	Salt      uint8  `json:"salt"`
}

type MetaDataObj struct {
	RowKey    []byte `json:"rowKey"`
	OwnerKey  []byte `json:"ownerKey"`
	Qualifier []byte `json:"qualifier"`
}

type RealDataObj struct {
	RowKey []byte `json:"rowKey"`
	Data   []byte `json:"data"`
}

type BaseDataObj struct {
	MetaData MetaDataObj `json:"meta"`
	RealData RealDataObj `json:"real"`
}

type MetaDataQueryObj struct {
	Start     uint64 `json:"start"`
	End       uint64 `json:"end"`
	OwnerKey  []byte `json:"ownerKey"`
	Qualifier []byte `json:"qualifier"`
}

type RealDataQueryObj struct {
	RowKeys [][]byte `json:"rowKeys"`
}

const (
	OwnerKeyLen = 32
)

// 주어진 DataQuery로부터 시작할 지점(startByte)과 마지막 지점(endByte)을 구한다.
func CreateStartByteAndEndByte(query MetaDataQueryObj) ([]byte, []byte) {
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
