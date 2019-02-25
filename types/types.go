package types

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
)

//TODO offset 추가
type KeyObj struct {
	Timestamp []byte `json:"timestamp"`
	Salt      []byte `json:"salt"`
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

type QueryObj struct {
	Start     []byte `json:"start"`
	End       []byte `json:"end"`
	OwnerKey  []byte `json:"ownerKey"`
	Qualifier []byte `json:"qualifier"`
}

type FetchObj struct {
	RowKeys [][]byte `json:"rowKeys"`
}

// 주어진 DataQuery로부터 시작할 지점(startByte)과 마지막 지점(endByte)을 구한다.
func CreateStartByteAndEndByte(query QueryObj) ([]byte, []byte) {
	salt := make([]byte, 2)
	binary.BigEndian.PutUint16(salt, 0x0000)
	startKeyObj := KeyObj{Timestamp: query.Start, Salt: salt}
	endKeyObj := KeyObj{Timestamp: query.End, Salt: salt}

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
