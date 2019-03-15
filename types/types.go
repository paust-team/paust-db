package types

import (
	"encoding/binary"
)

type MetaDataObj struct {
	RowKey    []byte `json:"rowKey"`
	OwnerId   string `json:"ownerId"`
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
	Start     uint64 `json:"start"`
	End       uint64 `json:"end"`
	OwnerId   string `json:"ownerId"`
	Qualifier []byte `json:"qualifier"`
}

type FetchObj struct {
	RowKeys [][]byte `json:"rowKeys"`
}

func GetRowKey(timestamp uint64, salt uint16) []byte {
	rowKey := make([]byte, 10)
	binary.BigEndian.PutUint64(rowKey[0:], timestamp)
	binary.BigEndian.PutUint16(rowKey[8:], salt)

	return rowKey
}
