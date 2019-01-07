package types

import (
	"encoding/binary"
)

type Data struct {
	//Timestamp는 client에서 nano단위로 들어옴.
	Timestamp int64  `json:"timestamp"`
	UserKey   []byte `json:"userKey"`
	Qualifier string `json:"qualifier"`
	Data      []byte `json:"data"`
}

type DataSlice []Data

type MetaData struct {
	UserKey   []byte `json:"userKey"`
	Qualifier string `json:"qualifier"`
}

type DataQuery struct {
	Start     int64  `json:"start"`
	End       int64  `json:"end"`
	UserKey   []byte `json:"userKey"`
	Qualifier string `json:"qualifier"`
}

func DataToRowKey(data Data) []byte {
	timestamp := make([]byte, 8)
	qualifier := make([]byte, 20)
	binary.BigEndian.PutUint64(timestamp, uint64(data.Timestamp))
	qualifier = QualifierToByteArr(data.Qualifier)

	rowKey := append(timestamp, data.UserKey...)
	rowKey = append(rowKey, qualifier...)

	return rowKey
}

//string -> byte with padding
func QualifierToByteArr(qualifier string) []byte {
	qualifierArr := make([]byte, 20)
	for i := 0; i < len(qualifier); i++ {
		qualifierArr[i] = qualifier[i]
	}
	for i := len(qualifier); i < 20; i++ {
		qualifierArr[i] = 0
	}

	return qualifierArr
}
