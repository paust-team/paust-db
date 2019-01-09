package types

import (
	"encoding/binary"
	"errors"
	"fmt"
)

type RealData struct {
	//Timestamp는 client에서 nano단위로 들어옴.
	Timestamp int64  `json:"timestamp"`
	UserKey   []byte `json:"userKey"`
	Qualifier string `json:"qualifier"`
	Data      []byte `json:"data"`
}

type DataSlice []RealData

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

type MetaResponse struct {
	Timestamp int64  `json:"timestamp"`
	UserKey   []byte `json:"userKey"`
	Qualifier string `json:"qualifier"`
}

type MetaResponseSlice []MetaResponse

func DataToRowKey(data RealData) []byte {
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

func RowKeyAndValueToData(key, value []byte) RealData {

	if len(key) != 60 {
		fmt.Println("rowkey len error len :", len(key))
	}
	realData := RealData{}

	timestamp := binary.BigEndian.Uint64(key[0:8])

	realData.Timestamp = int64(timestamp)
	realData.UserKey = make([]byte, 32)
	copy(realData.UserKey, key[8:40])

	qualifier := QualifierWithoutPadding(key[40:60])
	realData.Qualifier = string(qualifier)
	realData.Data = value

	return realData
}

func QualifierWithoutPadding(keySlice []byte) []byte {
	qualifierArr := make([]byte, 0)
	for i := 0; i < 20; i++ {
		if keySlice[i] != 0x00 {
			qualifierArr = append(qualifierArr, keySlice[i])
		} else {
			i = 20
		}
	}
	return qualifierArr
}

//MetaResponse에서 offset을 추가한 timestamp
func MetaDataAndKeyToMetaResponse(key []byte, meta MetaData) (MetaResponse, error) {
	metaResponse := MetaResponse{}

	if len(key) != 60 {
		return metaResponse, errors.New("row key len error")
	}

	timestamp := binary.BigEndian.Uint64(key[0:8])

	metaResponse.Timestamp = int64(timestamp)
	metaResponse.UserKey = meta.UserKey
	metaResponse.Qualifier = meta.Qualifier

	return metaResponse, nil

}