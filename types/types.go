package types

import (
	"encoding/binary"
	"errors"
	"fmt"
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

type MetaResponse struct {
	Timestamp int64  `json:"timestamp"`
	UserKey   []byte `json:"userKey"`
	Qualifier string `json:"qualifier"`
}

type MetaResponseSlice []MetaResponse

func DataToRowKey(data Data) []byte {
	timestamp := make([]byte, 8)
	qualifier := make([]byte, 20)
	binary.BigEndian.PutUint64(timestamp, uint64(data.Timestamp))
	qualifier = QualifierToByteArr(data.Qualifier)

	rowKey := append(timestamp, data.UserKey...)
	rowKey = append(rowKey, qualifier...)

	return rowKey
}

func RowKeyAndValueToData(key, value []byte) Data {

	if len(key) != 60 {
		fmt.Println("rowkey len error")
	}
	data := Data{}

	timestamp := binary.BigEndian.Uint64(key[0:8])

	data.Timestamp = int64(timestamp)
	data.UserKey = make([]byte, 32)
	copy(data.UserKey, key[8:40])

	qualifier := QualifierWithoutPadding(key[40:60])
	data.Qualifier = string(qualifier)
	data.Data = value

	return data
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
		return metaResponse, errors.New("invalid byte array length")
	}

	timestamp := binary.BigEndian.Uint64(key[0:8])

	metaResponse.Timestamp = int64(timestamp)
	metaResponse.UserKey = meta.UserKey
	metaResponse.Qualifier = meta.Qualifier

	return metaResponse, nil

}

// 주어진 DataQuery로부터 시작할 지점(startByte)과 마지막 지점(endByte)을 구한다.
func CreateStartByteAndEndByte(query DataQuery) ([]byte, []byte) {
	startByte := make([]byte, 8)
	endByte := make([]byte, 8)

	binary.BigEndian.PutUint64(startByte, uint64(query.Start))
	binary.BigEndian.PutUint64(endByte, uint64(query.End))
	/*
	 * type, UserKey의 nil여부에 따라 4가지 경우가 존재한다.
	 */
	userKey := make([]byte, 32)
	qualifier := make([]byte, 20)
	switch {
	case query.UserKey == nil && query.Qualifier == "":
		{
			startByte = append(startByte, userKey...)
			startByte = append(startByte, qualifier...)
			endByte = append(endByte, userKey...)
			endByte = append(endByte, qualifier...)
		}
	case query.Qualifier == "":
		{
			startByte = append(startByte, query.UserKey...)
			startByte = append(startByte, qualifier...)
			endByte = append(endByte, userKey...)
			endByte = append(endByte, qualifier...)
		}
	case query.UserKey == nil:
		{
			typePadding := QualifierToByteArr(query.Qualifier)
			startByte = append(startByte, userKey...)
			startByte = append(startByte, typePadding...)
			endByte = append(endByte, userKey...)
			endByte = append(endByte, qualifier...)
		}
	default:
		{
			typePadding := QualifierToByteArr(query.Qualifier)
			startByte = append(startByte, query.UserKey...)
			startByte = append(startByte, typePadding...)
			endByte = append(endByte, userKey...)
			endByte = append(endByte, qualifier...)
		}
	}

	return startByte, endByte

}
