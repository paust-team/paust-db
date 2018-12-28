package types

import (
	"encoding/binary"
	"errors"
)

type Data struct {
	//Timestamp는 client에서 nano단위로 들어옴.
	Timestamp int64  `json:"timestamp"`
	UserKey   []byte `json:"userKey"`
	Type      string `json:"type"`
	Data      []byte `json:"data"`
}

type DataSlice []Data

type MetaData struct {
	UserKey []byte `json:"userKey"`
	Type    string `json:"type"`
}

type DataQuery struct {
	Start   int64  `json:"start"`
	End     int64  `json:"end"`
	UserKey []byte `json:"userKey"`
	Type    string `json:"type"`
}

type MetaResponse struct {
	Timestamp int64  `json:"timestamp"`
	UserKey   []byte `json:"userKey"`
	Type      string `json:"type"`
}

type MetaResponseSlice []MetaResponse

func DataToRowKey(data Data) []byte {
	timestamp := make([]byte, 8)
	dType := make([]byte, 20)
	binary.BigEndian.PutUint64(timestamp, uint64(data.Timestamp))
	dType = typeToByteArr(data.Type)

	rowKey := append(timestamp, data.UserKey...)
	rowKey = append(rowKey, dType...)

	return rowKey
}

func RowKeyToData(key, value []byte) Data {
	data := Data{}

	timestamp := binary.BigEndian.Uint64(key[0:8])

	data.Timestamp = int64(timestamp)
	data.UserKey = make([]byte, 32)
	copy(data.UserKey, key[8:40])

	dType := typeWithoutPadding(key[40:60])
	data.Type = string(dType)
	data.Data = value

	return data
}

//string -> byte with padding
func typeToByteArr(dType string) []byte {
	typeArr := make([]byte, 20)
	for i := 0; i < len(dType); i++ {
		typeArr[i] = dType[i]
	}
	for i := len(dType); i < 20; i++ {
		typeArr[i] = 0
	}

	return typeArr
}

func typeWithoutPadding(keySlice []byte) []byte {
	typeArr := make([]byte, 0)
	for i := 0; i < 20; i++ {
		if keySlice[i] != 0x00 {
			typeArr = append(typeArr, keySlice[i])
		} else {
			i = 20
		}
	}
	return typeArr
}

//MetaResponse에서 offset을 추가한 timestamp
func MetaDataToMetaResponse(key []byte, meta MetaData) (MetaResponse, error) {
	metaResponse := MetaResponse{}

	if len(key) != 60 {
		return metaResponse, errors.New("invalid byte array length")
	}

	timestamp := binary.BigEndian.Uint64(key[0:8])

	metaResponse.Timestamp = int64(timestamp)
	metaResponse.UserKey = meta.UserKey
	metaResponse.Type = meta.Type

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
	dType := make([]byte, 20)
	switch {
	case query.UserKey == nil && query.Type == "":
		{
			startByte = append(startByte, userKey...)
			startByte = append(startByte, dType...)
			endByte = append(endByte, userKey...)
			endByte = append(endByte, dType...)
		}
	case query.Type == "":
		{
			startByte = append(startByte, query.UserKey...)
			endByte = append(endByte, userKey...)
			startByte = append(startByte, dType...)
			endByte = append(endByte, dType...)
		}
	case query.UserKey == nil:
		{
			startByte = append(startByte, userKey...)
			startByte = append(startByte, []byte(query.Type)...)
			endByte = append(endByte, userKey...)
			endByte = append(endByte, dType...)
		}
	default:
		{
			startByte = append(startByte, query.UserKey...)
			startByte = append(startByte, []byte(query.Type)...)
			endByte = append(endByte, userKey...)
			endByte = append(endByte, dType...)
		}
	}

	return startByte, endByte

}
