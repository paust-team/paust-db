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

//rowkey = timestamp + userkey + datatype + offset
func DataToRowKey(data Data) []byte {
	timestamp := make([]byte, 8)
	offset := make([]byte, 4)
	dType := make([]byte, 20)
	binary.BigEndian.PutUint64(timestamp, uint64((data.Timestamp/1000000000)*1000000000))
	binary.BigEndian.PutUint32(offset, uint32(data.Timestamp%1000000000))
	dType = typeToByteArr(data.Type)

	rowKey := append(timestamp, data.UserKey...)
	rowKey = append(rowKey, dType...)
	rowKey = append(rowKey, offset...)

	return rowKey
}

func RowKeyToData(key, value []byte) Data {
	data := Data{}
	timeWindow := binary.BigEndian.Uint64(key[0:8])
	timeOffset := uint64(binary.BigEndian.Uint32(key[60:64]))

	data.Timestamp = int64(timeWindow + timeOffset)
	data.UserKey = key[8:40]
	data.Type = string(key[40:60])
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

//MetaResponse에서 offset을 추가한 timestamp
func MetaDataToMetaResponse(key []byte, meta MetaData) (MetaResponse, error) {
	metaResponse := MetaResponse{}

	if len(key) != 64 {
		return metaResponse, errors.New("invalid byte array length")
	}

	timestamp := binary.BigEndian.Uint64(key[0:8])
	offset := binary.BigEndian.Uint32(key[60:64])

	metaResponse.Timestamp = int64(timestamp + uint64(offset))
	metaResponse.UserKey = meta.UserKey
	metaResponse.Type = meta.Type

	return metaResponse, nil

}

// 주어진 DataQuery로부터 시작할 지점(startByte)과 마지막 지점(endByte)을 구한다.
// Query가 이상 미만이라고 가정한다. offset이 아닌 timestamp사이의 조회이다.
func CreateStartByteAndEndByte(query DataQuery) ([]byte, []byte) {
	startByte := make([]byte, 8)
	endByte := make([]byte, 8)
	binary.BigEndian.PutUint64(startByte, uint64((query.Start/1000000000)*1000000000))
	//미만이므로 -1
	binary.BigEndian.PutUint64(endByte, uint64((query.End/1000000000)-1)*1000000000)
	/*
	 * type, UserKey의 nil여부에 따라 4가지 경우가 존재한다.
	 */

	switch {
	case query.UserKey == nil && query.Type == "":
		{
			userKey := make([]byte, 32)
			dType := make([]byte, 20)
			startByte = append(startByte, userKey...)
			startByte = append(startByte, dType...)
			for i := 0; i < 32; i++ {
				userKey[i] = 0xff
			}
			for i := 0; i < 20; i++ {
				dType[i] = 0xff
			}
			endByte = append(endByte, userKey...)
			endByte = append(endByte, dType...)
		}
	case query.Type == "":
		{
			startByte = append(startByte, query.UserKey...)
			endByte = append(endByte, query.UserKey...)
			dType := make([]byte, 20)
			startByte = append(startByte, dType...)
			for i := 0; i < 20; i++ {
				dType[i] = 0xff
			}
			endByte = append(endByte, dType...)
		}
	case query.UserKey == nil:
		{
			userKey := make([]byte, 32)
			for i := 0; i < 32; i++ {
				userKey[i] = 0x00
			}
			startByte = append(startByte, userKey...)
			startByte = append(startByte, []byte(query.Type)...)
			for i := 0; i < 32; i++ {
				userKey[i] = 0xff
			}
			endByte = append(endByte, userKey...)
			endByte = append(endByte, []byte(query.Type)...)
		}
	default:
		{
			startByte = append(startByte, query.UserKey...)
			startByte = append(startByte, []byte(query.Type)...)
			endByte = append(endByte, query.UserKey...)
			endByte = append(endByte, []byte(query.Type)...)
		}
	}
	startOffset := make([]byte, 4)
	endOffset := make([]byte, 4)
	for i := 0; i < 4; i++ {
		endOffset[i] = 0xff
	}
	startByte = append(startByte, startOffset...)
	endByte = append(endByte, endOffset...)

	return startByte, endByte

}
