package types

import (
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/thoas/go-funk"
)

type RealData struct {
	//Timestamp는 client에서 nano단위로 들어옴.
	Timestamp int64  `json:"timestamp"`
	UserKey   []byte `json:"userKey"`
	Qualifier string `json:"qualifier"`
	Data      []byte `json:"data"`
}

type RealDataSlice []RealData

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

const (
	TimeLen      = 8
	UserKeyLen   = 32
	QualifierLen = 20
	RowKeyLen    = TimeLen + UserKeyLen + QualifierLen
)

func DataToRowKey(data RealData) []byte {
	timestamp := make([]byte, TimeLen)
	qualifier := make([]byte, QualifierLen)
	binary.BigEndian.PutUint64(timestamp, uint64(data.Timestamp))
	qualifier = QualifierToByteArr(data.Qualifier)
	rowKey := funk.FlattenDeep([][]byte{timestamp, data.UserKey, qualifier})

	return rowKey.([]byte)
}

//string -> byte with padding
func QualifierToByteArr(qualifier string) []byte {
	qualifierArr := make([]byte, QualifierLen)
	for i := 0; i < len(qualifier); i++ {
		qualifierArr[i] = qualifier[i]
	}
	for i := len(qualifier); i < QualifierLen; i++ {
		qualifierArr[i] = 0
	}

	return qualifierArr
}

func RowKeyAndValueToRealData(key, value []byte) RealData {

	if len(key) != RowKeyLen {
		fmt.Println("rowkey len error len :", len(key))
	}
	realData := RealData{}

	timestamp := binary.BigEndian.Uint64(key[0:TimeLen])

	realData.Timestamp = int64(timestamp)
	realData.UserKey = make([]byte, UserKeyLen)
	copy(realData.UserKey, key[TimeLen:TimeLen+UserKeyLen])

	qualifier := QualifierWithoutPadding(key[TimeLen+UserKeyLen : RowKeyLen])
	realData.Qualifier = string(qualifier)
	realData.Data = value

	return realData
}

func QualifierWithoutPadding(keySlice []byte) []byte {
	qualifierArr := make([]byte, 0)
	for i := 0; i < QualifierLen; i++ {
		if keySlice[i] != 0x00 {
			qualifierArr = append(qualifierArr, keySlice[i])
		} else {
			i = QualifierLen
		}
	}
	return qualifierArr
}

//MetaResponse에서 offset을 추가한 timestamp
func MetaDataAndKeyToMetaResponse(key []byte, meta MetaData) (MetaResponse, error) {
	metaResponse := MetaResponse{}

	if len(key) != RowKeyLen {
		return metaResponse, errors.New("row key len error")
	}

	timestamp := binary.BigEndian.Uint64(key[0:TimeLen])

	metaResponse.Timestamp = int64(timestamp)
	metaResponse.UserKey = meta.UserKey
	metaResponse.Qualifier = meta.Qualifier

	return metaResponse, nil

}

// 주어진 DataQuery로부터 시작할 지점(startByte)과 마지막 지점(endByte)을 구한다.
func CreateStartByteAndEndByte(query DataQuery) ([]byte, []byte) {
	startTimestamp := make([]byte, TimeLen)
	endTimestamp := make([]byte, TimeLen)

	binary.BigEndian.PutUint64(startTimestamp, uint64(query.Start))
	binary.BigEndian.PutUint64(endTimestamp, uint64(query.End))

	userKey := make([]byte, UserKeyLen)
	qualifier := make([]byte, QualifierLen)

	startByte := make([]byte, RowKeyLen)
	endByte := make([]byte, RowKeyLen)
	/*
	 * Qualifier, UserKey의 nil여부에 따라 4가지 경우가 존재한다.
	 */

	switch {
	case query.UserKey == nil && query.Qualifier == "":
		{
			start := funk.FlattenDeep([][]byte{startTimestamp, userKey, qualifier})
			startByte = start.([]byte)
		}
	case query.Qualifier == "":
		{
			start := funk.FlattenDeep([][]byte{startTimestamp, query.UserKey, qualifier})
			startByte = start.([]byte)

		}
	case query.UserKey == nil:
		{
			qualifierPadding := QualifierToByteArr(query.Qualifier)
			start := funk.FlattenDeep([][]byte{startTimestamp, userKey, qualifierPadding})
			startByte = start.([]byte)
		}
	default:
		{
			qualifierPadding := QualifierToByteArr(query.Qualifier)
			start := funk.FlattenDeep([][]byte{startTimestamp, query.UserKey, qualifierPadding})
			startByte = start.([]byte)
		}
	}
	end := funk.FlattenDeep([][]byte{endTimestamp, userKey, qualifier})
	endByte = end.([]byte)

	return startByte, endByte

}
