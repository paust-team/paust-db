package types

import (
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/thoas/go-funk"
)

type WRealDataObj struct {
	//Timestamp는 client에서 nano단위로 들어옴.
	Timestamp uint64 `json:"timestamp"`
	UserKey   []byte `json:"userKey"`
	Qualifier string `json:"qualifier"`
	Data      []byte `json:"data"`
}

type WRealDataObjs []WRealDataObj

type WMetaDataObj struct {
	UserKey   []byte `json:"userKey"`
	Qualifier string `json:"qualifier"`
}

type RDataQueryObj struct {
	Start     uint64 `json:"start"`
	End       uint64 `json:"end"`
	UserKey   []byte `json:"userKey"`
	Qualifier string `json:"qualifier"`
}

type RMetaResObj struct {
	Timestamp uint64 `json:"timestamp"`
	UserKey   []byte `json:"userKey"`
	Qualifier string `json:"qualifier"`
}

type RMetaResObjs []RMetaResObj

const (
	TimeLen      = 8
	UserKeyLen   = 32
	QualifierLen = 20
	RowKeyLen    = TimeLen + UserKeyLen + QualifierLen
)

func WRealDataObjToRowKey(data WRealDataObj) []byte {
	timestamp := make([]byte, TimeLen)
	qualifier := make([]byte, QualifierLen)
	binary.BigEndian.PutUint64(timestamp, data.Timestamp)
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

func RowKeyAndValueToWRealDataObj(key, value []byte) WRealDataObj {

	if len(key) != RowKeyLen {
		fmt.Println("rowkey len error len :", len(key))
	}
	realData := WRealDataObj{}

	timestamp := binary.BigEndian.Uint64(key[0:TimeLen])

	realData.Timestamp = timestamp
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
func WMetaDataObjAndKeyToRMetaResObj(key []byte, meta WMetaDataObj) (RMetaResObj, error) {
	metaResponse := RMetaResObj{}

	if len(key) != RowKeyLen {
		return metaResponse, errors.New("row key len error")
	}

	timestamp := binary.BigEndian.Uint64(key[0:TimeLen])

	metaResponse.Timestamp = timestamp
	metaResponse.UserKey = meta.UserKey
	metaResponse.Qualifier = meta.Qualifier

	return metaResponse, nil

}

// 주어진 DataQuery로부터 시작할 지점(startByte)과 마지막 지점(endByte)을 구한다.
func CreateStartByteAndEndByte(query RDataQueryObj) ([]byte, []byte) {
	startTimestamp := make([]byte, TimeLen)
	endTimestamp := make([]byte, TimeLen)

	binary.BigEndian.PutUint64(startTimestamp, query.Start)
	binary.BigEndian.PutUint64(endTimestamp, query.End)

	userKey := make([]byte, UserKeyLen)
	qualifier := make([]byte, QualifierLen)

	startByte := make([]byte, RowKeyLen)
	endByte := make([]byte, RowKeyLen)
	/*
	 * Qualifier, UserKey의 nil여부에 따라 4가지 경우가 존재한다.
	 */

	switch {
	case query.UserKey == nil && query.Qualifier == "":
		startByte = funk.FlattenDeep([][]byte{startTimestamp, userKey, qualifier}).([]byte)
	case query.Qualifier == "":
		startByte = funk.FlattenDeep([][]byte{startTimestamp, query.UserKey, qualifier}).([]byte)
	case query.UserKey == nil:
		{
			qualifierPadding := QualifierToByteArr(query.Qualifier)
			startByte = funk.FlattenDeep([][]byte{startTimestamp, userKey, qualifierPadding}).([]byte)
		}
	default:
		{
			qualifierPadding := QualifierToByteArr(query.Qualifier)
			startByte = funk.FlattenDeep([][]byte{startTimestamp, query.UserKey, qualifierPadding}).([]byte)
		}
	}
	endByte = funk.FlattenDeep([][]byte{endTimestamp, userKey, qualifier}).([]byte)

	return startByte, endByte

}
