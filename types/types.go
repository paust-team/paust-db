package types

import (
	"encoding/binary"
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

//rowkey = timestamp + userkey + datatype + offset
func DataKeyToByteArr(data Data) []byte {

	timestamp := make([]byte, 8)
	binary.BigEndian.PutUint64(timestamp, uint64((data.Timestamp/1000000000)*1000000000))

	offset := make([]byte, 4)
	binary.BigEndian.PutUint32(offset, uint32(data.Timestamp%1000000000))

	dType := make([]byte, 20)
	dType = typeToByteArr(data.Type)

	ret := append(timestamp, data.UserKey...)
	ret = append(ret, dType...)
	ret = append(ret, offset...)

	return ret
}

//string -> byte with padding
func typeToByteArr(dType string) []byte {
	ret := make([]byte, 20)
	for i := 0; i < len(dType); i++ {
		ret[i] = dType[i]
	}
	for i := len(dType); i < 20; i++ {
		ret[i] = 0
	}

	return ret
}
