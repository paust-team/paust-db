package types

import (
	"encoding/binary"
)

type Data struct {
	//Timestamp는 client에서 nano단위로 들어옴.
	Timestamp int64 `json:"timestamp"`
	UserKey []byte `json:"userKey"`
	Type string `json:"type"`
	Data []byte `json:"data"`
}

type BetweenQuery struct {
	Start int64 `json:"start"`
	Stop int64 `json:"stop"`
}

func DataKeyToByteArr(data Data) []byte{

	timestamp := make([]byte, 8)
	binary.BigEndian.PutUint64(timestamp, uint64((data.Timestamp / 1000000000) * 1000000000))

	offset := make([]byte, 4)

	binary.BigEndian.PutUint32(offset, uint32(data.Timestamp % 1000000000))

	ret := append(timestamp, data.UserKey...)
	ret = append(ret, offset...)

	return ret
}