// Package client는 paust-db의 read, write를 위한 go client library임
package client

import (
	ctypes "github.com/tendermint/tendermint/rpc/core/types"
)

// Client는 paust-db와 communicate하는 기본적인 client임
type Client interface {
	// Put는 InputDataObj slice의 데이터를 write하고 그 결과를 tendermint의 ResultBroadcastTx로 return.
	Put(dataObjs []InputDataObj) (*ctypes.ResultBroadcastTx, error)

	// Query는 start와 end사이에 있는 데이터의 metadata를 ResultABCIQuery에 담아서 return.
	// ownerKey와 qualifier가 명시된 경우 해당 ownerKey, qualifier와 일치하는 데이터만을 read.
	// ResultABCIQuery.Response.Value에 실제 read한 데이터가 OutputQueryObj의 slice로 담겨있음.
	Query(start uint64, end uint64, ownerKey []byte, qualifier []byte) (*ctypes.ResultABCIQuery, error)

	// Fetch는 InputFetchObj와 일치하는 데이터를 tendermint의 ResultABCIQuery에 담아서 return.
	// ResultABCIQuery.Response.Value에 실제 read한 데이터가 OutputFetchObj의 slice로 담겨있음.
	Fetch(fetchObj InputFetchObj) (*ctypes.ResultABCIQuery, error)
}
