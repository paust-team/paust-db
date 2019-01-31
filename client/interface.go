package client

import (
	ctypes "github.com/tendermint/tendermint/rpc/core/types"
)

type Client interface {
	WriteData(dataObjs []InputDataObj) (*ctypes.ResultBroadcastTx, error)
	Query(start uint64, end uint64, ownerKey []byte, qualifier []byte) (*ctypes.ResultABCIQuery, error)
	Fetch(queryObj InputQueryObj) (*ctypes.ResultABCIQuery, error)
}
