package client

import (
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"github.com/paust-team/paust-db/consts"
	"github.com/paust-team/paust-db/types"
	"github.com/pkg/errors"
	rpcClient "github.com/tendermint/tendermint/rpc/client"
	ctypes "github.com/tendermint/tendermint/rpc/core/types"
	"math/rand"
	"time"
)

// HTTPClient is a HTTP jsonrpc implementation of Client.
type HTTPClient struct {
	rpcClient rpcClient.Client
}

// NewHTTPClient creates HTTPClient with the given remote address.
func NewHTTPClient(remote string) *HTTPClient {
	c := rpcClient.NewHTTP(remote, consts.WsEndpoint)
	rand.Seed(time.Now().UnixNano())

	return &HTTPClient{
		rpcClient: c,
	}
}

func (client *HTTPClient) Put(dataObjs []InputDataObj) (*ctypes.ResultBroadcastTxCommit, error) {
	var baseDataObjs []types.BaseDataObj
	for _, dataObj := range dataObjs {
		if len(dataObj.OwnerKey) != consts.OwnerKeyLen {
			return nil, errors.Errorf("%s: wrong ownerKey length. Expected %v, got %v", base64.StdEncoding.EncodeToString(dataObj.OwnerKey), consts.OwnerKeyLen, len(dataObj.OwnerKey))
		}
		timestamp := make([]byte, 8)
		binary.BigEndian.PutUint64(timestamp, dataObj.Timestamp)
		salt := make([]byte, 2)
		binary.BigEndian.PutUint16(salt, uint16(rand.Intn(65536)))
		rowKey, err := json.Marshal(types.KeyObj{Timestamp: timestamp, Salt: salt})
		if err != nil {
			return nil, errors.Wrap(err, "marshal failed")
		}
		baseDataObjs = append(baseDataObjs, types.BaseDataObj{MetaData: types.MetaDataObj{RowKey: rowKey, OwnerKey: dataObj.OwnerKey, Qualifier: []byte(dataObj.Qualifier)}, RealData: types.RealDataObj{RowKey: rowKey, Data: dataObj.Data}})
	}

	jsonBytes, err := json.Marshal(baseDataObjs)
	if err != nil {
		return nil, errors.Wrap(err, "marshal failed")
	}

	bres, err := client.rpcClient.BroadcastTxCommit(jsonBytes)
	return bres, err
}

func (client *HTTPClient) Query(start uint64, end uint64, ownerKey []byte, qualifier string) (*ctypes.ResultABCIQuery, error) {
	if ownerKey == nil {
		return nil, errors.Errorf("ownerKey must not be nil.")
	}

	if len(ownerKey) != 0 && len(ownerKey) != consts.OwnerKeyLen {
		return nil, errors.Errorf("wrong ownerKey length. Expected %v, got %v", consts.OwnerKeyLen, len(ownerKey))
	}

	startTimestamp := make([]byte, 8)
	endTimestamp := make([]byte, 8)
	binary.BigEndian.PutUint64(startTimestamp, start)
	binary.BigEndian.PutUint64(endTimestamp, end)
	jsonBytes, err := json.Marshal(types.QueryObj{Start: startTimestamp, End: endTimestamp, OwnerKey: ownerKey, Qualifier: []byte(qualifier)})
	if err != nil {
		return nil, errors.Wrap(err, "marshal failed")
	}

	res, err := client.rpcClient.ABCIQuery(consts.QueryPath, jsonBytes)
	if err != nil {
		return nil, err
	}

	deserializedValue, err := deSerializeKeyObj(res.Response.Value, true)
	if err != nil {
		return nil, errors.Wrap(err, "deserialize key object failed")
	}

	res.Response.Value = deserializedValue
	return res, nil
}

func (client *HTTPClient) Fetch(fetchObj InputFetchObj) (*ctypes.ResultABCIQuery, error) {
	var convertedFetchObj types.FetchObj
	for _, id := range fetchObj.Ids {
		convertedFetchObj.RowKeys = append(convertedFetchObj.RowKeys, id)
	}

	jsonBytes, err := json.Marshal(convertedFetchObj)
	if err != nil {
		return nil, errors.Wrap(err, "marshal failed")
	}

	res, err := client.rpcClient.ABCIQuery(consts.FetchPath, jsonBytes)
	if err != nil {
		return nil, err
	}

	deserializedValue, err := deSerializeKeyObj(res.Response.Value, false)
	if err != nil {
		return nil, errors.Wrap(err, "deserialize key object failed")
	}

	res.Response.Value = deserializedValue
	return res, nil
}

func deSerializeKeyObj(obj []byte, isMeta bool) ([]byte, error) {
	if isMeta == true {
		var metaDataObjs []types.MetaDataObj
		if err := json.Unmarshal(obj, &metaDataObjs); err != nil {
			return nil, errors.Wrap(err, "unmarshal failed")
		}

		var deserializedMeta []OutputQueryObj
		for _, metaDataObj := range metaDataObjs {
			var keyObj = types.KeyObj{}
			if err := json.Unmarshal(metaDataObj.RowKey, &keyObj); err != nil {
				return nil, errors.Wrap(err, "unmarshal failed")
			}
			deserializedMeta = append(deserializedMeta, OutputQueryObj{Id: metaDataObj.RowKey, Timestamp: binary.BigEndian.Uint64(keyObj.Timestamp), OwnerKey: metaDataObj.OwnerKey, Qualifier: string(metaDataObj.Qualifier)})
		}
		deserializedObj, err := json.Marshal(deserializedMeta)
		if err != nil {
			return nil, errors.Wrap(err, "marshal failed")
		}
		return deserializedObj, nil
	} else {
		var realDataObjs []types.RealDataObj
		if err := json.Unmarshal(obj, &realDataObjs); err != nil {
			return nil, errors.Wrap(err, "unmarshal failed")
		}

		var deserializedReal []OutputFetchObj
		for _, realDataObj := range realDataObjs {
			var keyObj = types.KeyObj{}
			if err := json.Unmarshal(realDataObj.RowKey, &keyObj); err != nil {
				return nil, errors.Wrap(err, "unmarshal failed")
			}
			deserializedReal = append(deserializedReal, OutputFetchObj{Id: realDataObj.RowKey, Timestamp: binary.BigEndian.Uint64(keyObj.Timestamp), Data: realDataObj.Data})
		}
		deserializedObj, err := json.Marshal(deserializedReal)
		if err != nil {
			return nil, errors.Wrap(err, "marshal failed")
		}
		return deserializedObj, nil
	}
}
