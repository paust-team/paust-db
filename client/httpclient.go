package client

import (
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
		if dataObj.Timestamp == 0 {
			return nil, errors.Errorf("timestamp must not be 0.")
		}
		if len(dataObj.OwnerId) > consts.OwnerIdLenLimit || len(dataObj.OwnerId) == 0 {
			return nil, errors.Errorf("%s: wrong ownerId length. Expect %v or below, got %v", dataObj.OwnerId, consts.OwnerIdLenLimit, len(dataObj.OwnerId))
		}
		rowKey := types.GetRowKey(dataObj.Timestamp, uint16(rand.Intn(65536)))
		baseDataObjs = append(baseDataObjs, types.BaseDataObj{MetaData: types.MetaDataObj{RowKey: rowKey, OwnerId: dataObj.OwnerId, Qualifier: []byte(dataObj.Qualifier)}, RealData: types.RealDataObj{RowKey: rowKey, Data: dataObj.Data}})
	}

	jsonBytes, err := json.Marshal(baseDataObjs)
	if err != nil {
		return nil, errors.Wrap(err, "marshal failed")
	}

	bres, err := client.rpcClient.BroadcastTxCommit(jsonBytes)
	return bres, err
}

func (client *HTTPClient) Query(queryObj InputQueryObj) (*ctypes.ResultABCIQuery, error) {

	if len(queryObj.OwnerId) > consts.OwnerIdLenLimit {
		return nil, errors.Errorf("wrong ownerId length. Expect %v or below, got %v", consts.OwnerIdLenLimit, len(queryObj.OwnerId))
	}

	if queryObj.Start > queryObj.End {
		err := errors.New("query start must be greater than end")
		return nil, err
	}

	jsonBytes, err := json.Marshal(types.QueryObj{Start: queryObj.Start, End: queryObj.End, OwnerId: queryObj.OwnerId, Qualifier: []byte(queryObj.Qualifier)})
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
			deserializedMeta = append(deserializedMeta, OutputQueryObj{Id: metaDataObj.RowKey, Timestamp: binary.BigEndian.Uint64(metaDataObj.RowKey[0:8]), OwnerId: metaDataObj.OwnerId, Qualifier: string(metaDataObj.Qualifier)})
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
			deserializedReal = append(deserializedReal, OutputFetchObj{Id: realDataObj.RowKey, Timestamp: binary.BigEndian.Uint64(realDataObj.RowKey[0:8]), Data: realDataObj.Data})
		}
		deserializedObj, err := json.Marshal(deserializedReal)
		if err != nil {
			return nil, errors.Wrap(err, "marshal failed")
		}
		return deserializedObj, nil
	}
}
