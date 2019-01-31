package client

import (
	"encoding/base64"
	"encoding/json"
	"github.com/paust-team/paust-db/consts"
	"github.com/paust-team/paust-db/types"
	"github.com/pkg/errors"
	rpcClient "github.com/tendermint/tendermint/rpc/client"
	ctypes "github.com/tendermint/tendermint/rpc/core/types"
	"math/rand"
	"time"
)

type HTTPClient struct {
	rpcClient rpcClient.Client
}

func NewHTTPClient(remote string) *HTTPClient {
	c := rpcClient.NewHTTP(remote, consts.WsEndpoint)
	rand.Seed(time.Now().UnixNano())

	return &HTTPClient{
		rpcClient: c,
	}
}

func (client *HTTPClient) WriteData(dataObjs []InputDataObj) (*ctypes.ResultBroadcastTx, error) {
	var baseDataObjs []types.BaseDataObj
	for _, dataObj := range dataObjs {
		if len(dataObj.OwnerKey) != consts.OwnerKeyLen {
			return nil, errors.Errorf("%s: wrong ownerKey length. Expected %v, got %v", base64.StdEncoding.EncodeToString(dataObj.OwnerKey), consts.OwnerKeyLen, len(dataObj.OwnerKey))
		}
		rowKey, err := json.Marshal(types.KeyObj{Timestamp: dataObj.Timestamp, Salt: uint8(rand.Intn(256))})
		if err != nil {
			return nil, errors.Wrap(err, "marshal failed")
		}
		baseDataObjs = append(baseDataObjs, types.BaseDataObj{MetaData: types.MetaDataObj{RowKey: rowKey, OwnerKey: dataObj.OwnerKey, Qualifier: dataObj.Qualifier}, RealData: types.RealDataObj{RowKey: rowKey, Data: dataObj.Data}})
	}

	jsonBytes, err := json.Marshal(baseDataObjs)
	if err != nil {
		return nil, errors.Wrap(err, "marshal failed")
	}

	bres, err := client.rpcClient.BroadcastTxSync(jsonBytes)
	return bres, err
}

func (client *HTTPClient) Query(start uint64, end uint64, ownerKey []byte, qualifier []byte) (*ctypes.ResultABCIQuery, error) {
	if len(ownerKey) != 0 && len(ownerKey) != consts.OwnerKeyLen {
		return nil, errors.Errorf("wrong ownerKey length. Expected %v, got %v", consts.OwnerKeyLen, len(ownerKey))
	}

	jsonBytes, err := json.Marshal(types.MetaDataQueryObj{Start: start, End: end, OwnerKey: ownerKey, Qualifier: qualifier})
	if err != nil {
		return nil, errors.Wrap(err, "marshal failed")
	}

	res, err := client.rpcClient.ABCIQuery(consts.MetaDataQueryPath, jsonBytes)
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

func (client *HTTPClient) Fetch(queryObj InputQueryObj) (*ctypes.ResultABCIQuery, error) {
	var realDataQueryObj types.RealDataQueryObj
	for _, id := range queryObj.Ids {
		realDataQueryObj.RowKeys = append(realDataQueryObj.RowKeys, id)
	}

	jsonBytes, err := json.Marshal(realDataQueryObj)
	if err != nil {
		return nil, errors.Wrap(err, "marshal failed")
	}

	res, err := client.rpcClient.ABCIQuery(consts.RealDataQueryPath, jsonBytes)
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

		var deserializedMeta []OutputMetaDataObj
		for _, metaDataObj := range metaDataObjs {
			var keyObj = types.KeyObj{}
			if err := json.Unmarshal(metaDataObj.RowKey, &keyObj); err != nil {
				return nil, errors.Wrap(err, "unmarshal failed")
			}
			deserializedMeta = append(deserializedMeta, OutputMetaDataObj{Id: metaDataObj.RowKey, Timestamp: keyObj.Timestamp, OwnerKey: metaDataObj.OwnerKey, Qualifier: metaDataObj.Qualifier})
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

		var deserializedReal []OutputRealDataObj
		for _, realDataObj := range realDataObjs {
			var keyObj = types.KeyObj{}
			if err := json.Unmarshal(realDataObj.RowKey, &keyObj); err != nil {
				return nil, errors.Wrap(err, "unmarshal failed")
			}
			deserializedReal = append(deserializedReal, OutputRealDataObj{Id: realDataObj.RowKey, Timestamp: keyObj.Timestamp, Data: realDataObj.Data})
		}
		deserializedObj, err := json.Marshal(deserializedReal)
		if err != nil {
			return nil, errors.Wrap(err, "marshal failed")
		}
		return deserializedObj, nil
	}
}
