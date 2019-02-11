package client

import (
	"encoding/base64"
	"encoding/json"
	"github.com/paust-team/paust-db/types"
	"github.com/stretchr/testify/require"
	"testing"
)

const (
	TestPubKey    = "Pe8PPI4Mq7kJIjDJjffoTl6s5EezGQSyIcu5Y2KYDaE="
	TestQualifier = "testQualifier"
)

func TestHTTPClient_deSerializeKeyObj(t *testing.T) {
	require := require.New(t)
	var timestamp1 uint64 = 1547772882435375000
	var timestamp2 uint64 = 1547772960049177000
	rowKey1, err := json.Marshal(types.KeyObj{Timestamp: timestamp1, Salt: 0})
	require.Nil(err, "json marshal err: %+v", err)
	rowKey2, err := json.Marshal(types.KeyObj{Timestamp: timestamp2, Salt: 0})
	require.Nil(err, "json marshal err: %+v", err)

	// MetaDataResObj deserialize
	pubKeyBytes, err := base64.StdEncoding.DecodeString(TestPubKey)
	require.Nil(err, "base64 decode err: %+v", err)
	metaDataObjs, err := json.Marshal([]types.MetaDataObj{{RowKey: rowKey1, OwnerKey: pubKeyBytes, Qualifier: []byte(TestQualifier)}, {RowKey: rowKey2, OwnerKey: pubKeyBytes, Qualifier: []byte(TestQualifier)}})
	require.Nil(err, "json marshal err: %+v", err)
	outputQueryObjs, err := json.Marshal([]OutputQueryObj{{Id: rowKey1, Timestamp: timestamp1, OwnerKey: pubKeyBytes, Qualifier: []byte(TestQualifier)}, {Id: rowKey2, Timestamp: timestamp2, OwnerKey: pubKeyBytes, Qualifier: []byte(TestQualifier)}})
	require.Nil(err, "json marshal err: %+v", err)

	deserializedBytes, err := deSerializeKeyObj(metaDataObjs, true)
	require.Nil(err, "SerializeKeyObj err: %+v", err)

	require.EqualValues(outputQueryObjs, deserializedBytes)

	// RealDataResObj deserialize
	realDataObjs, err := json.Marshal([]types.RealDataObj{{RowKey: rowKey1, Data: []byte("testData1")}, {RowKey: rowKey2, Data: []byte("testData2")}})
	require.Nil(err, "json marshal err: %+v", err)
	outputFetchObjs, err := json.Marshal([]OutputFetchObj{{Id: rowKey1, Timestamp: timestamp1, Data: []byte("testData1")}, {Id: rowKey2, Timestamp: timestamp2, Data: []byte("testData2")}})
	require.Nil(err, "json marshal err: %+v", err)

	deserializedBytes, err = deSerializeKeyObj(realDataObjs, false)
	require.Nil(err, "SerializeKeyObj err: %+v", err)

	require.EqualValues(outputFetchObjs, deserializedBytes)
}
