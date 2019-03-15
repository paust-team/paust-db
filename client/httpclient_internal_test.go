package client

import (
	"encoding/json"
	"github.com/paust-team/paust-db/types"
	"github.com/stretchr/testify/require"
	"testing"
)

const (
	TestOwnerId   = "ownertest"
	TestQualifier = "testQualifier"
)

func TestHTTPClient_deSerializeKeyObj(t *testing.T) {
	require := require.New(t)
	var timestamp1 uint64 = 1547772882435375000
	var timestamp2 uint64 = 1547772960049177000
	rowKey1 := types.GetRowKey(timestamp1, 0)
	rowKey2 := types.GetRowKey(timestamp2, 0)

	// MetaDataResObj deserialize
	metaDataObjs, err := json.Marshal([]types.MetaDataObj{{RowKey: rowKey1, OwnerId: TestOwnerId, Qualifier: []byte(TestQualifier)}, {RowKey: rowKey2, OwnerId: TestOwnerId, Qualifier: []byte(TestQualifier)}})
	require.Nil(err, "json marshal err: %+v", err)
	outputQueryObjs, err := json.Marshal([]OutputQueryObj{{Id: rowKey1, Timestamp: timestamp1, OwnerId: TestOwnerId, Qualifier: TestQualifier}, {Id: rowKey2, Timestamp: timestamp2, OwnerId: TestOwnerId, Qualifier: TestQualifier}})
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
