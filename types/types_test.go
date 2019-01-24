package types_test

import (
	"encoding/base64"
	"encoding/json"
	"github.com/paust-team/paust-db/types"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestWRealDataObjToRowKey(t *testing.T) {
	//given
	ownerKeyBytes, err := base64.StdEncoding.DecodeString("oimd8ZdzgUHzF9CPChJU8gb89VaMYg+1SpX6WT8nQHE=")
	assert.Nil(t, err)
	givenData := types.WRealDataObj{Timestamp: 1545982882435375000, OwnerKey: ownerKeyBytes, Qualifier: []byte("Memory"), Data: []byte("doNotUse")}

	//when
	actualRowKey := types.WRealDataObjToRowKey(givenData)

	//then
	expectKeyObj := types.KeyObj{Timestamp: givenData.Timestamp}
	expectRowKey, _ := json.Marshal(expectKeyObj)

	assert.Equal(t, expectRowKey, actualRowKey)
}

func TestCreateStartByteAndEndByte(t *testing.T) {
	//given
	givenQuery := types.RMetaDataQueryObj{Start: 0, End: 1545982882435375000}

	//when
	actualStartByte, actualEndByte := types.CreateStartByteAndEndByte(givenQuery)

	//then
	expectStartKeyObj := types.KeyObj{Timestamp: givenQuery.Start}
	expectEndKeyObj := types.KeyObj{Timestamp: givenQuery.End}

	expectStartByte, err := json.Marshal(expectStartKeyObj)
	assert.Nil(t, err)

	expectEndByte, err := json.Marshal(expectEndKeyObj)
	assert.Nil(t, err)

	assert.Equal(t, expectStartByte, actualStartByte)
	assert.Equal(t, expectEndByte, actualEndByte)

}
