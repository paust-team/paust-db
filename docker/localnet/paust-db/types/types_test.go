package types_test

import (
	"encoding/json"
	"github.com/paust-team/paust-db/types"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestCreateStartByteAndEndByte(t *testing.T) {
	//given
	givenQuery := types.MetaDataQueryObj{Start: 0, End: 1545982882435375000}

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
