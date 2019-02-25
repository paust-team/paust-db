package types_test

import (
	"encoding/binary"
	"encoding/json"
	"github.com/paust-team/paust-db/types"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestCreateStartByteAndEndByte(t *testing.T) {
	//given
	start := make([]byte, 8)
	end := make([]byte, 8)
	binary.BigEndian.PutUint64(start, 0)
	binary.BigEndian.PutUint64(end, 1545982882435375000)
	givenQuery := types.QueryObj{Start: start, End: end}

	//when
	actualStartByte, actualEndByte := types.CreateStartByteAndEndByte(givenQuery)

	//then
	salt := make([]byte, 2)
	binary.BigEndian.PutUint16(salt, 0x0000)
	expectStartKeyObj := types.KeyObj{Timestamp: givenQuery.Start, Salt: salt}
	expectEndKeyObj := types.KeyObj{Timestamp: givenQuery.End, Salt: salt}

	expectStartByte, err := json.Marshal(expectStartKeyObj)
	assert.Nil(t, err)

	expectEndByte, err := json.Marshal(expectEndKeyObj)
	assert.Nil(t, err)

	assert.Equal(t, expectStartByte, actualStartByte)
	assert.Equal(t, expectEndByte, actualEndByte)

}
