package types_test

import (
	"encoding/binary"
	"github.com/paust-team/paust-db/types"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestGetRowKey(t *testing.T) {
	timestamp := uint64(1544772882435375000)
	salt := uint16(0)

	rowKey := types.GetRowKey(timestamp, salt)

	ts := binary.BigEndian.Uint64(rowKey[0:8])
	slt := binary.BigEndian.Uint16(rowKey[8:10])

	require.Equal(t, timestamp, ts)
	require.Equal(t, salt, slt)
}
