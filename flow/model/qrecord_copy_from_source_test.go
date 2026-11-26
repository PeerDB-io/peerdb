package model

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

// The snapshot's COPY into Postgres writes an empty binary value (nil Val, as MySQL sources read it) as empty
// bytes, not NULL; a NULL (QValueNull) stays NULL.
func TestQRecordCopyFromSourceNilBytes(t *testing.T) {
	stream := NewQRecordStream(1)
	stream.Records <- []types.QValue{
		types.QValueBytes{Val: nil},
		types.QValueNull(types.QValueKindBytes),
		types.QValueBytes{Val: []byte("ab")},
	}
	stream.Close(nil)

	src := NewQRecordCopyFromSource(stream)
	require.True(t, src.Next())
	values, err := src.Values()
	require.NoError(t, err)
	require.Equal(t, []byte{}, values[0])
	require.NotNil(t, values[0].([]byte), "an empty value, not NULL")
	require.Nil(t, values[1])
	require.Equal(t, []byte("ab"), values[2])
}
