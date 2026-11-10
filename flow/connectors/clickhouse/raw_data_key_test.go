package connclickhouse

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
)

func TestRawDataKey(t *testing.T) {
	require.Equal(t, "'a'", rawDataKey(&protos.FieldDescription{Name: "a"}))
	require.Equal(t,
		"multiIf(JSONHas(_peerdb_data, 'c'), 'c', JSONHas(_peerdb_data, 'b'), 'b', JSONHas(_peerdb_data, 'a'), 'a', 'c')",
		rawDataKey(&protos.FieldDescription{Name: "c", PreviousNames: []string{"b", "a"}}))
}
