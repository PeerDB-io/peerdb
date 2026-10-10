package internal

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPeerDBClickHouseEnableReplicatedQuorumDefaultsEnabled(t *testing.T) {
	idx, ok := DynamicIndex["PEERDB_CLICKHOUSE_ENABLE_REPLICATED_QUORUM"]
	require.True(t, ok)
	defaultValue, err := strconv.ParseBool(DynamicSettings[idx].DefaultValue)
	require.NoError(t, err)
	require.True(t, defaultValue,
		"Replicated ClickHouse clusters must write with quorum by default: normalize reads back the raw table it just wrote and silently advances batch pointers past rows a lagging replica has not replicated yet")
}

func TestPeerDBClickHouseEnableReplicatedQuorumEnvOverride(t *testing.T) {
	quorum, err := PeerDBClickHouseEnableReplicatedQuorum(t.Context(), map[string]string{
		"PEERDB_CLICKHOUSE_ENABLE_REPLICATED_QUORUM": "false",
	})
	require.NoError(t, err)
	require.False(t, quorum)

	quorum, err = PeerDBClickHouseEnableReplicatedQuorum(t.Context(), map[string]string{
		"PEERDB_CLICKHOUSE_ENABLE_REPLICATED_QUORUM": "true",
	})
	require.NoError(t, err)
	require.True(t, quorum)
}
