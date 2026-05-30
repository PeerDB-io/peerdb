package internal

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
)

func TestPeerDBCDCStoreEnabledForDestination(t *testing.T) {
	cases := []struct {
		env    map[string]string
		name   string
		destDB protos.DBType
		want   bool
	}{
		{
			name: "ClickHouse: global false wins over ClickHouse override true (preserve OOM mitigation)",
			env: map[string]string{
				"PEERDB_CDC_STORE_ENABLED":            "false",
				"PEERDB_CLICKHOUSE_CDC_STORE_ENABLED": "true",
			},
			destDB: protos.DBType_CLICKHOUSE,
			want:   false,
		},
		{
			name: "ClickHouse: global true + ClickHouse override false disables for ClickHouse",
			env: map[string]string{
				"PEERDB_CDC_STORE_ENABLED":            "true",
				"PEERDB_CLICKHOUSE_CDC_STORE_ENABLED": "false",
			},
			destDB: protos.DBType_CLICKHOUSE,
			want:   false,
		},
		{
			name: "ClickHouse: both true keeps store enabled",
			env: map[string]string{
				"PEERDB_CDC_STORE_ENABLED":            "true",
				"PEERDB_CLICKHOUSE_CDC_STORE_ENABLED": "true",
			},
			destDB: protos.DBType_CLICKHOUSE,
			want:   true,
		},
		{
			name: "Non-ClickHouse: global false honored, ClickHouse override ignored",
			env: map[string]string{
				"PEERDB_CDC_STORE_ENABLED":            "false",
				"PEERDB_CLICKHOUSE_CDC_STORE_ENABLED": "true",
			},
			destDB: protos.DBType_SNOWFLAKE,
			want:   false,
		},
		{
			name: "Non-ClickHouse: global true honored, ClickHouse override ignored",
			env: map[string]string{
				"PEERDB_CDC_STORE_ENABLED":            "true",
				"PEERDB_CLICKHOUSE_CDC_STORE_ENABLED": "false",
			},
			destDB: protos.DBType_BIGQUERY,
			want:   true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := PeerDBCDCStoreEnabledForDestination(t.Context(), tc.env, tc.destDB)
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}
