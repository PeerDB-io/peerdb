package internal

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
)

func TestPeerDBCDCStoreEnabled(t *testing.T) {
	cases := []struct {
		env      map[string]string
		name     string
		destDB   protos.DBType
		expected bool
	}{
		{
			name:     "ClickHouse: unset defaults to false",
			env:      map[string]string{"PEERDB_CDC_STORE_ENABLED": ""},
			destDB:   protos.DBType_CLICKHOUSE,
			expected: false,
		},
		{
			name:     "Non-ClickHouse: unset defaults to true",
			env:      map[string]string{"PEERDB_CDC_STORE_ENABLED": ""},
			destDB:   protos.DBType_SNOWFLAKE,
			expected: true,
		},
		{
			name:     "ClickHouse: explicit preserved",
			env:      map[string]string{"PEERDB_CDC_STORE_ENABLED": "true"},
			destDB:   protos.DBType_CLICKHOUSE,
			expected: true,
		},
		{
			name:     "Non-ClickHouse: explicit preserved",
			env:      map[string]string{"PEERDB_CDC_STORE_ENABLED": "false"},
			destDB:   protos.DBType_SNOWFLAKE,
			expected: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			actual, err := PeerDBCDCStoreEnabled(t.Context(), tc.env, tc.destDB)
			require.NoError(t, err)
			require.Equal(t, tc.expected, actual)
		})
	}
}
