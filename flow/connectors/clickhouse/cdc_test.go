package connclickhouse

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	peerdb_clickhouse "github.com/PeerDB-io/peerdb/flow/pkg/clickhouse"
)

func TestExtractSingleQuotedStrings(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected []string
	}{
		{
			name:     "standard distributed engine_full",
			input:    "Distributed('cicluster', 'mydb', 'mytable_shard', rand())",
			expected: []string{"cicluster", "mydb", "mytable_shard"},
		},
		{
			name:     "shard table with timestamp suffix",
			input:    "Distributed('cicluster', 'e2e_test_abc', 'test_table_shard1710425678', rand())",
			expected: []string{"cicluster", "e2e_test_abc", "test_table_shard1710425678"},
		},
		{
			name:     "with policy parameter",
			input:    "Distributed('cluster', 'db', 'shard_tbl', rand(), 'default')",
			expected: []string{"cluster", "db", "shard_tbl", "default"},
		},
		{
			name:     "escaped single quote",
			input:    `Distributed('clust\'er', 'db', 'tbl')`,
			expected: []string{"clust'er", "db", "tbl"},
		},
		{
			name:     "escaped backslash",
			input:    `Distributed('clus\\ter', 'db', 'tbl')`,
			expected: []string{"clus\\ter", "db", "tbl"},
		},
		{
			name:     "escaped tab and newline",
			input:    `Distributed('a\tb', 'c\nd', 'tbl')`,
			expected: []string{"atb", "cnd", "tbl"},
		},
		{
			name:     "escaped backtick",
			input:    "Distributed('a\\`b', 'db', 'tbl')",
			expected: []string{"a`b", "db", "tbl"},
		},
		{
			name:     "no quotes",
			input:    "ReplacingMergeTree()",
			expected: nil,
		},
		{
			name:     "empty string",
			input:    "",
			expected: nil,
		},
		{
			name:     "single unclosed quote",
			input:    "'abc",
			expected: nil,
		},
		{
			name:     "backslash at end of quoted string",
			input:    `'abc\`,
			expected: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := extractSingleQuotedStrings(tt.input)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestCreateRawTableHasTTL(t *testing.T) {
	for _, tc := range []struct {
		name         string
		overrideDays string
		expectedDays int
	}{
		{name: "default 90-day TTL", overrideDays: "", expectedDays: 90},
		{name: "env override applies", overrideDays: "42", expectedDays: 42},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const ttlKey = "PEERDB_CLICKHOUSE_RAW_TABLE_TTL_DAYS"
			if tc.overrideDays != "" {
				t.Setenv(ttlKey, tc.overrideDays)
			} else {
				// t.Setenv records the prior value and restores on cleanup; Unsetenv ensures the dyn
				// config falls back to its registered default for this subtest.
				t.Setenv(ttlKey, "")
				require.NoError(t, os.Unsetenv(ttlKey))
			}
			// NewClickHouseConnector requires a staging bucket; CreateRawTable itself doesn't use it.
			t.Setenv("PEERDB_CLICKHOUSE_AWS_S3_BUCKET_NAME", "dummy-bucket-for-ttl-test")

			ctx := t.Context()
			conn, err := NewClickHouseConnector(ctx, internal.NewSettings(nil), &protos.ClickhouseConfig{
				Host:       internal.ClickHouseTestHost(),
				Port:       internal.ClickHouseTestPort(),
				Database:   "default",
				DisableTls: true,
			})
			require.NoError(t, err)
			defer conn.Close()

			flowName := fmt.Sprintf("test_raw_table_ttl_%d_%d", tc.expectedDays, time.Now().UnixNano())
			table, err := conn.CreateRawTable(ctx, &protos.CreateRawTableInput{FlowJobName: flowName})
			require.NoError(t, err)
			t.Cleanup(func() {
				_ = conn.execWithLogging(ctx,
					"DROP TABLE IF EXISTS "+peerdb_clickhouse.QuoteIdentifier(table.TableIdentifier))
			})

			var engineFull string
			require.NoError(t, conn.queryRow(ctx, fmt.Sprintf(
				"SELECT engine_full FROM system.tables WHERE database = %s AND name = %s",
				peerdb_clickhouse.QuoteLiteral("default"),
				peerdb_clickhouse.QuoteLiteral(table.TableIdentifier),
			)).Scan(&engineFull))

			require.Contains(t, engineFull, "TTL", "engine_full should declare a TTL: %s", engineFull)
			require.Contains(t, engineFull, "fromUnixTimestamp64Nano(_peerdb_timestamp)",
				"TTL should reference _peerdb_timestamp: %s", engineFull)
			require.Contains(t, engineFull, fmt.Sprintf("toIntervalDay(%d)", tc.expectedDays),
				"TTL interval should be %d days: %s", tc.expectedDays, engineFull)
		})
	}
}
