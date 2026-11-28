package connclickhouse

import (
	"context"
	"database/sql"
	"log/slog"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/log"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/shared"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
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

// engineFullRow answers the system.tables lookup for a Distributed table's engine_full.
type engineFullRow struct {
	driver.Row
	engineFull string
}

func (*engineFullRow) Err() error { return nil }

func (r *engineFullRow) Scan(dest ...any) error {
	if r.engineFull == "" {
		return sql.ErrNoRows
	}
	*dest[0].(*string) = r.engineFull
	return nil
}

// schemaDeltaConn records the statements it receives and reports engineFull
// as the Distributed engine definition of the destination table, if set.
type schemaDeltaConn struct {
	driver.Conn
	engineFull string
	execs      []string
}

func (c *schemaDeltaConn) Exec(_ context.Context, query string, _ ...any) error {
	c.execs = append(c.execs, query)
	return nil
}

func (c *schemaDeltaConn) QueryRow(_ context.Context, _ string, _ ...any) driver.Row {
	return &engineFullRow{engineFull: c.engineFull}
}

func TestReplayTableSchemaDeltasOnCluster(t *testing.T) {
	for _, tc := range []struct {
		name       string
		engineFull string
		expected   []string
	}{
		{
			name:       "distributed table alters shard table first",
			engineFull: "Distributed('cluster', 'db', 't1_shard1710425678', cityHash64(id))",
			expected: []string{
				"ALTER TABLE `t1_shard1710425678` ON CLUSTER `cluster` ADD COLUMN IF NOT EXISTS `c2` Int32",
				"ALTER TABLE `t1` ON CLUSTER `cluster` ADD COLUMN IF NOT EXISTS `c2` Int32",
			},
		},
		{
			name:       "non-distributed table is altered directly",
			engineFull: "",
			expected: []string{
				"ALTER TABLE `t1` ON CLUSTER `cluster` ADD COLUMN IF NOT EXISTS `c2` Int32",
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			conn := &schemaDeltaConn{engineFull: tc.engineFull}
			c := &ClickHouseConnector{
				database: conn,
				logger:   log.NewStructuredLogger(slog.New(slog.DiscardHandler)),
				Config:   &protos.ClickhouseConfig{Database: "db", Cluster: "cluster"},
			}

			err := c.ReplayTableSchemaDeltas(t.Context(), nil, "test_flow", nil, []*protos.TableSchemaDelta{{
				SrcTableName: "public.t1",
				DstTableName: "t1",
				AddedColumns: []*protos.FieldDescription{{Name: "c2", Type: string(types.QValueKindInt32), TypeModifier: -1}},
			}}, nil, shared.InternalVersion_Latest, protos.DBType_POSTGRES)
			require.NoError(t, err)
			require.Equal(t, tc.expected, conn.execs)
		})
	}
}
