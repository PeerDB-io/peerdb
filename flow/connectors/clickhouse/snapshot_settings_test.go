package connclickhouse

import (
	"context"
	"maps"
	"testing"

	chproto "github.com/ClickHouse/clickhouse-go/v2/lib/proto"
	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/shared"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

func TestSnapshotInsertSettings(t *testing.T) {
	for _, tc := range []struct {
		name       string
		replicated bool
		quorum     string
		parallel   bool
		wantError  bool
	}{
		{name: "replicated quorum", replicated: true, quorum: "true", parallel: true},
		{name: "quorum disabled", replicated: true, quorum: "false"},
		{name: "non replicated", quorum: "true"},
		{name: "invalid override", replicated: true, quorum: "invalid", wantError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			config := &protos.QRepConfig{
				Version: shared.InternalVersion_JsonEscapeDotsInKeys,
				Env: map[string]string{
					"PEERDB_CLICKHOUSE_ENABLE_REPLICATED_QUORUM": tc.quorum,
					"PEERDB_SOURCE_SCHEMA_AS_DESTINATION_COLUMN": "false",
				},
			}
			originalEnv := maps.Clone(config.Env)
			sync := NewClickHouseAvroSyncMethod(config, &ClickHouseConnector{
				Config:    &protos.ClickhouseConfig{Replicated: tc.replicated},
				chVersion: &chproto.Version{Major: 25, Minor: 8},
			})
			settings, err := sync.snapshotInsertSettings(ctx, config)
			require.Equal(t, originalEnv, config.Env)
			if tc.wantError {
				require.ErrorContains(t, err, "replicated quorum config for snapshot")
				return
			}
			require.NoError(t, err)
			insertConfig := &insertFromTableFunctionConfig{
				destinationTable: "snapshot_destination",
				schema: types.QRecordSchema{Fields: []types.QField{
					{Name: "id", Type: types.QValueKindInt64},
				}},
				config: config,
			}
			query, err := buildInsertFromTableFunctionQuery(ctx, insertConfig, "s3('snapshot.avro')", settings)
			require.NoError(t, err)
			chunkedQuery, err := buildInsertFromTableFunctionQueryWithPartitioning(
				ctx, insertConfig, "s3('snapshot.avro')", 0, 2, settings)
			require.NoError(t, err)
			for _, sql := range []string{query, chunkedQuery} {
				require.Contains(t, sql, "throw_on_max_partitions_per_insert_block=0")
				require.Contains(t, sql, "type_json_skip_duplicated_paths=1")
				require.Contains(t, sql, "json_type_escape_dots_in_keys=1")
				if tc.parallel {
					require.Contains(t, sql, "insert_quorum='auto'")
					require.Contains(t, sql, "insert_quorum_parallel=1")
					require.Contains(t, sql, "select_sequential_consistency=0")
				} else {
					require.NotContains(t, sql, "insert_quorum")
					require.NotContains(t, sql, "select_sequential_consistency")
				}
			}
		})
	}
}
