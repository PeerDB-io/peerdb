package connclickhouse

import (
	"context"
	"fmt"
	"testing"

	chproto "github.com/ClickHouse/clickhouse-go/v2/lib/proto"
	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	chinternal "github.com/PeerDB-io/peerdb/flow/internal/clickhouse"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

func TestBuildInsertFromTableFunctionQuery(t *testing.T) {
	ctx := context.Background()

	schema := types.QRecordSchema{
		Fields: []types.QField{
			{Name: "id", Type: types.QValueKindInt64, Nullable: false},
			{Name: "name", Type: types.QValueKindString, Nullable: false},
		},
	}

	config := &insertFromTableFunctionConfig{
		destinationTable: "t1",
		schema:           schema,
		config: &protos.QRepConfig{
			Env: map[string]string{
				"PEERDB_SOURCE_SCHEMA_AS_DESTINATION_COLUMN": "false",
			},
		},
	}

	tableFunctionExpr := "s3('s3://bucket/key', 'format')"
	chSettings := chinternal.NewCHSettings(&chproto.Version{Major: 25, Minor: 8})
	chSettings.Add(chinternal.SettingTypeJsonSkipDuplicatedPaths, "1")

	// without partitioning
	query, err := buildInsertFromTableFunctionQuery(ctx, config, tableFunctionExpr, chSettings)
	require.NoError(t, err)
	require.Equal(t,
		fmt.Sprintf("INSERT INTO `t1`(`id`,`name`) SELECT `id`,`name` FROM s3('s3://bucket/key', 'format') SETTINGS %s=%s",
			string(chinternal.SettingTypeJsonSkipDuplicatedPaths), "1"),
		query)

	// with partitioning
	totalPartitions := uint64(8)
	for idx := range totalPartitions {
		query, err := buildInsertFromTableFunctionQueryWithPartitioning(ctx, config, tableFunctionExpr, idx, totalPartitions, chSettings)
		require.NoError(t, err)
		require.Equal(t,
			"INSERT INTO `t1`(`id`,`name`) SELECT `id`,`name` FROM s3('s3://bucket/key', 'format')"+
				fmt.Sprintf(" WHERE cityHash64(`id`) %% 8 = %d SETTINGS %s=%s",
					idx, string(chinternal.SettingTypeJsonSkipDuplicatedPaths), "1"),
			query)
	}
}

// TestBuildInsertFromTableFunctionQueryResyncInPlace verifies that in-place resync stamps
// re-ingested rows with the configured _peerdb_version so ReplacingMergeTree prefers them.
func TestBuildInsertFromTableFunctionQueryResyncInPlace(t *testing.T) {
	ctx := context.Background()

	schema := types.QRecordSchema{
		Fields: []types.QField{
			{Name: "id", Type: types.QValueKindInt64, Nullable: false},
			{Name: "name", Type: types.QValueKindString, Nullable: false},
		},
	}

	const resyncVersion int64 = 1788197373708072877
	config := &insertFromTableFunctionConfig{
		destinationTable: "t1",
		schema:           schema,
		config: &protos.QRepConfig{
			Env: map[string]string{
				"PEERDB_SOURCE_SCHEMA_AS_DESTINATION_COLUMN": "false",
			},
			ResyncInPlaceVersion: resyncVersion,
		},
	}

	tableFunctionExpr := "s3('s3://bucket/key', 'format')"
	chSettings := chinternal.NewCHSettings(&chproto.Version{Major: 25, Minor: 8})
	chSettings.Add(chinternal.SettingTypeJsonSkipDuplicatedPaths, "1")

	// without partitioning: version literal is appended as the trailing _peerdb_version column
	query, err := buildInsertFromTableFunctionQuery(ctx, config, tableFunctionExpr, chSettings)
	require.NoError(t, err)
	require.Equal(t,
		fmt.Sprintf("INSERT INTO `t1`(`id`,`name`,`_peerdb_version`) SELECT `id`,`name`,%d FROM s3('s3://bucket/key', 'format') SETTINGS %s=%s",
			resyncVersion, string(chinternal.SettingTypeJsonSkipDuplicatedPaths), "1"),
		query)

	// the partitioning variant wraps the base builder, so it inherits the version stamp
	totalPartitions := uint64(4)
	for idx := range totalPartitions {
		query, err := buildInsertFromTableFunctionQueryWithPartitioning(ctx, config, tableFunctionExpr, idx, totalPartitions, chSettings)
		require.NoError(t, err)
		require.Equal(t,
			fmt.Sprintf("INSERT INTO `t1`(`id`,`name`,`_peerdb_version`) SELECT `id`,`name`,%d FROM s3('s3://bucket/key', 'format')"+
				" WHERE cityHash64(`id`) %% 4 = %d SETTINGS %s=%s",
				resyncVersion, idx, string(chinternal.SettingTypeJsonSkipDuplicatedPaths), "1"),
			query)
	}
}
