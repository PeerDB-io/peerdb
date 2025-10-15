package connclickhouse

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
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
	settings := map[string]string{"key": "val"}

	// without partitioning
	query, err := buildInsertFromTableFunctionQuery(ctx, config, tableFunctionExpr, settings)
	require.NoError(t, err)
	require.Equal(t, "INSERT INTO `t1`(`id`,`name`) SELECT `id`,`name` FROM s3('s3://bucket/key', 'format') SETTINGS key=val", query)

	// with partitioning
	totalPartitions := uint64(8)
	for idx := range totalPartitions {
		query, err := buildInsertFromTableFunctionQueryWithPartitioning(ctx, config, tableFunctionExpr, idx, totalPartitions, settings)
		require.NoError(t, err)
		require.Equal(t, query,
			"INSERT INTO `t1`(`id`,`name`) SELECT `id`,`name` FROM s3('s3://bucket/key', 'format')"+
				fmt.Sprintf(" WHERE cityHash64(`id`) %% 8 = %d SETTINGS key=val", idx))
	}
}

func TestBuildSettingsStr(t *testing.T) {
	settings := map[string]string{}
	result := buildSettingsStr(settings)
	require.Empty(t, result)

	settings = map[string]string{
		"max_threads": "8",
	}
	result = buildSettingsStr(settings)
	require.Equal(t, " SETTINGS max_threads=8", result)

	settings = map[string]string{
		"max_threads":    "8",
		"max_block_size": "65536",
	}
	result = buildSettingsStr(settings)
	require.Equal(t, " SETTINGS max_threads=8, max_block_size=65536", result)
}
