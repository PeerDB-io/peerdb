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
