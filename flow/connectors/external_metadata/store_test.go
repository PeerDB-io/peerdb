//go:build tilt

package connmetadata

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/model"
)

func TestInitializeQueryCDCReplicationState(t *testing.T) {
	ctx := t.Context()
	pool, err := internal.GetCatalogConnectionPoolFromEnv(ctx)
	require.NoError(t, err)
	metadata := NewPostgresMetadataFromCatalog(internal.LoggerFromCtx(ctx), pool)

	flowName := "test_initialize_query_cdc_replication_state_" + uuid.NewString()
	firstTable := "first_table"
	t.Cleanup(func() {
		_, _ = pool.Exec(ctx, `DELETE FROM `+queryCDCReplicationStateTableName+` WHERE flow_name = $1`, flowName)
	})

	require.NoError(t, metadata.InitializeQueryCDCReplicationState(ctx, flowName, firstTable, "snapshot-checkpoint"))
	state, err := metadata.GetQueryCDCReplicationState(ctx, flowName, firstTable)
	require.NoError(t, err)
	require.Equal(t, "snapshot-checkpoint", state.CursorText)

	// Existing progress must not be replaced if the activity is restarted.
	require.NoError(t, metadata.InitializeQueryCDCReplicationState(ctx, flowName, firstTable, "newer-checkpoint"))
	state, err = metadata.GetQueryCDCReplicationState(ctx, flowName, firstTable)
	require.NoError(t, err)
	require.Equal(t, "snapshot-checkpoint", state.CursorText)

	// Attempts and successful syncs advance their respective timestamps.
	attemptedAt := time.Now().UTC().Truncate(time.Microsecond)
	syncedAt := attemptedAt.Add(time.Second)
	require.NoError(t, metadata.RecordQueryCDCAttempt(ctx, flowName, firstTable, attemptedAt))
	require.NoError(t, metadata.RecordQueryCDCSync(
		ctx, flowName, firstTable, "next-checkpoint", syncedAt, 0,
	))
	state, err = metadata.GetQueryCDCReplicationState(ctx, flowName, firstTable)
	require.NoError(t, err)
	require.Equal(t, "next-checkpoint", state.CursorText)
	require.WithinDuration(t, attemptedAt, state.LastAttemptAt, 0)
	require.WithinDuration(t, syncedAt, state.LastSyncedAt, 0)
}

func TestOffloadRestoreSensitivePartitionRanges(t *testing.T) {
	const encKeyID = "test_enc_key"
	t.Setenv("PEERDB_CURRENT_ENC_KEY_ID", encKeyID)
	t.Setenv("PEERDB_ENC_KEYS", `[{"id":"`+encKeyID+`","value":"cGVlcmRiX2NpX3Rlc3RfZW5jX2tleV8zMl9ieXRlcyE="}]`)

	ctx := t.Context()
	pool, err := internal.GetCatalogConnectionPoolFromEnv(ctx)
	require.NoError(t, err)

	for _, tc := range []struct {
		name   string
		ranges []*protos.PartitionRange
	}{
		{
			name: "int",
			ranges: []*protos.PartitionRange{
				{Range: &protos.PartitionRange_IntRange{IntRange: &protos.IntPartitionRange{Start: 1, End: 100}}},
				{Range: &protos.PartitionRange_IntRange{IntRange: &protos.IntPartitionRange{Start: 101, End: 200}}},
			},
		},
		{
			name: "timestamp",
			ranges: []*protos.PartitionRange{
				{Range: &protos.PartitionRange_TimestampRange{TimestampRange: &protos.TimestampPartitionRange{
					Start: timestamppb.New(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)),
					End:   timestamppb.New(time.Date(2026, 6, 30, 23, 59, 59, 0, time.UTC)),
				}}},
				{Range: &protos.PartitionRange_TimestampRange{TimestampRange: &protos.TimestampPartitionRange{
					Start: timestamppb.New(time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)),
					End:   timestamppb.New(time.Date(2026, 12, 31, 23, 59, 59, 0, time.UTC)),
				}}},
			},
		},
		{
			name: "string_uuid",
			ranges: []*protos.PartitionRange{
				{Range: &protos.PartitionRange_StringRange{
					StringRange: &protos.StringPartitionRange{Start: uuid.NewString(), End: uuid.NewString()},
				}},
				{Range: &protos.PartitionRange_StringRange{
					StringRange: &protos.StringPartitionRange{Start: uuid.NewString(), End: uuid.NewString(), EndInclusive: true},
				}},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			parentMirrorName := "test_offload_restore_partition_ranges_" + tc.name
			runUUID := uuid.NewString()
			t.Cleanup(func() {
				_, _ = pool.Exec(ctx, `DELETE FROM `+qrepPartitionRangesTableName+` WHERE parent_mirror_name=$1`, parentMirrorName)
			})

			partitions := make([]*protos.QRepPartition, len(tc.ranges))
			expectedRanges := make([]*protos.PartitionRange, len(tc.ranges))
			for i, r := range tc.ranges {
				partitions[i] = &protos.QRepPartition{PartitionId: uuid.NewString(), Range: r}
				expectedRanges[i] = proto.Clone(r).(*protos.PartitionRange)
			}

			require.NoError(t, OffloadPartitionRanges(ctx, pool, parentMirrorName, runUUID, partitions))
			for _, p := range partitions {
				require.Nil(t, p.Range)
				require.True(t, p.RangeOffloaded)
			}

			rows, err := pool.Query(ctx,
				`SELECT enc_key_id, range_payload FROM `+qrepPartitionRangesTableName+` WHERE parent_mirror_name=$1`,
				parentMirrorName)
			require.NoError(t, err)
			results, err := pgx.CollectRows(rows, pgx.RowToStructByPos[struct {
				KeyID   string
				Payload []byte
			}])
			require.NoError(t, err)
			require.Len(t, results, len(partitions))
			for _, res := range results {
				require.Equal(t, encKeyID, res.KeyID)
				require.NotEmpty(t, res.Payload)
				for _, r := range expectedRanges {
					unencrypted, err := proto.Marshal(r)
					require.NoError(t, err)
					require.NotContains(t, string(res.Payload), string(unencrypted))
				}
			}

			require.NoError(t, RestoreOffloadedPartitionRanges(ctx, pool, runUUID, partitions))
			for i, p := range partitions {
				require.Truef(t, proto.Equal(expectedRanges[i], p.Range),
					"partition %d range mismatch after restore: want %v, got %v", i, expectedRanges[i], p.Range)
			}
		})
	}
}

func TestFinishBatchSavesSchemaChangesWithCheckpoint(t *testing.T) {
	ctx := t.Context()
	pool, err := internal.GetCatalogConnectionPoolFromEnv(ctx)
	require.NoError(t, err)
	metadata := NewPostgresMetadataFromCatalog(internal.LoggerFromCtx(ctx), pool)

	flowName := "test_finish_batch_schema_" + uuid.NewString()
	tableName := "public.target"
	initialSchema, err := proto.Marshal(&protos.TableSchema{
		TableIdentifier: tableName,
		Columns:         []*protos.FieldDescription{{Name: "id", Type: "int32"}},
	})
	require.NoError(t, err)
	_, err = pool.Exec(ctx, "INSERT INTO table_schema_mapping(flow_name, table_name, table_schema) VALUES ($1, $2, $3)",
		flowName, tableName, initialSchema)
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx := context.WithoutCancel(ctx)
		_, _ = pool.Exec(cleanupCtx, "DELETE FROM table_schema_mapping WHERE flow_name = $1", flowName)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM `+lastSyncStateTableName+` WHERE job_name = $1`, flowName)
	})
	deltas := []*protos.TableSchemaDelta{{
		DstTableName: tableName,
		AddedColumns: []*protos.FieldDescription{{Name: "added", Type: "int32"}},
	}}

	require.NoError(t, metadata.FinishBatch(ctx, flowName, 1, model.CdcCheckpoint{ID: 10, Text: "first"}, nil))
	schema, err := internal.LoadTableSchemaFromCatalog(ctx, pool, flowName, tableName)
	require.NoError(t, err)
	require.Len(t, schema.Columns, 1, "a batch without schema changes must not change the catalog schema")

	require.NoError(t, metadata.FinishBatch(ctx, flowName, 2, model.CdcCheckpoint{ID: 20, Text: "second"}, deltas))
	batchID, err := metadata.GetLastSyncBatchID(ctx, flowName)
	require.NoError(t, err)
	require.Equal(t, int64(2), batchID)
	offset, err := metadata.GetLastOffset(ctx, flowName)
	require.NoError(t, err)
	require.Equal(t, int64(20), offset.ID)
	require.Equal(t, "second", offset.Text)
	schema, err = internal.LoadTableSchemaFromCatalog(ctx, pool, flowName, tableName)
	require.NoError(t, err)
	require.Len(t, schema.Columns, 2)
	require.Equal(t, "added", schema.Columns[1].Name)

	require.NoError(t, metadata.FinishBatch(ctx, flowName, 2, model.CdcCheckpoint{ID: 20, Text: "second"}, deltas))
	schema, err = internal.LoadTableSchemaFromCatalog(ctx, pool, flowName, tableName)
	require.NoError(t, err)
	require.Len(t, schema.Columns, 2, "a retried batch added the column twice")

	// An unreadable stored schema makes the schema write fail.
	_, err = pool.Exec(ctx, "UPDATE table_schema_mapping SET table_schema = $1 WHERE flow_name = $2", []byte{0xff}, flowName)
	require.NoError(t, err)
	require.Error(t, metadata.FinishBatch(ctx, flowName, 3, model.CdcCheckpoint{ID: 30, Text: "third"}, deltas))
	batchID, err = metadata.GetLastSyncBatchID(ctx, flowName)
	require.NoError(t, err)
	require.Equal(t, int64(2), batchID)
}
