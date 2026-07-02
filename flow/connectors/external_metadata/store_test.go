package connmetadata

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
)

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
