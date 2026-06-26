package connmetadata

import (
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

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

	parentMirrorName := "test_offload_restore_sensitive_partition_ranges"
	runUUID := uuid.NewString()
	t.Cleanup(func() {
		_, _ = pool.Exec(ctx, `DELETE FROM `+qrepPartitionRangesTableName+` WHERE parent_mirror_name=$1`, parentMirrorName)
	})

	p1Start, p1End, p2Start, p2End := "bar", "foo", "hello", "world"
	partitions := []*protos.QRepPartition{
		{
			PartitionId: uuid.NewString(),
			Range: &protos.PartitionRange{Range: &protos.PartitionRange_StringRange{
				StringRange: &protos.StringPartitionRange{Start: p1Start, End: p1End},
			}},
		},
		{
			PartitionId: uuid.NewString(),
			Range: &protos.PartitionRange{Range: &protos.PartitionRange_StringRange{
				StringRange: &protos.StringPartitionRange{Start: p2Start, End: p2End, EndInclusive: true},
			}},
		},
	}

	require.NoError(t, OffloadSensitivePartitionRanges(ctx, pool, parentMirrorName, runUUID, partitions))
	for _, p := range partitions {
		require.Empty(t, p.Range.GetStringRange().Start)
		require.Empty(t, p.Range.GetStringRange().End)
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
		for _, plaintext := range []string{p1Start, p1End, p2Start, p2End} {
			require.NotContains(t, string(res.Payload), plaintext)
		}
	}

	require.NoError(t, RestoreSensitivePartitionRanges(ctx, pool, runUUID, partitions))
	require.Equal(t, p1Start, partitions[0].Range.GetStringRange().Start)
	require.Equal(t, p1End, partitions[0].Range.GetStringRange().End)
	require.False(t, partitions[0].Range.GetStringRange().EndInclusive)
	require.Equal(t, p2Start, partitions[1].Range.GetStringRange().Start)
	require.Equal(t, p2End, partitions[1].Range.GetStringRange().End)
	require.True(t, partitions[1].Range.GetStringRange().EndInclusive)
}
