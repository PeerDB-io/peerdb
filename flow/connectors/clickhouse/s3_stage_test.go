package connclickhouse

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/connectors/utils"
	"github.com/PeerDB-io/peerdb/flow/internal"
)

func TestHasAvroStageInBatchRange(t *testing.T) {
	ctx := t.Context()
	pool, err := internal.GetCatalogConnectionPoolFromEnv(ctx)
	if err != nil {
		t.Skipf("catalog not available: %v", err)
	}

	flowJobName := fmt.Sprintf("test_has_avro_stage_%s", t.Name())
	t.Cleanup(func() {
		_, _ = pool.Exec(ctx, `DELETE FROM ch_s3_stage WHERE flow_job_name = $1`, flowJobName)
	})

	emptyAvro := utils.AvroFile{FilePath: "/tmp/empty.avro", NumRecords: 0}
	nonEmptyAvro := utils.AvroFile{FilePath: "/tmp/data.avro", NumRecords: 3}

	require.NoError(t, SetAvroStage(ctx, flowJobName, 5, emptyAvro))
	require.NoError(t, SetAvroStage(ctx, flowJobName, 6, nonEmptyAvro))

	t.Run("empty avro stage in range is ignored", func(t *testing.T) {
		exists, err := hasAvroStageInBatchRange(ctx, flowJobName, 4, 5)
		require.NoError(t, err)
		require.False(t, exists)
	})

	t.Run("non-empty avro stage in range is detected", func(t *testing.T) {
		exists, err := hasAvroStageInBatchRange(ctx, flowJobName, 5, 6)
		require.NoError(t, err)
		require.True(t, exists)
	})

	t.Run("batch outside range is ignored", func(t *testing.T) {
		exists, err := hasAvroStageInBatchRange(ctx, flowJobName, 6, 7)
		require.NoError(t, err)
		require.False(t, exists)
	})
}
