//go:build tilt

package monitoring

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/model"
)

func TestAddCDCBatchForFlowResetsReusedBatchID(t *testing.T) {
	ctx := t.Context()
	pool, err := internal.GetCatalogConnectionPoolFromEnv(ctx)
	require.NoError(t, err)

	flowName := fmt.Sprintf("test_cdc_batches_%d", time.Now().UnixNano())
	require.NoError(t, InitializeCDCFlow(ctx, pool, flowName))
	t.Cleanup(func() {
		_, err := pool.Exec(context.Background(), "DELETE FROM peerdb_stats.cdc_flows WHERE flow_name=$1", flowName)
		require.NoError(t, err)
	})

	batch := CDCBatchInfo{BatchID: 7, StartTime: time.Now().UTC()}
	require.NoError(t, AddCDCBatchForFlow(ctx, pool, flowName, batch))
	require.NoError(t, UpdateNumRowsAndEndLSNForCDCBatch(
		ctx, pool, flowName, batch.BatchID, 10, model.CdcCheckpoint{ID: 99, Text: "0/63"}, &batch.StartTime, &batch.StartTime))
	require.NoError(t, UpdateEndTimeForCDCBatch(ctx, pool, flowName, batch.BatchID))
	require.NoError(t, AddCDCBatchForFlow(ctx, pool, flowName, batch))

	var count, rows, endLSN int64
	var endLSNText string
	var openUnsynced bool
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT count(*), coalesce(sum(rows_in_batch), 0), coalesce(sum(batch_end_lsn), 0)::bigint,
		coalesce(max(batch_end_lsn_text), ''), coalesce(bool_and(end_time IS NULL AND sync_time IS NULL
		AND first_row_received_at IS NULL AND first_row_commit_time IS NULL), false)
		FROM peerdb_stats.cdc_batches WHERE flow_name=$1`, flowName,
	).Scan(&count, &rows, &endLSN, &endLSNText, &openUnsynced))
	require.Equal(t, int64(1), count)
	require.Zero(t, rows)
	require.Zero(t, endLSN)
	require.Empty(t, endLSNText)
	require.True(t, openUnsynced)
}
