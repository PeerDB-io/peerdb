//go:build tilt

package connmysql

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/shared"
)

// A watermark table that information_schema.columns no longer lists (dropped, or not readable by the user)
// must surface as a missing source table rather than an unclassified "no columns selected" error.
func TestPullQRepRecordsMissingWatermarkTable(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	connector := newTestConnector(t, ctx)

	dbName := testDBName("qrep_missing")
	createTestDB(t, ctx, connector, dbName)

	_, _, err := connector.PullQRepRecords(ctx, shared.CatalogPool{}, nil, &protos.QRepConfig{
		FlowJobName:    "test_qrep_missing_watermark_table",
		WatermarkTable: dbName + ".missing",
	}, protos.DBType_CLICKHOUSE, &protos.QRepPartition{FullTablePartition: true}, model.NewQRecordStream(1))
	require.ErrorIs(t, err, shared.ErrTableDoesNotExist)
}
