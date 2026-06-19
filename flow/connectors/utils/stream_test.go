package utils

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/shared"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

func TestPreprocessRecordItemsClampsClickHouseTemporalByVersion(t *testing.T) {
	inputDate := time.Date(1000, time.January, 1, 0, 0, 0, 0, time.UTC)
	inputTimestamp := time.Date(9999, time.December, 31, 23, 59, 59, 999999000, time.UTC)

	items := model.NewRecordItems(2)
	items.AddColumn("d", types.QValueDate{Val: inputDate})
	items.AddColumn("dt", types.QValueTimestamp{Val: inputTimestamp})

	preClampVersion := shared.InternalVersion_ClickHouseClampTemporal - 1
	oldStats := model.NewCdcTableNumericTruncator("dst", nil, nil)
	oldItems := preprocessRecordItems(items, protos.DBType_CLICKHOUSE, preClampVersion, false, oldStats)
	require.True(t, inputDate.Equal(oldItems.ColToVal["d"].(types.QValueDate).Val))
	require.True(t, inputTimestamp.Equal(oldItems.ColToVal["dt"].(types.QValueTimestamp).Val))

	newStats := model.NewCdcTableNumericTruncator("dst", nil, nil)
	newItems := preprocessRecordItems(
		items, protos.DBType_CLICKHOUSE, shared.InternalVersion_ClickHouseClampTemporal, false, newStats)
	require.True(t, time.Date(1900, time.January, 1, 0, 0, 0, 0, time.UTC).
		Equal(newItems.ColToVal["d"].(types.QValueDate).Val))
	require.True(t, time.Date(2299, time.December, 31, 23, 59, 59, 999999000, time.UTC).
		Equal(newItems.ColToVal["dt"].(types.QValueTimestamp).Val))

	warnings := shared.QRepWarnings{}
	newStats.CollectWarnings(&warnings)
	require.Len(t, warnings, 2)
	require.Contains(t, warnings[0].Error()+warnings[1].Error(), "dst.d")
	require.Contains(t, warnings[0].Error()+warnings[1].Error(), "dst.dt")
}
