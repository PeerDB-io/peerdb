package utils

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

func TestRecordsToTypedCDCStream(t *testing.T) {
	const dstTable = "test_dst_tbl"
	schema := types.QRecordSchema{
		Fields: []types.QField{
			{Name: "id", Type: types.QValueKindInt32},
			{Name: "name", Type: types.QValueKindString, Nullable: true},
			{Name: "_peerdb_is_deleted", Type: types.QValueKindInt64},
			{Name: "_peerdb_version", Type: types.QValueKindInt64},
		},
	}
	sourceColumnByDest := map[string]string{"id": "id", "name": "name"}
	numericTruncator := model.NewStreamNumericTruncator(nil, map[string]struct{}{})

	records := make(chan model.Record[model.RecordItems], 3)
	insertItems := model.NewRecordItems(2)
	insertItems.AddColumn("id", types.QValueInt32{Val: 1})
	insertItems.AddColumn("name", types.QValueString{Val: "alice"})
	records <- &model.InsertRecord[model.RecordItems]{
		Items: insertItems, SourceTableName: "src", DestinationTableName: dstTable,
	}

	// update record missing the "name" column in NewItems must fall back to NULL, not panic.
	updateItems := model.NewRecordItems(1)
	updateItems.AddColumn("id", types.QValueInt32{Val: 1})
	records <- &model.UpdateRecord[model.RecordItems]{
		NewItems: updateItems, SourceTableName: "src", DestinationTableName: dstTable,
	}

	deleteItems := model.NewRecordItems(2)
	deleteItems.AddColumn("id", types.QValueInt32{Val: 1})
	deleteItems.AddColumn("name", types.QValueString{Val: "alice"})
	records <- &model.DeleteRecord[model.RecordItems]{
		Items: deleteItems, SourceTableName: "src", DestinationTableName: dstTable,
	}
	close(records)

	rowCounts := &model.RecordTypeCounts{}
	stream, err := RecordsToTypedCDCStream(
		records, dstTable, schema, sourceColumnByDest, protos.DBType_CLICKHOUSE, false, numericTruncator, rowCounts,
	)
	require.NoError(t, err)

	var rows [][]types.QValue
	for row := range stream.Records {
		rows = append(rows, row)
	}
	require.NoError(t, stream.Err())
	require.Len(t, rows, 3)

	// insert
	assert.Equal(t, int32(1), rows[0][0].Value())
	assert.Equal(t, "alice", rows[0][1].Value())
	assert.Equal(t, int64(0), rows[0][2].Value())

	// update with a missing column falls back to NULL rather than panicking
	assert.Equal(t, int32(1), rows[1][0].Value())
	assert.Nil(t, rows[1][1].Value())
	assert.Equal(t, int64(0), rows[1][2].Value())

	// delete is flagged is_deleted=1
	assert.Equal(t, int32(1), rows[2][0].Value())
	assert.Equal(t, "alice", rows[2][1].Value())
	assert.Equal(t, int64(1), rows[2][2].Value())

	assert.Equal(t, int32(1), rowCounts.InsertCount.Load())
	assert.Equal(t, int32(1), rowCounts.UpdateCount.Load())
	assert.Equal(t, int32(1), rowCounts.DeleteCount.Load())
}
