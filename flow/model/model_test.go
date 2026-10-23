package model

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
)

func TestCdcStreamGetLastCheckpointPanic(t *testing.T) {
	defer func() {
		r := recover()
		require.NotNil(t, r, "code did not panic")
		require.Equal(t, "last checkpoint not set, stream is still active", r)
	}()
	NewCDCStream[RecordItems](0).GetLastCheckpoint()
}

func TestCdcStreamAddRecordCancellation(t *testing.T) {
	t.Parallel()
	// minute to get coverage on timer case
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	stream := NewCDCStream[RecordItems](0)
	require.Equal(t, context.DeadlineExceeded,
		stream.AddRecord(ctx, &MessageRecord[RecordItems]{Prefix: "prefix", Content: "content"}))
}

func TestJsonOptionsUnnestCols(t *testing.T) {
	opts := NewToJSONOptions([]string{"column"}, false)
	_, ok1 := opts.UnnestColumns["column"]
	_, ok2 := opts.UnnestColumns["missing"]
	require.True(t, ok1)
	require.False(t, ok2)
}

// TestColumnDestinationTypes checks that the first setting for a column wins, as in the ClickHouse DDL,
// even when it leaves the destination type empty.
func TestColumnDestinationTypes(t *testing.T) {
	t.Parallel()

	result := ColumnDestinationTypes([]*protos.ColumnSetting{
		{SourceName: "explicit_first", DestinationType: "JSON"},
		{SourceName: "explicit_first", DestinationType: "String"},
		{SourceName: "empty_first", NullableEnabled: true},
		{SourceName: "empty_first", DestinationType: "JSON"},
	})
	require.Equal(t, "JSON", result["explicit_first"])
	require.Empty(t, result["empty_first"])
}
