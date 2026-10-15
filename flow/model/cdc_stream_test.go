package model

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func waitAndCheckEmptyAsync(stream *CDCStream[RecordItems]) <-chan bool {
	result := make(chan bool, 1)
	go func() { result <- stream.WaitAndCheckEmpty() }()
	return result
}

func requireBlocked[T any](t *testing.T, ch <-chan T) {
	t.Helper()
	select {
	case v := <-ch:
		t.Fatalf("expected WaitAndCheckEmpty to block, got %v", v)
	case <-time.After(50 * time.Millisecond):
	}
}

func TestCDCStreamCloseWithoutRowsIsEmpty(t *testing.T) {
	t.Parallel()

	stream := NewCDCStream[RecordItems](10)
	empty := waitAndCheckEmptyAsync(stream)
	requireBlocked(t, empty)

	stream.Close()
	require.True(t, <-empty)
	require.False(t, stream.NeedsNormalize())
	_, _, firstRowSet := stream.FirstRowTimes()
	require.False(t, firstRowSet)
}

func TestCDCStreamFirstRowSet(t *testing.T) {
	t.Parallel()

	stream := NewCDCStream[RecordItems](10)
	empty := waitAndCheckEmptyAsync(stream)

	expectedCommitTime := time.Now()
	require.NoError(t, stream.AddRecord(t.Context(), &InsertRecord[RecordItems]{
		BaseRecord: BaseRecord{CommitTimeNano: expectedCommitTime.UnixNano()},
	}))

	require.False(t, <-empty)
	require.True(t, stream.NeedsNormalize())
	_, commitTime, firstRowSet := stream.FirstRowTimes()
	require.True(t, firstRowSet)
	require.True(t, expectedCommitTime.Equal(commitTime))

	<-stream.GetRecords()

	// Close after a row must not double-close the readiness channel.
	stream.Close()
	require.False(t, stream.WaitAndCheckEmpty())
}
