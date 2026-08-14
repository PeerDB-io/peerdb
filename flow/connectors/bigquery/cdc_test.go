package connbigquery

import (
	"context"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

func TestPollWindow(t *testing.T) {
	checkpoint := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)

	t.Run("caps at queryWindow past checkpoint when now is far ahead", func(t *testing.T) {
		now := checkpoint.Add(queryWindow * 10)
		upper, ok := pollWindow(checkpoint, now)
		require.True(t, ok)
		assert.True(t, upper.Equal(checkpoint.Add(queryWindow)))
	})

	t.Run("caps at safetyLag behind now when now is close", func(t *testing.T) {
		now := checkpoint.Add(time.Hour)
		upper, ok := pollWindow(checkpoint, now)
		require.True(t, ok)
		assert.True(t, upper.Equal(now.Add(-safetyLag)))
	})

	t.Run("nothing new to scan when safety lag hasn't cleared", func(t *testing.T) {
		now := checkpoint.Add(safetyLag / 2)
		upper, ok := pollWindow(checkpoint, now)
		assert.False(t, ok)
		// upper is still reported (as now-safetyLag), just not usable, since it
		// doesn't move past checkpoint.
		assert.True(t, upper.Equal(now.Add(-safetyLag)))
		assert.False(t, upper.After(checkpoint))
	})

	t.Run("exactly at the boundary is not ok (upper must strictly move past checkpoint)", func(t *testing.T) {
		now := checkpoint.Add(safetyLag)
		upper, ok := pollWindow(checkpoint, now)
		assert.False(t, ok)
		assert.True(t, upper.Equal(checkpoint))
	})
}

func TestWaitForNextPoll(t *testing.T) {
	t.Run("first-ever call (zero lastPollAt) returns immediately", func(t *testing.T) {
		c := &BigQueryConnector{}
		start := time.Now()
		err := c.waitForNextPoll(context.Background(), time.Hour)
		require.NoError(t, err)
		assert.Less(t, time.Since(start), 100*time.Millisecond)
	})

	t.Run("waits out the remainder of idleTimeout since lastPollAt", func(t *testing.T) {
		c := &BigQueryConnector{lastPollAt: time.Now()}
		idleTimeout := 50 * time.Millisecond
		start := time.Now()
		err := c.waitForNextPoll(context.Background(), idleTimeout)
		require.NoError(t, err)
		elapsed := time.Since(start)
		assert.GreaterOrEqual(t, elapsed, idleTimeout-5*time.Millisecond)
	})

	t.Run("no wait once idleTimeout has already elapsed", func(t *testing.T) {
		c := &BigQueryConnector{lastPollAt: time.Now().Add(-time.Hour)}
		start := time.Now()
		err := c.waitForNextPoll(context.Background(), time.Second)
		require.NoError(t, err)
		assert.Less(t, time.Since(start), 100*time.Millisecond)
	})

	t.Run("context cancellation interrupts the wait", func(t *testing.T) {
		c := &BigQueryConnector{lastPollAt: time.Now()}
		ctx, cancel := context.WithCancel(context.Background())
		go func() {
			time.Sleep(10 * time.Millisecond)
			cancel()
		}()
		start := time.Now()
		err := c.waitForNextPoll(ctx, time.Hour)
		require.ErrorIs(t, err, context.Canceled)
		assert.Less(t, time.Since(start), time.Second)
	})
}

func changeRow(changeType string, isForUpdate bool, t time.Time, pk ...bigquery.Value) bigQueryChangeRow {
	return bigQueryChangeRow{changeType: changeType, changeTime: t, isForUpdate: isForUpdate, pk: pk}
}

func TestPairBigQueryChanges(t *testing.T) {
	t1 := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	t2 := t1.Add(time.Second)

	t.Run("delete+insert pair with matching timestamp and pk becomes one update", func(t *testing.T) {
		rows := []bigQueryChangeRow{
			changeRow(bigQueryChangeTypeDelete, true, t1, int64(1)),
			changeRow(bigQueryChangeTypeInsert, true, t1, int64(1)),
		}
		got := pairBigQueryChanges(rows)
		require.Len(t, got, 1)
		assert.Equal(t, bigQueryPairedChange{kind: bigQueryChangeUpdate, deleteIdx: 0, insertIdx: 1}, got[0])
	})

	t.Run("standalone insert (not flagged for update) passes through", func(t *testing.T) {
		rows := []bigQueryChangeRow{
			changeRow(bigQueryChangeTypeInsert, false, t1, int64(2)),
		}
		got := pairBigQueryChanges(rows)
		require.Len(t, got, 1)
		assert.Equal(t, bigQueryPairedChange{kind: bigQueryChangeInsert, deleteIdx: -1, insertIdx: 0}, got[0])
	})

	t.Run("standalone delete (not flagged for update) passes through", func(t *testing.T) {
		rows := []bigQueryChangeRow{
			changeRow(bigQueryChangeTypeDelete, false, t1, int64(3)),
		}
		got := pairBigQueryChanges(rows)
		require.Len(t, got, 1)
		assert.Equal(t, bigQueryPairedChange{kind: bigQueryChangeDelete, deleteIdx: 0, insertIdx: -1}, got[0])
	})

	t.Run("flagged delete followed by insert with a different timestamp does not pair", func(t *testing.T) {
		rows := []bigQueryChangeRow{
			changeRow(bigQueryChangeTypeDelete, true, t1, int64(1)),
			changeRow(bigQueryChangeTypeInsert, true, t2, int64(1)),
		}
		got := pairBigQueryChanges(rows)
		require.Len(t, got, 2)
		assert.Equal(t, bigQueryPairedChange{kind: bigQueryChangeDelete, deleteIdx: 0, insertIdx: -1}, got[0])
		assert.Equal(t, bigQueryPairedChange{kind: bigQueryChangeInsert, deleteIdx: -1, insertIdx: 1}, got[1])
	})

	t.Run("flagged delete followed by insert with a different pk does not pair", func(t *testing.T) {
		rows := []bigQueryChangeRow{
			changeRow(bigQueryChangeTypeDelete, true, t1, int64(1)),
			changeRow(bigQueryChangeTypeInsert, true, t1, int64(2)),
		}
		got := pairBigQueryChanges(rows)
		require.Len(t, got, 2)
		assert.Equal(t, bigQueryChangeDelete, got[0].kind)
		assert.Equal(t, bigQueryChangeInsert, got[1].kind)
	})

	t.Run("flagged delete followed by an insert that isn't flagged for update does not pair", func(t *testing.T) {
		rows := []bigQueryChangeRow{
			changeRow(bigQueryChangeTypeDelete, true, t1, int64(1)),
			changeRow(bigQueryChangeTypeInsert, false, t1, int64(1)),
		}
		got := pairBigQueryChanges(rows)
		require.Len(t, got, 2)
		assert.Equal(t, bigQueryChangeDelete, got[0].kind)
		assert.Equal(t, bigQueryChangeInsert, got[1].kind)
	})

	t.Run("flagged delete with no following row (end of batch) passes through standalone", func(t *testing.T) {
		rows := []bigQueryChangeRow{
			changeRow(bigQueryChangeTypeInsert, false, t1, int64(9)),
			changeRow(bigQueryChangeTypeDelete, true, t2, int64(1)),
		}
		got := pairBigQueryChanges(rows)
		require.Len(t, got, 2)
		assert.Equal(t, bigQueryPairedChange{kind: bigQueryChangeInsert, deleteIdx: -1, insertIdx: 0}, got[0])
		assert.Equal(t, bigQueryPairedChange{kind: bigQueryChangeDelete, deleteIdx: 1, insertIdx: -1}, got[1])
	})

	t.Run("multiple pairs in sequence each pair up independently", func(t *testing.T) {
		rows := []bigQueryChangeRow{
			changeRow(bigQueryChangeTypeDelete, true, t1, int64(1)),
			changeRow(bigQueryChangeTypeInsert, true, t1, int64(1)),
			changeRow(bigQueryChangeTypeDelete, true, t2, int64(2)),
			changeRow(bigQueryChangeTypeInsert, true, t2, int64(2)),
		}
		got := pairBigQueryChanges(rows)
		require.Len(t, got, 2)
		assert.Equal(t, bigQueryPairedChange{kind: bigQueryChangeUpdate, deleteIdx: 0, insertIdx: 1}, got[0])
		assert.Equal(t, bigQueryPairedChange{kind: bigQueryChangeUpdate, deleteIdx: 2, insertIdx: 3}, got[1])
	})

	t.Run("a paired insert is consumed and not re-considered as its own entry", func(t *testing.T) {
		// If the pairing consumed index 1 correctly, the loop resumes at index 2
		// rather than re-examining the insert at index 1 as a standalone row.
		rows := []bigQueryChangeRow{
			changeRow(bigQueryChangeTypeDelete, true, t1, int64(1)),
			changeRow(bigQueryChangeTypeInsert, true, t1, int64(1)),
			changeRow(bigQueryChangeTypeInsert, false, t2, int64(5)),
		}
		got := pairBigQueryChanges(rows)
		require.Len(t, got, 2)
		assert.Equal(t, bigQueryChangeUpdate, got[0].kind)
		assert.Equal(t, bigQueryPairedChange{kind: bigQueryChangeInsert, deleteIdx: -1, insertIdx: 2}, got[1])
	})

	t.Run("empty input yields no changes", func(t *testing.T) {
		assert.Empty(t, pairBigQueryChanges(nil))
	})
}

func TestBigQueryPKEqual(t *testing.T) {
	t.Run("equal scalar values", func(t *testing.T) {
		assert.True(t, bigQueryPKEqual([]bigquery.Value{int64(1), "a"}, []bigquery.Value{int64(1), "a"}))
	})

	t.Run("different scalar values", func(t *testing.T) {
		assert.False(t, bigQueryPKEqual([]bigquery.Value{int64(1)}, []bigquery.Value{int64(2)}))
	})

	t.Run("different lengths", func(t *testing.T) {
		assert.False(t, bigQueryPKEqual([]bigquery.Value{int64(1)}, []bigquery.Value{int64(1), "a"}))
	})

	t.Run("equal time.Time values compare via Equal, not reflect.DeepEqual", func(t *testing.T) {
		base := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
		// Same instant, different Location pointer/representation -- reflect.DeepEqual
		// would consider these different even though they represent the same instant.
		other := base.In(time.FixedZone("UTC", 0))
		assert.True(t, bigQueryPKEqual([]bigquery.Value{base}, []bigquery.Value{other}))
	})

	t.Run("different time.Time values", func(t *testing.T) {
		base := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
		assert.False(t, bigQueryPKEqual([]bigquery.Value{base}, []bigquery.Value{base.Add(time.Second)}))
	})
}

func TestBigQueryRowToRecordItems(t *testing.T) {
	schema := bigquery.Schema{
		{Name: "id", Type: bigquery.IntegerFieldType},
		{Name: "name", Type: bigquery.StringFieldType},
		{Name: "excluded_col", Type: bigquery.StringFieldType},
		{Name: bigQueryChangeTypeColumn, Type: bigquery.StringFieldType},
		{Name: bigQueryChangeTimestampColumn, Type: bigquery.TimestampFieldType},
		{Name: bigQueryChangeIsForUpdateColumn, Type: bigquery.BooleanFieldType},
	}
	qfields := make([]types.QField, len(schema))
	for i, f := range schema {
		qfields[i] = BigQueryFieldToQField(f)
	}
	row := []bigquery.Value{
		int64(1), "alice", "secret", bigQueryChangeTypeInsert, time.Now(), true,
	}

	items, err := bigQueryRowToRecordItems(schema, qfields, row, map[string]struct{}{"excluded_col": {}})
	require.NoError(t, err)
	assert.Equal(t, types.QValueInt64{Val: 1}, items.GetColumnValue("id"))
	assert.Equal(t, types.QValueString{Val: "alice"}, items.GetColumnValue("name"))
	assert.Nil(t, items.GetColumnValue("excluded_col"))
	assert.Nil(t, items.GetColumnValue(bigQueryChangeTypeColumn))
	assert.Nil(t, items.GetColumnValue(bigQueryChangeTimestampColumn))
	assert.Nil(t, items.GetColumnValue(bigQueryChangeIsForUpdateColumn))
	assert.Len(t, items.ColToVal, 2)
}
