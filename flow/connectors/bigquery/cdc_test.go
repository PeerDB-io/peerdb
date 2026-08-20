package connbigquery

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/googleapi"

	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

func TestPollWindow(t *testing.T) {
	checkpoint := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	const safetyLag = time.Minute
	const maxQueryWindow = 24 * time.Hour

	t.Run("caps at maxQueryWindow past checkpoint when now is far ahead", func(t *testing.T) {
		now := checkpoint.Add(maxQueryWindow * 10)
		upper, ok := pollWindow(checkpoint, now, safetyLag, maxQueryWindow)
		require.True(t, ok)
		assert.True(t, upper.Equal(checkpoint.Add(maxQueryWindow)))
	})

	t.Run("caps at safetyLag behind now when now is close", func(t *testing.T) {
		now := checkpoint.Add(time.Hour)
		upper, ok := pollWindow(checkpoint, now, safetyLag, maxQueryWindow)
		require.True(t, ok)
		assert.True(t, upper.Equal(now.Add(-safetyLag)))
	})

	t.Run("nothing new to scan when safety lag hasn't cleared", func(t *testing.T) {
		now := checkpoint.Add(safetyLag / 2)
		upper, ok := pollWindow(checkpoint, now, safetyLag, maxQueryWindow)
		assert.False(t, ok)
		// upper is still reported (as now-safetyLag), just not usable, since it
		// doesn't move past checkpoint.
		assert.True(t, upper.Equal(now.Add(-safetyLag)))
		assert.False(t, upper.After(checkpoint))
	})

	t.Run("exactly at the boundary is not ok (upper must strictly move past checkpoint)", func(t *testing.T) {
		now := checkpoint.Add(safetyLag)
		upper, ok := pollWindow(checkpoint, now, safetyLag, maxQueryWindow)
		assert.False(t, ok)
		assert.True(t, upper.Equal(checkpoint))
	})
}

func TestBigQueryCDCCheckpointLegacyUpgrade(t *testing.T) {
	legacy := "2026-08-01T00:00:00Z"
	cp, err := parseBigQueryCDCCheckpoint(legacy, []string{"project.dataset.a", "project.dataset.b"})
	require.NoError(t, err)

	want := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	assert.Equal(t, want, cp.Tables["project.dataset.a"].SyncedThrough)
	assert.Equal(t, want, cp.Tables["project.dataset.a"].Target)
	assert.Equal(t, want, cp.Tables["project.dataset.b"].SyncedThrough)

	encoded, err := cp.Marshal()
	require.NoError(t, err)
	assert.JSONEq(t, `{
		"version": 1,
		"tables": {
			"project.dataset.a": {"synced_through": "2026-08-01T00:00:00Z", "target": "2026-08-01T00:00:00Z", "active": true},
			"project.dataset.b": {"synced_through": "2026-08-01T00:00:00Z", "target": "2026-08-01T00:00:00Z", "active": true}
		}
	}`, encoded)
}

func TestBigQueryCDCCheckpointRecordsIndependentTableOutcomes(t *testing.T) {
	start := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	target := start.Add(time.Hour)
	cp, err := parseBigQueryCDCCheckpoint(start.Format(time.RFC3339Nano), []string{"a", "b"})
	require.NoError(t, err)

	cp.RecordSuccess("a", target)
	assert.True(t, cp.RecordFailure("b", target), "first failure in an episode should be reported")
	assert.False(t, cp.RecordFailure("b", target.Add(time.Hour)), "repeated failure should not be reported again")

	assert.Equal(t, target, cp.Tables["a"].SyncedThrough)
	assert.Equal(t, target, cp.Tables["a"].Target)
	assert.Equal(t, start, cp.Tables["b"].SyncedThrough)
	assert.Equal(t, target.Add(time.Hour), cp.Tables["b"].Target)

	cp.RecordSuccess("b", target.Add(2*time.Hour))
	assert.True(t, cp.RecordFailure("b", target.Add(3*time.Hour)), "failure after recovery starts a new episode")
}

func TestBigQueryCDCCheckpointRetainsRemovedTablesAsInactive(t *testing.T) {
	raw := `{
		"version": 1,
		"tables": {
			"removed": {"synced_through": "2026-08-01T00:00:00Z", "target": "2026-08-01T01:00:00Z", "active": true}
		}
	}`
	cp, err := parseBigQueryCDCCheckpoint(raw, []string{"added"})
	require.NoError(t, err)

	assert.False(t, cp.Tables["removed"].Active)
	assert.True(t, cp.Tables["added"].Active)
	assert.Equal(t, cp.Tables["removed"].SyncedThrough, cp.Tables["added"].SyncedThrough)
}

func TestBigQueryCDCBatchTableProgress(t *testing.T) {
	batch := `{
		"version": 1,
		"tables": {
			"a": {"synced_through": "2026-08-01T01:00:00Z", "target": "2026-08-01T01:00:00Z", "active": true},
			"b": {"synced_through": "2026-08-01T00:00:00Z", "target": "2026-08-01T01:00:00Z", "active": true}
		}
	}`
	latest := `{
		"version": 1,
		"tables": {
			"a": {"synced_through": "2026-08-01T02:00:00Z", "target": "2026-08-01T02:00:00Z", "active": true},
			"b": {"synced_through": "2026-08-01T00:30:00Z", "target": "2026-08-01T01:30:00Z", "active": true}
		}
	}`

	progress, ok := BigQueryCDCBatchTableProgress(batch, latest)
	require.True(t, ok)
	assert.Equal(t, 1, progress.Completed)
	assert.Equal(t, 2, progress.Total)
	assert.Equal(t, []string{"b"}, progress.LaggingTables)

	_, ok = BigQueryCDCBatchTableProgress("2026-08-01T01:00:00Z", latest)
	assert.False(t, ok, "legacy batches do not carry immutable table membership")
}

func TestBigQueryCDCCheckpointRejectsUnknownVersion(t *testing.T) {
	_, err := parseBigQueryCDCCheckpoint(`{"version":2,"tables":{}}`, nil)
	require.ErrorContains(t, err, "unsupported BigQuery CDC checkpoint version 2")
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

func TestBigQueryRowToRecordItems(t *testing.T) {
	schema := bigquery.Schema{
		{Name: "id", Type: bigquery.IntegerFieldType},
		{Name: "name", Type: bigquery.StringFieldType},
		{Name: bigQueryChangeTypeColumn, Type: bigquery.StringFieldType},
		{Name: bigQueryChangeTimestampColumn, Type: bigquery.TimestampFieldType},
		{Name: bigQueryChangeIsForUpdateColumn, Type: bigquery.BooleanFieldType},
	}
	qfields := make([]types.QField, len(schema))
	for i, f := range schema {
		qfields[i] = BigQueryFieldToQField(f)
	}
	row := []bigquery.Value{
		int64(1), "alice", bigQueryChangeTypeInsert, time.Now(), true,
	}

	items, err := bigQueryRowToRecordItems(schema, qfields, row)
	require.NoError(t, err)
	assert.Equal(t, types.QValueInt64{Val: 1}, items.GetColumnValue("id"))
	assert.Equal(t, types.QValueString{Val: "alice"}, items.GetColumnValue("name"))
	assert.Nil(t, items.GetColumnValue(bigQueryChangeTypeColumn))
	assert.Nil(t, items.GetColumnValue(bigQueryChangeTimestampColumn))
	assert.Nil(t, items.GetColumnValue(bigQueryChangeIsForUpdateColumn))
	assert.Len(t, items.ColToVal, 2)
}

func TestExceptClause(t *testing.T) {
	assert.Empty(t, exceptClause(nil))
	assert.Empty(t, exceptClause(map[string]struct{}{}))
	assert.Equal(t, " EXCEPT (`a`, `b`)", exceptClause(map[string]struct{}{"b": {}, "a": {}}))
}

func TestMissingExceptColumns(t *testing.T) {
	candidates := map[string]struct{}{"secret_column": {}, "large_payload": {}, "id": {}}

	err := &googleapi.Error{
		Code:    400,
		Message: "Column secret_column in SELECT * EXCEPT list does not exist at [1:18]",
	}
	assert.Equal(t, map[string]struct{}{"secret_column": {}}, missingExceptColumns(err, candidates))
	assert.Equal(t, map[string]struct{}{"secret_column": {}}, missingExceptColumns(fmt.Errorf("query failed: %w", err), candidates))

	assert.Empty(t, missingExceptColumns(errors.New("some unrelated failure"), candidates))
	assert.Empty(t, missingExceptColumns(&googleapi.Error{Code: 404, Message: "secret_column"}, candidates))
	assert.Empty(t, missingExceptColumns(&googleapi.Error{Code: 400, Message: "Unrecognized name: secret_column"}, candidates))

	// Excluding both "id" and "user_id": losing user_id must not also flag "id".
	idCandidates := map[string]struct{}{"id": {}, "user_id": {}}
	userIDErr := &googleapi.Error{
		Code:    400,
		Message: "Column user_id in SELECT * EXCEPT list does not exist at [1:18]",
	}
	assert.Equal(t, map[string]struct{}{"user_id": {}}, missingExceptColumns(userIDErr, idCandidates))

	// A column literally named after a word in the error's fixed boilerplate must
	// not be flagged by some other column's error.
	boilerplateCandidates := map[string]struct{}{"at": {}, "list": {}}
	assert.Empty(t, missingExceptColumns(err, boilerplateCandidates))
}

func TestEffectiveExclude(t *testing.T) {
	c := &BigQueryConnector{droppedExcludeColumns: map[string]map[string]struct{}{
		"ds.tbl": {"secret_column": {}},
	}}
	exclude := map[string]struct{}{"secret_column": {}, "large_payload": {}}

	assert.Equal(t, map[string]struct{}{"large_payload": {}}, c.effectiveExclude("ds.tbl", exclude))
	assert.Equal(t, exclude, c.effectiveExclude("ds.other", exclude))
}

func TestBuildPullQuery(t *testing.T) {
	assert.Equal(t,
		"SELECT * FROM APPENDS(TABLE `ds`.`tbl`, @start, @end)",
		buildPullQuery("APPENDS", "`ds`.`tbl`", nil, ""),
	)
	assert.Equal(t,
		"SELECT * EXCEPT (`secret`) FROM APPENDS(TABLE `ds`.`tbl`, @start, @end)",
		buildPullQuery("APPENDS", "`ds`.`tbl`", map[string]struct{}{"secret": {}}, ""),
	)
	assert.Equal(t,
		"SELECT * EXCEPT (`secret`) FROM CHANGES(TABLE `ds`.`tbl`, @start, @end) ORDER BY `_CHANGE_TIMESTAMP`",
		buildPullQuery("CHANGES", "`ds`.`tbl`", map[string]struct{}{"secret": {}}, "`_CHANGE_TIMESTAMP`"),
	)
}
