package connbigquery

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/googleapi"

	"github.com/PeerDB-io/peerdb/flow/model"
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

func TestBigQueryCDCCheckpointInitialization(t *testing.T) {
	start := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	cp := newBigQueryCDCCheckpoint(start, []string{"project.dataset.a", "project.dataset.b"})

	assert.Equal(t, start, cp.Tables["project.dataset.a"].SyncedThrough)
	assert.Equal(t, start, cp.Tables["project.dataset.a"].Target)
	assert.Equal(t, start, cp.Tables["project.dataset.b"].SyncedThrough)

	encoded, err := cp.Marshal()
	require.NoError(t, err)
	assert.JSONEq(t, `{
		"version": 1,
		"tables": {
			"project.dataset.a": {"synced_through": "2026-08-01T00:00:00Z", "target": "2026-08-01T00:00:00Z", "synced_batch_id": 0},
			"project.dataset.b": {"synced_through": "2026-08-01T00:00:00Z", "target": "2026-08-01T00:00:00Z", "synced_batch_id": 0}
		}
	}`, encoded)
}

func TestBigQueryCDCCheckpointRecordsIndependentTableOutcomes(t *testing.T) {
	start := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	target := start.Add(time.Hour)
	cp := newBigQueryCDCCheckpoint(start, []string{"a", "b"})

	cp.RecordSuccess("a", target, 7)
	assert.True(t, cp.RecordFailure("b", target), "first failure in an episode should be reported")
	assert.False(t, cp.RecordFailure("b", target.Add(time.Hour)), "repeated failure should not be reported again")

	assert.Equal(t, target, cp.Tables["a"].SyncedThrough)
	assert.Equal(t, target, cp.Tables["a"].Target)
	assert.Equal(t, start, cp.Tables["b"].SyncedThrough)
	assert.Equal(t, target.Add(time.Hour), cp.Tables["b"].Target)

	cp.RecordSuccess("b", target.Add(2*time.Hour), 8)
	assert.True(t, cp.RecordFailure("b", target.Add(3*time.Hour)), "failure after recovery starts a new episode")
}

func TestBigQueryCDCCheckpointTableBackpressure(t *testing.T) {
	start := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	cp := newBigQueryCDCCheckpoint(start, []string{"a", "b"})
	cp.RecordSuccess("a", start.Add(time.Hour), 10)
	cp.RecordSuccess("b", start.Add(time.Hour), 20)

	assert.False(t, cp.IsBackpressured("a", 9, 2))
	assert.True(t, cp.IsBackpressured("a", 8, 2), "the exact buffer boundary applies pressure")
	assert.True(t, cp.IsBackpressured("b", 18, 2))

	before := cp.Tables["a"]
	cp.RecordSuccess("b", start.Add(2*time.Hour), 21)
	assert.Equal(t, before, cp.Tables["a"], "a skipped table retains its complete progress")
}

func TestTableIsBackpressuredUsesDestinationProgressAndGlobalFloor(t *testing.T) {
	start := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	cp := newBigQueryCDCCheckpoint(start, []string{"source.a", "source.b"})
	cp.RecordSuccess("source.a", start.Add(time.Hour), 10)
	cp.RecordSuccess("source.b", start.Add(time.Hour), 10)
	pressure := &model.TableBackpressure{
		GlobalNormalizedID: 7,
		NormalizedBatchIDs: map[string]int64{"destination.a": 8},
		BufferSize:         2,
	}

	assert.True(t, tableIsBackpressured(cp, "source.a", "destination.a", pressure))
	assert.True(t, tableIsBackpressured(cp, "source.b", "destination.b", pressure))
	pressure.GlobalNormalizedID = 9
	assert.False(t, tableIsBackpressured(cp, "source.b", "destination.b", pressure),
		"the global frontier covers batches where a sparse table had no rows")
	assert.False(t, tableIsBackpressured(cp, "source.a", "destination.a", nil))
}

func TestBigQueryCDCCheckpointDropsRemovedTables(t *testing.T) {
	raw := `{
		"version": 1,
		"tables": {
			"removed": {"synced_through": "2026-08-01T00:00:00Z", "target": "2026-08-01T01:00:00Z"}
		}
	}`
	cp, err := parseBigQueryCDCCheckpoint(raw, []string{"added"})
	require.NoError(t, err)

	assert.NotContains(t, cp.Tables, "removed")
	assert.Equal(t, time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC), cp.Tables["added"].SyncedThrough)
}

func TestBigQueryCDCBatchTableProgress(t *testing.T) {
	batch := `{
		"version": 1,
		"tables": {
			"a": {"synced_through": "2026-08-01T01:00:00Z", "target": "2026-08-01T01:00:00Z"},
			"b": {"synced_through": "2026-08-01T00:00:00Z", "target": "2026-08-01T01:00:00Z"}
		}
	}`
	latest := `{
		"version": 1,
		"tables": {
			"a": {"synced_through": "2026-08-01T02:00:00Z", "target": "2026-08-01T02:00:00Z"},
			"b": {"synced_through": "2026-08-01T00:30:00Z", "target": "2026-08-01T01:30:00Z"}
		}
	}`

	progress, ok := BigQueryCDCBatchTableProgress(batch, latest)
	require.True(t, ok)
	assert.Equal(t, 1, progress.Completed)
	assert.Equal(t, 2, progress.Total)
	assert.Equal(t, []string{"b"}, progress.LaggingTables)

	latestAfterRemovingB := `{
		"version": 1,
		"tables": {
			"a": {"synced_through": "2026-08-01T02:00:00Z", "target": "2026-08-01T02:00:00Z"}
		}
	}`
	progress, ok = BigQueryCDCBatchTableProgress(batch, latestAfterRemovingB)
	require.True(t, ok)
	assert.Equal(t, 1, progress.Completed)
	assert.Equal(t, 1, progress.Total)
	assert.Empty(t, progress.LaggingTables)

	_, ok = BigQueryCDCBatchTableProgress("2026-08-01T01:00:00Z", latest)
	assert.False(t, ok, "non-BigQuery checkpoints do not carry table membership")
}

func TestBigQueryCDCCheckpointRejectsNonJSON(t *testing.T) {
	_, err := parseBigQueryCDCCheckpoint("2026-08-01T00:00:00Z", nil)
	require.ErrorContains(t, err, "failed to parse BigQuery CDC checkpoint JSON")
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
	c := &BigQueryConnector{}
	c.droppedExcludeColumns.Store("ds.tbl", map[string]struct{}{"secret_column": {}})
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

func testPollPlan(
	table string,
	pull func(context.Context, string, model.NameAndExclude, time.Time, time.Time,
		func(context.Context, model.Record[model.RecordItems]) error) (int64, error),
) tablePollPlan {
	start := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	return tablePollPlan{start: start, upper: start.Add(time.Hour), table: table, pull: pull}
}

func TestRunTablePollsIsolatesFailures(t *testing.T) {
	failure := errors.New("boom")
	plans := []tablePollPlan{
		testPollPlan("ds.ok", func(
			context.Context, string, model.NameAndExclude, time.Time, time.Time,
			func(context.Context, model.Record[model.RecordItems]) error,
		) (int64, error) {
			return 42, nil
		}),
		testPollPlan("ds.bad", func(
			context.Context, string, model.NameAndExclude, time.Time, time.Time,
			func(context.Context, model.Record[model.RecordItems]) error,
		) (int64, error) {
			return 0, failure
		}),
	}

	results, err := runTablePolls(t.Context(), plans, 2, nil, nil)
	require.NoError(t, err)
	require.Len(t, results, 2)
	require.NoError(t, results[0].err)
	assert.Equal(t, int64(42), results[0].bytesProcessed)
	assert.ErrorIs(t, results[1].err, failure)
}

func TestRunTablePollsFailsBatchWhenTableAlreadyEmitted(t *testing.T) {
	failure := errors.New("boom")
	plans := []tablePollPlan{
		testPollPlan("ds.bad", func(
			ctx context.Context, _ string, _ model.NameAndExclude, _ time.Time, _ time.Time,
			addRecord func(context.Context, model.Record[model.RecordItems]) error,
		) (int64, error) {
			if err := addRecord(ctx, &model.InsertRecord[model.RecordItems]{}); err != nil {
				return 0, err
			}
			return 0, failure
		}),
	}

	_, err := runTablePolls(t.Context(), plans, 1, nil, func(context.Context, model.Record[model.RecordItems]) error {
		return nil
	})
	require.ErrorIs(t, err, failure)
	assert.Contains(t, err.Error(), "ds.bad")
}

func TestRunTablePollsRespectsParallelism(t *testing.T) {
	const parallelism = 2
	var mu sync.Mutex
	inFlight, maxInFlight := 0, 0

	plans := make([]tablePollPlan, 0, 8)
	for i := range 8 {
		plans = append(plans, testPollPlan(fmt.Sprintf("ds.t%d", i), func(
			context.Context, string, model.NameAndExclude, time.Time, time.Time,
			func(context.Context, model.Record[model.RecordItems]) error,
		) (int64, error) {
			mu.Lock()
			inFlight++
			maxInFlight = max(maxInFlight, inFlight)
			mu.Unlock()
			time.Sleep(5 * time.Millisecond)
			mu.Lock()
			inFlight--
			mu.Unlock()
			return 0, nil
		}))
	}

	_, err := runTablePolls(t.Context(), plans, parallelism, nil, nil)
	require.NoError(t, err)
	mu.Lock()
	defer mu.Unlock()
	assert.LessOrEqual(t, maxInFlight, parallelism)
	assert.Greater(t, maxInFlight, 1, "tables should actually run concurrently")
}

func TestRunTablePollsReturnsContextError(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	plans := []tablePollPlan{
		testPollPlan("ds.tbl", func(
			ctx context.Context, _ string, _ model.NameAndExclude, _ time.Time, _ time.Time,
			_ func(context.Context, model.Record[model.RecordItems]) error,
		) (int64, error) {
			return 0, ctx.Err()
		}),
	}

	_, err := runTablePolls(ctx, plans, 1, nil, nil)
	assert.ErrorIs(t, err, context.Canceled)
}
