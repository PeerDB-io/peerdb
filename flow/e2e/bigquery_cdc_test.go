package e2e

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"

	connclickhouse "github.com/PeerDB-io/peerdb/flow/connectors/clickhouse"
	connmetadata "github.com/PeerDB-io/peerdb/flow/connectors/external_metadata"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/shared"
)

type bqCdcRow struct {
	Val string
	ID  int64
}

func createBigQueryCdcSourceTable(
	ctx context.Context, t *testing.T, source *bigQuerySource, tableName string, enableChangeHistory bool,
) string {
	t.Helper()

	table := source.client.DatasetInProject(source.config.ProjectId, source.config.DatasetId).Table(tableName)
	err := table.Create(ctx, &bigquery.TableMetadata{
		Schema: bigquery.Schema{
			{Name: "id", Type: bigquery.IntegerFieldType, Required: true},
			{Name: "val", Type: bigquery.StringFieldType, Required: false},
		},
		TableConstraints: &bigquery.TableConstraints{
			PrimaryKey: &bigquery.PrimaryKey{Columns: []string{"id"}},
		},
	})
	require.NoError(t, err, "should create BigQuery CDC source table %s", tableName)

	t.Cleanup(func() {
		if err := table.Delete(context.Background()); err != nil {
			t.Logf("Warning: failed to delete test table %s: %v", tableName, err)
		}
	})

	fqn := fmt.Sprintf("%s.%s.%s", source.config.ProjectId, source.config.DatasetId, tableName)
	if enableChangeHistory {
		require.NoError(t, source.Exec(ctx, fmt.Sprintf("ALTER TABLE `%s` SET OPTIONS(enable_change_history=true)", fqn)),
			"should enable enable_change_history on %s", tableName)
	}
	return fqn
}

func bqInsertRows(ctx context.Context, t *testing.T, source *bigQuerySource, tableFQN string, rows []bqCdcRow) {
	t.Helper()
	if len(rows) == 0 {
		return
	}

	values := make([]string, len(rows))
	for i, row := range rows {
		values[i] = fmt.Sprintf("(%d, %s)", row.ID, bqQuoteStringLiteral(row.Val))
	}
	sql := fmt.Sprintf("INSERT INTO `%s` (id, val) VALUES %s", tableFQN, strings.Join(values, ", "))
	require.NoError(t, source.Exec(ctx, sql), "should insert rows into %s", tableFQN)
}

func bqUpdateRowVal(ctx context.Context, t *testing.T, source *bigQuerySource, tableFQN string, id int64, newVal string) {
	t.Helper()
	sql := fmt.Sprintf("UPDATE `%s` SET val = %s WHERE id = %d", tableFQN, bqQuoteStringLiteral(newVal), id)
	require.NoError(t, source.Exec(ctx, sql), "should update row %d in %s", id, tableFQN)
}

func bqDeleteRow(ctx context.Context, t *testing.T, source *bigQuerySource, tableFQN string, id int64) {
	t.Helper()
	sql := fmt.Sprintf("DELETE FROM `%s` WHERE id = %d", tableFQN, id)
	require.NoError(t, source.Exec(ctx, sql), "should delete row %d from %s", id, tableFQN)
}

func bqQuoteStringLiteral(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "\\'") + "'"
}

func createBigQueryAllTypesCdcSourceTable(
	ctx context.Context, t *testing.T, source *bigQuerySource, tableName string,
) string {
	t.Helper()
	fqn := fmt.Sprintf("%s.%s.%s", source.config.ProjectId, source.config.DatasetId, tableName)
	err := source.Exec(ctx, fmt.Sprintf(`CREATE TABLE %s (
		id INT64 NOT NULL,
		str_col STRING,
		bytes_col BYTES,
		int_col INT64,
		float_col FLOAT64,
		bool_col BOOL,
		ts_col TIMESTAMP,
		date_col DATE,
		time_col TIME,
		datetime_col DATETIME,
		numeric_col NUMERIC,
		bignumeric_col BIGNUMERIC,
		geography_col GEOGRAPHY,
		json_col JSON,
		interval_col INTERVAL,
		range_date_col RANGE<DATE>,
		record_col STRUCT<nested_str STRING, nested_int INT64>,
		array_str ARRAY<STRING>,
		array_bytes ARRAY<BYTES>,
		array_int ARRAY<INT64>,
		array_float ARRAY<FLOAT64>,
		array_bool ARRAY<BOOL>,
		array_ts ARRAY<TIMESTAMP>,
		array_date ARRAY<DATE>,
		array_datetime ARRAY<DATETIME>,
		array_time ARRAY<TIME>,
		array_numeric ARRAY<NUMERIC>,
		array_bignumeric ARRAY<BIGNUMERIC>,
		array_geography ARRAY<GEOGRAPHY>,
		array_json ARRAY<JSON>,
		array_interval ARRAY<INTERVAL>,
		array_range ARRAY<RANGE<DATE>>,
		array_record ARRAY<STRUCT<item_name STRING, item_count INT64>>,
		PRIMARY KEY(id) NOT ENFORCED
	)`, quoteBigQueryTableFQN(fqn)))
	require.NoError(t, err, "should create all-types BigQuery CDC source table %s", tableName)

	t.Cleanup(func() {
		table := source.client.DatasetInProject(source.config.ProjectId, source.config.DatasetId).Table(tableName)
		if err := table.Delete(context.Background()); err != nil {
			t.Logf("Warning: failed to delete test table %s: %v", tableName, err)
		}
	})
	return fqn
}

func quoteBigQueryTableFQN(fqn string) string {
	return "`" + strings.ReplaceAll(fqn, "`", "\\`") + "`"
}

func bqCdcFlowConnectionConfig(
	s BigQueryClickhouseSuite, srcTable, dstTable string, eventsFunction protos.BigqueryCdcEventsFunction,
) *protos.FlowConnectionConfigs {
	source := s.Source().(*bigQuerySource)
	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName: AddSuffix(s, srcTable),
		TableMappings: []*protos.TableMapping{
			{
				SourceTableIdentifier:      fmt.Sprintf("%s.%s", source.config.DatasetId, srcTable),
				DestinationTableIdentifier: s.DestinationTable(dstTable),
				BigqueryCdcEventsFunction:  eventsFunction,
			},
		},
		Destination: s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true
	flowConnConfig.InitialSnapshotOnly = false
	flowConnConfig.SnapshotStagingPath = bigQueryTestStagingPath(s, srcTable)
	flowConnConfig.SourceConnectorConfig = &protos.FlowConnectionConfigs_BigqueryCdcConfig{
		BigqueryCdcConfig: &protos.BigqueryCdcConfig{
			ReplicationMode: protos.BigQueryReplicationMode_BIGQUERY_REPLICATION_MODE_EVENTS,
		},
	}
	flowConnConfig.IdleTimeoutSeconds = 5
	flowConnConfig.Env = map[string]string{
		"PEERDB_BIGQUERY_CDC_SAFETY_LAG_SECONDS": "5",
	}

	return flowConnConfig
}

func (s BigQueryClickhouseSuite) Test_BigQuery_CDC_Snapshot_To_CDC_Handoff() {
	t := s.T()
	ctx := t.Context()

	source := s.Source().(*bigQuerySource)
	srcTable := AddSuffix(s, "cdc_handoff")
	dstTable := srcTable + "_dst"
	tableFQN := createBigQueryCdcSourceTable(ctx, t, source, srcTable, false)

	// present before the mirror exists - must land via the initial snapshot.
	bqInsertRows(ctx, t, source, tableFQN, []bqCdcRow{{ID: 1, Val: "pre-snapshot-1"}, {ID: 2, Val: "pre-snapshot-2"}})

	flowConnConfig := bqCdcFlowConnectionConfig(s, srcTable, dstTable, protos.BigqueryCdcEventsFunction_BIGQUERY_CDC_EVENTS_FUNCTION_APPENDS)

	tc := NewTemporalClient(t)
	env := ExecutePeerflow(t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(t, env, flowConnConfig)

	EnvWaitForEqualTablesWithNames(env, s, "initial snapshot landed", srcTable, dstTable, "id,val")

	// inserted strictly after T - must arrive via CDC polling, not the snapshot.
	bqInsertRows(ctx, t, source, tableFQN, []bqCdcRow{{ID: 3, Val: "post-snapshot-1"}})

	EnvWaitFor(t, env, 4*time.Minute, "post-snapshot insert picked up by CDC poll", func() bool {
		rows, err := s.GetRows(dstTable, "id,val")
		if err != nil {
			t.Log(err)
			return false
		}
		return len(rows.Records) == 3
	})
	RequireEqualTablesWithNames(s, srcTable, dstTable, "id,val")

	env.Cancel(ctx)
	RequireEnvCanceled(t, env)
}

// Test_BigQuery_CDC_Appends_Insert_Only covers APPENDS mode
func (s BigQueryClickhouseSuite) Test_BigQuery_CDC_Appends_Insert_Only() {
	t := s.T()
	ctx := t.Context()

	source := s.Source().(*bigQuerySource)
	srcTable := AddSuffix(s, "cdc_appends")
	dstTable := srcTable + "_dst"
	tableFQN := createBigQueryCdcSourceTable(ctx, t, source, srcTable, false)

	bqInsertRows(ctx, t, source, tableFQN, []bqCdcRow{{ID: 1, Val: "initial-1"}})

	flowConnConfig := bqCdcFlowConnectionConfig(s, srcTable, dstTable, protos.BigqueryCdcEventsFunction_BIGQUERY_CDC_EVENTS_FUNCTION_APPENDS)

	tc := NewTemporalClient(t)
	env := ExecutePeerflow(t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(t, env, flowConnConfig)

	EnvWaitForEqualTablesWithNames(env, s, "initial snapshot landed", srcTable, dstTable, "id,val")

	// first CDC wave.
	bqInsertRows(ctx, t, source, tableFQN, []bqCdcRow{{ID: 2, Val: "wave-1-a"}, {ID: 3, Val: "wave-1-b"}})
	EnvWaitFor(t, env, 4*time.Minute, "first CDC wave picked up", func() bool {
		rows, err := s.GetRows(dstTable, "id,val")
		if err != nil {
			t.Log(err)
			return false
		}
		return len(rows.Records) == 3
	})

	// second CDC wave, on a later poll cycle.
	bqInsertRows(ctx, t, source, tableFQN, []bqCdcRow{{ID: 4, Val: "wave-2-a"}})
	EnvWaitFor(t, env, 4*time.Minute, "second CDC wave picked up", func() bool {
		rows, err := s.GetRows(dstTable, "id,val")
		if err != nil {
			t.Log(err)
			return false
		}
		return len(rows.Records) == 4
	})
	RequireEqualTablesWithNames(s, srcTable, dstTable, "id,val")

	env.Cancel(ctx)
	RequireEnvCanceled(t, env)
}

// Test_BigQuery_CDC_All_Types inserts its row only after replication setup, so
// every value must travel through APPENDS(), qvalueFromBigQueryValue, and the CDC
// destination path. This complements Test_Types, which exercises snapshot export.
func (s BigQueryClickhouseSuite) Test_BigQuery_CDC_All_Types() {
	t := s.T()
	ctx := t.Context()

	source := s.Source().(*bigQuerySource)
	srcTable := AddSuffix(s, "cdc_all_types")
	dstTable := srcTable + "_dst"
	tableFQN := createBigQueryAllTypesCdcSourceTable(ctx, t, source, srcTable)

	flowConnConfig := bqCdcFlowConnectionConfig(
		s, srcTable, dstTable, protos.BigqueryCdcEventsFunction_BIGQUERY_CDC_EVENTS_FUNCTION_APPENDS,
	)
	flowConnConfig.DoInitialSnapshot = false

	tc := NewTemporalClient(t)
	env := ExecutePeerflow(t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(t, env, flowConnConfig)
	EnvWaitFor(t, env, 2*time.Minute, "CDC-only mirror is running", func() bool {
		return env.GetFlowStatus(t) == protos.FlowStatus_STATUS_RUNNING
	})

	err := source.Exec(ctx, fmt.Sprintf(`INSERT INTO %s VALUES (
		1,
		'text value',
		b'hi',
		-42,
		3.5,
		TRUE,
		TIMESTAMP '2024-01-02 03:04:05.123456+00',
		DATE '2024-01-02',
		TIME '03:04:05.123456',
		DATETIME '2024-01-02 03:04:05.123456',
		NUMERIC '123.456789123',
		BIGNUMERIC '0.12345678901234567890123456789012345678',
		ST_GEOGFROMTEXT('POINT(1 2)'),
		JSON '{"n":18446744073709551615}',
		INTERVAL '1-2 3 4:5:6' YEAR TO SECOND,
		RANGE<DATE> '[2024-01-01, 2024-02-01)',
		STRUCT('nested', 7),
		['a', 'b'],
		[b'hi', b'bye'],
		[1, 2],
		[1.5, 2.5],
		[TRUE, FALSE],
		[TIMESTAMP '2024-01-02 03:04:05+00'],
		[DATE '2024-01-02'],
		[DATETIME '2024-01-02 03:04:05.123456'],
		[TIME '03:04:05.123456'],
		[NUMERIC '1.123456789'],
		[BIGNUMERIC '0.12345678901234567890123456789012345678'],
		[ST_GEOGFROMTEXT('POINT(1 2)')],
		[JSON '{"x":1}', JSON '[true]'],
		[INTERVAL 2 DAY],
		[RANGE<DATE> '[2024-01-01, 2024-02-01)'],
		[STRUCT('item', 9)]
	)`, quoteBigQueryTableFQN(tableFQN)))
	require.NoError(t, err, "should insert all-types CDC row")

	EnvWaitFor(t, env, 4*time.Minute, "all-types row picked up by CDC", func() bool {
		rows, err := s.GetRows(dstTable, "id")
		if err != nil {
			t.Log(err)
			return false
		}
		return len(rows.Records) == 1
	})

	rows, err := s.GetRows(dstTable,
		"bignumeric_col,interval_col,range_date_col,record_col,array_bytes,array_time,array_json,array_record")
	require.NoError(t, err)
	require.Len(t, rows.Records, 1)
	row := rows.Records[0]

	bigNumeric, ok := row[0].Value().(decimal.Decimal)
	require.True(t, ok, "BIGNUMERIC should land as decimal.Decimal")
	require.Equal(t, "0.12345678901234567890123456789012345678", bigNumeric.String())
	require.Equal(t, "1-2 3 4:5:6", row[1].Value())
	require.Equal(t, "[2024-01-01, 2024-02-01)", row[2].Value())
	require.JSONEq(t, `{"nested_str":"nested","nested_int":7}`, row[3].Value().(string))
	require.Equal(t, []string{"aGk=", "Ynll"}, row[4].Value())
	require.Equal(t, []string{"03:04:05.123456"}, row[5].Value())
	require.JSONEq(t, `[{"x":1},[true]]`, row[6].Value().(string))
	arrayRecord, ok := row[7].Value().([]string)
	require.True(t, ok)
	require.Len(t, arrayRecord, 1)
	require.JSONEq(t, `{"item_count":9,"item_name":"item"}`, arrayRecord[0])

	env.Cancel(ctx)
	RequireEnvCanceled(t, env)
}

// Test_BigQuery_CDC_Changes_Insert_Update_Delete covers CHANGES mode
func (s BigQueryClickhouseSuite) Test_BigQuery_CDC_Changes_Insert_Update_Delete() {
	t := s.T()
	ctx := t.Context()

	source := s.Source().(*bigQuerySource)
	srcTable := AddSuffix(s, "cdc_changes")
	dstTable := srcTable + "_dst"
	tableFQN := createBigQueryCdcSourceTable(ctx, t, source, srcTable, true)

	bqInsertRows(ctx, t, source, tableFQN, []bqCdcRow{
		{ID: 1, Val: "initial-1"},
		{ID: 2, Val: "initial-2"},
		{ID: 3, Val: "initial-3"},
	})

	flowConnConfig := bqCdcFlowConnectionConfig(s, srcTable, dstTable, protos.BigqueryCdcEventsFunction_BIGQUERY_CDC_EVENTS_FUNCTION_CHANGES)
	flowConnConfig.Env = map[string]string{"PEERDB_BIGQUERY_CDC_SAFETY_LAG_SECONDS": "5"}

	tc := NewTemporalClient(t)
	env := ExecutePeerflow(t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(t, env, flowConnConfig)

	EnvWaitForEqualTablesWithNames(env, s, "initial snapshot landed", srcTable, dstTable, "id,val")

	// insert.
	bqInsertRows(ctx, t, source, tableFQN, []bqCdcRow{{ID: 4, Val: "inserted"}})
	// update
	bqUpdateRowVal(ctx, t, source, tableFQN, 1, "updated")
	// delete.
	bqDeleteRow(ctx, t, source, tableFQN, 2)

	// post-change expected set: {1: updated, 3: initial-3, 4: inserted} - id=2 deleted.
	var valByID map[int64]string
	EnvWaitFor(t, env, 4*time.Minute, "insert/update/delete picked up by CHANGES CDC poll", func() bool {
		rows, err := s.GetRows(dstTable, "id,val")
		if err != nil {
			t.Log(err)
			return false
		}
		valByID = make(map[int64]string, len(rows.Records))
		for _, rec := range rows.Records {
			valByID[rec[0].Value().(int64)] = rec[1].Value().(string)
		}
		return valByID[1] == "updated" && valByID[3] == "initial-3" && valByID[4] == "inserted" && len(valByID) == 3
	})
	RequireEqualTablesWithNames(s, srcTable, dstTable, "id,val")

	require.Equal(t, "updated", valByID[1], "update should have replaced the row's val, not appeared alongside the old value")
	require.NotContains(t, valByID, int64(2), "deleted row should not be present in the destination")
	require.Equal(t, "inserted", valByID[4])

	env.Cancel(ctx)
	RequireEnvCanceled(t, env)
}

// Test_BigQuery_CDC_Restart_Mid_Window_Resume covers resuming from the
// persisted per-table cursor (cdc_table_replication_state, written by
// RecordTableReplicationSync in the isolated per-table CDC path) rather than
// re-scanning already-synced rows or dropping rows written while the mirror
// wasn't polling.
func (s BigQueryClickhouseSuite) Test_BigQuery_CDC_Restart_Mid_Window_Resume() {
	t := s.T()
	ctx := t.Context()

	source := s.Source().(*bigQuerySource)
	srcTable := AddSuffix(s, "cdc_restart_resume")
	dstTable := srcTable + "_dst"
	sourceTableIdentifier := fmt.Sprintf("%s.%s", source.config.DatasetId, srcTable)
	tableFQN := createBigQueryCdcSourceTable(ctx, t, source, srcTable, false)

	bqInsertRows(ctx, t, source, tableFQN, []bqCdcRow{{ID: 1, Val: "initial-1"}})

	flowConnConfig := bqCdcFlowConnectionConfig(s, srcTable, dstTable, protos.BigqueryCdcEventsFunction_BIGQUERY_CDC_EVENTS_FUNCTION_APPENDS)

	tc := NewTemporalClient(t)
	env := ExecutePeerflow(t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(t, env, flowConnConfig)

	EnvWaitForEqualTablesWithNames(env, s, "initial snapshot landed", srcTable, dstTable, "id,val")

	// batch A: synced and checkpointed before the pause.
	bqInsertRows(ctx, t, source, tableFQN, []bqCdcRow{{ID: 2, Val: "batch-a"}})
	EnvWaitFor(t, env, 4*time.Minute, "batch A picked up before pause", func() bool {
		rows, err := s.GetRows(dstTable, "id,val")
		if err != nil {
			t.Log(err)
			return false
		}
		return len(rows.Records) == 2
	})

	pool, err := catalogTestAccessPool()
	require.NoError(t, err)
	checkpointBeforePause := readBigQueryTableCursor(t, pool, flowConnConfig.FlowJobName, sourceTableIdentifier)
	require.False(t, checkpointBeforePause.IsZero(), "checkpoint should be persisted after batch A lands")

	SignalWorkflow(ctx, env, model.FlowSignal, model.PauseSignal)
	EnvWaitFor(t, env, 1*time.Minute, "paused workflow", func() bool {
		return env.GetFlowStatus(t) == protos.FlowStatus_STATUS_PAUSED
	})

	checkpointAtPause := readBigQueryTableCursor(t, pool, flowConnConfig.FlowJobName, sourceTableIdentifier)

	// batch B: written while the mirror isn't polling - must not be lost.
	bqInsertRows(ctx, t, source, tableFQN, []bqCdcRow{{ID: 3, Val: "batch-b"}})

	// checkpoint must not move while paused - nothing is being scanned.
	require.Equal(t, checkpointAtPause, readBigQueryTableCursor(t, pool, flowConnConfig.FlowJobName, sourceTableIdentifier),
		"checkpoint should stay put while the mirror is paused")

	// CDCDynamicPropertiesSignal auto-unpauses regardless of whether it
	// carries any actual property changes (see addCdcPropertiesSignalListener
	// in workflows/cdc_flow.go) - same resume mechanism
	// Test_Mongo_Can_Resume_After_Delete_Table uses.
	SignalWorkflow(ctx, env, model.CDCDynamicPropertiesSignal, &protos.CDCFlowConfigUpdate{})
	EnvWaitFor(t, env, 1*time.Minute, "resumed workflow", func() bool {
		return env.GetFlowStatus(t) == protos.FlowStatus_STATUS_RUNNING
	})

	EnvWaitFor(t, env, 4*time.Minute, "batch B picked up after resume", func() bool {
		rows, err := s.GetRows(dstTable, "id,val")
		if err != nil {
			t.Log(err)
			return false
		}
		return len(rows.Records) == 3
	})
	// exact-once delivery: if the resumed pull had re-scanned from scratch or
	// duplicated batch A, this full-table comparison against the source would
	// catch it; if batch B had been dropped, the row count check above would
	// already have timed out.
	RequireEqualTablesWithNames(s, srcTable, dstTable, "id,val")

	require.True(t, readBigQueryTableCursor(t, pool, flowConnConfig.FlowJobName, sourceTableIdentifier).After(checkpointAtPause),
		"checkpoint should have advanced past batch B after resuming")

	env.Cancel(ctx)
	RequireEnvCanceled(t, env)
}

// Test_BigQuery_CDC_Isolated_Table_Failure_Does_Not_Block_Sibling drops one
// source table mid-CDC to force isolatedTablePullSyncLoop's poll-failure path
// (flow/activities/flowable_isolated_cdc.go) and proves a sibling table keeps
// replicating unaffected.
func (s BigQueryClickhouseSuite) Test_BigQuery_CDC_Isolated_Table_Failure_Does_Not_Block_Sibling() {
	t := s.T()
	ctx := t.Context()

	source := s.Source().(*bigQuerySource)
	failedSrc := AddSuffix(s, "cdc_isolation_failed")
	healthySrc := AddSuffix(s, "cdc_isolation_healthy")
	failedDst := failedSrc + "_dst"
	healthyDst := healthySrc + "_dst"
	failedFQN := createBigQueryCdcSourceTable(ctx, t, source, failedSrc, false)
	healthyFQN := createBigQueryCdcSourceTable(ctx, t, source, healthySrc, false)
	failedSourceID := fmt.Sprintf("%s.%s", source.config.DatasetId, failedSrc)

	bqInsertRows(ctx, t, source, failedFQN, []bqCdcRow{{ID: 1, Val: "failed-initial"}})
	bqInsertRows(ctx, t, source, healthyFQN, []bqCdcRow{{ID: 1, Val: "healthy-initial"}})

	appends := protos.BigqueryCdcEventsFunction_BIGQUERY_CDC_EVENTS_FUNCTION_APPENDS
	flowConnConfig := bqCdcFlowConnectionConfig(s, failedSrc, failedDst, appends)
	flowConnConfig.TableMappings = append(flowConnConfig.TableMappings, &protos.TableMapping{
		SourceTableIdentifier:      fmt.Sprintf("%s.%s", source.config.DatasetId, healthySrc),
		DestinationTableIdentifier: s.DestinationTable(healthyDst),
		BigqueryCdcEventsFunction:  appends,
	})

	tc := NewTemporalClient(t)
	env := ExecutePeerflow(t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(t, env, flowConnConfig)

	EnvWaitForEqualTablesWithNames(env, s, "failed table initial snapshot landed", failedSrc, failedDst, "id,val")
	EnvWaitForEqualTablesWithNames(env, s, "healthy table initial snapshot landed", healthySrc, healthyDst, "id,val")

	pool, err := catalogTestAccessPool()
	require.NoError(t, err)
	stateBeforeDrop, err := queryBigQueryTableReplicationState(ctx, pool, flowConnConfig.FlowJobName, failedSourceID)
	require.NoError(t, err)

	require.NoError(t, source.Exec(ctx, "DROP TABLE "+quoteBigQueryTableFQN(failedFQN)),
		"should drop the failing source table")

	EnvWaitFor(t, env, 2*time.Minute, "failed table keeps retrying its poll after its source table is dropped", func() bool {
		state, err := queryBigQueryTableReplicationState(ctx, pool, flowConnConfig.FlowJobName, failedSourceID)
		if err != nil {
			t.Log(err)
			return false
		}
		return state.LastAttemptAt.After(stateBeforeDrop.LastAttemptAt)
	})

	// healthy sibling keeps advancing while the failed table just retries.
	bqInsertRows(ctx, t, source, healthyFQN, []bqCdcRow{{ID: 2, Val: "healthy-after-sibling-failure"}})
	EnvWaitForEqualTablesWithNames(env, s, "healthy row synced despite failed sibling", healthySrc, healthyDst, "id,val")

	require.Equal(t, protos.FlowStatus_STATUS_RUNNING, env.GetFlowStatus(t))

	stateAfterRetries, err := queryBigQueryTableReplicationState(ctx, pool, flowConnConfig.FlowJobName, failedSourceID)
	require.NoError(t, err)
	require.Equal(t, stateBeforeDrop.CursorText, stateAfterRetries.CursorText,
		"failed table's cursor must not move once its source table is gone")
	require.Equal(t, stateBeforeDrop.SyncedBatchID, stateAfterRetries.SyncedBatchID,
		"failed table must not advance its synced batch id")

	errorCount, err := GetLogCount(ctx, shared.CatalogPool{Pool: pool}, flowConnConfig.FlowJobName, "error",
		failedSourceID+"; replication for other tables will continue")
	require.NoError(t, err)
	require.Equal(t, 1, errorCount,
		"the customer-visible error should be emitted once for the table failure episode")

	env.Cancel(ctx)
	RequireEnvCanceled(t, env)
}

// Test_BigQuery_CDC_Isolated_Table_Backpressure_Does_Not_Block_Sibling renames
// away one table's ClickHouse destination so its own NormalizeTableCDC always
// fails, then proves that table's own sync loop backpressures at
// normBufferSize (flow/activities/flowable_isolated_cdc.go) without slowing
// down a healthy sibling table at all.
func (s BigQueryClickhouseSuite) Test_BigQuery_CDC_Isolated_Table_Backpressure_Does_Not_Block_Sibling() {
	t := s.T()
	ctx := t.Context()

	source := s.Source().(*bigQuerySource)
	stuckSrc := AddSuffix(s, "cdc_backpressure_stuck")
	healthySrc := AddSuffix(s, "cdc_backpressure_healthy")
	stuckDst := stuckSrc + "_dst"
	healthyDst := healthySrc + "_dst"
	stuckFQN := createBigQueryCdcSourceTable(ctx, t, source, stuckSrc, false)
	healthyFQN := createBigQueryCdcSourceTable(ctx, t, source, healthySrc, false)
	stuckSourceID := fmt.Sprintf("%s.%s", source.config.DatasetId, stuckSrc)

	bqInsertRows(ctx, t, source, stuckFQN, []bqCdcRow{{ID: 1, Val: "stuck-initial"}})
	bqInsertRows(ctx, t, source, healthyFQN, []bqCdcRow{{ID: 1, Val: "healthy-initial"}})

	appends := protos.BigqueryCdcEventsFunction_BIGQUERY_CDC_EVENTS_FUNCTION_APPENDS
	flowConnConfig := bqCdcFlowConnectionConfig(s, stuckSrc, stuckDst, appends)
	flowConnConfig.TableMappings = append(flowConnConfig.TableMappings, &protos.TableMapping{
		SourceTableIdentifier:      fmt.Sprintf("%s.%s", source.config.DatasetId, healthySrc),
		DestinationTableIdentifier: s.DestinationTable(healthyDst),
		BigqueryCdcEventsFunction:  appends,
	})
	// normBufferSize = max(normBufferHours*3600/idleTimeout, 2), so this floors
	// the backpressure threshold at 2 synced-but-unnormalized batches.
	flowConnConfig.Env["PEERDB_NORMALIZE_BUFFER_HOURS"] = "0"

	tc := NewTemporalClient(t)
	env := ExecutePeerflow(t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(t, env, flowConnConfig)

	EnvWaitForEqualTablesWithNames(env, s, "stuck table initial snapshot landed", stuckSrc, stuckDst, "id,val")
	EnvWaitForEqualTablesWithNames(env, s, "healthy table initial snapshot landed", healthySrc, healthyDst, "id,val")

	ch, err := connclickhouse.Connect(ctx, nil, s.Peer().GetClickhouseConfig())
	require.NoError(t, err)
	t.Cleanup(func() { ch.Close() })
	brokenDst := stuckDst + "_renamed_away"
	require.NoError(t, ch.Exec(ctx, fmt.Sprintf("RENAME TABLE `%s` TO `%s`", stuckDst, brokenDst)),
		"should rename the stuck table's destination to force normalize failures")

	pool, err := catalogTestAccessPool()
	require.NoError(t, err)

	bqInsertRows(ctx, t, source, stuckFQN, []bqCdcRow{{ID: 2, Val: "wave-1"}})
	EnvWaitFor(t, env, 2*time.Minute, "stuck table stages its first CDC batch", func() bool {
		state, err := queryBigQueryTableReplicationState(ctx, pool, flowConnConfig.FlowJobName, stuckSourceID)
		if err != nil {
			t.Log(err)
			return false
		}
		return state.SyncedBatchID >= 1
	})

	bqInsertRows(ctx, t, source, stuckFQN, []bqCdcRow{{ID: 3, Val: "wave-2"}})
	EnvWaitFor(t, env, 2*time.Minute, "stuck table's own backpressure caps its sync/normalize gap at 2", func() bool {
		state, err := queryBigQueryTableReplicationState(ctx, pool, flowConnConfig.FlowJobName, stuckSourceID)
		if err != nil {
			t.Log(err)
			return false
		}
		return state.SyncedBatchID == 2 && state.NormalizedBatchID == 0
	})

	// a third wave must not get synced while backpressured - the cap must hold.
	bqInsertRows(ctx, t, source, stuckFQN, []bqCdcRow{{ID: 4, Val: "wave-3"}})
	require.Never(t, func() bool {
		state, err := queryBigQueryTableReplicationState(ctx, pool, flowConnConfig.FlowJobName, stuckSourceID)
		return err == nil && state.SyncedBatchID > 2
	}, 15*time.Second, time.Second,
		"backpressured table must not sync past its own normalize buffer while normalize keeps failing")

	// healthy sibling is unaffected by the stuck table's backpressure.
	bqInsertRows(ctx, t, source, healthyFQN, []bqCdcRow{{ID: 2, Val: "healthy-wave-1"}})
	EnvWaitForEqualTablesWithNames(env, s, "healthy row synced despite stuck sibling", healthySrc, healthyDst, "id,val")
	bqInsertRows(ctx, t, source, healthyFQN, []bqCdcRow{{ID: 3, Val: "healthy-wave-2"}})
	EnvWaitForEqualTablesWithNames(env, s, "second healthy row synced despite stuck sibling", healthySrc, healthyDst, "id,val")

	require.Equal(t, protos.FlowStatus_STATUS_RUNNING, env.GetFlowStatus(t))

	errorCount, err := GetLogCount(ctx, shared.CatalogPool{Pool: pool}, flowConnConfig.FlowJobName, "error",
		stuckSourceID+"; replication for other tables will continue")
	require.NoError(t, err)
	require.Equal(t, 1, errorCount,
		"the customer-visible normalize-failure error should be emitted once for the backpressure episode")

	env.Cancel(ctx)
	RequireEnvCanceled(t, env)
}

// Test_BigQuery_CDC_Isolated_Table_Removal_Mid_CDC runs CDC on two tables,
// pauses the mirror, removes one table from the mirror config
// and checks that: the removed table's per-table state is pruned, it stops receiving updates, the retained
// table is unaffected, and the CDC batches API keeps working correctly.
func (s BigQueryClickhouseSuite) Test_BigQuery_CDC_Isolated_Table_Removal_Mid_CDC() {
	t := s.T()
	ctx := t.Context()

	source := s.Source().(*bigQuerySource)
	retainedSrc := AddSuffix(s, "cdc_isolated_remove_retained")
	removedSrc := AddSuffix(s, "cdc_isolated_remove_removed")
	retainedDst := retainedSrc + "_dst"
	removedDst := removedSrc + "_dst"
	retainedFQN := createBigQueryCdcSourceTable(ctx, t, source, retainedSrc, false)
	removedFQN := createBigQueryCdcSourceTable(ctx, t, source, removedSrc, false)
	removedSourceID := fmt.Sprintf("%s.%s", source.config.DatasetId, removedSrc)

	bqInsertRows(ctx, t, source, retainedFQN, []bqCdcRow{{ID: 1, Val: "retained-initial"}})
	bqInsertRows(ctx, t, source, removedFQN, []bqCdcRow{{ID: 1, Val: "removed-initial"}})

	appends := protos.BigqueryCdcEventsFunction_BIGQUERY_CDC_EVENTS_FUNCTION_APPENDS
	flowConnConfig := bqCdcFlowConnectionConfig(s, retainedSrc, retainedDst, appends)
	removedMapping := &protos.TableMapping{
		SourceTableIdentifier:      removedSourceID,
		DestinationTableIdentifier: s.DestinationTable(removedDst),
		BigqueryCdcEventsFunction:  appends,
	}
	flowConnConfig.TableMappings = append(flowConnConfig.TableMappings, removedMapping)

	tc := NewTemporalClient(t)
	env := ExecutePeerflow(t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(t, env, flowConnConfig)

	EnvWaitForEqualTablesWithNames(env, s, "retained table initial snapshot landed", retainedSrc, retainedDst, "id,val")
	EnvWaitForEqualTablesWithNames(env, s, "removed table initial snapshot landed", removedSrc, removedDst, "id,val")

	// cdc for a while: both tables get a wave of changes before the removal.
	bqInsertRows(ctx, t, source, retainedFQN, []bqCdcRow{{ID: 2, Val: "retained-cdc-1"}})
	bqInsertRows(ctx, t, source, removedFQN, []bqCdcRow{{ID: 2, Val: "removed-cdc-1"}})
	EnvWaitForEqualTablesWithNames(env, s, "retained table CDC wave landed", retainedSrc, retainedDst, "id,val")
	EnvWaitForEqualTablesWithNames(env, s, "removed table CDC wave landed", removedSrc, removedDst, "id,val")

	apiClient, err := NewApiClient()
	require.NoError(t, err)
	pool, err := catalogTestAccessPool()
	require.NoError(t, err)

	batchesBeforeRemoval, err := apiClient.GetCDCBatches(ctx, &protos.GetCDCBatchesRequest{
		FlowJobName: flowConnConfig.FlowJobName, Limit: 1,
	})
	require.NoError(t, err)
	require.NotEmpty(t, batchesBeforeRemoval.CdcBatches, "at least one CDC batch should be recorded before removal")
	maxBatchIDBeforeRemoval := batchesBeforeRemoval.CdcBatches[0].BatchId

	SignalWorkflow(ctx, env, model.FlowSignal, model.PauseSignal)
	EnvWaitFor(t, env, 1*time.Minute, "paused workflow", func() bool {
		return env.GetFlowStatus(t) == protos.FlowStatus_STATUS_PAUSED
	})

	// CDCDynamicPropertiesSignal auto-unpauses regardless of payload
	SignalWorkflow(ctx, env, model.CDCDynamicPropertiesSignal, &protos.CDCFlowConfigUpdate{
		RemovedTables: []*protos.TableMapping{removedMapping},
	})
	EnvWaitFor(t, env, 1*time.Minute, "resumed workflow after table removal", func() bool {
		return env.GetFlowStatus(t) == protos.FlowStatus_STATUS_RUNNING
	})

	EnvWaitFor(t, env, 2*time.Minute, "removed table's replication state is pruned", func() bool {
		exists, err := bigQueryTableReplicationStateExists(ctx, pool, flowConnConfig.FlowJobName, removedSourceID)
		if err != nil {
			t.Log(err)
			return false
		}
		return !exists
	})

	// retained table keeps replicating after the removal.
	bqInsertRows(ctx, t, source, retainedFQN, []bqCdcRow{{ID: 3, Val: "retained-cdc-2"}})
	EnvWaitForEqualTablesWithNames(env, s, "retained table CDC continues after removal", retainedSrc, retainedDst, "id,val")

	// removed table's source keeps changing, but the mirror must no longer pick it up.
	bqInsertRows(ctx, t, source, removedFQN, []bqCdcRow{{ID: 3, Val: "removed-after-removal"}})
	require.Never(t, func() bool {
		rows, err := s.GetRows(removedDst, "id,val")
		return err == nil && len(rows.Records) > 2
	}, 20*time.Second, 2*time.Second,
		"removed table must stop receiving CDC updates once dropped from the mirror")

	// GetCDCBatches keeps working after a table removal - new batches from the
	// retained table's continued sync still show up.
	EnvWaitFor(t, env, 2*time.Minute, "CDC batches handler reports new batches from the retained table after removal", func() bool {
		response, err := apiClient.GetCDCBatches(ctx, &protos.GetCDCBatchesRequest{
			FlowJobName: flowConnConfig.FlowJobName, Limit: 1,
		})
		if err != nil {
			t.Log(err)
			return false
		}
		return len(response.CdcBatches) == 1 && response.CdcBatches[0].BatchId > maxBatchIDBeforeRemoval &&
			response.CdcBatches[0].NumRows > 0 && response.CdcBatches[0].EndTime != nil
	})

	require.Equal(t, protos.FlowStatus_STATUS_RUNNING, env.GetFlowStatus(t))

	env.Cancel(ctx)
	RequireEnvCanceled(t, env)
}

func readBigQueryTableCursor(t *testing.T, pool *pgxpool.Pool, flowJobName string, sourceTableIdentifier string) time.Time {
	t.Helper()
	var cursorText string
	require.NoError(t, pool.QueryRow(
		t.Context(),
		"SELECT cursor_text FROM cdc_table_replication_state WHERE flow_name = $1 AND source_table_identifier = $2",
		flowJobName, sourceTableIdentifier,
	).Scan(&cursorText))
	cursor, err := time.Parse(time.RFC3339Nano, cursorText)
	require.NoError(t, err, "cursor_text should be a valid RFC3339Nano timestamp")
	return cursor
}

func queryBigQueryTableReplicationState(
	ctx context.Context, pool *pgxpool.Pool, flowJobName string, sourceTableIdentifier string,
) (connmetadata.TableReplicationState, error) {
	pgMetadata := connmetadata.NewPostgresMetadataFromCatalog(internal.LoggerFromCtx(ctx), shared.CatalogPool{Pool: pool})
	return pgMetadata.GetTableReplicationState(ctx, flowJobName, sourceTableIdentifier)
}

func bigQueryTableReplicationStateExists(
	ctx context.Context, pool *pgxpool.Pool, flowJobName string, sourceTableIdentifier string,
) (bool, error) {
	var exists bool
	err := pool.QueryRow(ctx,
		"SELECT EXISTS(SELECT 1 FROM cdc_table_replication_state WHERE flow_name = $1 AND source_table_identifier = $2)",
		flowJobName, sourceTableIdentifier,
	).Scan(&exists)
	return exists, err
}
