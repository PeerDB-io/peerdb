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

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/model"
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
		"PEERDB_CDC_SAFETY_LAG_SECONDS": "5",
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
	flowConnConfig.Env = map[string]string{"PEERDB_CDC_SAFETY_LAG_SECONDS": "5"}

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
// persisted checkpoint (chunk 4's UpdateReplStateLastOffset/SetLastOffset)
// rather than re-scanning already-synced rows or dropping rows written while
// the mirror wasn't polling.
func (s BigQueryClickhouseSuite) Test_BigQuery_CDC_Restart_Mid_Window_Resume() {
	t := s.T()
	ctx := t.Context()

	source := s.Source().(*bigQuerySource)
	srcTable := AddSuffix(s, "cdc_restart_resume")
	dstTable := srcTable + "_dst"
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
	checkpointBeforePause := readBigQueryCheckpointText(t, pool, flowConnConfig.FlowJobName)
	require.NotEmpty(t, checkpointBeforePause, "checkpoint should be persisted after batch A lands")

	SignalWorkflow(ctx, env, model.FlowSignal, model.PauseSignal)
	EnvWaitFor(t, env, 1*time.Minute, "paused workflow", func() bool {
		return env.GetFlowStatus(t) == protos.FlowStatus_STATUS_PAUSED
	})

	// batch B: written while the mirror isn't polling - must not be lost.
	bqInsertRows(ctx, t, source, tableFQN, []bqCdcRow{{ID: 3, Val: "batch-b"}})

	// checkpoint must not move while paused - nothing is being scanned.
	require.Equal(t, checkpointBeforePause, readBigQueryCheckpointText(t, pool, flowConnConfig.FlowJobName),
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

	require.Greater(t, readBigQueryCheckpointText(t, pool, flowConnConfig.FlowJobName), checkpointBeforePause,
		"checkpoint should have advanced past batch B after resuming")

	env.Cancel(ctx)
	RequireEnvCanceled(t, env)
}

func readBigQueryCheckpointText(t *testing.T, pool *pgxpool.Pool, flowJobName string) string {
	t.Helper()
	var lastText string
	require.NoError(t, pool.QueryRow(
		t.Context(), "SELECT last_text FROM metadata_last_sync_state WHERE job_name = $1", flowJobName,
	).Scan(&lastText))
	return lastText
}
