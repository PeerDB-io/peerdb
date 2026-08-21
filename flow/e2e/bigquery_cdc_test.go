package e2e

import (
	"context"
	"encoding/json"
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
	flowConnConfig.Env = map[string]string{"PEERDB_BIGQUERY_CDC_SAFETY_LAG_SECONDS": "5"}
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

// Test_BigQuery_CDC_Table_Failure_Does_Not_Block_Sibling drops the first
// source table in poll order, waits until its checkpoint records a failed
// window, then proves a later healthy table can still advance through CDC.
func (s BigQueryClickhouseSuite) Test_BigQuery_CDC_Table_Failure_Does_Not_Block_Sibling() {
	t := s.T()
	ctx := t.Context()

	source := s.Source().(*bigQuerySource)
	failedSrc := AddSuffix(s, "a_cdc_isolation_failed")
	healthySrc := AddSuffix(s, "z_cdc_isolation_healthy")
	failedDst := failedSrc + "_dst"
	healthyDst := healthySrc + "_dst"
	failedFQN := createBigQueryCdcSourceTable(ctx, t, source, failedSrc, false)
	healthyFQN := createBigQueryCdcSourceTable(ctx, t, source, healthySrc, false)
	failedSourceID := fmt.Sprintf("%s.%s", source.config.DatasetId, failedSrc)
	healthySourceID := fmt.Sprintf("%s.%s", source.config.DatasetId, healthySrc)
	require.Less(t, failedSourceID, healthySourceID, "failed table must be polled before its healthy sibling")

	bqInsertRows(ctx, t, source, failedFQN, []bqCdcRow{{ID: 1, Val: "failed-initial"}})
	bqInsertRows(ctx, t, source, healthyFQN, []bqCdcRow{{ID: 1, Val: "healthy-initial"}})

	appends := protos.BigqueryCdcEventsFunction_BIGQUERY_CDC_EVENTS_FUNCTION_APPENDS
	flowConnConfig := bqCdcFlowConnectionConfig(s, failedSrc, failedDst, appends)
	flowConnConfig.TableMappings = append(flowConnConfig.TableMappings, &protos.TableMapping{
		SourceTableIdentifier:      healthySourceID,
		DestinationTableIdentifier: s.DestinationTable(healthyDst),
		BigqueryCdcEventsFunction:  appends,
	})
	flowConnConfig.IdleTimeoutSeconds = 5
	flowConnConfig.Env = map[string]string{"PEERDB_BIGQUERY_CDC_SAFETY_LAG_SECONDS": "1"}

	tc := NewTemporalClient(t)
	env := ExecutePeerflow(t, tc, flowConnConfig)
	t.Cleanup(func() { env.Cancel(context.Background()) })
	SetupCDCFlowStatusQuery(t, env, flowConnConfig)

	EnvWaitForEqualTablesWithNames(env, s, "failed table initial snapshot landed", failedSrc, failedDst, "id,val")
	EnvWaitForEqualTablesWithNames(env, s, "healthy table initial snapshot landed", healthySrc, healthyDst, "id,val")

	pool, err := catalogTestAccessPool()
	require.NoError(t, err)
	require.NoError(t, source.client.DatasetInProject(
		source.config.ProjectId, source.config.DatasetId,
	).Table(failedSrc).Delete(ctx), "should drop the failing source table")

	var checkpointAtFailure bigQueryCDCTestCheckpoint
	EnvWaitFor(t, env, 2*time.Minute, "failed table checkpoint lags its attempted target", func() bool {
		checkpointText, err := queryBigQueryCheckpointText(ctx, pool, flowConnConfig.FlowJobName)
		if err != nil {
			t.Log(err)
			return false
		}
		checkpoint, err := decodeBigQueryCDCTestCheckpoint(checkpointText)
		if err != nil {
			if strings.HasPrefix(strings.TrimSpace(checkpointText), "{") {
				t.Log(err)
			}
			return false
		}
		failedProgress, failedOK := checkpoint.Tables[failedSourceID]
		_, healthyOK := checkpoint.Tables[healthySourceID]
		if !failedOK || !healthyOK || !failedProgress.Target.After(failedProgress.SyncedThrough) {
			return false
		}
		checkpointAtFailure = checkpoint
		return true
	})
	require.Equal(t, protos.FlowStatus_STATUS_RUNNING, env.GetFlowStatus(t))

	bqInsertRows(ctx, t, source, healthyFQN, []bqCdcRow{{ID: 2, Val: "healthy-after-sibling-failure"}})
	EnvWaitFor(t, env, 4*time.Minute, "healthy CDC row synced despite failed sibling", func() bool {
		rows, err := s.GetRows(healthyDst, "id,val")
		if err != nil {
			t.Log(err)
			return false
		}
		for _, record := range rows.Records {
			if record[0].Value() == int64(2) && record[1].Value() == "healthy-after-sibling-failure" {
				return true
			}
		}
		return false
	})
	RequireEqualTablesWithNames(s, healthySrc, healthyDst, "id,val")

	var latestCheckpoint bigQueryCDCTestCheckpoint
	EnvWaitFor(t, env, 2*time.Minute, "healthy checkpoint advances while failed checkpoint stays put", func() bool {
		checkpointText, err := queryBigQueryCheckpointText(ctx, pool, flowConnConfig.FlowJobName)
		if err != nil {
			t.Log(err)
			return false
		}
		checkpoint, err := decodeBigQueryCDCTestCheckpoint(checkpointText)
		if err != nil {
			t.Log(err)
			return false
		}
		failedProgress, failedOK := checkpoint.Tables[failedSourceID]
		healthyProgress, healthyOK := checkpoint.Tables[healthySourceID]
		if !failedOK || !healthyOK ||
			failedProgress.SyncedThrough != checkpointAtFailure.Tables[failedSourceID].SyncedThrough ||
			!failedProgress.Target.After(failedProgress.SyncedThrough) ||
			!failedProgress.Target.After(checkpointAtFailure.Tables[failedSourceID].Target) ||
			!healthyProgress.SyncedThrough.After(checkpointAtFailure.Tables[healthySourceID].SyncedThrough) ||
			healthyProgress.Target != healthyProgress.SyncedThrough {
			return false
		}
		latestCheckpoint = checkpoint
		return true
	})
	require.Equal(t,
		checkpointAtFailure.Tables[failedSourceID].SyncedThrough,
		latestCheckpoint.Tables[failedSourceID].SyncedThrough,
		"failed table must retain its last confirmed checkpoint",
	)
	require.True(t,
		latestCheckpoint.Tables[failedSourceID].Target.After(latestCheckpoint.Tables[failedSourceID].SyncedThrough),
		"failed table must remain behind its attempted target",
	)
	require.True(t,
		latestCheckpoint.Tables[healthySourceID].SyncedThrough.After(
			checkpointAtFailure.Tables[healthySourceID].SyncedThrough,
		),
		"healthy table checkpoint must advance after the sibling failure",
	)
	require.Equal(t,
		latestCheckpoint.Tables[healthySourceID].Target,
		latestCheckpoint.Tables[healthySourceID].SyncedThrough,
		"healthy table must complete its latest attempted window",
	)
	require.Equal(t, protos.FlowStatus_STATUS_RUNNING, env.GetFlowStatus(t))
	var customerErrorCount int
	require.NoError(t, pool.QueryRow(ctx, `SELECT COUNT(*)
		FROM peerdb_stats.flow_errors
		WHERE flow_name = $1
			AND error_type = 'error'
			AND position($2 in error_message) > 0
			AND position('replication for other tables will continue' in error_message) > 0`,
		flowConnConfig.FlowJobName, failedSourceID,
	).Scan(&customerErrorCount))
	require.Equal(t, 1, customerErrorCount,
		"the customer-visible error should be emitted once for the table failure episode")

	apiClient, err := NewApiClient()
	require.NoError(t, err)
	EnvWaitFor(t, env, 2*time.Minute, "CDC batch reports the dropped table as lagging", func() bool {
		response, err := apiClient.GetCDCBatches(ctx, &protos.GetCDCBatchesRequest{
			FlowJobName: flowConnConfig.FlowJobName,
			Limit:       10,
		})
		if err != nil {
			t.Log(err)
			return false
		}
		for _, batch := range response.CdcBatches {
			if batch.Status == protos.CDCBatchStatus_CDC_BATCH_STATUS_PARTIAL &&
				batch.TablesCompleted == 1 && batch.TablesTotal == 2 &&
				len(batch.LaggingTables) == 1 && batch.LaggingTables[0] == failedSourceID {
				return true
			}
		}
		return false
	})

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

	// batch A: synced before the pause.
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

	SignalWorkflow(ctx, env, model.FlowSignal, model.PauseSignal)
	EnvWaitFor(t, env, 1*time.Minute, "paused workflow", func() bool {
		return env.GetFlowStatus(t) == protos.FlowStatus_STATUS_PAUSED
	})
	// The destination row can become visible before the source checkpoint is
	// persisted. Read the baseline only after PAUSED confirms the in-flight sync
	// activity has stopped.
	checkpointBeforePause := readBigQueryCheckpointText(t, pool, flowConnConfig.FlowJobName)
	require.NotEmpty(t, checkpointBeforePause, "checkpoint should be persisted after batch A lands")
	checkpointTimeBeforePause := bigQueryTableCheckpointTime(
		t, checkpointBeforePause, fmt.Sprintf("%s.%s", source.config.DatasetId, srcTable),
	)

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

	EnvWaitFor(t, env, 1*time.Minute, "checkpoint advanced past batch B after resuming", func() bool {
		checkpointAfterPause := readBigQueryCheckpointText(t, pool, flowConnConfig.FlowJobName)
		return bigQueryTableCheckpointTime(
			t, checkpointAfterPause, fmt.Sprintf("%s.%s", source.config.DatasetId, srcTable),
		).After(checkpointTimeBeforePause)
	})

	env.Cancel(ctx)
	RequireEnvCanceled(t, env)
}

// Test_BigQuery_Source_CDC_Table_Removal_Drops_Checkpoint removes table from mirror mid cdc
// and checks that the checkpoint is dropped for the removed table.
// The other table should continue to replicate without issue.
func (s BigQueryClickhouseSuite) Test_BigQuery_Source_CDC_Table_Removal_Drops_Checkpoint() {
	t := s.T()
	ctx := t.Context()
	source := s.Source().(*bigQuerySource)

	retainedSrc := AddSuffix(s, "cdc_remove_retained")
	removedSrc := AddSuffix(s, "cdc_remove_removed")
	retainedDst := retainedSrc + "_dst"
	removedDst := removedSrc + "_dst"
	retainedFQN := createBigQueryCdcSourceTable(ctx, t, source, retainedSrc, false)
	removedFQN := createBigQueryCdcSourceTable(ctx, t, source, removedSrc, false)

	bqInsertRows(ctx, t, source, retainedFQN, []bqCdcRow{{ID: 1, Val: "retained-snapshot"}})
	bqInsertRows(ctx, t, source, removedFQN, []bqCdcRow{{ID: 1, Val: "removed-snapshot"}})

	appends := protos.BigqueryCdcEventsFunction_BIGQUERY_CDC_EVENTS_FUNCTION_APPENDS
	flowConnConfig := bqCdcFlowConnectionConfig(s, retainedSrc, retainedDst, appends)
	retainedMapping := flowConnConfig.TableMappings[0]
	removedMapping := &protos.TableMapping{
		SourceTableIdentifier:      fmt.Sprintf("%s.%s", source.config.DatasetId, removedSrc),
		DestinationTableIdentifier: s.DestinationTable(removedDst),
		BigqueryCdcEventsFunction:  appends,
	}
	flowConnConfig.TableMappings = append(flowConnConfig.TableMappings, removedMapping)

	tc := NewTemporalClient(t)
	env := ExecutePeerflow(t, tc, flowConnConfig)
	t.Cleanup(func() { env.Cancel(context.Background()) })
	SetupCDCFlowStatusQuery(t, env, flowConnConfig)

	EnvWaitForEqualTablesWithNames(
		env, s, "retained table initial snapshot landed", retainedSrc, retainedDst, "id,val",
	)
	EnvWaitForEqualTablesWithNames(
		env, s, "removed table initial snapshot landed", removedSrc, removedDst, "id,val",
	)

	apiClient, err := NewApiClient()
	require.NoError(t, err)
	pool, err := catalogTestAccessPool()
	require.NoError(t, err)

	// Bound the batch search to work created below. This avoids accidentally
	// selecting an earlier row-bearing batch if setup behavior changes.
	var batchIDBeforeWave int64
	response, err := apiClient.GetCDCBatches(ctx, &protos.GetCDCBatchesRequest{
		FlowJobName: flowConnConfig.FlowJobName,
		Limit:       1,
	})
	require.NoError(t, err)
	if len(response.CdcBatches) != 0 {
		batchIDBeforeWave = response.CdcBatches[0].BatchId
	}

	_, err = apiClient.FlowStateChange(ctx, &protos.FlowStateChangeRequest{
		FlowJobName:        flowConnConfig.FlowJobName,
		RequestedFlowState: protos.FlowStatus_STATUS_PAUSED,
	})
	require.NoError(t, err)
	EnvWaitFor(t, env, 3*time.Minute, "mirror paused before two-table CDC batch", func() bool {
		return env.GetFlowStatus(t) == protos.FlowStatus_STATUS_PAUSED
	})

	// Stage both writes while polling is paused. BigQuery commits the two INSERT
	// jobs at different times, so they may land in one or two CDC batches.
	bqInsertRows(ctx, t, source, retainedFQN, []bqCdcRow{{ID: 2, Val: "retained-cdc"}})
	bqInsertRows(ctx, t, source, removedFQN, []bqCdcRow{{ID: 2, Val: "removed-cdc"}})
	_, err = apiClient.FlowStateChange(ctx, &protos.FlowStateChangeRequest{
		FlowJobName:        flowConnConfig.FlowJobName,
		RequestedFlowState: protos.FlowStatus_STATUS_RUNNING,
	})
	require.NoError(t, err)

	EnvWaitForEqualTablesWithNames(
		env, s, "retained table CDC row landed", retainedSrc, retainedDst, "id,val",
	)
	EnvWaitForEqualTablesWithNames(
		env, s, "removed table CDC row landed", removedSrc, removedDst, "id,val",
	)

	var historicalBatch *protos.CDCBatch
	EnvWaitFor(t, env, 3*time.Minute, "completed two-table CDC batch", func() bool {
		response, err := apiClient.GetCDCBatches(ctx, &protos.GetCDCBatchesRequest{
			FlowJobName: flowConnConfig.FlowJobName,
			Ascending:   true,
			BeforeId:    -1,
			AfterId:     batchIDBeforeWave,
		})
		if err != nil {
			t.Log(err)
			return false
		}
		for _, batch := range response.CdcBatches {
			if batch.NumRows > 0 && batch.EndTime != nil &&
				batch.Status == protos.CDCBatchStatus_CDC_BATCH_STATUS_COMPLETED &&
				batch.TablesCompleted == 2 && batch.TablesTotal == 2 && len(batch.LaggingTables) == 0 {
				historicalBatch = batch
				return true
			}
		}
		return false
	})
	require.NotNil(t, historicalBatch)
	require.Positive(t, historicalBatch.BatchId)
	require.Positive(t, historicalBatch.NumRows)
	require.NotNil(t, historicalBatch.EndTime)
	require.Equal(t, protos.CDCBatchStatus_CDC_BATCH_STATUS_COMPLETED, historicalBatch.Status)
	require.Equal(t, uint32(2), historicalBatch.TablesCompleted)
	require.Equal(t, uint32(2), historicalBatch.TablesTotal)
	require.Empty(t, historicalBatch.LaggingTables)

	checkpointText, err := queryBigQueryCheckpointText(ctx, pool, flowConnConfig.FlowJobName)
	require.NoError(t, err)
	checkpointBeforeRemoval, err := decodeBigQueryCDCTestCheckpoint(checkpointText)
	require.NoError(t, err)
	require.Contains(t, checkpointBeforeRemoval.Tables, retainedMapping.SourceTableIdentifier)
	require.Contains(t, checkpointBeforeRemoval.Tables, removedMapping.SourceTableIdentifier)

	_, err = apiClient.FlowStateChange(ctx, &protos.FlowStateChangeRequest{
		FlowJobName:        flowConnConfig.FlowJobName,
		RequestedFlowState: protos.FlowStatus_STATUS_PAUSED,
	})
	require.NoError(t, err)
	EnvWaitFor(t, env, 3*time.Minute, "mirror paused before table removal", func() bool {
		return env.GetFlowStatus(t) == protos.FlowStatus_STATUS_PAUSED
	})

	_, err = apiClient.FlowStateChange(ctx, &protos.FlowStateChangeRequest{
		FlowJobName:        flowConnConfig.FlowJobName,
		RequestedFlowState: protos.FlowStatus_STATUS_RUNNING,
		FlowConfigUpdate: &protos.FlowConfigUpdate{
			Update: &protos.FlowConfigUpdate_CdcFlowConfigUpdate{
				CdcFlowConfigUpdate: &protos.CDCFlowConfigUpdate{
					RemovedTables: []*protos.TableMapping{removedMapping},
				},
			},
		},
	})
	require.NoError(t, err)

	EnvWaitFor(t, env, 3*time.Minute, "latest checkpoint drops removed table", func() bool {
		if env.GetFlowStatus(t) != protos.FlowStatus_STATUS_RUNNING {
			return false
		}
		checkpointText, err := queryBigQueryCheckpointText(ctx, pool, flowConnConfig.FlowJobName)
		if err != nil {
			t.Log(err)
			return false
		}
		checkpoint, err := decodeBigQueryCDCTestCheckpoint(checkpointText)
		if err != nil {
			t.Log(err)
			return false
		}
		_, retained := checkpoint.Tables[retainedMapping.SourceTableIdentifier]
		_, removed := checkpoint.Tables[removedMapping.SourceTableIdentifier]
		return retained && !removed && len(checkpoint.Tables) == 1
	})

	checkpointText, err = queryBigQueryCheckpointText(ctx, pool, flowConnConfig.FlowJobName)
	require.NoError(t, err)
	checkpointAfterRemoval, err := decodeBigQueryCDCTestCheckpoint(checkpointText)
	require.NoError(t, err)
	require.Contains(t, checkpointAfterRemoval.Tables, retainedMapping.SourceTableIdentifier)
	require.NotContains(t, checkpointAfterRemoval.Tables, removedMapping.SourceTableIdentifier)
	require.Len(t, checkpointAfterRemoval.Tables, 1)

	historicalBatchID := historicalBatch.BatchId
	historicalBatchRows := historicalBatch.NumRows
	var recalculatedBatch *protos.CDCBatch
	EnvWaitFor(t, env, 2*time.Minute, "historical batch uses current table set", func() bool {
		response, err := apiClient.GetCDCBatches(ctx, &protos.GetCDCBatchesRequest{
			FlowJobName: flowConnConfig.FlowJobName,
			Limit:       1,
			Ascending:   true,
			BeforeId:    -1,
			AfterId:     historicalBatchID - 1,
		})
		if err != nil {
			t.Log(err)
			return false
		}
		if len(response.CdcBatches) != 1 || response.CdcBatches[0].BatchId != historicalBatchID {
			return false
		}
		batch := response.CdcBatches[0]
		recalculatedBatch = batch
		return batch.NumRows == historicalBatchRows && batch.EndTime != nil &&
			batch.Status == protos.CDCBatchStatus_CDC_BATCH_STATUS_COMPLETED &&
			batch.TablesCompleted == 1 && batch.TablesTotal == 1 && len(batch.LaggingTables) == 0
	})
	require.NotNil(t, recalculatedBatch)
	require.Equal(t, historicalBatchID, recalculatedBatch.BatchId)
	require.Equal(t, historicalBatchRows, recalculatedBatch.NumRows)
	require.NotNil(t, recalculatedBatch.EndTime)
	require.Equal(t, protos.CDCBatchStatus_CDC_BATCH_STATUS_COMPLETED, recalculatedBatch.Status)
	require.Equal(t, uint32(1), recalculatedBatch.TablesCompleted)
	require.Equal(t, uint32(1), recalculatedBatch.TablesTotal)
	require.Empty(t, recalculatedBatch.LaggingTables)

	env.Cancel(ctx)
	RequireEnvCanceled(t, env)
}

func readBigQueryCheckpointText(t *testing.T, pool *pgxpool.Pool, flowJobName string) string {
	t.Helper()
	lastText, err := queryBigQueryCheckpointText(t.Context(), pool, flowJobName)
	require.NoError(t, err)
	return lastText
}

func queryBigQueryCheckpointText(ctx context.Context, pool *pgxpool.Pool, flowJobName string) (string, error) {
	var lastText string
	err := pool.QueryRow(
		ctx, "SELECT last_text FROM metadata_last_sync_state WHERE job_name = $1", flowJobName,
	).Scan(&lastText)
	return lastText, err
}

type bigQueryCDCTestTableProgress struct {
	SyncedThrough time.Time `json:"synced_through"`
	Target        time.Time `json:"target"`
	SyncedBatchID int64     `json:"synced_batch_id"`
}

type bigQueryCDCTestCheckpoint struct {
	Tables  map[string]bigQueryCDCTestTableProgress `json:"tables"`
	Version int                                     `json:"version"`
}

func decodeBigQueryCDCTestCheckpoint(checkpointText string) (bigQueryCDCTestCheckpoint, error) {
	var checkpoint bigQueryCDCTestCheckpoint
	if err := json.Unmarshal([]byte(checkpointText), &checkpoint); err != nil {
		return bigQueryCDCTestCheckpoint{}, err
	}
	if checkpoint.Version != 1 {
		return bigQueryCDCTestCheckpoint{}, fmt.Errorf("unexpected BigQuery CDC checkpoint version %d", checkpoint.Version)
	}
	return checkpoint, nil
}

func bigQueryTableCheckpointTime(t *testing.T, checkpointText, sourceTable string) time.Time {
	t.Helper()
	checkpoint, err := decodeBigQueryCDCTestCheckpoint(checkpointText)
	require.NoError(t, err)
	tableCheckpoint, ok := checkpoint.Tables[sourceTable]
	require.True(t, ok, "checkpoint should contain source table %s", sourceTable)
	return tableCheckpoint.SyncedThrough
}
