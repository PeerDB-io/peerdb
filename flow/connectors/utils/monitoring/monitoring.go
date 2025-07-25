package monitoring

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/log"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/shared"
	shared_mysql "github.com/PeerDB-io/peerdb/flow/shared/mysql"
)

type CDCBatchInfo struct {
	StartTime   time.Time
	BatchID     int64
	BatchEndlSN int64
	RowsInBatch uint32
}

func InitializeCDCFlow(ctx context.Context, pool shared.CatalogPool, flowJobName string) error {
	if _, err := pool.Exec(ctx,
		`INSERT INTO peerdb_stats.cdc_flows(flow_name,latest_lsn_at_source,latest_lsn_at_target) VALUES($1,0,0) ON CONFLICT DO NOTHING`,
		flowJobName,
	); err != nil {
		return fmt.Errorf("error while inserting flow into cdc_flows: %w", err)
	}
	return nil
}

func UpdateLatestLSNAtSourceForCDCFlow(ctx context.Context, pool shared.CatalogPool, flowJobName string,
	latestLSNAtSource int64,
) error {
	if _, err := pool.Exec(ctx,
		"UPDATE peerdb_stats.cdc_flows SET latest_lsn_at_source=$1 WHERE flow_name=$2",
		uint64(latestLSNAtSource), flowJobName,
	); err != nil {
		return fmt.Errorf("[source] error while updating flow in cdc_flows: %w", err)
	}
	return nil
}

func UpdateLatestLSNAtTargetForCDCFlow(ctx context.Context, pool shared.CatalogPool, flowJobName string,
	latestLSNAtTarget int64,
) error {
	if _, err := pool.Exec(ctx,
		"UPDATE peerdb_stats.cdc_flows SET latest_lsn_at_target=$1 WHERE flow_name=$2",
		uint64(latestLSNAtTarget), flowJobName,
	); err != nil {
		return fmt.Errorf("[target] error while updating flow in cdc_flows: %w", err)
	}
	return nil
}

func AddCDCBatchForFlow(ctx context.Context, pool shared.CatalogPool, flowJobName string,
	batchInfo CDCBatchInfo,
) error {
	if _, err := pool.Exec(ctx,
		`INSERT INTO peerdb_stats.cdc_batches(flow_name,batch_id,rows_in_batch,batch_start_lsn,batch_end_lsn,
		start_time) VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT DO NOTHING`,
		flowJobName, batchInfo.BatchID, batchInfo.RowsInBatch, 0,
		uint64(batchInfo.BatchEndlSN), batchInfo.StartTime,
	); err != nil {
		return fmt.Errorf("error while inserting batch into cdc_batch: %w", err)
	}
	return nil
}

// update num records and end-lsn for a cdc batch
func UpdateNumRowsAndEndLSNForCDCBatch(
	ctx context.Context,
	pool shared.CatalogPool,
	flowJobName string,
	batchID int64,
	numRows uint32,
	batchEndCheckpoint model.CdcCheckpoint,
) error {
	if _, err := pool.Exec(ctx,
		"UPDATE peerdb_stats.cdc_batches SET rows_in_batch=$1,batch_end_lsn=$2,batch_end_lsn_text=$3 WHERE flow_name=$4 AND batch_id=$5",
		numRows, uint64(batchEndCheckpoint.ID), batchEndCheckpoint.Text, flowJobName, batchID,
	); err != nil {
		return fmt.Errorf("error while updating batch in cdc_batch: %w", err)
	}
	return nil
}

func UpdateEndTimeForCDCBatch(
	ctx context.Context,
	pool shared.CatalogPool,
	flowJobName string,
	batchID int64,
) error {
	if _, err := pool.Exec(ctx,
		`UPDATE peerdb_stats.cdc_batches
		SET end_time = NOW()
		WHERE flow_name = $1 AND batch_id <= $2 AND end_time IS NULL`,
		flowJobName, batchID,
	); err != nil {
		return fmt.Errorf("error while updating batch in cdc_batch: %w", err)
	}
	return nil
}

func AddCDCBatchTablesForFlow(ctx context.Context, pool shared.CatalogPool, flowJobName string,
	batchID int64, tableNameRowsMapping map[string]*model.RecordTypeCounts,
) error {
	insertBatchTablesTx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("error while beginning transaction for inserting statistics into cdc_batch_table: %w", err)
	}
	defer shared.RollbackTx(insertBatchTablesTx, internal.LoggerFromCtx(ctx))

	for destinationTableName, rowCounts := range tableNameRowsMapping {
		inserts := rowCounts.InsertCount.Load()
		updates := rowCounts.UpdateCount.Load()
		deletes := rowCounts.DeleteCount.Load()
		totalRows := inserts + updates + deletes

		// Insert into cdc_batch_table
		if _, err := insertBatchTablesTx.Exec(ctx,
			`INSERT INTO peerdb_stats.cdc_batch_table
			(flow_name,batch_id,destination_table_name,num_rows,
			insert_count,update_count,delete_count)
			 VALUES($1,$2,$3,$4,$5,$6,$7) ON CONFLICT DO NOTHING`,
			flowJobName, batchID, destinationTableName,
			totalRows, inserts, updates, deletes,
		); err != nil {
			return fmt.Errorf("error while inserting statistics into cdc_batch_table: %w", err)
		}
		// Update the aggregated counts table
		query := `
			INSERT INTO peerdb_stats.cdc_table_aggregate_counts AS stats (
				flow_name,
				destination_table_name,
				inserts_count,
				updates_count,
				deletes_count,
				total_count,
				latest_batch_id,
				last_updated_at
			)
			VALUES (
				$1,   -- flow_name
				$2,   -- destination_table_name
				$3,   -- inserts_count
				$4,   -- updates_count
				$5,   -- deletes_count
				$6,   -- total_count
				$7,   -- latest_batch_id
				NOW() -- last_updated_at
			)
			ON CONFLICT (flow_name, destination_table_name)
			DO UPDATE
			SET
				inserts_count     = stats.inserts_count + EXCLUDED.inserts_count,
				updates_count     = stats.updates_count + EXCLUDED.updates_count,
				deletes_count     = stats.deletes_count + EXCLUDED.deletes_count,
				total_count       = stats.total_count + EXCLUDED.total_count,
				latest_batch_id   = GREATEST(stats.latest_batch_id, EXCLUDED.latest_batch_id),
				last_updated_at   = NOW()
		`

		if _, err := insertBatchTablesTx.Exec(ctx, query,
			flowJobName,
			destinationTableName,
			inserts,
			updates,
			deletes,
			totalRows,
			batchID,
		); err != nil {
			return fmt.Errorf("error while updating aggregate statistics in cdc_table_aggregate_counts: %w", err)
		}
	}
	if err := insertBatchTablesTx.Commit(ctx); err != nil {
		return fmt.Errorf("error while committing transaction for inserting and updating statistics: %w", err)
	}
	return nil
}

func UpdateSyncStatusSuccess(
	ctx context.Context, pool shared.CatalogPool, flowJobName string, batchID int64,
) error {
	if _, err := pool.Exec(ctx, `
		UPDATE peerdb_stats.granular_status
		SET sync_succeeding = true, sync_is_internal_error = false,
			sync_last_successful_batch_id = $1, sync_updated_at = utc_now()
		WHERE flow_name = $2
		`, batchID, flowJobName,
	); err != nil {
		return fmt.Errorf("failed to update granular status for sync batch: %w", err)
	}
	return nil
}

func UpdateNormalizeStatusSuccess(
	ctx context.Context, pool shared.CatalogPool, flowJobName string, batchID int64,
) error {
	if _, err := pool.Exec(ctx, `
		UPDATE peerdb_stats.granular_status
		SET normalize_succeeding = true, normalize_is_internal_error = false,
			normalize_last_successful_batch_id = $1, normalize_updated_at = utc_now()
		WHERE flow_name = $2
		`, batchID, flowJobName,
	); err != nil {
		return fmt.Errorf("failed to update granular status for normalize batch: %w", err)
	}
	return nil
}

func UpdateSyncStatusError(
	ctx context.Context, pool shared.CatalogPool, flowJobName string, isInternalError bool,
) error {
	syncIsInternalError := "sync_is_internal_error" // don't modify the value
	if isInternalError {
		syncIsInternalError = "true"
	}
	if _, err := pool.Exec(ctx, `
		UPDATE peerdb_stats.granular_status
		SET sync_succeeding = false, sync_is_internal_error = `+syncIsInternalError+`,
			sync_updated_at = utc_now()
		WHERE flow_name = $1
		`, flowJobName,
	); err != nil {
		return fmt.Errorf("failed to update granular status for sync batch: %w", err)
	}
	return nil
}

func UpdateNormalizeStatusError(
	ctx context.Context, pool shared.CatalogPool, flowJobName string, isInternalError bool,
) error {
	normalizeIsInternalError := "normalize_is_internal_error" // don't modify the value
	if isInternalError {
		normalizeIsInternalError = "true"
	}
	if _, err := pool.Exec(ctx, `
		UPDATE peerdb_stats.granular_status
		SET normalize_succeeding = false, normalize_is_internal_error = `+normalizeIsInternalError+`,
			normalize_updated_at = utc_now()
		WHERE flow_name = $1
		`, flowJobName,
	); err != nil {
		return fmt.Errorf("failed to update granular status for normalize batch: %w", err)
	}
	return nil
}

func InitializeSnapshot(
	ctx context.Context,
	logger log.Logger,
	pool shared.CatalogPool,
	flowName string,
) (int32, error) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return -1, fmt.Errorf("error while starting transaction to initialize snapshot: %w", err)
	}
	defer shared.RollbackTx(tx, logger)

	row := tx.QueryRow(ctx,
		"INSERT INTO peerdb_stats.snapshots(flow_name,start_time)"+
			" VALUES($1,utc_now(),$2) ON CONFLICT DO NOTHING RETURNING snapshot_id",
		flowName)
	var snapshotId int32
	if err := row.Scan(&snapshotId); err != nil {
		return -1, fmt.Errorf("error while inserting snapshot in peerdb_stats: %w", err)
	}

	if _, err := tx.Exec(ctx,
		"UPDATE peerdb_stats.granular_status"+
			" SET snapshot_current_id = $1, snapshot_succeeding = true, snapshot_failing_qrep_run_ids = {},"+
			" snapshot_failing_partition_ids = {}, snapshot_is_internal_error = false,"+
			" snapshot_updated_at = utc_now()"+
			" WHERE flow_name = $2",
		snapshotId, flowName,
	); err != nil {
		return -1, fmt.Errorf("error while initializing granular status for snapshot: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return -1, fmt.Errorf("error while committing transaction to initialize snapshot: %w", err)
	}

	return snapshotId, nil
}

func FinishSnapshot(
	ctx context.Context,
	logger log.Logger,
	pool shared.CatalogPool,
	flowName string,
	snapshotID int32,
) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("error while starting transaction to finish snapshot: %w", err)
	}
	defer shared.RollbackTx(tx, logger)

	if _, err := tx.Exec(ctx,
		"UPDATE peerdb_stats.snapshots SET end_time = utc_now() where snapshot_id = $1",
		snapshotID,
	); err != nil {
		return fmt.Errorf("error while updating end time for snapshot: %w", err)
	}

	if _, err := pool.Exec(ctx, `
		UPDATE peerdb_stats.granular_status
		SET snapshot_succeeding = true, snapshot_failing_qrep_run_ids = {},
			snapshot_failing_partition_ids = {}, snapshot_is_internal_error = false,
			snapshot_updated_at = utc_now()
		WHERE flow_name = $1 and snapshot_current_id = $2
		`, flowName, snapshotID,
	); err != nil {
		return fmt.Errorf("error while updating granular status for snapshot: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("error while committing transaction to finish snapshot: %w", err)
	}

	return nil
}

func UpdateQRepStatusSuccess(
	ctx context.Context, pool shared.CatalogPool, flowJobName string, snapshotID int32, runUUID string,
) error {
	if _, err := pool.Exec(ctx, `
		UPDATE peerdb_stats.granular_status
		SET snapshot_failing_qrep_run_ids = array_remove(snapshot_failing_qrep_run_ids, $1),
			snapshot_updated_at = utc_now()
		WHERE flow_name = $2 and snapshot_current_id = $3
		`, runUUID, flowJobName, snapshotID,
	); err != nil {
		return fmt.Errorf("failed to update granular status for qrep run: %w", err)
	}
	return nil
}

func InitializeQRepRun(
	ctx context.Context,
	logger log.Logger,
	pool shared.CatalogPool,
	config *protos.QRepConfig,
	runUUID string,
	partitions []*protos.QRepPartition,
	parentMirrorName string,
) error {
	flowJobName := config.GetFlowJobName()
	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("error while starting transaction to initialize qrep run: %w", err)
	}
	defer shared.RollbackTx(tx, logger)

	if _, err := tx.Exec(ctx,
		"INSERT INTO peerdb_stats.qrep_runs(flow_name,run_uuid,source_table,destination_table,parent_mirror_name)"+
			" VALUES($1,$2,$3,$4,$5) ON CONFLICT DO NOTHING",
		flowJobName, runUUID, config.WatermarkTable, config.DestinationTableIdentifier, parentMirrorName,
	); err != nil {
		return fmt.Errorf("error while inserting qrep run in qrep_runs: %w", err)
	}

	for _, partition := range partitions {
		if err := addPartitionToQRepRun(ctx, tx, flowJobName, runUUID, partition, parentMirrorName); err != nil {
			return fmt.Errorf("unable to add partition to qrep run: %w", err)
		}
	}

	return tx.Commit(ctx)
}

func UpdateStartTimeForQRepRun(ctx context.Context, pool shared.CatalogPool, runUUID string) error {
	if _, err := pool.Exec(ctx,
		"UPDATE peerdb_stats.qrep_runs SET start_time=$1, fetch_complete=true WHERE run_uuid=$2",
		time.Now(), runUUID,
	); err != nil {
		return fmt.Errorf("error while updating start time for run_uuid %s in qrep_runs: %w", runUUID, err)
	}

	return nil
}

func UpdateEndTimeForQRepRun(ctx context.Context, pool shared.CatalogPool, runUUID string) error {
	if _, err := pool.Exec(ctx,
		"UPDATE peerdb_stats.qrep_runs SET end_time=$1, consolidate_complete=true WHERE run_uuid=$2",
		time.Now(), runUUID,
	); err != nil {
		return fmt.Errorf("error while updating end time for run_uuid %s in qrep_runs: %w", runUUID, err)
	}

	return nil
}

func AppendSlotSizeInfo(
	ctx context.Context,
	pool shared.CatalogPool,
	peerName string,
	slotInfo *protos.SlotInfo,
) error {
	if _, err := pool.Exec(ctx,
		"INSERT INTO peerdb_stats.peer_slot_size"+
			"(peer_name, slot_name, restart_lsn, redo_lsn, confirmed_flush_lsn, slot_size, wal_status) "+
			"VALUES($1,$2,$3,$4,$5,$6,$7) ON CONFLICT DO NOTHING;",
		peerName,
		slotInfo.SlotName,
		slotInfo.RestartLSN,
		slotInfo.RedoLSN,
		slotInfo.ConfirmedFlushLSN,
		slotInfo.LagInMb,
		slotInfo.WalStatus,
	); err != nil {
		return fmt.Errorf("error while upserting row for slot_size: %w", err)
	}

	return nil
}

func isMySQLFullTablePartition(partition *protos.QRepPartition) bool {
	if partition == nil {
		return false
	}
	return partition.PartitionId == shared_mysql.MYSQL_FULL_TABLE_PARTITION_ID
}

func addPartitionToQRepRun(ctx context.Context, tx pgx.Tx, flowJobName string,
	runUUID string, partition *protos.QRepPartition, parentMirrorName string,
) error {
	if partition == nil {
		internal.LoggerFromCtx(ctx).Info("cannot add nil partition to qrep run",
			slog.String(string(shared.FlowNameKey), parentMirrorName))
		return errors.New("cannot add nil partition to qrep run")
	}
	if partition.Range == nil && partition.FullTablePartition && !isMySQLFullTablePartition(partition) {
		internal.LoggerFromCtx(ctx).Info("partition "+partition.PartitionId+
			" is a full table partition. Metrics logging is skipped.",
			slog.String(string(shared.FlowNameKey), parentMirrorName))
		return nil
	}

	var rangeStart, rangeEnd string
	if partition.Range != nil {
		switch x := partition.Range.Range.(type) {
		case *protos.PartitionRange_IntRange:
			rangeStart = strconv.FormatInt(x.IntRange.Start, 10)
			rangeEnd = strconv.FormatInt(x.IntRange.End, 10)
		case *protos.PartitionRange_UintRange:
			rangeStart = strconv.FormatUint(x.UintRange.Start, 10)
			rangeEnd = strconv.FormatUint(x.UintRange.End, 10)
		case *protos.PartitionRange_TimestampRange:
			rangeStart = x.TimestampRange.Start.AsTime().String()
			rangeEnd = x.TimestampRange.End.AsTime().String()
		case *protos.PartitionRange_TidRange:
			rangeStartValue, err := pgtype.TID{
				BlockNumber:  x.TidRange.Start.BlockNumber,
				OffsetNumber: uint16(x.TidRange.Start.OffsetNumber),
				Valid:        true,
			}.Value()
			if err != nil {
				return fmt.Errorf("unable to encode TID as string: %w", err)
			}
			rangeStart = rangeStartValue.(string)

			rangeEndValue, err := pgtype.TID{
				BlockNumber:  x.TidRange.End.BlockNumber,
				OffsetNumber: uint16(x.TidRange.End.OffsetNumber),
				Valid:        true,
			}.Value()
			if err != nil {
				return fmt.Errorf("unable to encode TID as string: %w", err)
			}
			rangeEnd = rangeEndValue.(string)
		case *protos.PartitionRange_ObjectIdRange:
			rangeStart = x.ObjectIdRange.Start
			rangeEnd = x.ObjectIdRange.End
		default:
			return fmt.Errorf("unknown range type: %v", x)
		}
	} else {
		internal.LoggerFromCtx(ctx).Warn("[monitoring]: partition "+partition.PartitionId+" has nil range",
			slog.String(string(shared.FlowNameKey), parentMirrorName))
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO peerdb_stats.qrep_partitions
		(flow_name,run_uuid,partition_uuid,partition_start,partition_end,restart_count,parent_mirror_name)
		 VALUES($1,$2,$3,$4,$5,$6,$7) ON CONFLICT(run_uuid,partition_uuid) DO UPDATE SET
		 restart_count=qrep_partitions.restart_count+1`,
		flowJobName, runUUID, partition.PartitionId, rangeStart, rangeEnd, 0, parentMirrorName,
	); err != nil {
		return fmt.Errorf("error while inserting qrep partition in qrep_partitions: %w", err)
	}

	return nil
}

func UpdateStartTimeForPartition(
	ctx context.Context,
	pool shared.CatalogPool,
	runUUID string,
	partition *protos.QRepPartition,
	startTime time.Time,
) error {
	if _, err := pool.Exec(ctx,
		`UPDATE peerdb_stats.qrep_partitions SET start_time=$1 WHERE run_uuid=$2 AND partition_uuid=$3`,
		startTime, runUUID, partition.PartitionId,
	); err != nil {
		return fmt.Errorf("error while updating qrep partition in qrep_partitions: %w", err)
	}
	return nil
}

func UpdatePullEndTimeAndRowsForPartition(ctx context.Context, pool shared.CatalogPool, runUUID string,
	partition *protos.QRepPartition, rowsInPartition int64,
) error {
	if _, err := pool.Exec(ctx,
		`UPDATE peerdb_stats.qrep_partitions SET pull_end_time=$1,rows_in_partition=$2 WHERE run_uuid=$3 AND partition_uuid=$4`,
		time.Now(), rowsInPartition, runUUID, partition.PartitionId,
	); err != nil {
		return fmt.Errorf("error while updating qrep partition in qrep_partitions: %w", err)
	}
	return nil
}

func FinishPartition(ctx context.Context, logger log.Logger, pool shared.CatalogPool, flowName string, runUUID string,
	partition *protos.QRepPartition,
) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("error while starting transaction to delete metadata: %w", err)
	}
	defer shared.RollbackTx(tx, logger)

	if _, err := tx.Exec(ctx,
		`UPDATE peerdb_stats.qrep_partitions SET end_time=$1 WHERE run_uuid=$2 AND partition_uuid=$3`,
		time.Now(), runUUID, partition.PartitionId,
	); err != nil {
		return fmt.Errorf("error while updating qrep partition in qrep_partitions: %w", err)
	}

	if _, err := pool.Exec(ctx, `
		UPDATE peerdb_stats.granular_status
		SET snapshot_failing_partition_ids = array_remove(snapshot_failing_partitions, $1),
			snapshot_succeeding = (
				snapshot_failing_id = 0 and
				array_length(snapshot_failing_qrep_run_ids) = 0 and
				array_length(snapshot_failing_partition_ids) = 0 and
				not snapshot_internal_error
			),
			snapshot_updated_at = utc_now()
		WHERE flow_name = $2
		`, partition.PartitionId, flowName,
	); err != nil {
		return fmt.Errorf("failed to update granular status for snapshot partition: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("error while committing transaction to finish partition: %w", err)
	}
	return nil
}

func UpdateRowsSyncedForPartition(ctx context.Context, pool shared.CatalogPool, rowsSynced int64, runUUID string,
	partition *protos.QRepPartition,
) error {
	if _, err := pool.Exec(ctx,
		`UPDATE peerdb_stats.qrep_partitions SET rows_synced=$1 WHERE run_uuid=$2 AND partition_uuid=$3`,
		rowsSynced, runUUID, partition.PartitionId,
	); err != nil {
		return fmt.Errorf("error while updating rows_synced in qrep_partitions: %w", err)
	}
	return nil
}

func DeleteMirrorStats(ctx context.Context, logger log.Logger, pool shared.CatalogPool, flowJobName string) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("error while starting transaction to delete metadata: %w", err)
	}
	defer shared.RollbackTx(tx, logger)

	if _, err := tx.Exec(ctx, `DELETE FROM peerdb_stats.qrep_partitions WHERE parent_mirror_name = $1`, flowJobName); err != nil {
		return fmt.Errorf("error while deleting qrep_partitions: %w", err)
	}

	if _, err := tx.Exec(ctx, `DELETE FROM peerdb_stats.qrep_runs WHERE parent_mirror_name = $1`, flowJobName); err != nil {
		return fmt.Errorf("error while deleting qrep_runs: %w", err)
	}

	if _, err := tx.Exec(ctx, `DELETE FROM peerdb_stats.cdc_batches WHERE flow_name = $1`, flowJobName); err != nil {
		return fmt.Errorf("error while deleting cdc_batches: %w", err)
	}

	if _, err := tx.Exec(ctx, `DELETE FROM peerdb_stats.cdc_batch_table WHERE flow_name = $1`, flowJobName); err != nil {
		return fmt.Errorf("error while deleting cdc_batch_table: %w", err)
	}

	if _, err := tx.Exec(ctx, `DELETE FROM peerdb_stats.cdc_table_aggregate_counts WHERE flow_name = $1`, flowJobName); err != nil {
		return fmt.Errorf("error while deleting cdc_table_aggregate_counts: %w", err)
	}

	if _, err := tx.Exec(ctx, `DELETE FROM peerdb_stats.cdc_flows WHERE flow_name = $1`, flowJobName); err != nil {
		return fmt.Errorf("error while deleting cdc_flows: %w", err)
	}

	return tx.Commit(ctx)
}

func AuditSchemaDelta(ctx context.Context, pool *pgxpool.Pool,
	flowJobName string, rec *protos.TableSchemaDelta,
) error {
	activityInfo := activity.GetInfo(ctx)
	workflowID := activityInfo.WorkflowExecution.ID
	runID := activityInfo.WorkflowExecution.RunID

	if _, err := pool.Exec(ctx,
		`INSERT INTO
			 peerdb_stats.schema_deltas_audit_log(flow_job_name,workflow_id,run_id,delta_info)
			 VALUES($1,$2,$3,$4)`,
		flowJobName, workflowID, runID, rec); err != nil {
		return fmt.Errorf("failed to insert row into table: %w", err)
	}
	return nil
}
