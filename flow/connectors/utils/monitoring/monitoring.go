package monitoring

import (
	"context"
	"fmt"
	"time"

	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	log "github.com/sirupsen/logrus"
	"google.golang.org/protobuf/proto"
)

type CDCBatchInfo struct {
	BatchID       int64
	RowsInBatch   uint32
	BatchStartLSN pglogrepl.LSN
	BatchEndlSN   pglogrepl.LSN
	StartTime     time.Time
}

func InitializeCDCFlow(ctx context.Context, pool *pgxpool.Pool, flowJobName string) error {
	_, err := pool.Exec(ctx,
		`INSERT INTO peerdb_stats.cdc_flows(flow_name,latest_lsn_at_source,latest_lsn_at_target) VALUES($1,0,0)
		 ON CONFLICT DO NOTHING`, flowJobName)
	if err != nil {
		return fmt.Errorf("error while inserting flow into cdc_flows: %w", err)
	}
	return nil
}

func UpdateLatestLSNAtSourceForCDCFlow(ctx context.Context, pool *pgxpool.Pool, flowJobName string,
	latestLSNAtSource pglogrepl.LSN) error {
	_, err := pool.Exec(ctx,
		"UPDATE peerdb_stats.cdc_flows SET latest_lsn_at_source=$1 WHERE flow_name=$2",
		uint64(latestLSNAtSource), flowJobName)
	if err != nil {
		return fmt.Errorf("[source] error while updating flow in cdc_flows: %w", err)
	}
	return nil
}

func UpdateLatestLSNAtTargetForCDCFlow(ctx context.Context, pool *pgxpool.Pool, flowJobName string,
	latestLSNAtTarget pglogrepl.LSN) error {
	_, err := pool.Exec(ctx,
		"UPDATE peerdb_stats.cdc_flows SET latest_lsn_at_target=$1 WHERE flow_name=$2",
		uint64(latestLSNAtTarget), flowJobName)
	if err != nil {
		return fmt.Errorf("[target] error while updating flow in cdc_flows: %w", err)
	}
	return nil
}

func AddCDCBatchForFlow(ctx context.Context, pool *pgxpool.Pool, flowJobName string,
	batchInfo CDCBatchInfo) error {
	_, err := pool.Exec(ctx,
		`INSERT INTO peerdb_stats.cdc_batches(flow_name,batch_id,rows_in_batch,batch_start_lsn,batch_end_lsn,
		start_time) VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT DO NOTHING`,
		flowJobName, batchInfo.BatchID, batchInfo.RowsInBatch,
		uint64(batchInfo.BatchStartLSN), uint64(batchInfo.BatchEndlSN), batchInfo.StartTime)
	if err != nil {
		return fmt.Errorf("error while inserting batch into cdc_batch: %w", err)
	}
	return nil
}

// update num records and end-lsn for a cdc batch
func UpdateNumRowsAndEndLSNForCDCBatch(
	ctx context.Context,
	pool *pgxpool.Pool,
	flowJobName string,
	batchID int64,
	numRows uint32,
	batchEndLSN pglogrepl.LSN,
) error {
	_, err := pool.Exec(ctx,
		"UPDATE peerdb_stats.cdc_batches SET rows_in_batch=$1,batch_end_lsn=$2 WHERE flow_name=$3 AND batch_id=$4",
		numRows, uint64(batchEndLSN), flowJobName, batchID)
	if err != nil {
		return fmt.Errorf("error while updating batch in cdc_batch: %w", err)
	}
	return nil
}

func UpdateEndTimeForCDCBatch(
	ctx context.Context,
	pool *pgxpool.Pool,
	flowJobName string,
	batchID int64,
) error {
	_, err := pool.Exec(ctx,
		"UPDATE peerdb_stats.cdc_batches SET end_time=$1 WHERE flow_name=$2 AND batch_id=$3",
		time.Now(), flowJobName, batchID)
	if err != nil {
		return fmt.Errorf("error while updating batch in cdc_batch: %w", err)
	}
	return nil
}

func AddCDCBatchTablesForFlow(ctx context.Context, pool *pgxpool.Pool, flowJobName string,
	batchID int64, tableNameRowsMapping map[string]uint32) error {
	insertBatchTablesTx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("error while beginning transaction for inserting statistics into cdc_batch_table: %w", err)
	}
	defer func() {
		err = insertBatchTablesTx.Rollback(ctx)
		if err != pgx.ErrTxClosed && err != nil {
			log.Error("unexpected error during transaction rollback: %w", err)
		}
	}()

	for destinationTableName, numRows := range tableNameRowsMapping {
		_, err = insertBatchTablesTx.Exec(ctx,
			`INSERT INTO peerdb_stats.cdc_batch_table(flow_name,batch_id,destination_table_name,num_rows)
			 VALUES($1,$2,$3,$4) ON CONFLICT DO NOTHING`,
			flowJobName, batchID, destinationTableName, numRows)
		if err != nil {
			return fmt.Errorf("error while inserting statistics into cdc_batch_table: %w", err)
		}
	}
	err = insertBatchTablesTx.Commit(ctx)
	if err != nil {
		return fmt.Errorf("error while committing transaction for inserting statistics into cdc_batch_table: %w",
			err)
	}
	return nil
}

func InitializeQRepRun(
	ctx context.Context,
	pool *pgxpool.Pool,
	config *protos.QRepConfig,
	runUUID string,
	partitions []*protos.QRepPartition,
) error {
	flowJobName := config.GetFlowJobName()
	_, err := pool.Exec(ctx,
		"INSERT INTO peerdb_stats.qrep_runs(flow_name,run_uuid) VALUES($1,$2) ON CONFLICT DO NOTHING",
		flowJobName, runUUID)
	if err != nil {
		return fmt.Errorf("error while inserting qrep run in qrep_runs: %w", err)
	}

	cfgBytes, err := proto.Marshal(config)
	if err != nil {
		return fmt.Errorf("unable to marshal flow config: %w", err)
	}

	_, err = pool.Exec(ctx,
		"UPDATE peerdb_stats.qrep_runs SET config_proto = $1 WHERE flow_name = $2",
		cfgBytes, flowJobName)
	if err != nil {
		return fmt.Errorf("unable to update flow config in catalog: %w", err)
	}

	for _, partition := range partitions {
		if err := addPartitionToQRepRun(ctx, pool, flowJobName, runUUID, partition); err != nil {
			return fmt.Errorf("unable to add partition to qrep run: %w", err)
		}
	}

	return nil
}

func UpdateStartTimeForQRepRun(ctx context.Context, pool *pgxpool.Pool, runUUID string) error {
	_, err := pool.Exec(ctx,
		"UPDATE peerdb_stats.qrep_runs SET start_time=$1 WHERE run_uuid=$2",
		time.Now(), runUUID)
	if err != nil {
		return fmt.Errorf("error while updating start time for run_uuid %s in qrep_runs: %w", runUUID, err)
	}

	return nil
}

func UpdateEndTimeForQRepRun(ctx context.Context, pool *pgxpool.Pool, runUUID string) error {
	_, err := pool.Exec(ctx,
		"UPDATE peerdb_stats.qrep_runs SET end_time=$1 WHERE run_uuid=$2",
		time.Now(), runUUID)
	if err != nil {
		return fmt.Errorf("error while updating end time for run_uuid %s in qrep_runs: %w", runUUID, err)
	}

	return nil
}

func AppendSlotSizeInfo(
	ctx context.Context,
	pool *pgxpool.Pool,
	peerName string,
	slotInfo *protos.SlotInfo,
) error {
	_, err := pool.Exec(ctx,
		"INSERT INTO peerdb_stats.peer_slot_size"+
			"(peer_name, slot_name, restart_lsn, redo_lsn, confirmed_flush_lsn, slot_size, wal_status) "+
			"VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT DO NOTHING;",
		peerName,
		slotInfo.SlotName,
		slotInfo.RestartLSN,
		slotInfo.RedoLSN,
		slotInfo.ConfirmedFlushLSN,
		slotInfo.LagInMb,
		slotInfo.WalStatus,
	)
	if err != nil {
		return fmt.Errorf("error while upserting row for slot_size: %w", err)
	}

	return nil
}

func addPartitionToQRepRun(ctx context.Context, pool *pgxpool.Pool, flowJobName string,
	runUUID string, partition *protos.QRepPartition) error {
	if partition.Range == nil && partition.FullTablePartition {
		log.Infof("partition %s is a full table partition. Metrics logging is skipped.", partition.PartitionId)
		return nil
	}

	var rangeStart, rangeEnd string
	switch x := partition.Range.Range.(type) {
	case *protos.PartitionRange_IntRange:
		rangeStart = fmt.Sprint(x.IntRange.Start)
		rangeEnd = fmt.Sprint(x.IntRange.End)
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
	default:
		return fmt.Errorf("unknown range type: %v", x)
	}

	_, err := pool.Exec(ctx,
		`INSERT INTO peerdb_stats.qrep_partitions
		(flow_name,run_uuid,partition_uuid,partition_start,partition_end,restart_count)
		 VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT(run_uuid,partition_uuid) DO UPDATE SET
		 restart_count=qrep_partitions.restart_count+1`,
		flowJobName, runUUID, partition.PartitionId, rangeStart, rangeEnd, 0)
	if err != nil {
		return fmt.Errorf("error while inserting qrep partition in qrep_partitions: %w", err)
	}

	return nil
}

func UpdateStartTimeForPartition(
	ctx context.Context,
	pool *pgxpool.Pool,
	runUUID string,
	partition *protos.QRepPartition,
	startTime time.Time,
) error {
	_, err := pool.Exec(ctx, `UPDATE peerdb_stats.qrep_partitions SET start_time=$1
	 WHERE run_uuid=$2 AND partition_uuid=$3`, startTime, runUUID, partition.PartitionId)
	if err != nil {
		return fmt.Errorf("error while updating qrep partition in qrep_partitions: %w", err)
	}
	return nil
}

func UpdatePullEndTimeAndRowsForPartition(ctx context.Context, pool *pgxpool.Pool, runUUID string,
	partition *protos.QRepPartition, rowsInPartition int64) error {
	_, err := pool.Exec(ctx, `UPDATE peerdb_stats.qrep_partitions SET pull_end_time=$1,rows_in_partition=$2
	 WHERE run_uuid=$3 AND partition_uuid=$4`, time.Now(), rowsInPartition, runUUID, partition.PartitionId)
	if err != nil {
		return fmt.Errorf("error while updating qrep partition in qrep_partitions: %w", err)
	}
	return nil
}

func UpdateEndTimeForPartition(ctx context.Context, pool *pgxpool.Pool, runUUID string,
	partition *protos.QRepPartition) error {
	_, err := pool.Exec(ctx, `UPDATE peerdb_stats.qrep_partitions SET end_time=$1
	 WHERE run_uuid=$2 AND partition_uuid=$3`, time.Now(), runUUID, partition.PartitionId)
	if err != nil {
		return fmt.Errorf("error while updating qrep partition in qrep_partitions: %w", err)
	}
	return nil
}

func UpdateRowsSyncedForPartition(ctx context.Context, pool *pgxpool.Pool, rowsSynced int, runUUID string,
	partition *protos.QRepPartition) error {
	_, err := pool.Exec(ctx, `UPDATE peerdb_stats.qrep_partitions SET rows_synced=$1
	 WHERE run_uuid=$2 AND partition_uuid=$3`, rowsSynced, runUUID, partition.PartitionId)
	if err != nil {
		return fmt.Errorf("error while updating rows_synced in qrep_partitions: %w", err)
	}
	return nil
}
