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

type CatalogMirrorMonitor struct {
	catalogConn *pgxpool.Pool
}

type CDCBatchInfo struct {
	BatchID       int64
	RowsInBatch   uint32
	BatchStartLSN pglogrepl.LSN
	BatchEndlSN   pglogrepl.LSN
	StartTime     time.Time
}

func NewCatalogMirrorMonitor(catalogConn *pgxpool.Pool) *CatalogMirrorMonitor {
	return &CatalogMirrorMonitor{
		catalogConn: catalogConn,
	}
}

func (c *CatalogMirrorMonitor) IsActive() bool {
	return !(c == nil || c.catalogConn == nil)
}

func (c *CatalogMirrorMonitor) Close() {
	if c == nil || c.catalogConn == nil {
		return
	}
	c.catalogConn.Close()
}

func (c *CatalogMirrorMonitor) InitializeCDCFlow(ctx context.Context, flowJobName string) error {
	if c == nil || c.catalogConn == nil {
		return nil
	}

	_, err := c.catalogConn.Exec(ctx,
		`INSERT INTO peerdb_stats.cdc_flows(flow_name,latest_lsn_at_source,latest_lsn_at_target) VALUES($1,0,0)
		 ON CONFLICT DO NOTHING`, flowJobName)
	if err != nil {
		return fmt.Errorf("error while inserting flow into cdc_flows: %w", err)
	}
	return nil
}

func (c *CatalogMirrorMonitor) UpdateLatestLSNAtSourceForCDCFlow(ctx context.Context, flowJobName string,
	latestLSNAtSource pglogrepl.LSN) error {
	if c == nil || c.catalogConn == nil {
		return nil
	}

	_, err := c.catalogConn.Exec(ctx,
		"UPDATE peerdb_stats.cdc_flows SET latest_lsn_at_source=$1 WHERE flow_name=$2",
		uint64(latestLSNAtSource), flowJobName)
	if err != nil {
		return fmt.Errorf("[source] error while updating flow in cdc_flows: %w", err)
	}
	return nil
}

func (c *CatalogMirrorMonitor) UpdateLatestLSNAtTargetForCDCFlow(ctx context.Context, flowJobName string,
	latestLSNAtTarget pglogrepl.LSN) error {
	if c == nil || c.catalogConn == nil {
		return nil
	}

	_, err := c.catalogConn.Exec(ctx,
		"UPDATE peerdb_stats.cdc_flows SET latest_lsn_at_target=$1 WHERE flow_name=$2",
		uint64(latestLSNAtTarget), flowJobName)
	if err != nil {
		return fmt.Errorf("[target] error while updating flow in cdc_flows: %w", err)
	}
	return nil
}

func (c *CatalogMirrorMonitor) AddCDCBatchForFlow(ctx context.Context, flowJobName string,
	batchInfo CDCBatchInfo) error {
	if c == nil || c.catalogConn == nil {
		return nil
	}

	_, err := c.catalogConn.Exec(ctx,
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
func (c *CatalogMirrorMonitor) UpdateNumRowsAndEndLSNForCDCBatch(
	ctx context.Context,
	flowJobName string,
	batchID int64,
	numRows uint32,
	batchEndLSN pglogrepl.LSN,
) error {
	if c == nil || c.catalogConn == nil {
		return nil
	}

	_, err := c.catalogConn.Exec(ctx,
		"UPDATE peerdb_stats.cdc_batches SET rows_in_batch=$1,batch_end_lsn=$2 WHERE flow_name=$3 AND batch_id=$4",
		numRows, uint64(batchEndLSN), flowJobName, batchID)
	if err != nil {
		return fmt.Errorf("error while updating batch in cdc_batch: %w", err)
	}
	return nil
}

func (c *CatalogMirrorMonitor) UpdateEndTimeForCDCBatch(
	ctx context.Context,
	flowJobName string,
	batchID int64,
) error {
	if c == nil || c.catalogConn == nil {
		return nil
	}

	_, err := c.catalogConn.Exec(ctx,
		"UPDATE peerdb_stats.cdc_batches SET end_time=$1 WHERE flow_name=$2 AND batch_id=$3",
		time.Now(), flowJobName, batchID)
	if err != nil {
		return fmt.Errorf("error while updating batch in cdc_batch: %w", err)
	}
	return nil
}

func (c *CatalogMirrorMonitor) AddCDCBatchTablesForFlow(ctx context.Context, flowJobName string,
	batchID int64, tableNameRowsMapping map[string]uint32) error {
	if c == nil || c.catalogConn == nil {
		return nil
	}

	insertBatchTablesTx, err := c.catalogConn.Begin(ctx)
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

func (c *CatalogMirrorMonitor) InitializeQRepRun(
	ctx context.Context,
	config *protos.QRepConfig,
	runUUID string,
	partitions []*protos.QRepPartition,
) error {
	if c == nil || c.catalogConn == nil {
		return nil
	}

	flowJobName := config.GetFlowJobName()
	_, err := c.catalogConn.Exec(ctx,
		"INSERT INTO peerdb_stats.qrep_runs(flow_name,run_uuid) VALUES($1,$2) ON CONFLICT DO NOTHING",
		flowJobName, runUUID)
	if err != nil {
		return fmt.Errorf("error while inserting qrep run in qrep_runs: %w", err)
	}

	cfgBytes, err := proto.Marshal(config)
	if err != nil {
		return fmt.Errorf("unable to marshal flow config: %w", err)
	}

	_, err = c.catalogConn.Exec(ctx,
		"UPDATE peerdb_stats.qrep_runs SET config_proto = $1 WHERE flow_name = $2",
		cfgBytes, flowJobName)
	if err != nil {
		return fmt.Errorf("unable to update flow config in catalog: %w", err)
	}

	for _, partition := range partitions {
		if err := c.addPartitionToQRepRun(ctx, flowJobName, runUUID, partition); err != nil {
			return fmt.Errorf("unable to add partition to qrep run: %w", err)
		}
	}

	return nil
}

func (c *CatalogMirrorMonitor) UpdateStartTimeForQRepRun(ctx context.Context, runUUID string) error {
	if c == nil || c.catalogConn == nil {
		return nil
	}

	_, err := c.catalogConn.Exec(ctx,
		"UPDATE peerdb_stats.qrep_runs SET start_time=$1 WHERE run_uuid=$2",
		time.Now(), runUUID)
	if err != nil {
		return fmt.Errorf("error while updating num_rows_to_sync for run_uuid %s in qrep_runs: %w", runUUID, err)
	}

	return nil
}

func (c *CatalogMirrorMonitor) UpdateEndTimeForQRepRun(ctx context.Context, runUUID string) error {
	if c == nil || c.catalogConn == nil {
		return nil
	}

	_, err := c.catalogConn.Exec(ctx,
		"UPDATE peerdb_stats.qrep_runs SET end_time=$1 WHERE run_uuid=$2",
		time.Now(), runUUID)
	if err != nil {
		return fmt.Errorf("error while updating num_rows_to_sync for run_uuid %s in qrep_runs: %w", runUUID, err)
	}

	return nil
}

func (c *CatalogMirrorMonitor) addPartitionToQRepRun(ctx context.Context, flowJobName string,
	runUUID string, partition *protos.QRepPartition) error {
	if c == nil || c.catalogConn == nil {
		return nil
	}

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

	_, err := c.catalogConn.Exec(ctx,
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

func (c *CatalogMirrorMonitor) UpdateStartTimeForPartition(
	ctx context.Context,
	runUUID string,
	partition *protos.QRepPartition,
	startTime time.Time,
) error {
	if c == nil || c.catalogConn == nil {
		return nil
	}

	_, err := c.catalogConn.Exec(ctx, `UPDATE peerdb_stats.qrep_partitions SET start_time=$1
	 WHERE run_uuid=$2 AND partition_uuid=$3`, startTime, runUUID, partition.PartitionId)
	if err != nil {
		return fmt.Errorf("error while updating qrep partition in qrep_partitions: %w", err)
	}
	return nil
}

func (c *CatalogMirrorMonitor) UpdatePullEndTimeAndRowsForPartition(ctx context.Context, runUUID string,
	partition *protos.QRepPartition, rowsInPartition int64) error {
	if c == nil || c.catalogConn == nil {
		return nil
	}

	_, err := c.catalogConn.Exec(ctx, `UPDATE peerdb_stats.qrep_partitions SET pull_end_time=$1,rows_in_partition=$2
	 WHERE run_uuid=$3 AND partition_uuid=$4`, time.Now(), rowsInPartition, runUUID, partition.PartitionId)
	if err != nil {
		return fmt.Errorf("error while updating qrep partition in qrep_partitions: %w", err)
	}
	return nil
}

func (c *CatalogMirrorMonitor) UpdateEndTimeForPartition(ctx context.Context, runUUID string,
	partition *protos.QRepPartition) error {
	if c == nil || c.catalogConn == nil {
		return nil
	}

	_, err := c.catalogConn.Exec(ctx, `UPDATE peerdb_stats.qrep_partitions SET end_time=$1
	 WHERE run_uuid=$2 AND partition_uuid=$3`, time.Now(), runUUID, partition.PartitionId)
	if err != nil {
		return fmt.Errorf("error while updating qrep partition in qrep_partitions: %w", err)
	}
	return nil
}
