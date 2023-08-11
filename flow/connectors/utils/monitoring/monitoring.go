package monitoring

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	log "github.com/sirupsen/logrus"
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

func NewCatalogMirrorMonitor(catalogConn *pgxpool.Pool) CatalogMirrorMonitor {
	return CatalogMirrorMonitor{
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
		return fmt.Errorf("error while updating flow in cdc_flows: %w", err)
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
		return fmt.Errorf("error while updating flow in cdc_flows: %w", err)
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
		start_time) VALUES($1,$2,$3,$4,$5,$6)`, flowJobName, batchInfo.BatchID, batchInfo.RowsInBatch,
		uint64(batchInfo.BatchStartLSN), uint64(batchInfo.BatchEndlSN), batchInfo.StartTime)
	if err != nil {
		return fmt.Errorf("error while inserting batch into cdc_batch: %w", err)
	}
	return nil
}

func (c *CatalogMirrorMonitor) UpdateEndTimeForCDCBatch(ctx context.Context, flowJobName string,
	batchID int64) error {
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
		if err != pgx.ErrNoRows && err != nil {
			log.Error("unexpected error during transaction rollback: %w", err)
		}
	}()

	for destinationTableName, numRows := range tableNameRowsMapping {
		_, err = insertBatchTablesTx.Exec(ctx,
			`INSERT INTO peerdb_stats.cdc_batch_table(flow_name,batch_id,destination_table_name,num_rows)
			 VALUES($1,$2,$3,$4)`,
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
