package connpostgres

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/PeerDB-io/peer-flow/connectors/utils/metrics"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model"
	util "github.com/PeerDB-io/peer-flow/utils"
	"github.com/jackc/pgx/v5"
	log "github.com/sirupsen/logrus"
	"google.golang.org/protobuf/encoding/protojson"
)

type QRepSyncMethod interface {
	SyncQRepRecords(
		flowJobName string,
		dstTableName string,
		partition *protos.QRepPartition,
		stream *model.QRecordStream,
	) (int, error)
}

type QRepStagingTableSync struct {
	connector *PostgresConnector
}

func (s *QRepStagingTableSync) SyncQRepRecords(
	flowJobName string,
	dstTableName *SchemaTable,
	partition *protos.QRepPartition,
	stream *model.QRecordStream,
) (int, error) {
	partitionID := partition.PartitionId
	runID, err := util.RandomUInt64()
	if err != nil {
		return -1, fmt.Errorf("failed to generate random runID: %v", err)
	}

	startTime := time.Now()
	pool := s.connector.pool

	// create a staging temporary table with the same schema as the destination table
	stagingTable := fmt.Sprintf("_%d_staging", runID)

	// create the staging temporary table if not exists
	tmpTableStmt := fmt.Sprintf(
		`CREATE TEMP TABLE %s AS SELECT * FROM %s LIMIT 0;`,
		stagingTable,
		dstTableName.String(),
	)
	_, err = pool.Exec(context.Background(), tmpTableStmt)
	if err != nil {
		log.WithFields(log.Fields{
			"flowName":         flowJobName,
			"partitionID":      partitionID,
			"destinationTable": dstTableName,
		}).Errorf(
			"failed to create staging temporary table %s, statement: '%s'. Error: %v",
			stagingTable,
			tmpTableStmt,
			err,
		)
		return 0, fmt.Errorf("failed to create staging temporary table %s: %w", stagingTable, err)
	}

	schema, err := stream.Schema()
	if err != nil {
		log.WithFields(log.Fields{
			"flowName":         flowJobName,
			"destinationTable": dstTableName,
			"partitionID":      partitionID,
		}).Errorf("failed to get schema from stream: %v", err)
		return 0, fmt.Errorf("failed to get schema from stream: %w", err)
	}

	// Step 2: Insert records into the staging table.
	copySource := model.NewQRecordBatchCopyFromSource(stream)

	// Perform the COPY FROM operation
	syncRecordsStartTime := time.Now()
	syncedRows, err := pool.CopyFrom(
		context.Background(),
		pgx.Identifier{stagingTable},
		schema.GetColumnNames(),
		copySource,
	)

	if err != nil {
		return -1, fmt.Errorf("failed to copy records into staging temporary table: %v", err)
	}
	metrics.LogQRepSyncMetrics(s.connector.ctx, flowJobName, syncedRows, time.Since(syncRecordsStartTime))

	// Second transaction - to handle rest of the processing
	tx2, err := pool.Begin(context.Background())
	if err != nil {
		return 0, fmt.Errorf("failed to begin transaction: %v", err)
	}
	defer func() {
		if err := tx2.Rollback(context.Background()); err != nil {
			if err != pgx.ErrTxClosed {
				log.WithFields(log.Fields{
					"flowName":         flowJobName,
					"partitionID":      partitionID,
					"destinationTable": dstTableName,
				}).Errorf("failed to rollback transaction tx2: %v", err)
			}
		}
	}()

	colNames := schema.GetColumnNames()
	// wrap the column names in double quotes to handle reserved keywords
	for i, colName := range colNames {
		colNames[i] = fmt.Sprintf("\"%s\"", colName)
	}
	colNamesStr := strings.Join(colNames, ", ")
	log.WithFields(log.Fields{
		"flowName":    flowJobName,
		"partitionID": partitionID,
	}).Infof("Obtained column names and quoted them in QRep sync")
	insertFromStagingStmt := fmt.Sprintf(
		"INSERT INTO %s (%s) SELECT %s FROM %s",
		dstTableName.String(),
		colNamesStr,
		colNamesStr,
		stagingTable,
	)

	_, err = tx2.Exec(context.Background(), insertFromStagingStmt)
	if err != nil {
		log.WithFields(log.Fields{
			"flowName":         flowJobName,
			"partitionID":      partitionID,
			"destinationTable": dstTableName,
		}).Errorf("failed to execute statement '%s': %v", insertFromStagingStmt, err)
		return -1, fmt.Errorf("failed to execute statements in a transaction: %v", err)
	}

	// marshal the partition to json using protojson
	pbytes, err := protojson.Marshal(partition)
	if err != nil {
		return -1, fmt.Errorf("failed to marshal partition to json: %v", err)
	}

	normalizeRecordsStartTime := time.Now()
	insertMetadataStmt := fmt.Sprintf(
		"INSERT INTO %s VALUES ($1, $2, $3, $4, $5);",
		qRepMetadataTableName,
	)
	log.WithFields(log.Fields{
		"flowName":         flowJobName,
		"partitionID":      partitionID,
		"destinationTable": dstTableName,
	}).Infof("Executing transaction inside Qrep sync")
	rows, err := tx2.Exec(
		context.Background(),
		insertMetadataStmt,
		flowJobName,
		partitionID,
		string(pbytes),
		startTime,
		time.Now(),
	)
	if err != nil {
		return -1, fmt.Errorf("failed to execute statements in a transaction: %v", err)
	}
	totalRecordsAtTarget, err := s.connector.getApproxTableCounts([]string{dstTableName.String()})
	if err != nil {
		return -1, fmt.Errorf("failed to get total records at target: %v", err)
	}
	metrics.LogQRepNormalizeMetrics(s.connector.ctx, flowJobName, rows.RowsAffected(),
		time.Since(normalizeRecordsStartTime), totalRecordsAtTarget)

	err = tx2.Commit(context.Background())
	if err != nil {
		return -1, fmt.Errorf("failed to commit transaction: %v", err)
	}

	numRowsInserted := copySource.NumRecords()
	log.WithFields(log.Fields{
		"flowName":    flowJobName,
		"partitionID": partitionID,
	}).Infof("pushed %d records to %s", numRowsInserted, dstTableName)
	return numRowsInserted, nil
}
