package connpostgres

import (
	"context"
	"fmt"
	"strings"
	"time"

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
		records *model.QRecordBatch) (int, error)
}

type QRepStagingTableSync struct {
	connector *PostgresConnector
}

func (s *QRepStagingTableSync) SyncQRepRecords(
	flowJobName string,
	dstTableName *SchemaTable,
	partition *protos.QRepPartition,
	records *model.QRecordBatch) (int, error) {
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
		log.Errorf("failed to create staging temporary table %s, statement: '%s'. Error: %v", stagingTable, tmpTableStmt, err)
		return 0, fmt.Errorf("failed to create staging temporary table %s: %w", stagingTable, err)
	}

	// Step 2: Insert records into the staging table.
	copySource := model.NewQRecordBatchCopyFromSource(records)

	// Perform the COPY FROM operation
	_, err = pool.CopyFrom(
		context.Background(),
		pgx.Identifier{stagingTable},
		records.Schema.GetColumnNames(),
		copySource,
	)
	if err != nil {
		return -1, fmt.Errorf("failed to copy records into staging temporary table: %v", err)
	}

	// Second transaction - to handle rest of the processing
	tx2, err := pool.Begin(context.Background())
	if err != nil {
		return 0, fmt.Errorf("failed to begin transaction: %v", err)
	}
	defer func() {
		if err := tx2.Rollback(context.Background()); err != nil {
			if err != pgx.ErrTxClosed {
				log.Errorf("failed to rollback transaction tx2: %v", err)
			}
		}
	}()

	colNames := records.Schema.GetColumnNames()
	colNamesStr := strings.Join(colNames, ", ")
	insertFromStagingStmt := fmt.Sprintf(
		"INSERT INTO %s SELECT %s FROM %s",
		dstTableName.String(),
		colNamesStr,
		stagingTable,
	)
	_, err = tx2.Exec(context.Background(), insertFromStagingStmt)
	if err != nil {
		return -1, fmt.Errorf("failed to execute statements in a transaction: %v", err)
	}

	// marshal the partition to json using protojson
	pbytes, err := protojson.Marshal(partition)
	if err != nil {
		return -1, fmt.Errorf("failed to marshal partition to json: %v", err)
	}

	insertMetadataStmt := fmt.Sprintf(
		"INSERT INTO %s VALUES ($1, $2, $3, $4, $5);",
		qRepMetadataTableName,
	)
	_, err = tx2.Exec(
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

	err = tx2.Commit(context.Background())
	if err != nil {
		return -1, fmt.Errorf("failed to commit transaction: %v", err)
	}

	numRowsInserted := records.NumRecords
	log.Printf("pushed %d records to %s", numRowsInserted, dstTableName)
	return int(numRowsInserted), nil
}
