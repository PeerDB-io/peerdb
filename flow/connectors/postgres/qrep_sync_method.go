package connpostgres

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/PeerDB-io/peer-flow/connectors/utils"
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
	dstTableName *utils.SchemaTable,
	partition *protos.QRepPartition,
	stream *model.QRecordStream,
	writeMode *protos.QRepWriteMode,
) (int, error) {
	partitionID := partition.PartitionId
	startTime := time.Now()

	pool := s.connector.pool
	schema, err := stream.Schema()
	if err != nil {
		log.WithFields(log.Fields{
			"flowName":         flowJobName,
			"destinationTable": dstTableName,
			"partitionID":      partitionID,
		}).Errorf("failed to get schema from stream: %v", err)
		return 0, fmt.Errorf("failed to get schema from stream: %w", err)
	}

	// Second transaction - to handle rest of the processing
	tx, err := pool.Begin(context.Background())
	if err != nil {
		return 0, fmt.Errorf("failed to begin transaction: %v", err)
	}
	defer func() {
		if err := tx.Rollback(context.Background()); err != nil {
			if err != pgx.ErrTxClosed {
				log.WithFields(log.Fields{
					"flowName":         flowJobName,
					"partitionID":      partitionID,
					"destinationTable": dstTableName,
				}).Errorf("failed to rollback transaction tx2: %v", err)
			}
		}
	}()

	// Step 2: Insert records into the destination table.
	copySource := model.NewQRecordBatchCopyFromSource(stream)

	var numRowsSynced int64

	if writeMode == nil ||
		writeMode.WriteType == protos.QRepWriteType_QREP_WRITE_MODE_APPEND {
		// Perform the COPY FROM operation
		numRowsSynced, err = tx.CopyFrom(
			context.Background(),
			pgx.Identifier{dstTableName.Schema, dstTableName.Table},
			schema.GetColumnNames(),
			copySource,
		)
		if err != nil {
			return -1, fmt.Errorf("failed to copy records into destination table: %v", err)
		}
	} else {
		// Step 2.1: Create a temp staging table
		stagingTableName := fmt.Sprintf("_peerdb_staging_%s", util.RandomString(8))
		stagingTableIdentifier := pgx.Identifier{s.connector.metadataSchema, stagingTableName}
		dstTableIdentifier := pgx.Identifier{dstTableName.Schema, dstTableName.Table}

		createStagingTableStmt := fmt.Sprintf(
			"CREATE UNLOGGED TABLE %s (LIKE %s);",
			stagingTableIdentifier.Sanitize(),
			dstTableIdentifier.Sanitize(),
		)

		log.Infof("Creating staging table %s - '%s'", stagingTableName, createStagingTableStmt)
		_, err = tx.Exec(context.Background(), createStagingTableStmt)

		if err != nil {
			return -1, fmt.Errorf("failed to create staging table: %v", err)
		}

		// Step 2.2: Insert records into the staging table
		numRowsSynced, err = tx.CopyFrom(
			context.Background(),
			stagingTableIdentifier,
			schema.GetColumnNames(),
			copySource,
		)
		if err != nil || numRowsSynced != int64(copySource.NumRecords()) {
			return -1, fmt.Errorf("failed to copy records into staging table: %v", err)
		}

		// construct the SET clause for the upsert operation
		upsertMatchColsList := writeMode.UpsertKeyColumns
		upsertMatchCols := make(map[string]struct{})
		for _, col := range upsertMatchColsList {
			upsertMatchCols[col] = struct{}{}
		}

		setClauseArray := make([]string, 0)
		selectStrArray := make([]string, 0)
		for _, col := range schema.GetColumnNames() {
			_, ok := upsertMatchCols[col]
			if !ok {
				setClauseArray = append(setClauseArray, fmt.Sprintf(`"%s" = EXCLUDED."%s"`, col, col))
			}
			selectStrArray = append(selectStrArray, fmt.Sprintf(`"%s"`, col))
		}

		setClause := strings.Join(setClauseArray, ",")
		selectStr := strings.Join(selectStrArray, ",")

		// Step 2.3: Perform the upsert operation, ON CONFLICT UPDATE
		upsertStmt := fmt.Sprintf(
			"INSERT INTO %s (%s) SELECT %s FROM %s ON CONFLICT (%s) DO UPDATE SET %s;",
			dstTableIdentifier.Sanitize(),
			selectStr,
			selectStr,
			stagingTableIdentifier.Sanitize(),
			strings.Join(writeMode.UpsertKeyColumns, ", "),
			setClause,
		)
		log.Infof("Performing upsert operation: %s", upsertStmt)
		res, err := tx.Exec(context.Background(), upsertStmt)
		if err != nil {
			return -1, fmt.Errorf("failed to perform upsert operation: %v", err)
		}

		numRowsSynced = res.RowsAffected()

		// Step 2.4: Drop the staging table
		dropStagingTableStmt := fmt.Sprintf(
			"DROP TABLE %s;",
			stagingTableIdentifier.Sanitize(),
		)
		log.Infof("Dropping staging table %s", stagingTableName)
		_, err = tx.Exec(context.Background(), dropStagingTableStmt)
		if err != nil {
			return -1, fmt.Errorf("failed to drop staging table: %v", err)
		}
	}

	log.WithFields(log.Fields{
		"flowName":         flowJobName,
		"partitionID":      partitionID,
		"destinationTable": dstTableName,
	}).Infof("pushed %d records to %s", numRowsSynced, dstTableName)

	// marshal the partition to json using protojson
	pbytes, err := protojson.Marshal(partition)
	if err != nil {
		return -1, fmt.Errorf("failed to marshal partition to json: %v", err)
	}

	metadataTableIdentifier := pgx.Identifier{s.connector.metadataSchema, qRepMetadataTableName}
	insertMetadataStmt := fmt.Sprintf(
		"INSERT INTO %s VALUES ($1, $2, $3, $4, $5);",
		metadataTableIdentifier.Sanitize(),
	)
	log.WithFields(log.Fields{
		"flowName":         flowJobName,
		"partitionID":      partitionID,
		"destinationTable": dstTableName,
	}).Infof("Executing transaction inside Qrep sync")
	_, err = tx.Exec(
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

	err = tx.Commit(context.Background())
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
