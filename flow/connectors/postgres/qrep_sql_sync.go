package connpostgres

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/PeerDB-io/peer-flow/connectors/utils"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/logger"
	"github.com/PeerDB-io/peer-flow/model"
	"github.com/PeerDB-io/peer-flow/shared"
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
	ctx context.Context,
	flowJobName string,
	dstTableName *utils.SchemaTable,
	partition *protos.QRepPartition,
	stream *model.QRecordStream,
	writeMode *protos.QRepWriteMode,
	syncedAtCol string,
) (int, error) {
	syncLog := slog.Group("sync-qrep-log",
		slog.String(string(shared.FlowNameKey), flowJobName),
		slog.String(string(shared.PartitionIDKey), partition.PartitionId),
		slog.String("destinationTable", dstTableName.String()),
	)
	partitionID := partition.PartitionId
	startTime := time.Now()
	schema, err := stream.Schema()
	if err != nil {
		logger.LoggerFromCtx(ctx).Error("failed to get schema from stream", slog.Any("error", err), syncLog)
		return 0, fmt.Errorf("failed to get schema from stream: %w", err)
	}

	txConfig := s.connector.conn.Config()
	txConn, err := pgx.ConnectConfig(ctx, txConfig)
	if err != nil {
		return 0, fmt.Errorf("failed to create tx pool: %w", err)
	}
	defer txConn.Close(ctx)

	err = shared.RegisterHStore(ctx, txConn)
	if err != nil {
		return 0, fmt.Errorf("failed to register hstore: %w", err)
	}

	// Second transaction - to handle rest of the processing
	tx, err := txConn.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() {
		if err := tx.Rollback(context.Background()); err != nil {
			if err != pgx.ErrTxClosed {
				logger.LoggerFromCtx(ctx).Error("failed to rollback transaction tx2", slog.Any("error", err), syncLog)
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
			return -1, fmt.Errorf("failed to copy records into destination table: %w", err)
		}

		if syncedAtCol != "" {
			updateSyncedAtStmt := fmt.Sprintf(
				`UPDATE %s SET %s = CURRENT_TIMESTAMP WHERE %s IS NULL;`,
				pgx.Identifier{dstTableName.Schema, dstTableName.Table}.Sanitize(),
				QuoteIdentifier(syncedAtCol),
				QuoteIdentifier(syncedAtCol),
			)
			_, err = tx.Exec(context.Background(), updateSyncedAtStmt)
			if err != nil {
				return -1, fmt.Errorf("failed to update synced_at column: %w", err)
			}
		}
	} else {
		// Step 2.1: Create a temp staging table
		stagingTableName := "_peerdb_staging_" + shared.RandomString(8)
		stagingTableIdentifier := pgx.Identifier{s.connector.metadataSchema, stagingTableName}
		dstTableIdentifier := pgx.Identifier{dstTableName.Schema, dstTableName.Table}

		createStagingTableStmt := fmt.Sprintf(
			"CREATE UNLOGGED TABLE %s (LIKE %s);",
			stagingTableIdentifier.Sanitize(),
			dstTableIdentifier.Sanitize(),
		)

		s.connector.logger.Info(fmt.Sprintf("Creating staging table %s - '%s'",
			stagingTableName, createStagingTableStmt), syncLog)
		_, err = tx.Exec(context.Background(), createStagingTableStmt)
		if err != nil {
			return -1, fmt.Errorf("failed to create staging table: %w", err)
		}

		// Step 2.2: Insert records into the staging table
		numRowsSynced, err = tx.CopyFrom(
			context.Background(),
			stagingTableIdentifier,
			schema.GetColumnNames(),
			copySource,
		)
		if err != nil || numRowsSynced != int64(copySource.NumRecords()) {
			return -1, fmt.Errorf("failed to copy records into staging table: %w", err)
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
			quotedCol := QuoteIdentifier(col)
			if !ok {
				setClauseArray = append(setClauseArray, fmt.Sprintf(`%s = EXCLUDED.%s`, quotedCol, quotedCol))
			}
			selectStrArray = append(selectStrArray, quotedCol)
		}
		setClauseArray = append(setClauseArray,
			QuoteIdentifier(syncedAtCol)+`= CURRENT_TIMESTAMP`)
		setClause := strings.Join(setClauseArray, ",")
		selectSQL := strings.Join(selectStrArray, ",")

		// Step 2.3: Perform the upsert operation, ON CONFLICT UPDATE
		upsertStmt := fmt.Sprintf(
			`INSERT INTO %s (%s, %s) SELECT %s, CURRENT_TIMESTAMP FROM %s ON CONFLICT (%s) DO UPDATE SET %s;`,
			dstTableIdentifier.Sanitize(),
			selectSQL,
			QuoteIdentifier(syncedAtCol),
			selectSQL,
			stagingTableIdentifier.Sanitize(),
			strings.Join(writeMode.UpsertKeyColumns, ", "),
			setClause,
		)
		s.connector.logger.Info("Performing upsert operation", slog.String("upsertStmt", upsertStmt), syncLog)
		res, err := tx.Exec(context.Background(), upsertStmt)
		if err != nil {
			return -1, fmt.Errorf("failed to perform upsert operation: %w", err)
		}

		numRowsSynced = res.RowsAffected()

		// Step 2.4: Drop the staging table
		dropStagingTableStmt := fmt.Sprintf(
			"DROP TABLE %s;",
			stagingTableIdentifier.Sanitize(),
		)
		s.connector.logger.Info("Dropping staging table", slog.String("stagingTable", stagingTableName), syncLog)
		_, err = tx.Exec(context.Background(), dropStagingTableStmt)
		if err != nil {
			return -1, fmt.Errorf("failed to drop staging table: %w", err)
		}
	}

	s.connector.logger.Info(fmt.Sprintf("pushed %d records to %s", numRowsSynced, dstTableName), syncLog)

	// marshal the partition to json using protojson
	pbytes, err := protojson.Marshal(partition)
	if err != nil {
		return -1, fmt.Errorf("failed to marshal partition to json: %w", err)
	}

	metadataTableIdentifier := pgx.Identifier{s.connector.metadataSchema, qRepMetadataTableName}
	insertMetadataStmt := fmt.Sprintf(
		"INSERT INTO %s VALUES ($1, $2, $3, $4, $5);",
		metadataTableIdentifier.Sanitize(),
	)
	s.connector.logger.Info("Executing transaction inside QRep sync", syncLog)
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
		return -1, fmt.Errorf("failed to execute statements in a transaction: %w", err)
	}

	err = tx.Commit(context.Background())
	if err != nil {
		return -1, fmt.Errorf("failed to commit transaction: %w", err)
	}

	numRowsInserted := copySource.NumRecords()
	s.connector.logger.Info(fmt.Sprintf("pushed %d records to %s", numRowsInserted, dstTableName), syncLog)
	return numRowsInserted, nil
}
