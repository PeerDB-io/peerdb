package connpostgres

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/pkg/common"
	"github.com/PeerDB-io/peerdb/flow/shared"
)

const qRepMetadataTableName = "_peerdb_query_replication_metadata"

type QRepSyncSink interface {
	GetColumnNames() ([]string, error)
	CopyInto(context.Context, *PostgresConnector, pgx.Tx, pgx.Identifier) (int64, error)
}

func (c *PostgresConnector) SyncQRepRecords(
	ctx context.Context,
	config *protos.QRepConfig,
	partition *protos.QRepPartition,
	stream *model.QRecordStream,
) (int64, shared.QRepWarnings, error) {
	return syncQRepRecords(c, ctx, config, partition, RecordStreamSink{
		QRecordStream:   stream,
		DestinationType: protos.DBType_POSTGRES,
	})
}

func (c *PostgresConnector) SyncPgQRepRecords(
	ctx context.Context,
	config *protos.QRepConfig,
	partition *protos.QRepPartition,
	pipe PgCopyReader,
) (int64, shared.QRepWarnings, error) {
	return syncQRepRecords(c, ctx, config, partition, pipe)
}

func syncQRepRecords(
	c *PostgresConnector,
	ctx context.Context,
	config *protos.QRepConfig,
	partition *protos.QRepPartition,
	sink QRepSyncSink,
) (int64, shared.QRepWarnings, error) {
	dstTable, err := common.ParseTableIdentifier(config.DestinationTableIdentifier)
	if err != nil {
		return 0, nil, fmt.Errorf("failed to parse destination table identifier: %w", err)
	}

	exists, err := c.tableExists(ctx, dstTable)
	if err != nil {
		return 0, nil, fmt.Errorf("failed to check if table exists: %w", err)
	}

	if !exists {
		return 0, nil, fmt.Errorf("table %s does not exist, used schema: %s", dstTable.Table, dstTable.Namespace)
	}

	c.logger.Info("SyncRecords called and initial checks complete.")

	flowJobName := config.FlowJobName
	writeMode := config.WriteMode
	syncedAtCol := config.SyncedAtColName

	syncLog := slog.Group("sync-qrep-log",
		slog.String(string(shared.FlowNameKey), flowJobName),
		slog.String(string(shared.PartitionIDKey), partition.PartitionId),
		slog.String("destinationTable", dstTable.String()),
	)
	partitionID := partition.PartitionId
	startTime := time.Now()

	txConfig := c.conn.Config()
	txConn, err := pgx.ConnectConfig(ctx, txConfig)
	if err != nil {
		return 0, nil, fmt.Errorf("failed to create tx pool: %w", err)
	}
	defer txConn.Close(ctx)

	if err := shared.RegisterExtensions(ctx, txConn, config.Version); err != nil {
		return 0, nil, fmt.Errorf("failed to register extensions: %w", err)
	}

	tx, err := txConn.Begin(ctx)
	if err != nil {
		return 0, nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer shared.RollbackTx(tx, c.logger)

	if config.System == protos.TypeSystem_PG {
		if _, err := tx.Exec(ctx, setSessionReplicaRoleSQL); err != nil {
			return 0, nil, fmt.Errorf("failed to set session_replication_role to replica: %w", err)
		}
		c.logger.Info("set session_replication_role to replica on destination for PG type system initial load")
	}

	// Step 2: Insert records into destination table
	var numRowsSynced int64

	if writeMode == nil ||
		writeMode.WriteType == protos.QRepWriteType_QREP_WRITE_MODE_APPEND ||
		writeMode.WriteType == protos.QRepWriteType_QREP_WRITE_MODE_OVERWRITE {
		if writeMode != nil && writeMode.WriteType == protos.QRepWriteType_QREP_WRITE_MODE_OVERWRITE {
			// Truncate destination table before copying records
			c.logger.Info(fmt.Sprintf("Truncating table %s for overwrite mode", dstTable), syncLog)
			_, err = c.execWithLoggingTx(ctx,
				"TRUNCATE TABLE "+dstTable.String(), tx)
			if err != nil {
				return -1, nil, fmt.Errorf("failed to TRUNCATE table before copy: %w", err)
			}
		}

		numRowsSynced, err = sink.CopyInto(ctx, c, tx, pgx.Identifier{dstTable.Namespace, dstTable.Table})
		if err != nil {
			return -1, nil, fmt.Errorf("failed to copy records into destination table: %w", err)
		}

		if syncedAtCol != "" {
			updateSyncedAtStmt := fmt.Sprintf(
				`UPDATE %s SET %s = CURRENT_TIMESTAMP WHERE %s IS NULL;`,
				pgx.Identifier{dstTable.Namespace, dstTable.Table}.Sanitize(),
				common.QuoteIdentifier(syncedAtCol),
				common.QuoteIdentifier(syncedAtCol),
			)
			if _, err := tx.Exec(ctx, updateSyncedAtStmt); err != nil {
				return -1, nil, fmt.Errorf("failed to update synced_at column: %w", err)
			}
		}
	} else {
		// Step 2.1: Create a temp staging table
		stagingTableName := "_peerdb_staging_" + common.RandomString(8)
		stagingTableIdentifier := pgx.Identifier{stagingTableName}
		dstTableIdentifier := pgx.Identifier{dstTable.Namespace, dstTable.Table}

		// From PG docs: The cost of setting a large value in sessions that do not actually need many
		// temporary buffers is only a buffer descriptor, or about 64 bytes, per increment in temp_buffers.
		if _, err := tx.Exec(ctx, "SET temp_buffers = '4GB';"); err != nil {
			return -1, nil, fmt.Errorf("failed to set temp_buffers: %w", err)
		}

		createStagingTableStmt := fmt.Sprintf(
			"CREATE TEMP TABLE %s (LIKE %s) ON COMMIT DROP;",
			stagingTableIdentifier.Sanitize(),
			dstTableIdentifier.Sanitize(),
		)

		c.logger.Info(fmt.Sprintf("Creating staging table %s - '%s'",
			stagingTableName, createStagingTableStmt), syncLog)
		if _, err := c.execWithLoggingTx(ctx, createStagingTableStmt, tx); err != nil {
			return -1, nil, fmt.Errorf("failed to create staging table: %w", err)
		}

		// Step 2.2: Insert records into the staging table
		numRowsSynced, err = sink.CopyInto(ctx, c, tx, stagingTableIdentifier)
		if err != nil {
			return -1, nil, fmt.Errorf("failed to copy records into staging table: %w", err)
		}

		// construct the SET clause for the upsert operation
		upsertMatchColsList := writeMode.UpsertKeyColumns
		upsertMatchCols := make(map[string]struct{}, len(upsertMatchColsList))
		for _, col := range upsertMatchColsList {
			upsertMatchCols[col] = struct{}{}
		}

		columnNames, err := sink.GetColumnNames()
		if err != nil {
			return -1, nil, fmt.Errorf("faild to get column names: %w", err)
		}
		setClauseArray := make([]string, 0, len(upsertMatchColsList)+1)
		selectStrArray := make([]string, 0, len(columnNames))
		for _, col := range columnNames {
			_, ok := upsertMatchCols[col]
			quotedCol := common.QuoteIdentifier(col)
			if !ok {
				setClauseArray = append(setClauseArray, fmt.Sprintf(`%s = EXCLUDED.%s`, quotedCol, quotedCol))
			}
			selectStrArray = append(selectStrArray, quotedCol)
		}
		setClauseArray = append(setClauseArray,
			common.QuoteIdentifier(syncedAtCol)+`= CURRENT_TIMESTAMP`)
		setClause := strings.Join(setClauseArray, ",")
		selectSQL := strings.Join(selectStrArray, ",")

		// Step 2.3: Perform the upsert operation, ON CONFLICT UPDATE
		upsertStmt := fmt.Sprintf(
			`INSERT INTO %s (%s, %s) SELECT %s, CURRENT_TIMESTAMP FROM %s ON CONFLICT (%s) DO UPDATE SET %s;`,
			dstTableIdentifier.Sanitize(),
			selectSQL,
			common.QuoteIdentifier(syncedAtCol),
			selectSQL,
			stagingTableIdentifier.Sanitize(),
			strings.Join(writeMode.UpsertKeyColumns, ", "),
			setClause,
		)
		c.logger.Info("Performing upsert operation", slog.String("upsertStmt", upsertStmt), syncLog)
		if _, err := tx.Exec(ctx, upsertStmt); err != nil {
			return -1, nil, fmt.Errorf("failed to perform upsert operation: %w", err)
		}
	}

	c.logger.Info(fmt.Sprintf("pushed %d records to %s", numRowsSynced, dstTable), syncLog)

	// marshal the partition to json using protojson
	pbytes, err := protojson.Marshal(partition)
	if err != nil {
		return -1, nil, fmt.Errorf("failed to marshal partition to json: %w", err)
	}

	metadataTableIdentifier := pgx.Identifier{c.metadataSchema, qRepMetadataTableName}
	insertMetadataStmt := fmt.Sprintf(
		"INSERT INTO %s VALUES ($1, $2, $3, $4, $5);",
		metadataTableIdentifier.Sanitize(),
	)
	c.logger.Info("Executing transaction inside QRep sync", syncLog)
	if _, err := tx.Exec(
		ctx,
		insertMetadataStmt,
		flowJobName,
		partitionID,
		string(pbytes),
		startTime,
		time.Now(),
	); err != nil {
		return -1, nil, fmt.Errorf("failed to execute statements in a transaction: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return -1, nil, fmt.Errorf("failed to commit transaction: %w", err)
	}

	c.logger.Info(fmt.Sprintf("pushed %d records to %s", numRowsSynced, dstTable), syncLog)
	return numRowsSynced, nil, nil
}

// SetupQRepMetadataTables function for postgres connector
func (c *PostgresConnector) SetupQRepMetadataTables(ctx context.Context, config *protos.QRepConfig) error {
	if err := c.createMetadataSchema(ctx); err != nil {
		return fmt.Errorf("error creating metadata schema: %w", err)
	}

	metadataTableIdentifier := pgx.Identifier{c.metadataSchema, qRepMetadataTableName}
	createQRepMetadataTableSQL := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s(
		flowJobName TEXT,
		partitionID TEXT,
		syncPartition JSONB,
		syncStartTime TIMESTAMP,
		syncFinishTime TIMESTAMP DEFAULT NOW()
	)`, metadataTableIdentifier.Sanitize())
	// execute create table query
	if _, err := c.execWithLogging(ctx, createQRepMetadataTableSQL); err != nil && !shared.IsSQLStateError(err, pgerrcode.UniqueViolation) {
		return fmt.Errorf("failed to create table %s: %w", qRepMetadataTableName, err)
	}
	c.logger.Info("Setup metadata table.")

	return nil
}

// IsQRepPartitionSynced checks whether a specific partition is synced
func (c *PostgresConnector) IsQRepPartitionSynced(ctx context.Context,
	req *protos.IsQRepPartitionSyncedInput,
) (bool, error) {
	// setup the query string
	metadataTableIdentifier := pgx.Identifier{c.metadataSchema, qRepMetadataTableName}
	queryString := fmt.Sprintf(
		"SELECT EXISTS(SELECT * FROM %s WHERE partitionID=$1)",
		metadataTableIdentifier.Sanitize(),
	)

	// prepare and execute the query
	var result bool
	if err := c.conn.QueryRow(ctx, queryString, req.PartitionId).Scan(&result); err != nil {
		return false, fmt.Errorf("failed to execute query: %w", err)
	}

	return result, nil
}
