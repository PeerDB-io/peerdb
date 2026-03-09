package connpostgres

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"text/template"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"go.temporal.io/sdk/log"
	"go.temporal.io/sdk/temporal"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/PeerDB-io/peerdb/flow/connectors/utils"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/otel_metrics"
	"github.com/PeerDB-io/peerdb/flow/pkg/common"
	"github.com/PeerDB-io/peerdb/flow/shared"
	"github.com/PeerDB-io/peerdb/flow/shared/exceptions"
)

const (
	qRepMetadataTableName = "_peerdb_query_replication_metadata"
	ctidColumnName        = "ctid"
)

type QRepPullSink interface {
	ExecuteQueryWithTx(context.Context, *QRepQueryExecutor, pgx.Tx, string, ...any) (int64, int64, error)
}

type QRepSyncSink interface {
	GetColumnNames() ([]string, error)
	CopyInto(context.Context, *PostgresConnector, pgx.Tx, pgx.Identifier) (int64, error)
}

func (c *PostgresConnector) GetQRepPartitions(
	ctx context.Context,
	config *protos.QRepConfig,
	last *protos.QRepPartition,
) ([]*protos.QRepPartition, error) {
	if config.WatermarkColumn == "" || config.NumPartitionsOverride == 1 {
		// if no watermark column is specified, return a single partition
		return []*protos.QRepPartition{
			{
				PartitionId:        uuid.NewString(),
				FullTablePartition: true,
				Range:              nil,
			},
		}, nil
	}

	// begin a transaction
	getPartitionsTx, err := c.conn.BeginTx(ctx, pgx.TxOptions{
		AccessMode: pgx.ReadOnly,
		IsoLevel:   pgx.RepeatableRead,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	// rollback transaction will no-op if transaction is committed
	defer shared.RollbackTx(getPartitionsTx, c.logger)

	if err := c.setTransactionSnapshot(ctx, getPartitionsTx, config.SnapshotName); err != nil {
		return nil, fmt.Errorf("failed to set transaction snapshot: %w", err)
	}

	partitions, err := c.getPartitions(ctx, getPartitionsTx, config, last)
	if err != nil {
		return nil, fmt.Errorf("failed to get partitions: %w", err)
	}

	// commit transaction
	if err := getPartitionsTx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("failed to commit transaction: %w", err)
	}

	return partitions, nil
}

func (c *PostgresConnector) GetDefaultPartitionKeyForTables(
	ctx context.Context,
	input *protos.GetDefaultPartitionKeyForTablesInput,
) (*protos.GetDefaultPartitionKeyForTablesOutput, error) {
	c.logger.Info("Evaluating if tables can perform parallel load")

	output := &protos.GetDefaultPartitionKeyForTablesOutput{
		TableDefaultPartitionKeyMapping: make(map[string]string, len(input.TableMappings)),
	}

	pgVersion, err := shared.GetMajorVersion(ctx, c.conn)
	if err != nil {
		return nil, fmt.Errorf("failed to determine server version: %w", err)
	}
	supportsTidScans := pgVersion >= shared.POSTGRES_14

	if supportsTidScans {
		for _, tm := range input.TableMappings {
			output.TableDefaultPartitionKeyMapping[tm.SourceTableIdentifier] = ctidColumnName
		}
	}

	if !supportsTidScans {
		// older versions fall back to full table partitions anyway; nothing more to do
		c.logger.Warn("Postgres version does not support TID scans, falling back to full table partitions")
		return output, nil
	}

	var hasTimescale bool
	if err := c.conn.QueryRow(ctx,
		"SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'timescaledb')").Scan(&hasTimescale); err != nil {
		return nil, fmt.Errorf("failed to check for timescaledb extension: %w", err)
	}
	if !hasTimescale {
		return output, nil
	}

	// compressed hypertables cannot do ctid scans, so disable for them
	// NOTE: it appears that the hypercore "TAM" may give us access to ctid scans, but that's to be removed in Timescale 2.22
	rows, err := c.conn.Query(ctx, `SELECT DISTINCT hypertable_schema, hypertable_name
		FROM timescaledb_information.chunks
		WHERE is_compressed='t';`)
	if err != nil {
		return nil, fmt.Errorf("query compressed hypertables: %w", err)
	}
	var schema, name string
	if _, err := pgx.ForEachRow(rows, []any{&schema, &name}, func() error {
		if _, ok := output.TableDefaultPartitionKeyMapping[fmt.Sprintf("%s.%s", schema, name)]; ok {
			table := fmt.Sprintf("%s.%s", schema, name)
			delete(output.TableDefaultPartitionKeyMapping, table)
			c.logger.Warn("table is a compressed hypertable, falling back to full table partition",
				slog.String("table", table))
		}
		return nil
	}); err != nil {
		return nil, fmt.Errorf("failed to get compressed hypertables: %w", err)
	}

	return output, nil
}

func (c *PostgresConnector) setTransactionSnapshot(ctx context.Context, tx pgx.Tx, snapshot string) error {
	if snapshot != "" {
		if _, err := tx.Exec(ctx, "SET TRANSACTION SNAPSHOT "+utils.QuoteLiteral(snapshot)); err != nil {
			if shared.IsSQLStateError(err, pgerrcode.UndefinedObject, pgerrcode.InvalidParameterValue) {
				return temporal.NewNonRetryableApplicationError("failed to set transaction snapshot",
					exceptions.ApplicationErrorTypeIrrecoverableInvalidSnapshot.String(), err)
			} else if shared.IsSQLStateErrorSubstring(err,
				pgerrcode.ObjectNotInPrerequisiteState, "could not import the requested snapshot") {
				return temporal.NewNonRetryableApplicationError("failed to set transaction snapshot",
					exceptions.ApplicationErrorTypeIrrecoverableCouldNotImportSnapshot.String(), err)
			}
			return fmt.Errorf("failed to set transaction snapshot: %w", err)
		}
	}

	return nil
}

func (c *PostgresConnector) getPartitions(
	ctx context.Context,
	tx pgx.Tx,
	config *protos.QRepConfig,
	last *protos.QRepPartition,
) ([]*protos.QRepPartition, error) {
	numRowsPerPartition := int64(config.NumRowsPerPartition)
	numPartitions := int64(config.NumPartitionsOverride)
	schemaTable, err := common.ParseTableIdentifier(config.WatermarkTable)
	if err != nil {
		return nil, fmt.Errorf("unable to parse watermark table: %w", err)
	}
	watermarkTable := schemaTable.String()
	watermarkColumn := common.QuoteIdentifier(config.WatermarkColumn)

	var lastRangeEnd any
	if last != nil && last.Range != nil {
		switch lastRange := last.Range.Range.(type) {
		case *protos.PartitionRange_IntRange:
			lastRangeEnd = lastRange.IntRange.End
		case *protos.PartitionRange_UintRange:
			lastRangeEnd = lastRange.UintRange.End
		case *protos.PartitionRange_TimestampRange:
			lastRangeEnd = lastRange.TimestampRange.End.AsTime()
		case *protos.PartitionRange_TidRange:
			lastRangeEnd = pgtype.TID{
				BlockNumber:  lastRange.TidRange.End.BlockNumber,
				OffsetNumber: uint16(lastRange.TidRange.End.OffsetNumber),
				Valid:        true,
			}
		default:
			return nil, fmt.Errorf("unknown range type %T", lastRange)
		}
	}

	partitionParams := PartitionParams{
		tx:              tx,
		watermarkTable:  watermarkTable,
		watermarkColumn: watermarkColumn,
		numPartitions:   numPartitions,
		lastRangeEnd:    lastRangeEnd,
		logger:          c.logger,
	}

	if config.NumPartitionsOverride <= 0 {
		computedNumPartitions, err := ComputeNumPartitions(ctx, partitionParams, numRowsPerPartition)
		if err != nil {
			return nil, err
		}
		partitionParams.numPartitions = computedNumPartitions
	}

	isCTIDWatermarkCol := config.WatermarkColumn == ctidColumnName
	hasPartitionOverride := config.NumPartitionsOverride > 0 // backwards-compatibility with old behavior
	hasCTIDOverride, err := internal.PeerDBPostgresApplyCtidBlockPartitioning(ctx, config.Env)
	if err != nil {
		c.logger.Warn("failed to get CTID partitioning config", slog.Any("error", err))
	}

	var partitionFunc PartitioningFunc
	switch {
	case isCTIDWatermarkCol && (hasCTIDOverride || hasPartitionOverride):
		partitionFunc = CTIDBlockPartitioningFunc
	case hasPartitionOverride:
		partitionFunc = MinMaxRangePartitioningFunc
	default:
		partitionFunc = NTileBucketPartitioningFunc
	}

	c.logger.Info("using partition function", slog.String("partitionFunc", fmt.Sprintf("%T", partitionFunc)))
	return partitionFunc(ctx, partitionParams)
}

func (c *PostgresConnector) getMinMaxValues(
	ctx context.Context,
	tx pgx.Tx,
	config *protos.QRepConfig,
	last *protos.QRepPartition,
) (any, any, error) {
	var minValue, maxValue any
	quotedWatermarkColumn := common.QuoteIdentifier(config.WatermarkColumn)

	parsedWatermarkTable, err := common.ParseTableIdentifier(config.WatermarkTable)
	if err != nil {
		return nil, nil, fmt.Errorf("unable to parse watermark table: %w", err)
	}

	// If there's a last partition, start from its end
	if last != nil && last.Range != nil {
		maxQuery := fmt.Sprintf("SELECT MAX(%[1]s) FROM %[2]s", quotedWatermarkColumn, parsedWatermarkTable.String())
		if err := tx.QueryRow(ctx, maxQuery).Scan(&maxValue); err != nil {
			return nil, nil, fmt.Errorf("failed to query for max value: %w", err)
		} else if maxValue != nil {
			switch lastRange := last.Range.Range.(type) {
			case *protos.PartitionRange_IntRange:
				minValue = lastRange.IntRange.End
				switch v := maxValue.(type) {
				case int16:
					maxValue = int64(v)
				case int32:
					maxValue = int64(v)
				}
			case *protos.PartitionRange_TimestampRange:
				minValue = lastRange.TimestampRange.End.AsTime()
			case *protos.PartitionRange_TidRange:
				minValue = lastRange.TidRange.End
				maxValue = &protos.TID{
					BlockNumber:  maxValue.(pgtype.TID).BlockNumber,
					OffsetNumber: uint32(maxValue.(pgtype.TID).OffsetNumber),
				}
			}
		}
	} else {
		minMaxQuery := fmt.Sprintf("SELECT MIN(%[1]s), MAX(%[1]s) FROM %[2]s", quotedWatermarkColumn, parsedWatermarkTable.String())
		if err := tx.QueryRow(ctx, minMaxQuery).Scan(&minValue, &maxValue); err != nil {
			c.logger.Error("failed to query for min value", slog.String("query", minMaxQuery), slog.Any("error", err))
			return nil, nil, fmt.Errorf("failed to query for min value: %w", err)
		} else if maxValue != nil {
			switch v := minValue.(type) {
			case int16:
				minValue = int64(v)
				maxValue = int64(maxValue.(int16))
			case int32:
				minValue = int64(v)
				maxValue = int64(maxValue.(int32))
			case pgtype.TID:
				minValue = &protos.TID{
					BlockNumber:  v.BlockNumber,
					OffsetNumber: uint32(v.OffsetNumber),
				}
				maxValue = &protos.TID{
					BlockNumber:  maxValue.(pgtype.TID).BlockNumber,
					OffsetNumber: uint32(maxValue.(pgtype.TID).OffsetNumber),
				}
			}
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, nil, fmt.Errorf("failed to commit transaction: %w", err)
	}

	return minValue, maxValue, nil
}

func (c *PostgresConnector) GetMaxValue(
	ctx context.Context,
	config *protos.QRepConfig,
	last *protos.QRepPartition,
) (any, error) {
	checkTx, err := c.conn.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("unable to begin transaction for getting max value: %w", err)
	}
	defer shared.RollbackTx(checkTx, c.logger)

	_, maxValue, err := c.getMinMaxValues(ctx, checkTx, config, last)
	return maxValue, err
}

func (c *PostgresConnector) PullQRepRecords(
	ctx context.Context,
	_otelManager *otel_metrics.OtelManager,
	config *protos.QRepConfig,
	dstType protos.DBType,
	partition *protos.QRepPartition,
	stream *model.QRecordStream,
) (int64, int64, error) {
	return corePullQRepRecords(c, ctx, config, partition, &RecordStreamSink{
		QRecordStream:   stream,
		DestinationType: dstType,
	})
}

func (c *PostgresConnector) PullPgQRepRecords(
	ctx context.Context,
	_otelManager *otel_metrics.OtelManager,
	config *protos.QRepConfig,
	_dstType protos.DBType,
	partition *protos.QRepPartition,
	stream PgCopyWriter,
) (int64, int64, error) {
	return corePullQRepRecords(c, ctx, config, partition, stream)
}

func corePullQRepRecords(
	c *PostgresConnector,
	ctx context.Context,
	config *protos.QRepConfig,
	partition *protos.QRepPartition,
	sink QRepPullSink,
) (int64, int64, error) {
	partitionIdLog := slog.String(string(shared.PartitionIDKey), partition.PartitionId)

	if partition.FullTablePartition {
		c.logger.Info("pulling full table partition", partitionIdLog)
		executor, err := c.NewQRepQueryExecutorSnapshot(ctx, config.Env, config.Version, config.SnapshotName,
			config.FlowJobName, partition.PartitionId)
		if err != nil {
			return 0, 0, fmt.Errorf("failed to create query executor: %w", err)
		}
		return executor.ExecuteQueryIntoSink(ctx, sink, config.Query)
	}
	c.logger.Info("Obtained ranges for partition for PullQRepStream", partitionIdLog)

	var rangeStart any
	var rangeEnd any

	// Depending on the type of the range, convert the range into the correct type
	switch x := partition.Range.Range.(type) {
	case *protos.PartitionRange_IntRange:
		rangeStart = x.IntRange.Start
		rangeEnd = x.IntRange.End
	case *protos.PartitionRange_TimestampRange:
		rangeStart = x.TimestampRange.Start.AsTime()
		rangeEnd = x.TimestampRange.End.AsTime()
	case *protos.PartitionRange_TidRange:
		rangeStart = pgtype.TID{
			BlockNumber:  x.TidRange.Start.BlockNumber,
			OffsetNumber: uint16(x.TidRange.Start.OffsetNumber),
			Valid:        true,
		}
		rangeEnd = pgtype.TID{
			BlockNumber:  x.TidRange.End.BlockNumber,
			OffsetNumber: uint16(x.TidRange.End.OffsetNumber),
			Valid:        true,
		}
	default:
		return 0, 0, fmt.Errorf("unknown range type: %v", x)
	}

	// Build the query to pull records within the range from the source table
	// Be sure to order the results by the watermark column to ensure consistency across pulls
	query, err := BuildQuery(c.logger, config.Query, config.FlowJobName)
	if err != nil {
		return 0, 0, err
	}

	executor, err := c.NewQRepQueryExecutorSnapshot(
		ctx, config.Env, config.Version, config.SnapshotName, config.FlowJobName, partition.PartitionId)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to create query executor: %w", err)
	}

	numRecords, numBytes, err := executor.ExecuteQueryIntoSink(ctx, sink, query, rangeStart, rangeEnd)
	if err != nil {
		return numRecords, numBytes, err
	}

	c.logger.Info(fmt.Sprintf("pulled %d records", numRecords),
		partitionIdLog,
		slog.Int64("records", numRecords),
		slog.Int64("bytes", numBytes))
	return numRecords, numBytes, nil
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
		stagingTableName := "_peerdb_staging_" + shared.RandomString(8)
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

func (c *PostgresConnector) PullXminRecordStream(
	ctx context.Context,
	config *protos.QRepConfig,
	dstType protos.DBType,
	partition *protos.QRepPartition,
	stream *model.QRecordStream,
) (int64, int64, int64, error) {
	return pullXminRecordStream(c, ctx, config, partition, RecordStreamSink{
		QRecordStream:   stream,
		DestinationType: dstType,
	})
}

func (c *PostgresConnector) PullXminPgRecordStream(
	ctx context.Context,
	config *protos.QRepConfig,
	_dstType protos.DBType,
	partition *protos.QRepPartition,
	pipe PgCopyWriter,
) (int64, int64, int64, error) {
	return pullXminRecordStream(c, ctx, config, partition, pipe)
}

func pullXminRecordStream(
	c *PostgresConnector,
	ctx context.Context,
	config *protos.QRepConfig,
	partition *protos.QRepPartition,
	sink QRepPullSink,
) (int64, int64, int64, error) {
	query := config.Query
	var queryArgs []any
	if partition.Range != nil {
		query += " WHERE age(xmin) > 0 AND age(xmin) <= age($1::xid)"
		queryArgs = []any{strconv.FormatInt(partition.Range.Range.(*protos.PartitionRange_IntRange).IntRange.Start&0xffffffff, 10)}
	}

	executor, err := c.NewQRepQueryExecutorSnapshot(ctx, config.Env, config.Version, config.SnapshotName,
		config.FlowJobName, partition.PartitionId)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("failed to create query executor: %w", err)
	}

	numRecords, numBytes, currentSnapshotXmin, err := executor.ExecuteQueryIntoSinkGettingCurrentSnapshotXmin(
		ctx,
		sink,
		query,
		queryArgs...,
	)
	if err != nil {
		return numRecords, numBytes, currentSnapshotXmin, err
	}

	c.logger.Info(fmt.Sprintf("pulled %d records", numRecords))
	return numRecords, numBytes, currentSnapshotXmin, nil
}

func BuildQuery(logger log.Logger, query string, flowJobName string) (string, error) {
	tmpl, err := template.New("query").Parse(query)
	if err != nil {
		return "", err
	}

	buf := new(bytes.Buffer)
	if err := tmpl.Execute(buf, map[string]string{
		"start": "$1",
		"end":   "$2",
	}); err != nil {
		return "", err
	}
	res := buf.String()

	logger.Info("[pg] templated query", slog.String("query", res))
	return res, nil
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
