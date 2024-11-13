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
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/PeerDB-io/peer-flow/connectors/utils"
	partition_utils "github.com/PeerDB-io/peer-flow/connectors/utils/partition"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model"
	"github.com/PeerDB-io/peer-flow/shared"
)

const qRepMetadataTableName = "_peerdb_query_replication_metadata"

type QRepPullSink interface {
	Close(error)
	ExecuteQueryWithTx(context.Context, *QRepQueryExecutor, pgx.Tx, string, ...interface{}) (int, error)
}

type QRepSyncSink interface {
	GetColumnNames() []string
	CopyInto(context.Context, *PostgresConnector, pgx.Tx, pgx.Identifier) (int64, error)
}

func (c *PostgresConnector) GetQRepPartitions(
	ctx context.Context,
	config *protos.QRepConfig,
	last *protos.QRepPartition,
) ([]*protos.QRepPartition, error) {
	if config.WatermarkColumn == "" {
		// if no watermark column is specified, return a single partition
		partition := &protos.QRepPartition{
			PartitionId:        uuid.New().String(),
			FullTablePartition: true,
			Range:              nil,
		}
		return []*protos.QRepPartition{partition}, nil
	}

	// begin a transaction
	getPartitionsTx, err := c.conn.BeginTx(ctx, pgx.TxOptions{
		AccessMode: pgx.ReadOnly,
		IsoLevel:   pgx.RepeatableRead,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer shared.RollbackTx(getPartitionsTx, c.logger)

	if err := c.setTransactionSnapshot(ctx, getPartitionsTx, config.SnapshotName); err != nil {
		return nil, fmt.Errorf("failed to set transaction snapshot: %w", err)
	}

	return c.getNumRowsPartitions(ctx, getPartitionsTx, config, last)
}

func (c *PostgresConnector) setTransactionSnapshot(ctx context.Context, tx pgx.Tx, snapshot string) error {
	if snapshot != "" {
		if _, err := tx.Exec(ctx, "SET TRANSACTION SNAPSHOT "+QuoteLiteral(snapshot)); err != nil {
			return fmt.Errorf("failed to set transaction snapshot: %w", err)
		}
	}

	return nil
}

func (c *PostgresConnector) getNumRowsPartitions(
	ctx context.Context,
	tx pgx.Tx,
	config *protos.QRepConfig,
	last *protos.QRepPartition,
) ([]*protos.QRepPartition, error) {
	numRowsPerPartition := int64(config.NumRowsPerPartition)
	quotedWatermarkColumn := QuoteIdentifier(config.WatermarkColumn)

	whereClause := ""
	if last != nil && last.Range != nil {
		whereClause = fmt.Sprintf(`WHERE %s > $1`, quotedWatermarkColumn)
	}

	parsedWatermarkTable, err := utils.ParseSchemaTable(config.WatermarkTable)
	if err != nil {
		return nil, fmt.Errorf("unable to parse watermark table: %w", err)
	}

	// Query to get the total number of rows in the table
	countQuery := fmt.Sprintf(`SELECT COUNT(*) FROM %s %s`, parsedWatermarkTable.String(), whereClause)
	var row pgx.Row
	var minVal interface{} = nil
	if last != nil && last.Range != nil {
		switch lastRange := last.Range.Range.(type) {
		case *protos.PartitionRange_IntRange:
			minVal = lastRange.IntRange.End
		case *protos.PartitionRange_TimestampRange:
			minVal = lastRange.TimestampRange.End.AsTime()
		}

		row = tx.QueryRow(ctx, countQuery, minVal)
	} else {
		row = tx.QueryRow(ctx, countQuery)
	}

	var totalRows pgtype.Int8
	if err := row.Scan(&totalRows); err != nil {
		return nil, fmt.Errorf("failed to query for total rows: %w", err)
	}

	if totalRows.Int64 == 0 {
		c.logger.Warn("no records to replicate, returning")
		return nil, nil
	}

	// Calculate the number of partitions
	numPartitions := shared.DivCeil(totalRows.Int64, numRowsPerPartition)
	c.logger.Info(fmt.Sprintf("total rows: %d, num partitions: %d, num rows per partition: %d",
		totalRows.Int64, numPartitions, numRowsPerPartition))

	// Query to get partitions using window functions
	var rows pgx.Rows
	if minVal != nil {
		partitionsQuery := fmt.Sprintf(
			`SELECT bucket, MIN(%[2]s) AS start, MAX(%[2]s) AS end
			FROM (
					SELECT NTILE(%[1]d) OVER (ORDER BY %[2]s) AS bucket, %[2]s
					FROM %[3]s WHERE %[2]s > $1
			) subquery
			GROUP BY bucket
			ORDER BY start
			`,
			numPartitions,
			quotedWatermarkColumn,
			parsedWatermarkTable.String(),
		)
		c.logger.Info("[row_based_next] partitions query: " + partitionsQuery)
		rows, err = tx.Query(ctx, partitionsQuery, minVal)
	} else {
		partitionsQuery := fmt.Sprintf(
			`SELECT bucket, MIN(%[2]s) AS start, MAX(%[2]s) AS end
			FROM (
					SELECT NTILE(%[1]d) OVER (ORDER BY %[2]s) AS bucket, %[2]s FROM %[3]s
			) subquery
			GROUP BY bucket
			ORDER BY start
			`,
			numPartitions,
			quotedWatermarkColumn,
			parsedWatermarkTable.String(),
		)
		c.logger.Info("[row_based] partitions query: " + partitionsQuery)
		rows, err = tx.Query(ctx, partitionsQuery)
	}
	if err != nil {
		return nil, shared.LogError(c.logger, fmt.Errorf("failed to query for partitions: %w", err))
	}
	defer rows.Close()

	partitionHelper := partition_utils.NewPartitionHelper()
	for rows.Next() {
		var bucket pgtype.Int8
		var start, end interface{}
		if err := rows.Scan(&bucket, &start, &end); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}

		if err := partitionHelper.AddPartition(start, end); err != nil {
			return nil, fmt.Errorf("failed to add partition: %w", err)
		}
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to read rows: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("failed to commit transaction: %w", err)
	}

	return partitionHelper.GetPartitions(), nil
}

func (c *PostgresConnector) getMinMaxValues(
	ctx context.Context,
	tx pgx.Tx,
	config *protos.QRepConfig,
	last *protos.QRepPartition,
) (interface{}, interface{}, error) {
	var minValue, maxValue interface{}
	quotedWatermarkColumn := QuoteIdentifier(config.WatermarkColumn)

	parsedWatermarkTable, err := utils.ParseSchemaTable(config.WatermarkTable)
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

func (c *PostgresConnector) CheckForUpdatedMaxValue(
	ctx context.Context,
	config *protos.QRepConfig,
	last *protos.QRepPartition,
) (bool, error) {
	checkTx, err := c.conn.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("unable to begin transaction for getting max value: %w", err)
	}
	defer shared.RollbackTx(checkTx, c.logger)

	_, maxValue, err := c.getMinMaxValues(ctx, checkTx, config, last)
	if err != nil {
		return false, fmt.Errorf("error while getting min and max values: %w", err)
	}

	if maxValue == nil || last == nil || last.Range == nil {
		return maxValue != nil, nil
	}

	switch x := last.Range.Range.(type) {
	case *protos.PartitionRange_IntRange:
		if maxValue.(int64) > x.IntRange.End {
			return true, nil
		}
	case *protos.PartitionRange_TimestampRange:
		if maxValue.(time.Time).After(x.TimestampRange.End.AsTime()) {
			return true, nil
		}
	default:
		return false, fmt.Errorf("unknown range type: %v", x)
	}

	return false, nil
}

func (c *PostgresConnector) PullQRepRecords(
	ctx context.Context,
	config *protos.QRepConfig,
	partition *protos.QRepPartition,
	stream *model.QRecordStream,
) (int, error) {
	return corePullQRepRecords(c, ctx, config, partition, &RecordStreamSink{
		QRecordStream: stream,
	})
}

func (c *PostgresConnector) PullPgQRepRecords(
	ctx context.Context,
	config *protos.QRepConfig,
	partition *protos.QRepPartition,
	stream PgCopyWriter,
) (int, error) {
	return corePullQRepRecords(c, ctx, config, partition, stream)
}

func corePullQRepRecords(
	c *PostgresConnector,
	ctx context.Context,
	config *protos.QRepConfig,
	partition *protos.QRepPartition,
	sink QRepPullSink,
) (int, error) {
	partitionIdLog := slog.String(string(shared.PartitionIDKey), partition.PartitionId)

	if partition.FullTablePartition {
		c.logger.Info("pulling full table partition", partitionIdLog)
		executor, err := c.NewQRepQueryExecutorSnapshot(ctx, config.SnapshotName,
			config.FlowJobName, partition.PartitionId)
		if err != nil {
			return 0, fmt.Errorf("failed to create query executor: %w", err)
		}
		_, err = executor.ExecuteQueryIntoSink(ctx, sink, config.Query)
		return 0, err
	}
	c.logger.Info("Obtained ranges for partition for PullQRepStream", partitionIdLog)

	var rangeStart interface{}
	var rangeEnd interface{}

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
		return 0, fmt.Errorf("unknown range type: %v", x)
	}

	// Build the query to pull records within the range from the source table
	// Be sure to order the results by the watermark column to ensure consistency across pulls
	query, err := BuildQuery(c.logger, config.Query, config.FlowJobName)
	if err != nil {
		return 0, err
	}

	executor, err := c.NewQRepQueryExecutorSnapshot(ctx, config.SnapshotName, config.FlowJobName,
		partition.PartitionId)
	if err != nil {
		return 0, fmt.Errorf("failed to create query executor: %w", err)
	}

	numRecords, err := executor.ExecuteQueryIntoSink(ctx, sink, query, rangeStart, rangeEnd)
	if err != nil {
		return 0, err
	}

	c.logger.Info(fmt.Sprintf("pulled %d records", numRecords), partitionIdLog)
	return numRecords, nil
}

func (c *PostgresConnector) SyncQRepRecords(
	ctx context.Context,
	config *protos.QRepConfig,
	partition *protos.QRepPartition,
	stream *model.QRecordStream,
) (int, error) {
	return syncQRepRecords(c, ctx, config, partition, RecordStreamSink{
		QRecordStream: stream,
	})
}

func (c *PostgresConnector) SyncPgQRepRecords(
	ctx context.Context,
	config *protos.QRepConfig,
	partition *protos.QRepPartition,
	pipe PgCopyReader,
) (int, error) {
	return syncQRepRecords(c, ctx, config, partition, pipe)
}

func syncQRepRecords(
	c *PostgresConnector,
	ctx context.Context,
	config *protos.QRepConfig,
	partition *protos.QRepPartition,
	sink QRepSyncSink,
) (int, error) {
	dstTable, err := utils.ParseSchemaTable(config.DestinationTableIdentifier)
	if err != nil {
		return 0, fmt.Errorf("failed to parse destination table identifier: %w", err)
	}

	exists, err := c.tableExists(ctx, dstTable)
	if err != nil {
		return 0, fmt.Errorf("failed to check if table exists: %w", err)
	}

	if !exists {
		return 0, fmt.Errorf("table %s does not exist, used schema: %s", dstTable.Table, dstTable.Schema)
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
		return 0, fmt.Errorf("failed to create tx pool: %w", err)
	}
	defer txConn.Close(ctx)

	if err := shared.RegisterHStore(ctx, txConn); err != nil {
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
				shared.LoggerFromCtx(ctx).Error("failed to rollback transaction tx2", slog.Any("error", err), syncLog)
			}
		}
	}()

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
				return -1, fmt.Errorf("failed to TRUNCATE table before copy: %w", err)
			}
		}

		numRowsSynced, err = sink.CopyInto(
			ctx,
			c,
			tx,
			pgx.Identifier{dstTable.Schema, dstTable.Table},
		)
		if err != nil {
			return -1, fmt.Errorf("failed to copy records into destination table: %w", err)
		}

		if syncedAtCol != "" {
			updateSyncedAtStmt := fmt.Sprintf(
				`UPDATE %s SET %s = CURRENT_TIMESTAMP WHERE %s IS NULL;`,
				pgx.Identifier{dstTable.Schema, dstTable.Table}.Sanitize(),
				QuoteIdentifier(syncedAtCol),
				QuoteIdentifier(syncedAtCol),
			)
			_, err = tx.Exec(ctx, updateSyncedAtStmt)
			if err != nil {
				return -1, fmt.Errorf("failed to update synced_at column: %w", err)
			}
		}
	} else {
		// Step 2.1: Create a temp staging table
		stagingTableName := "_peerdb_staging_" + shared.RandomString(8)
		stagingTableIdentifier := pgx.Identifier{stagingTableName}
		dstTableIdentifier := pgx.Identifier{dstTable.Schema, dstTable.Table}

		// From PG docs: The cost of setting a large value in sessions that do not actually need many
		// temporary buffers is only a buffer descriptor, or about 64 bytes, per increment in temp_buffers.
		_, err = tx.Exec(ctx, "SET temp_buffers = '4GB';")
		if err != nil {
			return -1, fmt.Errorf("failed to set temp_buffers: %w", err)
		}

		createStagingTableStmt := fmt.Sprintf(
			"CREATE TEMP TABLE %s (LIKE %s) ON COMMIT DROP;",
			stagingTableIdentifier.Sanitize(),
			dstTableIdentifier.Sanitize(),
		)

		c.logger.Info(fmt.Sprintf("Creating staging table %s - '%s'",
			stagingTableName, createStagingTableStmt), syncLog)
		_, err = c.execWithLoggingTx(ctx, createStagingTableStmt, tx)
		if err != nil {
			return -1, fmt.Errorf("failed to create staging table: %w", err)
		}

		// Step 2.2: Insert records into the staging table
		numRowsSynced, err = sink.CopyInto(
			ctx,
			c,
			tx,
			stagingTableIdentifier,
		)
		if err != nil {
			return -1, fmt.Errorf("failed to copy records into staging table: %w", err)
		}

		// construct the SET clause for the upsert operation
		upsertMatchColsList := writeMode.UpsertKeyColumns
		upsertMatchCols := make(map[string]struct{}, len(upsertMatchColsList))
		for _, col := range upsertMatchColsList {
			upsertMatchCols[col] = struct{}{}
		}

		columnNames := sink.GetColumnNames()
		setClauseArray := make([]string, 0, len(upsertMatchColsList)+1)
		selectStrArray := make([]string, 0, len(columnNames))
		for _, col := range columnNames {
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
		c.logger.Info("Performing upsert operation", slog.String("upsertStmt", upsertStmt), syncLog)
		_, err := tx.Exec(ctx, upsertStmt)
		if err != nil {
			return -1, fmt.Errorf("failed to perform upsert operation: %w", err)
		}
	}

	c.logger.Info(fmt.Sprintf("pushed %d records to %s", numRowsSynced, dstTable), syncLog)

	// marshal the partition to json using protojson
	pbytes, err := protojson.Marshal(partition)
	if err != nil {
		return -1, fmt.Errorf("failed to marshal partition to json: %w", err)
	}

	metadataTableIdentifier := pgx.Identifier{c.metadataSchema, qRepMetadataTableName}
	insertMetadataStmt := fmt.Sprintf(
		"INSERT INTO %s VALUES ($1, $2, $3, $4, $5);",
		metadataTableIdentifier.Sanitize(),
	)
	c.logger.Info("Executing transaction inside QRep sync", syncLog)
	_, err = tx.Exec(
		ctx,
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

	if err := tx.Commit(ctx); err != nil {
		return -1, fmt.Errorf("failed to commit transaction: %w", err)
	}

	c.logger.Info(fmt.Sprintf("pushed %d records to %s", numRowsSynced, dstTable), syncLog)
	return int(numRowsSynced), nil
}

// SetupQRepMetadataTables function for postgres connector
func (c *PostgresConnector) SetupQRepMetadataTables(ctx context.Context, config *protos.QRepConfig) error {
	err := c.createMetadataSchema(ctx)
	if err != nil {
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
	_, err = c.execWithLogging(ctx, createQRepMetadataTableSQL)
	if err != nil && !shared.IsSQLStateError(err, pgerrcode.UniqueViolation) {
		return fmt.Errorf("failed to create table %s: %w", qRepMetadataTableName, err)
	}
	c.logger.Info("Setup metadata table.")

	return nil
}

func (c *PostgresConnector) PullXminRecordStream(
	ctx context.Context,
	config *protos.QRepConfig,
	partition *protos.QRepPartition,
	stream *model.QRecordStream,
) (int, int64, error) {
	return pullXminRecordStream(c, ctx, config, partition, RecordStreamSink{
		QRecordStream: stream,
	})
}

func (c *PostgresConnector) PullXminPgRecordStream(
	ctx context.Context,
	config *protos.QRepConfig,
	partition *protos.QRepPartition,
	pipe PgCopyWriter,
) (int, int64, error) {
	return pullXminRecordStream(c, ctx, config, partition, pipe)
}

func pullXminRecordStream(
	c *PostgresConnector,
	ctx context.Context,
	config *protos.QRepConfig,
	partition *protos.QRepPartition,
	sink QRepPullSink,
) (int, int64, error) {
	query := config.Query
	var queryArgs []interface{}
	if partition.Range != nil {
		query += " WHERE age(xmin) > 0 AND age(xmin) <= age($1::xid)"
		queryArgs = []interface{}{strconv.FormatInt(partition.Range.Range.(*protos.PartitionRange_IntRange).IntRange.Start&0xffffffff, 10)}
	}

	executor, err := c.NewQRepQueryExecutorSnapshot(ctx, config.SnapshotName,
		config.FlowJobName, partition.PartitionId)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to create query executor: %w", err)
	}

	numRecords, currentSnapshotXmin, err := executor.ExecuteQueryIntoSinkGettingCurrentSnapshotXmin(
		ctx,
		sink,
		query,
		queryArgs...,
	)
	if err != nil {
		return 0, currentSnapshotXmin, err
	}

	c.logger.Info(fmt.Sprintf("pulled %d records", numRecords))
	return numRecords, currentSnapshotXmin, nil
}

func BuildQuery(logger log.Logger, query string, flowJobName string) (string, error) {
	tmpl, err := template.New("query").Parse(query)
	if err != nil {
		return "", err
	}

	buf := new(bytes.Buffer)
	if err := tmpl.Execute(buf, map[string]interface{}{
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
	err := c.conn.QueryRow(ctx, queryString, req.PartitionId).Scan(&result)
	if err != nil {
		return false, fmt.Errorf("failed to execute query: %w", err)
	}

	return result, nil
}
