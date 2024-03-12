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
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"go.temporal.io/sdk/log"

	"github.com/PeerDB-io/peer-flow/connectors/utils"
	partition_utils "github.com/PeerDB-io/peer-flow/connectors/utils/partition"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/logger"
	"github.com/PeerDB-io/peer-flow/model"
	"github.com/PeerDB-io/peer-flow/shared"
)

const (
	qRepMetadataTableName        = "_peerdb_query_replication_metadata"
	QRepOverwriteTempTablePrefix = "_peerdb_overwrite_"
)

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
	tx, err := c.conn.BeginTx(ctx, pgx.TxOptions{
		AccessMode: pgx.ReadOnly,
		IsoLevel:   pgx.RepeatableRead,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() {
		deferErr := tx.Rollback(ctx)
		if deferErr != pgx.ErrTxClosed && deferErr != nil {
			c.logger.Error("error rolling back transaction for get partitions", slog.Any("error", deferErr))
		}
	}()

	err = c.setTransactionSnapshot(ctx, tx)
	if err != nil {
		return nil, fmt.Errorf("failed to set transaction snapshot: %w", err)
	}

	// TODO re-enable locking of the watermark table.
	// // lock the table while we get the partitions.
	// lockQuery := fmt.Sprintf("LOCK %s IN EXCLUSIVE MODE", config.WatermarkTable)
	// if _, err = tx.Exec(c.ctx, lockQuery); err != nil {
	// 	// if we aren't able to lock, just log the error and continue
	// 	log.Warnf("failed to lock table %s: %v", config.WatermarkTable, err)
	// }

	return c.getNumRowsPartitions(ctx, tx, config, last)
}

func (c *PostgresConnector) setTransactionSnapshot(ctx context.Context, tx pgx.Tx) error {
	snapshot := c.config.TransactionSnapshot
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
	var err error
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
	if err = row.Scan(&totalRows); err != nil {
		return nil, fmt.Errorf("failed to query for total rows: %w", err)
	}

	if totalRows.Int64 == 0 {
		c.logger.Warn("no records to replicate, returning")
		return make([]*protos.QRepPartition, 0), nil
	}

	// Calculate the number of partitions
	numPartitions := totalRows.Int64 / numRowsPerPartition
	if totalRows.Int64%numRowsPerPartition != 0 {
		numPartitions++
	}
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
		c.logger.Error(fmt.Sprintf("failed to query for partitions: %v", err))
		return nil, fmt.Errorf("failed to query for partitions: %w", err)
	}
	defer rows.Close()

	partitionHelper := partition_utils.NewPartitionHelper()
	for rows.Next() {
		var bucket pgtype.Int8
		var start, end interface{}
		if err := rows.Scan(&bucket, &start, &end); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}

		err = partitionHelper.AddPartition(start, end)
		if err != nil {
			return nil, fmt.Errorf("failed to add partition: %w", err)
		}
	}

	err = rows.Err()
	if err != nil {
		return nil, fmt.Errorf("failed to read rows: %w", err)
	}

	err = tx.Commit(ctx)
	if err != nil {
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

	// Get the maximum value from the database
	maxQuery := fmt.Sprintf("SELECT MAX(%[1]s) FROM %[2]s", quotedWatermarkColumn, parsedWatermarkTable.String())
	row := tx.QueryRow(ctx, maxQuery)
	if err := row.Scan(&maxValue); err != nil {
		return nil, nil, fmt.Errorf("failed to query for max value: %w", err)
	}

	if last != nil && last.Range != nil {
		// If there's a last partition, start from its end
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
	} else {
		// Otherwise get the minimum value from the database
		minQuery := fmt.Sprintf("SELECT MIN(%[1]s) FROM %[2]s", quotedWatermarkColumn, parsedWatermarkTable.String())
		row := tx.QueryRow(ctx, minQuery)
		if err := row.Scan(&minValue); err != nil {
			c.logger.Error(fmt.Sprintf("failed to query [%s] for min value: %v", minQuery, err))
			return nil, nil, fmt.Errorf("failed to query for min value: %w", err)
		}

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

	err = tx.Commit(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to commit transaction: %w", err)
	}

	return minValue, maxValue, nil
}

func (c *PostgresConnector) CheckForUpdatedMaxValue(
	ctx context.Context,
	config *protos.QRepConfig,
	last *protos.QRepPartition,
) (bool, error) {
	tx, err := c.conn.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("unable to begin transaction for getting max value: %w", err)
	}
	defer func() {
		deferErr := tx.Rollback(ctx)
		if deferErr != pgx.ErrTxClosed && deferErr != nil {
			c.logger.Error("error rolling back transaction for getting max value", "error", err)
		}
	}()

	_, maxValue, err := c.getMinMaxValues(ctx, tx, config, last)
	if err != nil {
		return false, fmt.Errorf("error while getting min and max values: %w", err)
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
) (*model.QRecordBatch, error) {
	partitionIdLog := slog.String(string(shared.PartitionIDKey), partition.PartitionId)
	if partition.FullTablePartition {
		c.logger.Info("pulling full table partition", partitionIdLog)
		executor := c.NewQRepQueryExecutorSnapshot(c.config.TransactionSnapshot,
			config.FlowJobName, partition.PartitionId)
		query := config.Query
		return executor.ExecuteAndProcessQuery(ctx, query)
	}

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
		return nil, fmt.Errorf("unknown range type: %v", x)
	}
	c.logger.Info("Obtained ranges for partition for PullQRep", partitionIdLog)

	// Build the query to pull records within the range from the source table
	// Be sure to order the results by the watermark column to ensure consistency across pulls
	query, err := BuildQuery(c.logger, config.Query, config.FlowJobName)
	if err != nil {
		return nil, err
	}

	executor := c.NewQRepQueryExecutorSnapshot(c.config.TransactionSnapshot,
		config.FlowJobName, partition.PartitionId)

	records, err := executor.ExecuteAndProcessQuery(ctx, query, rangeStart, rangeEnd)
	if err != nil {
		return nil, err
	}

	return records, nil
}

func (c *PostgresConnector) PullQRepRecordStream(
	ctx context.Context,
	config *protos.QRepConfig,
	partition *protos.QRepPartition,
	stream *model.QRecordStream,
) (int, error) {
	partitionIdLog := slog.String(string(shared.PartitionIDKey), partition.PartitionId)
	if partition.FullTablePartition {
		c.logger.Info("pulling full table partition", partitionIdLog)
		executor := c.NewQRepQueryExecutorSnapshot(c.config.TransactionSnapshot,
			config.FlowJobName, partition.PartitionId)

		query := config.Query
		_, err := executor.ExecuteAndProcessQueryStream(ctx, stream, query)
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

	executor := c.NewQRepQueryExecutorSnapshot(c.config.TransactionSnapshot,
		config.FlowJobName, partition.PartitionId)

	numRecords, err := executor.ExecuteAndProcessQueryStream(ctx, stream, query, rangeStart, rangeEnd)
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
	dstTable, err := utils.ParseSchemaTable(partition.TableNameMapping.DestinationTableName)
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

	done, err := c.isPartitionSynced(ctx, partition.PartitionId)
	if err != nil {
		return 0, fmt.Errorf("failed to check if partition is synced: %w", err)
	}

	if done {
		c.logger.Info(fmt.Sprintf("partition %s already synced", partition.PartitionId))
		return 0, nil
	}
	c.logger.Info("SyncRecords called and initial checks complete.")

	stagingTableSync := &QRepStagingTableSync{connector: c}
	return stagingTableSync.SyncQRepRecords(ctx,
		config.FlowJobName, dstTable, partition, stream,
		config.WriteMode, config.SyncedAtColName)
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
	_, err = c.conn.Exec(ctx, createQRepMetadataTableSQL)
	if err != nil && !utils.IsUniqueError(err) {
		return fmt.Errorf("failed to create table %s: %w", qRepMetadataTableName, err)
	}
	c.logger.Info("Setup metadata table.")

	if config.WriteMode != nil &&
		config.WriteMode.WriteType == protos.QRepWriteType_QREP_WRITE_MODE_OVERWRITE &&
		config.SourcePeer.Type != protos.DBType_SNOWFLAKE {
		_, err = c.conn.Exec(ctx,
			"TRUNCATE TABLE "+config.DestinationTableIdentifier)
		if err != nil {
			return fmt.Errorf("failed to TRUNCATE table before query replication: %w", err)
		}
	}

	return nil
}

func (c *PostgresConnector) PullXminRecordStream(
	ctx context.Context,
	config *protos.QRepConfig,
	partition *protos.QRepPartition,
	stream *model.QRecordStream,
) (int, int64, error) {
	var currentSnapshotXmin int64
	query := config.Query
	oldxid := ""
	if partition.Range != nil {
		oldxid = strconv.FormatInt(partition.Range.Range.(*protos.PartitionRange_IntRange).IntRange.Start&0xffffffff, 10)
		query += " WHERE age(xmin) > 0 AND age(xmin) <= age($1::xid)"
	}

	executor := c.NewQRepQueryExecutorSnapshot(c.config.TransactionSnapshot,
		config.FlowJobName, partition.PartitionId)

	var err error
	var numRecords int
	if partition.Range != nil {
		numRecords, currentSnapshotXmin, err = executor.ExecuteAndProcessQueryStreamGettingCurrentSnapshotXmin(
			ctx,
			stream,
			query,
			oldxid,
		)
	} else {
		numRecords, currentSnapshotXmin, err = executor.ExecuteAndProcessQueryStreamGettingCurrentSnapshotXmin(
			ctx,
			stream,
			query,
		)
	}
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

	data := map[string]interface{}{
		"start": "$1",
		"end":   "$2",
	}

	buf := new(bytes.Buffer)

	err = tmpl.Execute(buf, data)
	if err != nil {
		return "", err
	}
	res := buf.String()

	logger.Info("templated query: " + res)
	return res, nil
}

// isPartitionSynced checks whether a specific partition is synced
func (c *PostgresConnector) isPartitionSynced(ctx context.Context, partitionID string) (bool, error) {
	// setup the query string
	metadataTableIdentifier := pgx.Identifier{c.metadataSchema, qRepMetadataTableName}
	queryString := fmt.Sprintf(
		"SELECT COUNT(*)>0 FROM %s WHERE partitionID=$1;",
		metadataTableIdentifier.Sanitize(),
	)

	// prepare and execute the query
	var result bool
	err := c.conn.QueryRow(ctx, queryString, partitionID).Scan(&result)
	if err != nil {
		return false, fmt.Errorf("failed to execute query: %w", err)
	}

	return result, nil
}

func (c *PostgresConnector) ConsolidateQRepPartitions(ctx context.Context, config *protos.QRepConfig) error {
	if config.SourcePeer.Type != protos.DBType_SNOWFLAKE ||
		config.WriteMode.WriteType != protos.QRepWriteType_QREP_WRITE_MODE_OVERWRITE {
		return nil
	}

	destinationTables := strings.Split(config.DestinationTableIdentifier, ";")

	constraintsHookExists := true
	_, err := c.conn.Exec(ctx, fmt.Sprintf("SELECT '_peerdb_post_run_hook_%s()'::regprocedure",
		config.FlowJobName))
	if err != nil {
		constraintsHookExists = false
	}

	tx, err := c.conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() {
		if err := tx.Rollback(ctx); err != nil {
			if err != pgx.ErrTxClosed {
				logger.LoggerFromCtx(ctx).Error("unexpected error during rollback transaction for consolidation tx",
					slog.Any("error", err))
			}
		}
	}()

	_, err = tx.Exec(ctx, "SET statement_timeout=0")
	if err != nil {
		return fmt.Errorf("failed to set statement_timeout: %w", err)
	}
	_, err = tx.Exec(ctx, "SET idle_in_transaction_session_timeout=0")
	if err != nil {
		return fmt.Errorf("failed to set idle_in_transaction_session_timeout: %w", err)
	}
	_, err = tx.Exec(ctx, "SET lock_timeout=0")
	if err != nil {
		return fmt.Errorf("failed to set lock_timeout: %w", err)
	}

	for _, dstTableName := range destinationTables {
		dstSchemaTable, err := utils.ParseSchemaTable(dstTableName)
		if err != nil {
			return fmt.Errorf("failed to parse destination table identifier: %w", err)
		}
		dstTableIdentifier := pgx.Identifier{dstSchemaTable.Schema, dstSchemaTable.Table}
		tempTableIdentifier := pgx.Identifier{dstSchemaTable.Schema, QRepOverwriteTempTablePrefix + dstSchemaTable.Table}

		_, err = tx.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s CASCADE", dstTableIdentifier.Sanitize()))
		if err != nil {
			return fmt.Errorf("failed to drop %s: %v", dstTableName, err)
		}
		_, err = tx.Exec(ctx, fmt.Sprintf("ALTER TABLE %s RENAME TO %s",
			tempTableIdentifier.Sanitize(), QuoteIdentifier(dstSchemaTable.Table)))
		if err != nil {
			return fmt.Errorf("failed to rename %s to %s: %v",
				tempTableIdentifier.Sanitize(), dstTableIdentifier.Sanitize(), err)
		}

		if config.SyncedAtColName != "" {
			updateSyncedAtStmt := fmt.Sprintf(
				`UPDATE %s SET %s = CURRENT_TIMESTAMP`,
				dstTableIdentifier.Sanitize(),
				QuoteIdentifier(config.SyncedAtColName),
			)
			_, err = tx.Exec(ctx, updateSyncedAtStmt)
			if err != nil {
				return fmt.Errorf("failed to update synced_at column: %v", err)
			}
		}
	}

	if constraintsHookExists {
		c.logger.Info("executing constraints hook", slog.String("procName",
			fmt.Sprintf("_peerdb_post_run_hook_%s()", config.FlowJobName)))
		_, err = tx.Exec(ctx, fmt.Sprintf("SELECT _peerdb_post_run_hook_%s()", config.FlowJobName))
		if err != nil {
			return fmt.Errorf("failed to execute stored procedure for applying constraints: %w", err)
		}
	} else {
		c.logger.Info("no constraints hook found", slog.String("procName",
			fmt.Sprintf("_peerdb_post_run_hook_%s()", config.FlowJobName)))
	}

	err = tx.Commit(ctx)
	if err != nil {
		return fmt.Errorf("failed to commit transaction for consolidation: %w", err)
	}

	return nil
}

func (c *PostgresConnector) CleanupQRepFlow(ctx context.Context, config *protos.QRepConfig) error {
	// we don't need to clean anything
	return nil
}
