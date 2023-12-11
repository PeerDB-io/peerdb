package connpostgres

import (
	"bytes"
	"fmt"
	"strconv"
	"text/template"
	"time"

	"github.com/PeerDB-io/peer-flow/connectors/utils"
	partition_utils "github.com/PeerDB-io/peer-flow/connectors/utils/partition"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	log "github.com/sirupsen/logrus"
)

const qRepMetadataTableName = "_peerdb_query_replication_metadata"

func (c *PostgresConnector) GetQRepPartitions(
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
	tx, err := c.pool.BeginTx(c.ctx, pgx.TxOptions{
		AccessMode: pgx.ReadOnly,
		IsoLevel:   pgx.RepeatableRead,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() {
		deferErr := tx.Rollback(c.ctx)
		if deferErr != pgx.ErrTxClosed && deferErr != nil {
			log.WithFields(log.Fields{
				"flowName": config.FlowJobName,
			}).Errorf("unexpected error rolling back transaction for get partitions: %v", err)
		}
	}()

	err = c.setTransactionSnapshot(tx)
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

	return c.getNumRowsPartitions(tx, config, last)
}

func (c *PostgresConnector) setTransactionSnapshot(tx pgx.Tx) error {
	snapshot := c.config.TransactionSnapshot
	if snapshot != "" {
		if _, err := tx.Exec(c.ctx, fmt.Sprintf("SET TRANSACTION SNAPSHOT '%s'", snapshot)); err != nil {
			return fmt.Errorf("failed to set transaction snapshot: %w", err)
		}
	}

	return nil
}

func (c *PostgresConnector) getNumRowsPartitions(
	tx pgx.Tx,
	config *protos.QRepConfig,
	last *protos.QRepPartition,
) ([]*protos.QRepPartition, error) {
	var err error
	numRowsPerPartition := int64(config.NumRowsPerPartition)
	quotedWatermarkColumn := fmt.Sprintf("\"%s\"", config.WatermarkColumn)

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

		row = tx.QueryRow(c.ctx, countQuery, minVal)
	} else {
		row = tx.QueryRow(c.ctx, countQuery)
	}

	var totalRows pgtype.Int8
	if err = row.Scan(&totalRows); err != nil {
		return nil, fmt.Errorf("failed to query for total rows: %w", err)
	}

	if totalRows.Int64 == 0 {
		log.Warnf("no records to replicate for flow job %s, returning", config.FlowJobName)
		return make([]*protos.QRepPartition, 0), nil
	}

	// Calculate the number of partitions
	numPartitions := totalRows.Int64 / numRowsPerPartition
	if totalRows.Int64%numRowsPerPartition != 0 {
		numPartitions++
	}
	log.Infof("total rows: %d, num partitions: %d, num rows per partition: %d",
		totalRows.Int64, numPartitions, numRowsPerPartition)

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
		log.Infof("[row_based_next] partitions query: %s", partitionsQuery)
		rows, err = tx.Query(c.ctx, partitionsQuery, minVal)
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
		log.Infof("[row_based] partitions query: %s", partitionsQuery)
		rows, err = tx.Query(c.ctx, partitionsQuery)
	}
	if err != nil {
		log.WithFields(log.Fields{
			"flowName": config.FlowJobName,
		}).Errorf("failed to query for partitions: %v", err)
		return nil, fmt.Errorf("failed to query for partitions: %w", err)
	}

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

	err = tx.Commit(c.ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to commit transaction: %w", err)
	}

	return partitionHelper.GetPartitions(), nil
}

func (c *PostgresConnector) getMinMaxValues(
	tx pgx.Tx,
	config *protos.QRepConfig,
	last *protos.QRepPartition,
) (interface{}, interface{}, error) {
	var minValue, maxValue interface{}
	quotedWatermarkColumn := fmt.Sprintf("\"%s\"", config.WatermarkColumn)

	parsedWatermarkTable, err := utils.ParseSchemaTable(config.WatermarkTable)
	if err != nil {
		return nil, nil, fmt.Errorf("unable to parse watermark table: %w", err)
	}

	// Get the maximum value from the database
	maxQuery := fmt.Sprintf("SELECT MAX(%[1]s) FROM %[2]s", quotedWatermarkColumn, parsedWatermarkTable.String())
	row := tx.QueryRow(c.ctx, maxQuery)
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
		row := tx.QueryRow(c.ctx, minQuery)
		if err := row.Scan(&minValue); err != nil {
			log.WithFields(log.Fields{
				"flowName": config.FlowJobName,
			}).Errorf("failed to query [%s] for min value: %v", minQuery, err)
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

	err = tx.Commit(c.ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to commit transaction: %w", err)
	}

	return minValue, maxValue, nil
}

func (c *PostgresConnector) CheckForUpdatedMaxValue(config *protos.QRepConfig,
	last *protos.QRepPartition) (bool, error) {
	tx, err := c.pool.Begin(c.ctx)
	if err != nil {
		return false, fmt.Errorf("unable to begin transaction for getting max value: %w", err)
	}
	defer func() {
		deferErr := tx.Rollback(c.ctx)
		if deferErr != pgx.ErrTxClosed && deferErr != nil {
			log.WithFields(log.Fields{
				"flowName": config.FlowJobName,
			}).Errorf("unexpected error rolling back transaction for getting max value: %v", err)
		}
	}()

	_, maxValue, err := c.getMinMaxValues(tx, config, last)
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
	config *protos.QRepConfig,
	partition *protos.QRepPartition) (*model.QRecordBatch, error) {
	if partition.FullTablePartition {
		log.WithFields(log.Fields{
			"partitionId": partition.PartitionId,
		}).Infof("pulling full table partition for flow job %s", config.FlowJobName)
		executor, err := NewQRepQueryExecutorSnapshot(c.pool, c.ctx, c.config.TransactionSnapshot,
			config.FlowJobName, partition.PartitionId)
		if err != nil {
			return nil, err
		}
		query := config.Query
		return executor.ExecuteAndProcessQuery(query)
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
	log.WithFields(log.Fields{
		"flowName":  config.FlowJobName,
		"partition": partition.PartitionId,
	}).Infof("Obtained ranges for partition for PullQRep")

	// Build the query to pull records within the range from the source table
	// Be sure to order the results by the watermark column to ensure consistency across pulls
	query, err := BuildQuery(config.Query, config.FlowJobName)
	if err != nil {
		return nil, err
	}

	executor, err := NewQRepQueryExecutorSnapshot(c.pool, c.ctx, c.config.TransactionSnapshot,
		config.FlowJobName, partition.PartitionId)
	if err != nil {
		return nil, err
	}

	records, err := executor.ExecuteAndProcessQuery(query,
		rangeStart, rangeEnd)
	if err != nil {
		return nil, err
	}

	return records, nil
}

func (c *PostgresConnector) PullQRepRecordStream(
	config *protos.QRepConfig,
	partition *protos.QRepPartition,
	stream *model.QRecordStream,
) (int, error) {
	if partition.FullTablePartition {
		log.WithFields(log.Fields{
			"flowName":    config.FlowJobName,
			"partitionId": partition.PartitionId,
		}).Infof("pulling full table partition for flow job %s", config.FlowJobName)
		executor, err := NewQRepQueryExecutorSnapshot(c.pool, c.ctx, c.config.TransactionSnapshot,
			config.FlowJobName, partition.PartitionId)
		if err != nil {
			return 0, err
		}

		query := config.Query
		_, err = executor.ExecuteAndProcessQueryStream(stream, query)
		return 0, err
	}
	log.WithFields(log.Fields{
		"flowName":  config.FlowJobName,
		"partition": partition.PartitionId,
	}).Infof("Obtained ranges for partition for PullQRepStream")

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
	query, err := BuildQuery(config.Query, config.FlowJobName)
	if err != nil {
		return 0, err
	}

	executor, err := NewQRepQueryExecutorSnapshot(c.pool, c.ctx, c.config.TransactionSnapshot,
		config.FlowJobName, partition.PartitionId)
	if err != nil {
		return 0, err
	}

	numRecords, err := executor.ExecuteAndProcessQueryStream(stream, query, rangeStart, rangeEnd)
	if err != nil {
		return 0, err
	}

	log.WithFields(log.Fields{
		"partition": partition.PartitionId,
	}).Infof("pulled %d records for flow job %s", numRecords, config.FlowJobName)
	return numRecords, nil
}

func (c *PostgresConnector) SyncQRepRecords(
	config *protos.QRepConfig,
	partition *protos.QRepPartition,
	stream *model.QRecordStream,
) (int, error) {
	dstTable, err := utils.ParseSchemaTable(config.DestinationTableIdentifier)
	if err != nil {
		return 0, fmt.Errorf("failed to parse destination table identifier: %w", err)
	}

	exists, err := c.tableExists(dstTable)
	if err != nil {
		return 0, fmt.Errorf("failed to check if table exists: %w", err)
	}

	if !exists {
		return 0, fmt.Errorf("table %s does not exist, used schema: %s", dstTable.Table, dstTable.Schema)
	}

	done, err := c.isPartitionSynced(partition.PartitionId)
	if err != nil {
		return 0, fmt.Errorf("failed to check if partition is synced: %w", err)
	}

	if done {
		log.WithFields(log.Fields{
			"flowName": config.FlowJobName,
		}).Infof("partition %s already synced", partition.PartitionId)
		return 0, nil
	}
	log.WithFields(log.Fields{
		"flowName":  config.FlowJobName,
		"partition": partition.PartitionId,
	}).Infof("SyncRecords called and initial checks complete.")

	stagingTableSync := &QRepStagingTableSync{connector: c}
	return stagingTableSync.SyncQRepRecords(
		config.FlowJobName, dstTable, partition, stream, config.WriteMode)
}

// SetupQRepMetadataTables function for postgres connector
func (c *PostgresConnector) SetupQRepMetadataTables(config *protos.QRepConfig) error {
	createQRepMetadataTableTx, err := c.pool.Begin(c.ctx)
	if err != nil {
		return fmt.Errorf("error starting transaction for creating qrep metadata table: %w", err)
	}
	defer func() {
		deferErr := createQRepMetadataTableTx.Rollback(c.ctx)
		if deferErr != pgx.ErrTxClosed && deferErr != nil {
			log.WithFields(log.Fields{
				"flowName": config.FlowJobName,
			}).Errorf("unexpected error rolling back transaction for creating qrep metadata table: %v", err)
		}
	}()

	err = c.createMetadataSchema(createQRepMetadataTableTx)
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
	_, err = createQRepMetadataTableTx.Exec(c.ctx, createQRepMetadataTableSQL)
	if err != nil {
		return fmt.Errorf("failed to create table %s: %w", qRepMetadataTableName, err)
	}
	log.WithFields(log.Fields{
		"flowName": config.FlowJobName,
	}).Infof("Setup metadata table.")

	if config.WriteMode != nil &&
		config.WriteMode.WriteType == protos.QRepWriteType_QREP_WRITE_MODE_OVERWRITE {
		_, err = createQRepMetadataTableTx.Exec(c.ctx,
			fmt.Sprintf("TRUNCATE TABLE %s", config.DestinationTableIdentifier))
		if err != nil {
			return fmt.Errorf("failed to TRUNCATE table before query replication: %w", err)
		}
	}

	err = createQRepMetadataTableTx.Commit(c.ctx)
	if err != nil {
		return fmt.Errorf("error committing transaction for creating qrep metadata table: %w", err)
	}

	return nil
}

func (c *PostgresConnector) PullXminRecordStream(
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

	executor, err := NewQRepQueryExecutorSnapshot(c.pool, c.ctx, c.config.TransactionSnapshot,
		config.FlowJobName, partition.PartitionId)
	if err != nil {
		return 0, currentSnapshotXmin, err
	}

	var numRecords int
	if partition.Range != nil {
		numRecords, currentSnapshotXmin, err = executor.ExecuteAndProcessQueryStreamGettingCurrentSnapshotXmin(stream, query, oldxid)
	} else {
		numRecords, currentSnapshotXmin, err = executor.ExecuteAndProcessQueryStreamGettingCurrentSnapshotXmin(stream, query)
	}
	if err != nil {
		return 0, currentSnapshotXmin, err
	}

	log.WithFields(log.Fields{
		"partition": partition.PartitionId,
	}).Infof("pulled %d records for flow job %s", numRecords, config.FlowJobName)
	return numRecords, currentSnapshotXmin, nil
}

func BuildQuery(query string, flowJobName string) (string, error) {
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

	log.WithFields(log.Fields{
		"flowName": flowJobName,
	}).Infof("templated query: %s", res)
	return res, nil
}

// isPartitionSynced checks whether a specific partition is synced
func (c *PostgresConnector) isPartitionSynced(partitionID string) (bool, error) {
	// setup the query string
	metadataTableIdentifier := pgx.Identifier{c.metadataSchema, qRepMetadataTableName}
	queryString := fmt.Sprintf(
		"SELECT COUNT(*)>0 FROM %s WHERE partitionID = $1;",
		metadataTableIdentifier.Sanitize(),
	)

	// prepare and execute the query
	var result bool
	err := c.pool.QueryRow(c.ctx, queryString, partitionID).Scan(&result)
	if err != nil {
		return false, fmt.Errorf("failed to execute query: %w", err)
	}

	return result, nil
}
