package connpostgres

import (
	"bytes"
	"fmt"
	"text/template"
	"time"

	"github.com/PeerDB-io/peer-flow/connectors/utils/metrics"
	utils "github.com/PeerDB-io/peer-flow/connectors/utils/partition"
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
	if config.WatermarkColumn == "xmin" {
		quotedWatermarkColumn = fmt.Sprintf("%s::text::bigint", quotedWatermarkColumn)

		minVal, maxVal, err := c.getMinMaxValues(tx, config, last)
		if err != nil {
			return nil, fmt.Errorf("failed to get min max values for xmin: %w", err)
		}

		// we know these are int64s so we can just cast them
		minValInt := minVal.(int64)
		maxValInt := maxVal.(int64)

		// we will only return 1 partition for xmin:
		// if there is no last partition, we will return a partition with the min and max values
		// if there is a last partition, we will return a partition with the last partition's end value + 1 and the max value
		if last != nil && last.Range != nil {
			minValInt += 1
		}

		if minValInt > maxValInt {
			// log and return an empty partition
			log.WithFields(log.Fields{
				"flowName": config.FlowJobName,
			}).Infof("xmin min value is greater than max value, returning empty partition")
			return make([]*protos.QRepPartition, 0), nil
		}

		log.WithFields(log.Fields{
			"flowName": config.FlowJobName,
		}).Infof("single xmin partition range: %v - %v", minValInt, maxValInt)

		partition := &protos.QRepPartition{
			PartitionId: uuid.New().String(),
			Range: &protos.PartitionRange{
				Range: &protos.PartitionRange_IntRange{
					IntRange: &protos.IntPartitionRange{
						Start: minValInt,
						End:   maxValInt,
					},
				},
			},
		}

		return []*protos.QRepPartition{partition}, nil
	}

	whereClause := ""
	if last != nil && last.Range != nil {
		whereClause = fmt.Sprintf(`WHERE %s > $1`, quotedWatermarkColumn)
	}

	// Query to get the total number of rows in the table
	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s %s", config.WatermarkTable, whereClause)
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

	var totalRows int64
	if err = row.Scan(&totalRows); err != nil {
		return nil, fmt.Errorf("failed to query for total rows: %w", err)
	}

	if totalRows == 0 {
		log.Warnf("no records to replicate for flow job %s, returning", config.FlowJobName)
		return make([]*protos.QRepPartition, 0), nil
	}

	// Calculate the number of partitions
	numPartitions := totalRows / numRowsPerPartition
	if totalRows%numRowsPerPartition != 0 {
		numPartitions++
	}
	log.Infof("total rows: %d, num partitions: %d, num rows per partition: %d",
		totalRows, numPartitions, numRowsPerPartition)

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
			config.WatermarkTable,
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
			config.WatermarkTable,
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

	partitionHelper := utils.NewPartitionHelper()
	for rows.Next() {
		var bucket int64
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
	if config.WatermarkColumn == "xmin" {
		quotedWatermarkColumn = fmt.Sprintf("%s::text::bigint", quotedWatermarkColumn)
	}

	// Get the maximum value from the database
	maxQuery := fmt.Sprintf("SELECT MAX(%[1]s) FROM %[2]s", quotedWatermarkColumn, config.WatermarkTable)
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
		minQuery := fmt.Sprintf("SELECT MIN(%[1]s) FROM %[2]s", quotedWatermarkColumn, config.WatermarkTable)
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

	err := tx.Commit(c.ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to commit transaction: %w", err)
	}

	return minValue, maxValue, nil
}

func (c *PostgresConnector) CheckForUpdatedMaxValue(config *protos.QRepConfig,
	last *protos.QRepPartition) (bool, error) {
	// for xmin lets always assume there are updates
	if config.WatermarkColumn == "xmin" {
		return true, nil
	}

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

	totalRecordsAtSource, err := c.getApproxTableCounts([]string{config.WatermarkTable})
	if err != nil {
		return nil, err
	}
	metrics.LogQRepPullMetrics(c.ctx, config.FlowJobName, int(records.NumRecords), totalRecordsAtSource)
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

	totalRecordsAtSource, err := c.getApproxTableCounts([]string{config.WatermarkTable})
	if err != nil {
		return 0, err
	}
	metrics.LogQRepPullMetrics(c.ctx, config.FlowJobName, numRecords, totalRecordsAtSource)
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
	dstTable, err := parseSchemaTable(config.DestinationTableIdentifier)
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

	syncMode := config.SyncMode
	switch syncMode {
	case protos.QRepSyncMode_QREP_SYNC_MODE_MULTI_INSERT:
		stagingTableSync := &QRepStagingTableSync{connector: c}
		return stagingTableSync.SyncQRepRecords(
			config.FlowJobName, dstTable, partition, stream, config.WriteMode)
	case protos.QRepSyncMode_QREP_SYNC_MODE_STORAGE_AVRO:
		return 0, fmt.Errorf("[postgres] SyncQRepRecords not implemented for storage avro sync mode")
	default:
		return 0, fmt.Errorf("unsupported sync mode: %s", syncMode)
	}
}

// SetupQRepMetadataTables function for postgres connector
func (c *PostgresConnector) SetupQRepMetadataTables(config *protos.QRepConfig) error {
	qRepMetadataSchema := `CREATE TABLE IF NOT EXISTS %s (
		flowJobName TEXT,
		partitionID TEXT,
		syncPartition JSONB,
		syncStartTime TIMESTAMP,
		syncFinishTime TIMESTAMP DEFAULT NOW()
	)`

	// replace table name in schema
	qRepMetadataSchema = fmt.Sprintf(qRepMetadataSchema, qRepMetadataTableName)

	// execute create table query
	_, err := c.pool.Exec(c.ctx, qRepMetadataSchema)
	if err != nil {
		return fmt.Errorf("failed to create table %s: %w", qRepMetadataTableName, err)
	}
	log.WithFields(log.Fields{
		"flowName": config.FlowJobName,
	}).Infof("Setup metadata table.")

	if config.WriteMode != nil &&
		config.WriteMode.WriteType == protos.QRepWriteType_QREP_WRITE_MODE_OVERWRITE {
		_, err = c.pool.Exec(c.ctx, fmt.Sprintf("TRUNCATE TABLE %s", config.DestinationTableIdentifier))
		if err != nil {
			return fmt.Errorf("failed to TRUNCATE table before query replication: %w", err)
		}
	}

	return nil
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
	queryString := fmt.Sprintf(
		"SELECT COUNT(*) FROM %s WHERE partitionID = $1;",
		qRepMetadataTableName,
	)

	// prepare and execute the query
	var count int
	err := c.pool.QueryRow(c.ctx, queryString, partitionID).Scan(&count)
	if err != nil {
		return false, fmt.Errorf("failed to execute query: %w", err)
	}

	return count > 0, nil
}
