package connpostgres

import (
	"bytes"
	"fmt"
	"text/template"
	"time"

	utils "github.com/PeerDB-io/peer-flow/connectors/utils/partition"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	log "github.com/sirupsen/logrus"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const qRepMetadataTableName = "_peerdb_query_replication_metadata"

func (c *PostgresConnector) GetQRepPartitions(
	config *protos.QRepConfig,
	last *protos.QRepPartition,
) ([]*protos.QRepPartition, error) {
	// begin a transaction
	tx, err := c.pool.Begin(c.ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() {
		deferErr := tx.Rollback(c.ctx)
		if deferErr != pgx.ErrTxClosed && deferErr != nil {
			log.Errorf("unexpected error rolling back transaction for get partitions: %v", err)
		}
	}()

	// lock the table while we get the partitions.
	lockQuery := fmt.Sprintf("LOCK %s IN EXCLUSIVE MODE", config.WatermarkTable)
	if _, err = tx.Exec(c.ctx, lockQuery); err != nil {
		return nil, fmt.Errorf("failed to lock table: %w", err)
	}

	if config.NumRowsPerPartition > 0 {
		return c.getNumRowsPartitions(tx, config, last)
	}

	minValue, maxValue, err := c.getMinMaxValues(tx, config, last)
	if err != nil {
		return nil, err
	}

	var partitions []*protos.QRepPartition
	switch v := minValue.(type) {
	case int64:
		maxValue := maxValue.(int64) + 1
		partitions, err = c.getIntPartitions(v, maxValue, config.BatchSizeInt)
	case time.Time:
		maxValue := maxValue.(time.Time).Add(time.Microsecond)
		partitions, err = c.getTimePartitions(v, maxValue, config.BatchDurationSeconds)
	// only hit when there is no data in the source table
	case nil:
		log.Warnf("no records to replicate for flow job %s, returning", config.FlowJobName)
		return make([]*protos.QRepPartition, 0), nil
	default:
		return nil, fmt.Errorf("unsupported type: %T", v)
	}

	if err != nil {
		return nil, err
	}

	return partitions, nil
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
		log.Infof("partitions query: %s", partitionsQuery)
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
		log.Infof("partitions query: %s", partitionsQuery)
		rows, err = tx.Query(c.ctx, partitionsQuery)
	}
	if err != nil {
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
		}
	} else {
		// Otherwise get the minimum value from the database
		minQuery := fmt.Sprintf("SELECT MIN(%[1]s) FROM %[2]s", quotedWatermarkColumn, config.WatermarkTable)
		row := tx.QueryRow(c.ctx, minQuery)
		if err := row.Scan(&minValue); err != nil {
			log.Errorf("failed to query [%s] for min value: %v", minQuery, err)
			return nil, nil, fmt.Errorf("failed to query for min value: %w", err)
		}

		switch v := minValue.(type) {
		case int16:
			minValue = int64(v)
			maxValue = int64(maxValue.(int16))
		case int32:
			minValue = int64(v)
			maxValue = int64(maxValue.(int32))
		}
	}

	err := tx.Commit(c.ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to commit transaction: %w", err)
	}

	return minValue, maxValue, nil
}

func (c *PostgresConnector) PullQRepRecords(
	config *protos.QRepConfig,
	partition *protos.QRepPartition) (*model.QRecordBatch, error) {
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
	default:
		return nil, fmt.Errorf("unknown range type: %v", x)
	}

	// Build the query to pull records within the range from the source table
	// Be sure to order the results by the watermark column to ensure consistency across pulls
	query, err := BuildQuery(config.Query)
	if err != nil {
		return nil, err
	}

	executor := NewQRepQueryExecutor(c.pool, c.ctx)

	// Execute the query with the range values
	rows, err := executor.ExecuteQuery(query, rangeStart, rangeEnd)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// get column names from field descriptions
	fieldDescriptions := rows.FieldDescriptions()

	// log the number of field descriptions and column names
	log.Debugf("field descriptions: %v\n", fieldDescriptions)

	// Process the rows and retrieve the records
	return executor.ProcessRows(rows, fieldDescriptions)
}

func (c *PostgresConnector) SyncQRepRecords(config *protos.QRepConfig,
	partition *protos.QRepPartition, records *model.QRecordBatch,
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
		return 0, fmt.Errorf("table %s does not exist", dstTable)
	}

	done, err := c.isPartitionSynced(partition.PartitionId)
	if err != nil {
		return 0, fmt.Errorf("failed to check if partition is synced: %w", err)
	}

	if done {
		log.Infof("partition %s already synced", partition.PartitionId)
		return 0, nil
	}

	syncMode := config.SyncMode
	switch syncMode {
	case protos.QRepSyncMode_QREP_SYNC_MODE_MULTI_INSERT:
		stagingTableSync := &QRepStagingTableSync{connector: c}
		return stagingTableSync.SyncQRepRecords(config.FlowJobName, dstTable, partition, records)
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

	return nil
}

func BuildQuery(query string) (string, error) {
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

	log.Infof("templated query: %s", res)
	return res, nil
}

func (c *PostgresConnector) ConsolidateQRepPartitions(config *protos.QRepConfig) error {
	log.Infof("Consolidating partitions for flow job %s", config.FlowJobName)
	log.Infof("This is a no-op for Postgres")
	return nil
}

// CleanupQRepFlow function for postgres connector
func (c *PostgresConnector) CleanupQRepFlow(config *protos.QRepConfig) error {
	log.Infof("Cleaning up QRep flow for flow job %s", config.FlowJobName)
	log.Infof("This is a no-op for Postgres")
	return nil
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

func (c *PostgresConnector) getTimePartitions(
	start time.Time,
	end time.Time,
	batchDurationSeconds uint32,
) ([]*protos.QRepPartition, error) {
	if batchDurationSeconds == 0 {
		return nil, fmt.Errorf("batch duration must be greater than 0")
	}

	batchDuration := time.Duration(batchDurationSeconds) * time.Second
	var partitions []*protos.QRepPartition

	for start.Before(end) {
		partitionEnd := start.Add(batchDuration)
		if partitionEnd.After(end) {
			partitionEnd = end
		}

		rangePartition := protos.PartitionRange{
			Range: &protos.PartitionRange_TimestampRange{
				TimestampRange: &protos.TimestampPartitionRange{
					Start: timestamppb.New(start),
					End:   timestamppb.New(partitionEnd),
				},
			},
		}

		partitions = append(partitions, &protos.QRepPartition{
			PartitionId: uuid.New().String(),
			Range:       &rangePartition,
		})

		start = partitionEnd
	}

	return partitions, nil
}

func (c *PostgresConnector) getIntPartitions(
	start int64, end int64, batchSizeInt uint32) ([]*protos.QRepPartition, error) {
	var partitions []*protos.QRepPartition
	batchSize := int64(batchSizeInt)

	if batchSize == 0 {
		return nil, fmt.Errorf("batch size cannot be 0")
	}

	for start <= end {
		partitionEnd := start + batchSize
		// safeguard against integer overflow
		if partitionEnd > end || partitionEnd < start {
			partitionEnd = end
		}

		rangePartition := protos.PartitionRange{
			Range: &protos.PartitionRange_IntRange{
				IntRange: &protos.IntPartitionRange{
					Start: start,
					End:   partitionEnd,
				},
			},
		}

		partitions = append(partitions, &protos.QRepPartition{
			PartitionId: uuid.New().String(),
			Range:       &rangePartition,
		})

		if partitionEnd == end {
			break
		}

		start = partitionEnd
	}

	return partitions, nil
}
