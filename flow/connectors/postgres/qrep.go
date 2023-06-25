package connpostgres

import (
	"bytes"
	"fmt"
	"text/template"
	"time"

	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const qRepMetadataTableName = "_peerdb_query_replication_metadata"

func (c *PostgresConnector) GetQRepPartitions(
	config *protos.QRepConfig,
	last *protos.QRepPartition,
) ([]*protos.QRepPartition, error) {
	minValue, maxValue, err := c.getMinMaxValues(config, last)
	if err != nil {
		return nil, err
	}

	var partitions []*protos.QRepPartition
	switch v := minValue.(type) {
	case int32, int64:
		partitions, err = c.getIntPartitions(v.(int64), maxValue.(int64), config.BatchSizeInt)
	case time.Time:
		partitions, err = c.getTimePartitions(v, maxValue.(time.Time), config.BatchDurationSeconds)
	default:
		return nil, fmt.Errorf("unsupported type: %T", v)
	}

	if err != nil {
		return nil, err
	}

	return partitions, nil
}

func (c *PostgresConnector) getMinMaxValues(
	config *protos.QRepConfig,
	last *protos.QRepPartition,
) (interface{}, interface{}, error) {
	var minValue, maxValue interface{}

	if last != nil && last.Range != nil {
		// If there's a last partition, start from its end
		switch lastRange := last.Range.Range.(type) {
		case *protos.PartitionRange_IntRange:
			minValue = lastRange.IntRange.End
		case *protos.PartitionRange_TimestampRange:
			minValue = lastRange.TimestampRange.End.AsTime()
		}
	} else {
		// Otherwise get the minimum value from the database
		minQuery := fmt.Sprintf("SELECT MIN(%[1]s) FROM %[2]s", config.WatermarkColumn, config.WatermarkTable)
		row := c.pool.QueryRow(c.ctx, minQuery)
		if err := row.Scan(&minValue); err != nil {
			log.Errorf("failed to query [%s] for min value: %v", minQuery, err)
			return nil, nil, fmt.Errorf("failed to query for min value: %w", err)
		}
	}

	// Get the maximum value from the database
	maxQuery := fmt.Sprintf("SELECT MAX(%[1]s) FROM %[2]s", config.WatermarkColumn, config.WatermarkTable)
	row := c.pool.QueryRow(c.ctx, maxQuery)
	if err := row.Scan(&maxValue); err != nil {
		return nil, nil, fmt.Errorf("failed to query for max value: %w", err)
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

	for start.Before(end) || start.Equal(end) {
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

		if partitionEnd.Equal(end) {
			break
		}

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
		if partitionEnd > end {
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
