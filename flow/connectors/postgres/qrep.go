package connpostgres

import (
	"bytes"
	"fmt"
	"text/template"
	"time"

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
	partitionQuery, lastEndValue := c.getPartitionQuery(config, last)

	var err error
	var rows pgx.Rows
	if lastEndValue != nil {
		log.Infof("[from %v] partition query: %s", lastEndValue, partitionQuery)
		rows, err = c.pool.Query(c.ctx, partitionQuery, lastEndValue)
	} else {
		log.Infof("[first run] partition query: %s", partitionQuery)
		rows, err = c.pool.Query(c.ctx, partitionQuery)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to query for partitions: %w", err)
	}
	defer rows.Close()

	var partitions []*protos.QRepPartition
	for rows.Next() {
		var startValue, endValue interface{}
		if err := rows.Scan(&startValue, &endValue); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}

		// Depending on the type of the startValue and endValue, convert them into
		// protos.TimestampPartitionRange or protos.IntPartitionRange
		var rangePartition protos.PartitionRange
		switch v := startValue.(type) {
		case int32:
			rangePartition = protos.PartitionRange{
				Range: &protos.PartitionRange_IntRange{
					IntRange: &protos.IntPartitionRange{
						Start: int64(v),
						End:   int64(endValue.(int32)),
					},
				},
			}
		case int64:
			rangePartition = protos.PartitionRange{
				Range: &protos.PartitionRange_IntRange{
					IntRange: &protos.IntPartitionRange{
						Start: v,
						End:   endValue.(int64),
					},
				},
			}
		case time.Time:
			rangePartition = protos.PartitionRange{
				Range: &protos.PartitionRange_TimestampRange{
					TimestampRange: &protos.TimestampPartitionRange{
						Start: timestamppb.New(v),
						End:   timestamppb.New(endValue.(time.Time)),
					},
				},
			}
		default:
			return nil, fmt.Errorf("unsupported type: %T", v)
		}

		partitions = append(partitions, &protos.QRepPartition{
			PartitionId: uuid.New().String(), // generate a new UUID as partition ID
			Range:       &rangePartition,
		})
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate over rows: %w", err)
	}

	// log the partitions for debugging
	log.Debugf("partitions: %v\n", partitions)

	return partitions, nil
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

func (c *PostgresConnector) getPartitionQuery(
	config *protos.QRepConfig,
	last *protos.QRepPartition,
) (string, interface{}) {
	var lastEndValue interface{}

	if last != nil && last.Range != nil {
		// extract end value from the last partition
		switch lastRange := last.Range.Range.(type) {
		case *protos.PartitionRange_IntRange:
			lastEndValue = lastRange.IntRange.End
		case *protos.PartitionRange_TimestampRange:
			lastEndValue = lastRange.TimestampRange.End.AsTime()
		}

		partitionQuery := fmt.Sprintf(`
			SELECT MIN(%[1]s) AS Start, MAX(%[1]s) AS End
			FROM (
				SELECT %[1]s, ROW_NUMBER() OVER (ORDER BY %[1]s) as row
				FROM %[2]s
				WHERE %[1]s > $1
			) sub
			GROUP BY (row - 1) / %[3]d`,
			config.WatermarkColumn, config.WatermarkTable, config.RowsPerPartition)

		return partitionQuery, lastEndValue
	}

	partitionQuery := fmt.Sprintf(`
		SELECT MIN(%[1]s) AS Start, MAX(%[1]s) AS End
		FROM (
			SELECT %[1]s, ROW_NUMBER() OVER (ORDER BY %[1]s) as row
			FROM %[2]s
		) sub
		GROUP BY (row - 1) / %[3]d`,
		config.WatermarkColumn, config.WatermarkTable, config.RowsPerPartition)

	return partitionQuery, nil
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
