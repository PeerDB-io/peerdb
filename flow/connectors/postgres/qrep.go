package connpostgres

import (
	"fmt"
	"time"

	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	log "github.com/sirupsen/logrus"
	"google.golang.org/protobuf/types/known/timestamppb"
)

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
	fmt.Printf("partitions: %v\n", partitions)

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
	query := fmt.Sprintf("%s WHERE %s BETWEEN $1 AND $2", config.Query, config.WatermarkColumn)

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
	fmt.Printf("field descriptions: %v\n", fieldDescriptions)

	// Process the rows and retrieve the records
	return executor.ProcessRows(rows, fieldDescriptions)
}

func (c *PostgresConnector) SyncQRepRecords(config *protos.QRepConfig,
	partition *protos.QRepPartition, records *model.QRecordBatch,
) (int, error) {
	return 0, fmt.Errorf("SyncQRepRecords not implemented for postgres connector")
}

func (c *PostgresConnector) SetupQRepMetadataTables(config *protos.QRepConfig) error {
	panic("SetupQRepMetadataTables not implemented for postgres connector")
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
			config.WatermarkColumn, config.SourceTableIdentifier, config.RowsPerPartition)

		return partitionQuery, lastEndValue
	}

	partitionQuery := fmt.Sprintf(`
		SELECT MIN(%[1]s) AS Start, MAX(%[1]s) AS End
		FROM (
			SELECT %[1]s, ROW_NUMBER() OVER (ORDER BY %[1]s) as row
			FROM %[2]s
		) sub
		GROUP BY (row - 1) / %[3]d`,
		config.WatermarkColumn, config.SourceTableIdentifier, config.RowsPerPartition)

	return partitionQuery, nil
}
