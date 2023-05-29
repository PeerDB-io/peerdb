package connpostgres

import (
	"fmt"
	"time"

	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (c *PostgresConnector) GetQRepPartitions(last *protos.QRepPartition) ([]*protos.QRepPartition, error) {
	config := last.Config

	// For the table `config.SourceTableIdentifier`
	// Get the min, max value (inclusive) of `config.WatermarkColumn`
	row := c.pool.QueryRow(c.ctx, "SELECT MIN($1), MAX($1) FROM $2",
		config.WatermarkColumn, config.SourceTableIdentifier)

	var minValue, maxValue interface{}
	if err := row.Scan(&minValue, &maxValue); err != nil {
		return nil, fmt.Errorf("failed to get min, max value: %w", err)
	}

	// Depending on the type of the minValue and maxValue, convert them into
	// protos.TimestampPartitionRange or protos.IntPartitionRange
	var rangePartition protos.PartitionRange
	switch v := minValue.(type) {
	case int32:
		rangePartition = protos.PartitionRange{
			Range: &protos.PartitionRange_IntRange{
				IntRange: &protos.IntPartitionRange{
					Start: int64(v),
					End:   int64(maxValue.(int32)),
				},
			},
		}
	case int64:
		rangePartition = protos.PartitionRange{
			Range: &protos.PartitionRange_IntRange{
				IntRange: &protos.IntPartitionRange{
					Start: v,
					End:   maxValue.(int64),
				},
			},
		}
	case time.Time:
		rangePartition = protos.PartitionRange{
			Range: &protos.PartitionRange_TimestampRange{
				TimestampRange: &protos.TimestampPartitionRange{
					Start: timestamppb.New(v),
					End:   timestamppb.New(maxValue.(time.Time)),
				},
			},
		}
	default:
		return nil, fmt.Errorf("unsupported type: %T", v)
	}

	// If last is not nil, then return partitions between last partition's max and current max
	if last != nil {
		switch lastRange := last.Range.Range.(type) {
		case *protos.PartitionRange_IntRange:
			if _, ok := maxValue.(int64); ok {
				lastRange.IntRange.End = maxValue.(int64)
			} else if _, ok := maxValue.(int32); ok {
				lastRange.IntRange.End = int64(maxValue.(int32))
			}
		case *protos.PartitionRange_TimestampRange:
			lastRange.TimestampRange.End = timestamppb.New(maxValue.(time.Time))
		}
	}

	// TODO I am currently returning only one partition for the entire range,
	// but this can be changed to return multiple partitions for the range
	// if this is past the max partition size which needs to be taken in as a
	// configuration parameter
	return []*protos.QRepPartition{
		{
			Config:      config,
			PartitionId: uuid.New().String(), // generate a new UUID as partition ID
			Range:       &rangePartition,
		},
	}, nil
}

func mapRowToQRecord(row pgx.Row, columns []string) (*model.QRecord, error) {
	record := &model.QRecord{}

	for _, column := range columns {
		var val model.QValue
		err := row.Scan(&val.Value)
		if err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}

		switch v := val.Value.(type) {
		case int32, int64:
			val.Kind = model.QValueKindInteger
		case float32, float64:
			val.Kind = model.QValueKindFloat
		case bool:
			val.Kind = model.QValueKindBoolean
		case string:
			val.Kind = model.QValueKindString
		case []interface{}:
			val.Kind = model.QValueKindArray
		case time.Time:
			et, err := model.NewExtendedTime(v, model.DateTimeKindType, "")
			if err != nil {
				return nil, fmt.Errorf("failed to create extended time: %w", err)
			}
			val.Value = et
			val.Kind = model.QValueKindETime
		default:
			val.Kind = model.QValueKindInvalid
		}

		(*record)[column] = val
	}

	return record, nil
}

func (c *PostgresConnector) PullQRepRecords(partition *protos.QRepPartition) (*model.QRecordBatch, error) {
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

	config := partition.Config

	// Build the query to pull records within the range from the source table
	// Be sure to order the results by the watermark column to ensure consistency across pulls
	query := fmt.Sprintf("%s WHERE %s BETWEEN $1 AND $2", config.Query, config.WatermarkColumn)

	// Execute the query with the range values
	rows, err := c.pool.Query(c.ctx, query, rangeStart, rangeEnd)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// get column names from field descriptions
	fieldDescriptions := rows.FieldDescriptions()
	columnNames := make([]string, len(fieldDescriptions))
	for i, desc := range fieldDescriptions {
		columnNames[i] = desc.Name
	}

	// Initialize the record batch
	batch := &model.QRecordBatch{
		NumRecords: 0,
		Records:    make([]*model.QRecord, 0),
	}

	// Map each row to a QRecord and add it to the batch
	for rows.Next() {
		record, err := mapRowToQRecord(rows, columnNames)
		if err != nil {
			return nil, fmt.Errorf("failed to map row to QRecord: %w", err)
		}
		batch.Records = append(batch.Records, record)
		batch.NumRecords++
	}

	// Check for any errors encountered during iteration
	if rows.Err() != nil {
		return nil, fmt.Errorf("row iteration failed: %w", rows.Err())
	}

	return batch, nil
}

func (c *PostgresConnector) SyncQRepRecords(partition *protos.QRepPartition, records *model.QRecordBatch) (int, error) {
	return 0, fmt.Errorf("SyncQRepRecords not implemented for postgres connector")
}
