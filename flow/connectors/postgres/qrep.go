package connpostgres

import (
	"database/sql"
	"fmt"
	"strconv"
	"time"

	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (c *PostgresConnector) GetQRepPartitions(config *protos.QRepConfig,
	last *protos.QRepPartition) ([]*protos.QRepPartition, error) {
	// For the table `config.SourceTableIdentifier`
	// Get the min, max value (inclusive) of `config.WatermarkColumn`
	extremaQuery := fmt.Sprintf("SELECT MIN(%s), MAX(%s) FROM %s",
		config.WatermarkColumn, config.WatermarkColumn, config.SourceTableIdentifier)
	row := c.pool.QueryRow(c.ctx, extremaQuery)

	var minValue, maxValue interface{}
	if err := row.Scan(&minValue, &maxValue); err != nil {
		return nil, fmt.Errorf("failed to get min, max value: %w", err)
	}

	// log the min, max value
	fmt.Printf("minValue: %v, maxValue: %v\n", minValue, maxValue)

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
	if last.Range != nil {
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
			PartitionId: uuid.New().String(), // generate a new UUID as partition ID
			Range:       &rangePartition,
		},
	}, nil
}

func mapRowToQRecord(row pgx.Row, fds []pgconn.FieldDescription) (*model.QRecord, error) {
	record := &model.QRecord{}

	scanArgs := make([]interface{}, len(fds))
	for i := range scanArgs {
		scanArgs[i] = new(sql.RawBytes)
	}

	err := row.Scan(scanArgs...)
	if err != nil {
		return nil, fmt.Errorf("failed to scan row: %w", err)
	}

	for i, fd := range fds {
		rawBytes := scanArgs[i].(*sql.RawBytes)

		// Determine the type of the value using the OID
		oid := fd.DataTypeOID
		switch oid {
		case pgtype.Int4OID, pgtype.Int8OID:
			val, _ := strconv.ParseInt(string(*rawBytes), 10, 64)
			(*record)[fd.Name] = model.QValue{Kind: model.QValueKindInteger, Value: val}
		case pgtype.Float4OID, pgtype.Float8OID:
			val, _ := strconv.ParseFloat(string(*rawBytes), 64)
			(*record)[fd.Name] = model.QValue{Kind: model.QValueKindFloat, Value: val}
		case pgtype.BoolOID:
			val, _ := strconv.ParseBool(string(*rawBytes))
			(*record)[fd.Name] = model.QValue{Kind: model.QValueKindBoolean, Value: val}
		case pgtype.TextOID:
			(*record)[fd.Name] = model.QValue{
				Kind:  model.QValueKindString,
				Value: string(*rawBytes),
			}
		case pgtype.TimestampOID, pgtype.TimestamptzOID:
			val, _ := time.Parse(time.RFC3339, string(*rawBytes))
			et, err := model.NewExtendedTime(val, model.DateTimeKindType, "")
			if err != nil {
				return nil, fmt.Errorf("failed to create extended time: %w", err)
			}
			(*record)[fd.Name] = model.QValue{Kind: model.QValueKindETime, Value: et}
		default:
			(*record)[fd.Name] = model.QValue{Kind: model.QValueKindInvalid, Value: nil}
		}
	}

	return record, nil
}

func (c *PostgresConnector) PullQRepRecords(config *protos.QRepConfig,
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

	// Execute the query with the range values
	rows, err := c.pool.Query(c.ctx, query, rangeStart, rangeEnd)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// get column names from field descriptions
	fieldDescriptions := rows.FieldDescriptions()

	// log the number of field descriptions and column names
	fmt.Printf("field descriptions: %v\n", fieldDescriptions)

	// Initialize the record batch
	batch := &model.QRecordBatch{
		NumRecords: 0,
		Records:    make([]*model.QRecord, 0),
	}

	// Map each row to a QRecord and add it to the batch
	for rows.Next() {
		record, err := mapRowToQRecord(rows, fieldDescriptions)
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

func (c *PostgresConnector) SyncQRepRecords(config *protos.QRepConfig,
	partition *protos.QRepPartition, records *model.QRecordBatch) (int, error) {
	return 0, fmt.Errorf("SyncQRepRecords not implemented for postgres connector")
}

func (c *PostgresConnector) SetupQRepMetadataTables(config *protos.QRepConfig) error {
	panic("SetupQRepMetadataTables not implemented for postgres connector")
}
