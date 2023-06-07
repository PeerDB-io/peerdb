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
	last *protos.QRepPartition,
) ([]*protos.QRepPartition, error) {
	// For the table `config.SourceTableIdentifier`
	// Get the start and end values for each group of rows
	partitionQuery := fmt.Sprintf(`
		SELECT MIN(%[1]s) AS Start, MAX(%[1]s) AS End
		FROM (
		    SELECT %[1]s, ROW_NUMBER() OVER (ORDER BY %[1]s) as row
		    FROM %[2]s
		) sub
		GROUP BY (row - 1) / %[3]d`,
		config.WatermarkColumn, config.SourceTableIdentifier, config.RowsPerPartition)

	rows, err := c.pool.Query(c.ctx, partitionQuery)
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

func mapRowToQRecord(row pgx.Row, fds []pgconn.FieldDescription) (*model.QRecord, error) {
	record := &model.QRecord{}

	scanArgs := make([]interface{}, len(fds))
	for i := range scanArgs {
		if fds[i].DataTypeOID == pgtype.BoolOID {
			scanArgs[i] = new(sql.NullBool)
		} else if fds[i].DataTypeOID == pgtype.TimestampOID || fds[i].DataTypeOID == pgtype.TimestamptzOID {
			scanArgs[i] = new(sql.NullTime)
		} else {
			scanArgs[i] = new(sql.RawBytes)
		}
	}

	err := row.Scan(scanArgs...)
	if err != nil {
		return nil, fmt.Errorf("failed to scan row: %w", err)
	}

	for i, fd := range fds {
		oid := fd.DataTypeOID
		var val model.QValue
		var err error

		switch oid {
		case pgtype.TimestampOID, pgtype.TimestamptzOID:
			nullTime := scanArgs[i].(*sql.NullTime)
			if nullTime.Valid {
				et, err := model.NewExtendedTime(nullTime.Time, model.DateTimeKindType, "")
				if err != nil {
					return nil, fmt.Errorf("failed to create extended time: %w", err)
				}
				val = model.QValue{Kind: model.QValueKindETime, Value: et}
			} else {
				val = model.QValue{Kind: model.QValueKindETime, Value: nil}
			}
		case pgtype.BoolOID:
			nullBool := scanArgs[i].(*sql.NullBool)
			if nullBool.Valid {
				val = model.QValue{Kind: model.QValueKindBoolean, Value: nullBool.Bool}
			} else {
				val = model.QValue{Kind: model.QValueKindBoolean, Value: nil}
			}
		default:
			rawBytes := scanArgs[i].(*sql.RawBytes)
			val, err = parseField(oid, rawBytes)
			if err != nil {
				return nil, fmt.Errorf("failed to parse field: %w", err)
			}
		}

		(*record)[fd.Name] = val
	}

	return record, nil
}

func parseField(oid uint32, rawBytes *sql.RawBytes) (model.QValue, error) {
	switch oid {
	case pgtype.Int4OID, pgtype.Int8OID:
		val, err := strconv.ParseInt(string(*rawBytes), 10, 64)
		if err != nil {
			return model.QValue{}, err
		}
		return model.QValue{Kind: model.QValueKindInteger, Value: val}, nil
	case pgtype.Float4OID, pgtype.Float8OID:
		val, err := strconv.ParseFloat(string(*rawBytes), 64)
		if err != nil {
			return model.QValue{}, err
		}
		return model.QValue{Kind: model.QValueKindFloat, Value: val}, nil
	case pgtype.BoolOID:
		val, err := strconv.ParseBool(string(*rawBytes))
		if err != nil {
			return model.QValue{}, err
		}
		return model.QValue{Kind: model.QValueKindBoolean, Value: val}, nil
	case pgtype.TextOID, pgtype.VarcharOID:
		return model.QValue{Kind: model.QValueKindString, Value: string(*rawBytes)}, nil
	case pgtype.TimestampOID, pgtype.TimestamptzOID:
		val, err := time.Parse(time.RFC3339, string(*rawBytes))
		if err != nil {
			return model.QValue{}, err
		}
		et, err := model.NewExtendedTime(val, model.DateTimeKindType, "")
		if err != nil {
			return model.QValue{}, fmt.Errorf("failed to create extended time: %w", err)
		}
		return model.QValue{Kind: model.QValueKindETime, Value: et}, nil
	case pgtype.NumericOID:
		return model.QValue{Kind: model.QValueKindNumeric, Value: string(*rawBytes)}, nil
	case pgtype.UUIDOID:
		return model.QValue{Kind: model.QValueKindString, Value: string(*rawBytes)}, nil
	case pgtype.ByteaOID:
		return model.QValue{Kind: model.QValueKindBytes, Value: []byte(*rawBytes)}, nil
	default:
		typ, _ := pgtype.NewMap().TypeForOID(oid)
		fmt.Printf("QValueKindInvalid => oid: %v, typename: %v\n", oid, typ)
		return model.QValue{Kind: model.QValueKindInvalid, Value: nil}, nil
	}
}

func (c *PostgresConnector) PullQRepRecords(config *protos.QRepConfig,
	partition *protos.QRepPartition,
) (*model.QRecordBatch, error) {
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
	partition *protos.QRepPartition, records *model.QRecordBatch,
) (int, error) {
	return 0, fmt.Errorf("SyncQRepRecords not implemented for postgres connector")
}

func (c *PostgresConnector) SetupQRepMetadataTables(config *protos.QRepConfig) error {
	panic("SetupQRepMetadataTables not implemented for postgres connector")
}
