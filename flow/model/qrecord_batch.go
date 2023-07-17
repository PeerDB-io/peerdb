package model

import (
	"fmt"
	"math/big"
	"time"

	"github.com/PeerDB-io/peer-flow/model/qvalue"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
)

// QRecordBatch holds a batch of QRecord objects.
type QRecordBatch struct {
	NumRecords uint32     // NumRecords represents the number of records in the batch.
	Records    []*QRecord // Records is a slice of pointers to QRecord objects.
	Schema     *QRecordSchema
}

// Equals checks if two QRecordBatches are identical.
func (q *QRecordBatch) Equals(other *QRecordBatch) bool {
	if other == nil {
		return q == nil
	}

	// First check simple attributes
	if q.NumRecords != other.NumRecords {
		// print num records
		fmt.Printf("q.NumRecords: %d\n", q.NumRecords)
		fmt.Printf("other.NumRecords: %d\n", other.NumRecords)

		return false
	}

	// Compare column names
	if !q.Schema.EqualNames(other.Schema) {
		return false
	}

	// Compare records
	for i, record := range q.Records {
		if !record.equals(other.Records[i]) {
			return false
		}
	}

	return true
}

type QRecordBatchCopyFromSource struct {
	currentIndex int
	records      *QRecordBatch
	err          error
}

func NewQRecordBatchCopyFromSource(
	records *QRecordBatch,
) *QRecordBatchCopyFromSource {
	return &QRecordBatchCopyFromSource{
		records: records,
	}
}

func (src *QRecordBatchCopyFromSource) Next() bool {
	return src.currentIndex < len(src.records.Records)
}

func (src *QRecordBatchCopyFromSource) Values() ([]interface{}, error) {
	record := src.records.Records[src.currentIndex]
	src.currentIndex++

	numEntries := len(record.Entries)

	values := make([]interface{}, numEntries)
	for i, qValue := range record.Entries {
		if qValue.Value == nil {
			values[i] = nil
			continue
		}

		switch qValue.Kind {
		case qvalue.QValueKindFloat32:
			v, ok := qValue.Value.(float32)
			if !ok {
				src.err = fmt.Errorf("invalid float32 value")
				return nil, src.err
			}
			values[i] = v

		case qvalue.QValueKindFloat64:
			v, ok := qValue.Value.(float64)
			if !ok {
				src.err = fmt.Errorf("invalid float64 value")
				return nil, src.err
			}
			values[i] = v

		case qvalue.QValueKindInt16, qvalue.QValueKindInt32:
			v, ok := qValue.Value.(int32)
			if !ok {
				src.err = fmt.Errorf("invalid int32 value")
				return nil, src.err
			}
			values[i] = v

		case qvalue.QValueKindInt64:
			v, ok := qValue.Value.(int64)
			if !ok {
				src.err = fmt.Errorf("invalid int64 value")
				return nil, src.err
			}
			values[i] = v

		case qvalue.QValueKindBoolean:
			v, ok := qValue.Value.(bool)
			if !ok {
				src.err = fmt.Errorf("invalid boolean value")
				return nil, src.err
			}
			values[i] = v

		case qvalue.QValueKindString:
			v, ok := qValue.Value.(string)
			if !ok {
				src.err = fmt.Errorf("invalid string value")
				return nil, src.err
			}
			values[i] = v

		case qvalue.QValueKindTimestamp:
			t, ok := qValue.Value.(time.Time)
			if !ok {
				src.err = fmt.Errorf("invalid ExtendedTime value")
				return nil, src.err
			}
			timestamp := pgtype.Timestamp{Time: t, Valid: true}
			values[i] = timestamp

		case qvalue.QValueKindTimestampTZ:
			t, ok := qValue.Value.(time.Time)
			if !ok {
				src.err = fmt.Errorf("invalid ExtendedTime value")
				return nil, src.err
			}
			timestampTZ := pgtype.Timestamptz{Time: t, Valid: true}
			values[i] = timestampTZ
		case qvalue.QValueKindUUID:
			if qValue.Value == nil {
				values[i] = nil
				break
			}

			v, ok := qValue.Value.([16]byte) // treat it as byte slice
			if !ok {
				src.err = fmt.Errorf("invalid UUID value %v", qValue.Value)
				return nil, src.err
			}
			values[i] = uuid.UUID(v)

		case qvalue.QValueKindNumeric:
			v, ok := qValue.Value.(*big.Rat)
			if !ok {
				src.err = fmt.Errorf("invalid Numeric value %v", qValue.Value)
				return nil, src.err
			}
			// TODO: account for precision and scale issues.
			values[i] = v.FloatString(38)

		case qvalue.QValueKindBytes, qvalue.QValueKindBit:
			v, ok := qValue.Value.([]byte)
			if !ok {
				src.err = fmt.Errorf("invalid Bytes value")
				return nil, src.err
			}
			values[i] = v

		// And so on for the other types...
		default:
			src.err = fmt.Errorf("unsupported value type %s", qValue.Kind)
			return nil, src.err
		}
	}
	return values, nil
}

func (src *QRecordBatchCopyFromSource) Err() error {
	return src.err
}
