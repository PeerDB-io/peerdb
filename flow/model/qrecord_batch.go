package model

import (
	"fmt"
	"log/slog"
	"math/big"
	"time"

	"github.com/PeerDB-io/peer-flow/geo"
	"github.com/PeerDB-io/peer-flow/model/qvalue"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
)

// QRecordBatch holds a batch of QRecord objects.
type QRecordBatch struct {
	NumRecords uint32 // NumRecords represents the number of records in the batch.
	Records    []QRecord
	Schema     *QRecordSchema
}

func (q *QRecordBatch) ToQRecordStream(buffer int) (*QRecordStream, error) {
	stream := NewQRecordStream(buffer)

	slog.Info(fmt.Sprintf("Converting %d records to QRecordStream", q.NumRecords))

	go func() {
		err := stream.SetSchema(q.Schema)
		if err != nil {
			slog.Warn(err.Error())
		}

		for _, record := range q.Records {
			stream.Records <- QRecordOrError{
				Record: record,
			}
		}
		close(stream.Records)
	}()

	return stream, nil
}

type QRecordBatchCopyFromSource struct {
	numRecords    int
	stream        *QRecordStream
	currentRecord QRecordOrError
	err           error
}

func NewQRecordBatchCopyFromSource(
	stream *QRecordStream,
) *QRecordBatchCopyFromSource {
	return &QRecordBatchCopyFromSource{
		numRecords:    0,
		stream:        stream,
		currentRecord: QRecordOrError{},
		err:           nil,
	}
}

func (src *QRecordBatchCopyFromSource) Next() bool {
	rec, ok := <-src.stream.Records
	if !ok {
		return false
	}

	src.currentRecord = rec
	src.numRecords++
	return true
}

func (src *QRecordBatchCopyFromSource) Values() ([]interface{}, error) {
	if src.currentRecord.Err != nil {
		src.err = src.currentRecord.Err
		return nil, src.err
	}

	record := src.currentRecord.Record
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

		case qvalue.QValueKindDate:
			t, ok := qValue.Value.(time.Time)
			if !ok {
				src.err = fmt.Errorf("invalid Date value")
				return nil, src.err
			}
			date := pgtype.Date{Time: t, Valid: true}
			values[i] = date

		case qvalue.QValueKindHStore:
			v, ok := qValue.Value.(string)
			if !ok {
				src.err = fmt.Errorf("invalid HStore value")
				return nil, src.err
			}

			values[i] = v
		case qvalue.QValueKindGeography, qvalue.QValueKindGeometry, qvalue.QValueKindPoint:
			v, ok := qValue.Value.(string)
			if !ok {
				src.err = fmt.Errorf("invalid Geospatial value")
				return nil, src.err
			}

			wkb, err := geo.GeoToWKB(v)
			if err != nil {
				src.err = fmt.Errorf("failed to convert Geospatial value to wkb")
				return nil, src.err
			}

			values[i] = wkb
		case qvalue.QValueKindArrayString:
			v, ok := qValue.Value.([]string)
			if !ok {
				src.err = fmt.Errorf("invalid ArrayString value")
				return nil, src.err
			}
			values[i] = pgtype.Array[string]{
				Elements: v,
				Dims:     []pgtype.ArrayDimension{{Length: int32(len(v)), LowerBound: 1}},
				Valid:    true,
			}

		case qvalue.QValueKindArrayInt32:
			v, ok := qValue.Value.([]int32)
			if !ok {
				src.err = fmt.Errorf("invalid ArrayInt32 value")
				return nil, src.err
			}
			values[i] = pgtype.Array[int32]{
				Elements: v,
				Dims:     []pgtype.ArrayDimension{{Length: int32(len(v)), LowerBound: 1}},
				Valid:    true,
			}

		case qvalue.QValueKindArrayInt64:
			v, ok := qValue.Value.([]int64)
			if !ok {
				src.err = fmt.Errorf("invalid ArrayInt64 value")
				return nil, src.err
			}
			values[i] = pgtype.Array[int64]{
				Elements: v,
				Dims:     []pgtype.ArrayDimension{{Length: int32(len(v)), LowerBound: 1}},
				Valid:    true,
			}

		case qvalue.QValueKindArrayFloat32:
			v, ok := qValue.Value.([]float32)
			if !ok {
				src.err = fmt.Errorf("invalid ArrayFloat32 value")
				return nil, src.err
			}
			values[i] = pgtype.Array[float32]{
				Elements: v,
				Dims:     []pgtype.ArrayDimension{{Length: int32(len(v)), LowerBound: 1}},
				Valid:    true,
			}

		case qvalue.QValueKindArrayFloat64:
			v, ok := qValue.Value.([]float64)
			if !ok {
				src.err = fmt.Errorf("invalid ArrayFloat64 value")
				return nil, src.err
			}
			values[i] = pgtype.Array[float64]{
				Elements: v,
				Dims:     []pgtype.ArrayDimension{{Length: int32(len(v)), LowerBound: 1}},
				Valid:    true,
			}

		case qvalue.QValueKindJSON:
			v, ok := qValue.Value.(string)
			if !ok {
				src.err = fmt.Errorf("invalid JSON value")
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

func (src *QRecordBatchCopyFromSource) NumRecords() int {
	return src.numRecords
}

func (src *QRecordBatchCopyFromSource) Err() error {
	return src.err
}
