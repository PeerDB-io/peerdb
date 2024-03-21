package model

import (
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/shopspring/decimal"

	"github.com/PeerDB-io/peer-flow/geo"
	"github.com/PeerDB-io/peer-flow/model/qvalue"
)

// QRecordBatch holds a batch of []QValue slices
type QRecordBatch struct {
	Records [][]qvalue.QValue
	Schema  *QRecordSchema
}

func (q *QRecordBatch) ToQRecordStream(buffer int) (*QRecordStream, error) {
	stream := NewQRecordStream(buffer)

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

func constructArray[T any](qValue qvalue.QValue, typeName string) (*pgtype.Array[T], error) {
	v, ok := qValue.Value.([]T)
	if !ok {
		return nil, fmt.Errorf("invalid %s value", typeName)
	}
	return &pgtype.Array[T]{
		Elements: v,
		Dims:     []pgtype.ArrayDimension{{Length: int32(len(v)), LowerBound: 1}},
		Valid:    true,
	}, nil
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
	numEntries := len(record)

	values := make([]interface{}, numEntries)
	for i, qValue := range record {
		if qValue.Value == nil {
			values[i] = nil
			continue
		}

		switch qValue.Kind {
		case qvalue.QValueKindFloat32:
			v, ok := qValue.Value.(float32)
			if !ok {
				src.err = errors.New("invalid float32 value")
				return nil, src.err
			}
			values[i] = v

		case qvalue.QValueKindFloat64:
			v, ok := qValue.Value.(float64)
			if !ok {
				src.err = errors.New("invalid float64 value")
				return nil, src.err
			}
			values[i] = v

		case qvalue.QValueKindInt16, qvalue.QValueKindInt32:
			v, ok := qValue.Value.(int32)
			if !ok {
				src.err = errors.New("invalid int32 value")
				return nil, src.err
			}
			values[i] = v

		case qvalue.QValueKindInt64:
			v, ok := qValue.Value.(int64)
			if !ok {
				src.err = errors.New("invalid int64 value")
				return nil, src.err
			}
			values[i] = v

		case qvalue.QValueKindBoolean:
			v, ok := qValue.Value.(bool)
			if !ok {
				src.err = errors.New("invalid boolean value")
				return nil, src.err
			}
			values[i] = v

		case qvalue.QValueKindQChar:
			v, ok := qValue.Value.(uint8)
			if !ok {
				src.err = errors.New("invalid \"char\" value")
				return nil, src.err
			}
			values[i] = rune(v)

		case qvalue.QValueKindString:
			v, ok := qValue.Value.(string)
			if !ok {
				src.err = errors.New("invalid string value")
				return nil, src.err
			}
			values[i] = v

		case qvalue.QValueKindCIDR, qvalue.QValueKindINET:
			v, ok := qValue.Value.(string)
			if !ok {
				src.err = errors.New("invalid INET/CIDR value")
				return nil, src.err
			}
			values[i] = v

		case qvalue.QValueKindTime:
			t, ok := qValue.Value.(time.Time)
			if !ok {
				src.err = errors.New("invalid Time value")
				return nil, src.err
			}
			time := pgtype.Time{Microseconds: t.UnixMicro(), Valid: true}
			values[i] = time

		case qvalue.QValueKindTimestamp:
			t, ok := qValue.Value.(time.Time)
			if !ok {
				src.err = errors.New("invalid ExtendedTime value")
				return nil, src.err
			}
			timestamp := pgtype.Timestamp{Time: t, Valid: true}
			values[i] = timestamp

		case qvalue.QValueKindTimestampTZ:
			t, ok := qValue.Value.(time.Time)
			if !ok {
				src.err = errors.New("invalid ExtendedTime value")
				return nil, src.err
			}
			timestampTZ := pgtype.Timestamptz{Time: t, Valid: true}
			values[i] = timestampTZ

		case qvalue.QValueKindUUID:
			v, ok := qValue.Value.([16]byte) // treat it as byte slice
			if !ok {
				src.err = fmt.Errorf("invalid UUID value %v", qValue.Value)
				return nil, src.err
			}
			values[i] = uuid.UUID(v)

		case qvalue.QValueKindNumeric:
			v, ok := qValue.Value.(decimal.Decimal)
			if !ok {
				src.err = fmt.Errorf("invalid Numeric value %v", qValue.Value)
				return nil, src.err
			}
			values[i] = v

		case qvalue.QValueKindBytes, qvalue.QValueKindBit:
			v, ok := qValue.Value.([]byte)
			if !ok {
				src.err = errors.New("invalid Bytes value")
				return nil, src.err
			}
			values[i] = v

		case qvalue.QValueKindDate:
			t, ok := qValue.Value.(time.Time)
			if !ok {
				src.err = errors.New("invalid Date value")
				return nil, src.err
			}
			date := pgtype.Date{Time: t, Valid: true}
			values[i] = date

		case qvalue.QValueKindHStore:
			v, ok := qValue.Value.(string)
			if !ok {
				src.err = errors.New("invalid HStore value")
				return nil, src.err
			}

			values[i] = v
		case qvalue.QValueKindGeography, qvalue.QValueKindGeometry, qvalue.QValueKindPoint:
			v, ok := qValue.Value.(string)
			if !ok {
				src.err = errors.New("invalid Geospatial value")
				return nil, src.err
			}

			geoWkt := v
			if strings.HasPrefix(v, "SRID=") {
				_, wkt, found := strings.Cut(v, ";")
				if found {
					geoWkt = wkt
				}
			}

			wkb, err := geo.GeoToWKB(geoWkt)
			if err != nil {
				src.err = fmt.Errorf("failed to convert Geospatial value to wkb: %v", err)
				return nil, src.err
			}

			values[i] = wkb
		case qvalue.QValueKindArrayString:
			v, err := constructArray[string](qValue, "ArrayString")
			if err != nil {
				src.err = err
				return nil, src.err
			}
			values[i] = v

		case qvalue.QValueKindArrayDate, qvalue.QValueKindArrayTimestamp, qvalue.QValueKindArrayTimestampTZ:
			v, err := constructArray[time.Time](qValue, "ArrayTime")
			if err != nil {
				src.err = err
				return nil, src.err
			}
			values[i] = v

		case qvalue.QValueKindArrayInt16:
			v, err := constructArray[int16](qValue, "ArrayInt16")
			if err != nil {
				src.err = err
				return nil, src.err
			}
			values[i] = v

		case qvalue.QValueKindArrayInt32:
			v, err := constructArray[int32](qValue, "ArrayInt32")
			if err != nil {
				src.err = err
				return nil, src.err
			}
			values[i] = v

		case qvalue.QValueKindArrayInt64:
			v, err := constructArray[int64](qValue, "ArrayInt64")
			if err != nil {
				src.err = err
				return nil, src.err
			}
			values[i] = v

		case qvalue.QValueKindArrayFloat32:
			v, err := constructArray[float32](qValue, "ArrayFloat32")
			if err != nil {
				src.err = err
				return nil, src.err
			}
			values[i] = v

		case qvalue.QValueKindArrayFloat64:
			v, err := constructArray[float64](qValue, "ArrayFloat64")
			if err != nil {
				src.err = err
				return nil, src.err
			}
			values[i] = v
		case qvalue.QValueKindArrayBoolean:
			v, err := constructArray[bool](qValue, "ArrayBool")
			if err != nil {
				src.err = err
				return nil, src.err
			}
			values[i] = v
		case qvalue.QValueKindJSON:
			v, ok := qValue.Value.(string)
			if !ok {
				src.err = errors.New("invalid JSON value")
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
