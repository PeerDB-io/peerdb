package model

import (
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/PeerDB-io/peer-flow/geo"
	"github.com/PeerDB-io/peer-flow/model/qvalue"
)

// QRecordBatch holds a batch of []QValue slices
type QRecordBatch struct {
	Schema  *QRecordSchema
	Records [][]qvalue.QValue
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
	v, ok := qValue.Value().([]T)
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
	err           error
	stream        *QRecordStream
	currentRecord QRecordOrError
	numRecords    int
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
		if qValue.Value() == nil {
			values[i] = nil
			continue
		}

		switch v := qValue.(type) {
		case qvalue.QValueFloat32:
			values[i] = v.Val
		case qvalue.QValueFloat64:
			values[i] = v.Val
		case qvalue.QValueInt16:
			values[i] = v.Val
		case qvalue.QValueInt32:
			values[i] = v.Val
		case qvalue.QValueInt64:
			values[i] = v.Val
		case qvalue.QValueBoolean:
			values[i] = v.Val
		case qvalue.QValueQChar:
			values[i] = rune(v.Val)
		case qvalue.QValueString:
			values[i] = v.Val
		case qvalue.QValueCIDR, qvalue.QValueINET:
			str, ok := v.Value().(string)
			if !ok {
				src.err = errors.New("invalid INET/CIDR value")
				return nil, src.err
			}
			values[i] = str

		case qvalue.QValueTime:
			values[i] = pgtype.Time{Microseconds: v.Val.UnixMicro(), Valid: true}
		case qvalue.QValueTimestamp:
			values[i] = pgtype.Timestamp{Time: v.Val, Valid: true}
		case qvalue.QValueTimestampTZ:
			values[i] = pgtype.Timestamptz{Time: v.Val, Valid: true}
		case qvalue.QValueUUID:
			values[i] = uuid.UUID(v.Val)
		case qvalue.QValueNumeric:
			values[i] = v.Val
		case qvalue.QValueBytes, qvalue.QValueBit:
			bytes, ok := v.Value().([]byte)
			if !ok {
				src.err = errors.New("invalid Bytes value")
				return nil, src.err
			}
			values[i] = bytes

		case qvalue.QValueDate:
			values[i] = pgtype.Date{Time: v.Val, Valid: true}
		case qvalue.QValueHStore:
			values[i] = v.Val
		case qvalue.QValueGeography, qvalue.QValueGeometry, qvalue.QValuePoint:
			geoWkt, ok := v.Value().(string)
			if !ok {
				src.err = errors.New("invalid Geospatial value")
				return nil, src.err
			}

			if strings.HasPrefix(geoWkt, "SRID=") {
				_, wkt, found := strings.Cut(geoWkt, ";")
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
		case qvalue.QValueArrayString:
			a, err := constructArray[string](qValue, "ArrayString")
			if err != nil {
				src.err = err
				return nil, src.err
			}
			values[i] = a

		case qvalue.QValueArrayDate, qvalue.QValueArrayTimestamp, qvalue.QValueArrayTimestampTZ:
			a, err := constructArray[time.Time](qValue, "ArrayTime")
			if err != nil {
				src.err = err
				return nil, src.err
			}
			values[i] = a

		case qvalue.QValueArrayInt16:
			a, err := constructArray[int16](qValue, "ArrayInt16")
			if err != nil {
				src.err = err
				return nil, src.err
			}
			values[i] = a

		case qvalue.QValueArrayInt32:
			a, err := constructArray[int32](qValue, "ArrayInt32")
			if err != nil {
				src.err = err
				return nil, src.err
			}
			values[i] = a

		case qvalue.QValueArrayInt64:
			a, err := constructArray[int64](qValue, "ArrayInt64")
			if err != nil {
				src.err = err
				return nil, src.err
			}
			values[i] = a

		case qvalue.QValueArrayFloat32:
			a, err := constructArray[float32](qValue, "ArrayFloat32")
			if err != nil {
				src.err = err
				return nil, src.err
			}
			values[i] = a

		case qvalue.QValueArrayFloat64:
			a, err := constructArray[float64](qValue, "ArrayFloat64")
			if err != nil {
				src.err = err
				return nil, src.err
			}
			values[i] = a
		case qvalue.QValueArrayBoolean:
			a, err := constructArray[bool](qValue, "ArrayBool")
			if err != nil {
				src.err = err
				return nil, src.err
			}
			values[i] = a
		case qvalue.QValueJSON:
			values[i] = v.Val

		// And so on for the other types...
		default:
			src.err = fmt.Errorf("unsupported value type %T", qValue)
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
