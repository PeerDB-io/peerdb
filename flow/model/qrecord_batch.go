package model

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"

	geo "github.com/PeerDB-io/peer-flow/datatypes"
	"github.com/PeerDB-io/peer-flow/model/qvalue"
)

// QRecordBatch holds a batch of []QValue slices
type QRecordBatch struct {
	Schema  qvalue.QRecordSchema
	Records [][]qvalue.QValue
}

func (q *QRecordBatch) ToQRecordStream(buffer int) *QRecordStream {
	stream := NewQRecordStream(min(buffer, len(q.Records)))
	go q.FeedToQRecordStream(stream)
	return stream
}

func (q *QRecordBatch) FeedToQRecordStream(stream *QRecordStream) {
	stream.SetSchema(q.Schema)

	for _, record := range q.Records {
		stream.Records <- record
	}
	close(stream.Records)
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
	currentRecord []qvalue.QValue
	numRecords    int
}

func NewQRecordBatchCopyFromSource(
	stream *QRecordStream,
) *QRecordBatchCopyFromSource {
	return &QRecordBatchCopyFromSource{
		numRecords:    0,
		stream:        stream,
		currentRecord: nil,
		err:           nil,
	}
}

func (src *QRecordBatchCopyFromSource) Next() bool {
	rec, ok := <-src.stream.Records
	if !ok {
		src.err = src.stream.Err()
		return false
	}

	src.currentRecord = rec
	src.numRecords++
	return true
}

func (src *QRecordBatchCopyFromSource) Values() ([]interface{}, error) {
	if src.err != nil {
		return nil, src.err
	}

	values := make([]interface{}, len(src.currentRecord))
	for i, qValue := range src.currentRecord {
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
		case qvalue.QValueCIDR, qvalue.QValueINET, qvalue.QValueMacaddr:
			str, ok := v.Value().(string)
			if !ok {
				src.err = errors.New("invalid INET/CIDR/MACADDR value")
				return nil, src.err
			}
			values[i] = str
		case qvalue.QValueTime:
			values[i] = pgtype.Time{Microseconds: v.Val.UnixMicro(), Valid: true}
		case qvalue.QValueTimestamp:
			values[i] = pgtype.Timestamp{Time: v.Val, Valid: true}
		case qvalue.QValueTimestampTZ:
			values[i] = pgtype.Timestamptz{Time: v.Val, Valid: true}
		case qvalue.QValueTSTZRange:
			values[i] = v.Val
		case qvalue.QValueUUID:
			values[i] = uuid.UUID(v.Val)
		case qvalue.QValueNumeric:
			values[i] = v.Val
		case qvalue.QValueBit:
			values[i] = v.Val
		case qvalue.QValueBytes:
			values[i] = v.Val
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
