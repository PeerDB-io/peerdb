package model

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"

	geo "github.com/PeerDB-io/peer-flow/datatypes"
	"github.com/PeerDB-io/peer-flow/model/qvalue"
)

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

type QRecordCopyFromSource struct {
	stream        *QRecordStream
	currentRecord []qvalue.QValue
}

func NewQRecordCopyFromSource(
	stream *QRecordStream,
) *QRecordCopyFromSource {
	return &QRecordCopyFromSource{
		stream:        stream,
		currentRecord: nil,
	}
}

func (src *QRecordCopyFromSource) Next() bool {
	rec, ok := <-src.stream.Records
	src.currentRecord = rec
	return ok || src.Err() != nil
}

func (src *QRecordCopyFromSource) Values() ([]interface{}, error) {
	if err := src.Err(); err != nil {
		return nil, err
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
				return nil, errors.New("invalid INET/CIDR/MACADDR value")
			}
			values[i] = str
		case qvalue.QValueTime:
			values[i] = pgtype.Time{Microseconds: v.Val.UnixMicro(), Valid: true}
		case qvalue.QValueTSTZRange:
			values[i] = v.Val
		case qvalue.QValueTimestamp:
			values[i] = pgtype.Timestamp{Time: v.Val, Valid: true}
		case qvalue.QValueTimestampTZ:
			values[i] = pgtype.Timestamptz{Time: v.Val, Valid: true}
		case qvalue.QValueUUID:
			values[i] = v.Val
		case qvalue.QValueArrayUUID:
			a, err := constructArray[uuid.UUID](qValue, "ArrayUUID")
			if err != nil {
				return nil, err
			}
			values[i] = a
		case qvalue.QValueNumeric:
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
				return nil, errors.New("invalid Geospatial value")
			}

			if strings.HasPrefix(geoWkt, "SRID=") {
				_, wkt, found := strings.Cut(geoWkt, ";")
				if found {
					geoWkt = wkt
				}
			}

			wkb, err := geo.GeoToWKB(geoWkt)
			if err != nil {
				return nil, fmt.Errorf("failed to convert Geospatial value to wkb: %v", err)
			}

			values[i] = wkb
		case qvalue.QValueArrayString:
			a, err := constructArray[string](qValue, "ArrayString")
			if err != nil {
				return nil, err
			}
			values[i] = a

		case qvalue.QValueArrayDate, qvalue.QValueArrayTimestamp, qvalue.QValueArrayTimestampTZ:
			a, err := constructArray[time.Time](qValue, "ArrayTime")
			if err != nil {
				return nil, err
			}
			values[i] = a

		case qvalue.QValueArrayInt16:
			a, err := constructArray[int16](qValue, "ArrayInt16")
			if err != nil {
				return nil, err
			}
			values[i] = a

		case qvalue.QValueArrayInt32:
			a, err := constructArray[int32](qValue, "ArrayInt32")
			if err != nil {
				return nil, err
			}
			values[i] = a

		case qvalue.QValueArrayInt64:
			a, err := constructArray[int64](qValue, "ArrayInt64")
			if err != nil {
				return nil, err
			}
			values[i] = a

		case qvalue.QValueArrayFloat32:
			a, err := constructArray[float32](qValue, "ArrayFloat32")
			if err != nil {
				return nil, err
			}
			values[i] = a

		case qvalue.QValueArrayFloat64:
			a, err := constructArray[float64](qValue, "ArrayFloat64")
			if err != nil {
				return nil, err
			}
			values[i] = a
		case qvalue.QValueArrayBoolean:
			a, err := constructArray[bool](qValue, "ArrayBool")
			if err != nil {
				return nil, err
			}
			values[i] = a
		case qvalue.QValueJSON:
			if v.IsArray {
				var arrayJ []interface{}
				if err := json.Unmarshal([]byte(v.Value().(string)), &arrayJ); err != nil {
					return nil, fmt.Errorf("failed to unmarshal JSON array: %v", err)
				}

				values[i] = arrayJ
			} else {
				values[i] = v.Value()
			}
		// And so on for the other types...
		default:
			return nil, fmt.Errorf("unsupported value type %T", qValue)
		}
	}
	return values, nil
}

func (src *QRecordCopyFromSource) Err() error {
	return src.stream.Err()
}
