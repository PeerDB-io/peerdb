package model

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgtype"

	geo "github.com/PeerDB-io/peerdb/flow/datatypes"
	"github.com/PeerDB-io/peerdb/flow/model/qvalue"
)

func constructArray[T any](v []T) *pgtype.Array[T] {
	return &pgtype.Array[T]{
		Elements: v,
		Dims:     []pgtype.ArrayDimension{{Length: int32(len(v)), LowerBound: 1}},
		Valid:    true,
	}
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

func geoWktToWkb(wkt string) ([]byte, error) {
	if strings.HasPrefix(wkt, "SRID=") {
		_, wktWithoutSRID, found := strings.Cut(wkt, ";")
		if found {
			wkt = wktWithoutSRID
		}
	}

	return geo.GeoToWKB(wkt)
}

func (src *QRecordCopyFromSource) Values() ([]any, error) {
	if err := src.Err(); err != nil {
		return nil, err
	}

	values := make([]any, len(src.currentRecord))
	for i, qValue := range src.currentRecord {
		if qValue == nil || qValue.Value() == nil {
			values[i] = nil
			continue
		}

		switch v := qValue.(type) {
		case qvalue.QValueFloat32:
			values[i] = v.Val
		case qvalue.QValueFloat64:
			values[i] = v.Val
		case qvalue.QValueInt8:
			values[i] = v.Val
		case qvalue.QValueInt16:
			values[i] = v.Val
		case qvalue.QValueInt32:
			values[i] = v.Val
		case qvalue.QValueInt64:
			values[i] = v.Val
		case qvalue.QValueUInt8:
			values[i] = v.Val
		case qvalue.QValueUInt16:
			values[i] = v.Val
		case qvalue.QValueUInt32:
			values[i] = v.Val
		case qvalue.QValueUInt64:
			values[i] = v.Val
		case qvalue.QValueBoolean:
			values[i] = v.Val
		case qvalue.QValueQChar:
			values[i] = rune(v.Val)
		case qvalue.QValueString:
			values[i] = v.Val
		case qvalue.QValueEnum:
			values[i] = v.Val
		case qvalue.QValueCIDR:
			values[i] = v.Val
		case qvalue.QValueINET:
			values[i] = v.Val
		case qvalue.QValueMacaddr:
			values[i] = v.Val
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
		case qvalue.QValueNumeric:
			values[i] = v.Val
		case qvalue.QValueBytes:
			values[i] = v.Val
		case qvalue.QValueDate:
			values[i] = pgtype.Date{Time: v.Val, Valid: true}
		case qvalue.QValueHStore:
			values[i] = v.Val
		case qvalue.QValueGeography:
			wkb, err := geoWktToWkb(v.Val)
			if err != nil {
				return nil, err
			}
			values[i] = wkb
		case qvalue.QValueGeometry:
			wkb, err := geoWktToWkb(v.Val)
			if err != nil {
				return nil, err
			}
			values[i] = wkb
		case qvalue.QValuePoint:
			wkb, err := geoWktToWkb(v.Val)
			if err != nil {
				return nil, err
			}
			values[i] = wkb
		case qvalue.QValueArrayString:
			values[i] = constructArray(v.Val)
		case qvalue.QValueArrayEnum:
			values[i] = constructArray(v.Val)
		case qvalue.QValueArrayDate:
			values[i] = constructArray(v.Val)
		case qvalue.QValueArrayTimestamp:
			values[i] = constructArray(v.Val)
		case qvalue.QValueArrayTimestampTZ:
			values[i] = constructArray(v.Val)
		case qvalue.QValueArrayInt16:
			values[i] = constructArray(v.Val)
		case qvalue.QValueArrayInt32:
			values[i] = constructArray(v.Val)
		case qvalue.QValueArrayInt64:
			values[i] = constructArray(v.Val)
		case qvalue.QValueArrayFloat32:
			values[i] = constructArray(v.Val)
		case qvalue.QValueArrayFloat64:
			values[i] = constructArray(v.Val)
		case qvalue.QValueArrayBoolean:
			values[i] = constructArray(v.Val)
		case qvalue.QValueArrayUUID:
			values[i] = constructArray(v.Val)
		case qvalue.QValueJSON:
			if v.IsArray {
				var arrayJ []any
				if err := json.Unmarshal([]byte(v.Val), &arrayJ); err != nil {
					return nil, fmt.Errorf("failed to unmarshal JSON array: %v", err)
				}

				values[i] = arrayJ
			} else {
				values[i] = v.Val
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
