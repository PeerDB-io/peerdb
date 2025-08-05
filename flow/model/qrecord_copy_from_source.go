package model

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgtype"

	geo "github.com/PeerDB-io/peerdb/flow/shared/datatypes"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
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
	currentRecord []types.QValue
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
		case types.QValueFloat32:
			values[i] = v.Val
		case types.QValueFloat64:
			values[i] = v.Val
		case types.QValueInt8:
			values[i] = v.Val
		case types.QValueInt16:
			values[i] = v.Val
		case types.QValueInt32:
			values[i] = v.Val
		case types.QValueInt64:
			values[i] = v.Val
		case types.QValueUInt8:
			values[i] = v.Val
		case types.QValueUInt16:
			values[i] = v.Val
		case types.QValueUInt32:
			values[i] = v.Val
		case types.QValueUInt64:
			values[i] = v.Val
		case types.QValueBoolean:
			values[i] = v.Val
		case types.QValueQChar:
			values[i] = rune(v.Val)
		case types.QValueString:
			values[i] = v.Val
		case types.QValueEnum:
			values[i] = v.Val
		case types.QValueCIDR:
			values[i] = v.Val
		case types.QValueINET:
			values[i] = v.Val
		case types.QValueMacaddr:
			values[i] = v.Val
		case types.QValueTime:
			values[i] = pgtype.Time{Microseconds: int64(v.Val / time.Microsecond), Valid: true}
		case types.QValueTimeTZ:
			values[i] = pgtype.Time{Microseconds: int64(v.Val / time.Microsecond), Valid: true}
		case types.QValueInterval:
			values[i] = v.Val
		case types.QValueTimestamp:
			values[i] = pgtype.Timestamp{Time: v.Val, Valid: true}
		case types.QValueTimestampTZ:
			values[i] = pgtype.Timestamptz{Time: v.Val, Valid: true}
		case types.QValueUUID:
			values[i] = v.Val
		case types.QValueNumeric:
			values[i] = v.Val
		case types.QValueBytes:
			values[i] = v.Val
		case types.QValueDate:
			values[i] = pgtype.Date{Time: v.Val, Valid: true}
		case types.QValueHStore:
			values[i] = v.Val
		case types.QValueGeography:
			wkb, err := geoWktToWkb(v.Val)
			if err != nil {
				return nil, err
			}
			values[i] = wkb
		case types.QValueGeometry:
			wkb, err := geoWktToWkb(v.Val)
			if err != nil {
				return nil, err
			}
			values[i] = wkb
		case types.QValuePoint:
			wkb, err := geoWktToWkb(v.Val)
			if err != nil {
				return nil, err
			}
			values[i] = wkb
		case types.QValueArrayString:
			values[i] = constructArray(v.Val)
		case types.QValueArrayEnum:
			values[i] = constructArray(v.Val)
		case types.QValueArrayDate:
			values[i] = constructArray(v.Val)
		case types.QValueArrayInterval:
			values[i] = constructArray(v.Val)
		case types.QValueArrayTimestamp:
			values[i] = constructArray(v.Val)
		case types.QValueArrayTimestampTZ:
			values[i] = constructArray(v.Val)
		case types.QValueArrayInt16:
			values[i] = constructArray(v.Val)
		case types.QValueArrayInt32:
			values[i] = constructArray(v.Val)
		case types.QValueArrayInt64:
			values[i] = constructArray(v.Val)
		case types.QValueArrayFloat32:
			values[i] = constructArray(v.Val)
		case types.QValueArrayFloat64:
			values[i] = constructArray(v.Val)
		case types.QValueArrayBoolean:
			values[i] = constructArray(v.Val)
		case types.QValueArrayUUID:
			values[i] = constructArray(v.Val)
		case types.QValueArrayNumeric:
			values[i] = constructArray(v.Val)
		case types.QValueJSON:
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
