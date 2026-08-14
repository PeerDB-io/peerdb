package connbigquery

import (
	"fmt"
	"math/big"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/civil"
	"github.com/shopspring/decimal"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/shared/datatypes"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

func qValueKindToBigQueryType(columnDescription *protos.FieldDescription, nullableEnabled bool) bigquery.FieldSchema {
	bqField := bigquery.FieldSchema{
		Name:     columnDescription.Name,
		Required: nullableEnabled && !columnDescription.Nullable,
	}
	switch types.QValueKind(columnDescription.Type) {
	// boolean
	case types.QValueKindBoolean:
		bqField.Type = bigquery.BooleanFieldType
	// integer types
	case types.QValueKindInt8, types.QValueKindInt16, types.QValueKindInt32, types.QValueKindInt64,
		types.QValueKindUInt8, types.QValueKindUInt16, types.QValueKindUInt32, types.QValueKindUInt64,
		types.QValueKindUint16Enum, types.QValueKindUint64Set:
		bqField.Type = bigquery.IntegerFieldType
	// decimal types
	case types.QValueKindFloat32, types.QValueKindFloat64:
		bqField.Type = bigquery.FloatFieldType
	case types.QValueKindNumeric:
		precision, scale := datatypes.GetNumericTypeForWarehouse(columnDescription.TypeModifier, datatypes.BigQueryNumericCompatibility{})
		bqField.Type = bigquery.BigNumericFieldType
		bqField.Precision = int64(precision)
		bqField.Scale = int64(scale)
	case types.QValueKindArrayNumeric:
		precision, scale := datatypes.GetNumericTypeForWarehouse(columnDescription.TypeModifier, datatypes.BigQueryNumericCompatibility{})
		bqField.Type = bigquery.BigNumericFieldType
		bqField.Precision = int64(precision)
		bqField.Scale = int64(scale)
		bqField.Repeated = true
	// string related
	case types.QValueKindString, types.QValueKindEnum:
		bqField.Type = bigquery.StringFieldType
	// json related
	case types.QValueKindJSON, types.QValueKindJSONB, types.QValueKindHStore:
		bqField.Type = bigquery.JSONFieldType
	// time related
	case types.QValueKindTimestamp, types.QValueKindTimestampTZ:
		bqField.Type = bigquery.TimestampFieldType
	case types.QValueKindDate:
		bqField.Type = bigquery.DateFieldType
	case types.QValueKindTime, types.QValueKindTimeTZ:
		bqField.Type = bigquery.TimeFieldType
	// bytes
	case types.QValueKindBytes:
		bqField.Type = bigquery.BytesFieldType
	case types.QValueKindArrayInt16, types.QValueKindArrayInt32, types.QValueKindArrayInt64:
		bqField.Type = bigquery.IntegerFieldType
		bqField.Repeated = true
	case types.QValueKindArrayFloat32, types.QValueKindArrayFloat64:
		bqField.Type = bigquery.FloatFieldType
		bqField.Repeated = true
	case types.QValueKindArrayBoolean:
		bqField.Type = bigquery.BooleanFieldType
		bqField.Repeated = true
	case types.QValueKindArrayTimestamp, types.QValueKindArrayTimestampTZ:
		bqField.Type = bigquery.TimestampFieldType
		bqField.Repeated = true
	case types.QValueKindArrayDate:
		bqField.Type = bigquery.DateFieldType
		bqField.Repeated = true
	case types.QValueKindArrayString, types.QValueKindArrayEnum, types.QValueKindArrayInterval:
		bqField.Type = bigquery.StringFieldType
		bqField.Repeated = true
	case types.QValueKindGeography, types.QValueKindGeometry, types.QValueKindPoint:
		bqField.Type = bigquery.GeographyFieldType
	// UUID related - stored as strings
	case types.QValueKindUUID:
		bqField.Type = bigquery.StringFieldType
	case types.QValueKindArrayUUID:
		bqField.Type = bigquery.StringFieldType
		bqField.Repeated = true
	// rest will be strings
	default:
		bqField.Type = bigquery.StringFieldType
	}

	return bqField
}

// BigQueryTypeToQValueKind converts a bigquery.FieldType to a QValueKind
func BigQueryTypeToQValueKind(fieldSchema *bigquery.FieldSchema) types.QValueKind {
	switch fieldSchema.Type {
	case bigquery.StringFieldType:
		if fieldSchema.Repeated {
			return types.QValueKindArrayString
		}
		return types.QValueKindString
	case bigquery.BytesFieldType:
		return types.QValueKindBytes
	case bigquery.IntegerFieldType:
		if fieldSchema.Repeated {
			return types.QValueKindArrayInt64
		}
		return types.QValueKindInt64
	case bigquery.FloatFieldType:
		if fieldSchema.Repeated {
			return types.QValueKindArrayFloat64
		}
		return types.QValueKindFloat64
	case bigquery.BooleanFieldType:
		if fieldSchema.Repeated {
			return types.QValueKindArrayBoolean
		}
		return types.QValueKindBoolean
	case bigquery.TimestampFieldType:
		if fieldSchema.Repeated {
			return types.QValueKindArrayTimestamp
		}
		return types.QValueKindTimestamp
	case bigquery.DateTimeFieldType:
		return types.QValueKindTimestamp
	case bigquery.DateFieldType:
		if fieldSchema.Repeated {
			return types.QValueKindArrayDate
		}
		return types.QValueKindDate
	case bigquery.TimeFieldType:
		return types.QValueKindTime
	case bigquery.NumericFieldType, bigquery.BigNumericFieldType:
		if fieldSchema.Repeated {
			return types.QValueKindArrayNumeric
		}
		return types.QValueKindNumeric
	case bigquery.GeographyFieldType:
		return types.QValueKindGeography
	case bigquery.JSONFieldType:
		return types.QValueKindJSON
	case bigquery.RecordFieldType:
		// We treat RECORD as STRING or ARRAY<STRING>
		// In the future, we can consider mapping to JSON.

		if fieldSchema.Repeated {
			return types.QValueKindArrayString
		}

		return types.QValueKindString
	default:
		return types.QValueKindInvalid
	}
}

func fieldNormalizedTypeName(field *bigquery.FieldSchema) string {
	typeName := createTableCompatibleTypeName(field.Type)

	switch field.Type {
	case bigquery.StringFieldType, bigquery.BytesFieldType:
		if field.MaxLength == 0 {
			break
		}

		typeName = fmt.Sprintf("%s(%d)", typeName, field.MaxLength)
	case bigquery.NumericFieldType, bigquery.BigNumericFieldType:
		if field.Precision == 0 {
			break
		}
		if field.Scale > 0 {
			typeName = fmt.Sprintf("%s(%d,%d)", typeName, field.Precision, field.Scale)
		} else {
			typeName = fmt.Sprintf("%s(%d)", typeName, field.Precision)
		}
	}

	if field.Repeated {
		typeName = fmt.Sprintf("ARRAY<%s>", typeName)
	}

	return typeName
}

func createTableCompatibleTypeName(schemaType bigquery.FieldType) string {
	if schemaType == bigquery.FloatFieldType {
		return "FLOAT64"
	}
	if schemaType == bigquery.BooleanFieldType {
		return "BOOL"
	}
	return string(schemaType)
}

func qValueKindToBigQueryTypeString(columnDescription *protos.FieldDescription, nullEnabled bool, forMerge bool) string {
	bqTypeSchema := qValueKindToBigQueryType(columnDescription, nullEnabled)
	bqType := createTableCompatibleTypeName(bqTypeSchema.Type)
	if bqTypeSchema.Type == bigquery.BigNumericFieldType && !forMerge {
		bqType = fmt.Sprintf("BIGNUMERIC(%d,%d)", bqTypeSchema.Precision, bqTypeSchema.Scale)
	}
	if bqTypeSchema.Repeated && !forMerge {
		return "ARRAY<" + bqType + ">"
	}
	return bqType
}

func BigQueryFieldToQField(bqField *bigquery.FieldSchema) types.QField {
	return types.QField{
		Name:      bqField.Name,
		Type:      BigQueryTypeToQValueKind(bqField),
		Precision: int16(bqField.Precision),
		Scale:     int16(bqField.Scale),
		Nullable:  !bqField.Required,
	}
}

// qvalueFromBigQueryValue converts one column of a query result row (e.g. from
// APPENDS()/CHANGES()) into the QValue variant matching qfield.Type, as computed by
// BigQueryFieldToQField from the same result schema. The Go SDK hands back an
// already-typed value per BigQuery type (see cloud.google.com/go/bigquery's
// convertBasicType): string, []byte, int64, float64, bool, time.Time (TIMESTAMP),
// civil.Date (DATE), civil.Time (TIME), civil.DateTime (DATETIME), *big.Rat
// (NUMERIC/BIGNUMERIC), or []bigquery.Value for a REPEATED column.
//
// RECORD/STRUCT columns aren't supported: their Go value is also []bigquery.Value,
// which doesn't match any non-array qfield.Type, so they fail here with a clear
// error rather than being silently misread.
func qvalueFromBigQueryValue(qfield types.QField, value bigquery.Value) (types.QValue, error) {
	if value == nil {
		return types.QValueNull(qfield.Type), nil
	}

	switch qfield.Type {
	case types.QValueKindArrayString, types.QValueKindArrayInt64, types.QValueKindArrayFloat64,
		types.QValueKindArrayBoolean, types.QValueKindArrayTimestamp, types.QValueKindArrayDate,
		types.QValueKindArrayNumeric:
		values, ok := value.([]bigquery.Value)
		if !ok {
			return nil, fmt.Errorf("expected []bigquery.Value for repeated column %s, got %T", qfield.Name, value)
		}
		return qvalueArrayFromBigQueryValues(qfield, values)
	}

	switch v := value.(type) {
	case bool:
		return types.QValueBoolean{Val: v}, nil
	case int64:
		return types.QValueInt64{Val: v}, nil
	case float64:
		return types.QValueFloat64{Val: v}, nil
	case []byte:
		return types.QValueBytes{Val: v}, nil
	case string:
		switch qfield.Type {
		case types.QValueKindJSON:
			return types.QValueJSON{Val: v}, nil
		case types.QValueKindGeography:
			return types.QValueGeography{Val: v}, nil
		default:
			return types.QValueString{Val: v}, nil
		}
	case time.Time:
		return types.QValueTimestamp{Val: v}, nil
	case civil.Date:
		return types.QValueDate{Val: v.In(time.UTC)}, nil
	case civil.DateTime:
		// BigQuery DATETIME is timezone-unaware; BigQueryTypeToQValueKind maps it
		// to QValueKindTimestamp (same as TIMESTAMP -- see the snapshot-export
		// path's CAST(... AS TIMESTAMP) treatment for the same reasoning), so it's
		// carried as a UTC time.Time here too.
		return types.QValueTimestamp{Val: v.In(time.UTC)}, nil
	case civil.Time:
		return types.QValueTime{Val: civilTimeToDuration(v)}, nil
	case *big.Rat:
		return types.QValueNumeric{
			Val:       decimal.NewFromBigRat(v, int32(qfield.Scale)),
			Precision: qfield.Precision,
			Scale:     qfield.Scale,
		}, nil
	default:
		return nil, fmt.Errorf("unsupported BigQuery value type %T for column %s", value, qfield.Name)
	}
}

// qvalueArrayFromBigQueryValues converts a REPEATED column's value, which the Go
// SDK returns as []bigquery.Value with one already-typed element per the field's
// base (non-repeated) type.
func qvalueArrayFromBigQueryValues(qfield types.QField, values []bigquery.Value) (types.QValue, error) {
	switch qfield.Type {
	case types.QValueKindArrayString:
		arr, err := castBigQueryArray[string](qfield, values)
		return types.QValueArrayString{Val: arr}, err
	case types.QValueKindArrayInt64:
		arr, err := castBigQueryArray[int64](qfield, values)
		return types.QValueArrayInt64{Val: arr}, err
	case types.QValueKindArrayFloat64:
		arr, err := castBigQueryArray[float64](qfield, values)
		return types.QValueArrayFloat64{Val: arr}, err
	case types.QValueKindArrayBoolean:
		arr, err := castBigQueryArray[bool](qfield, values)
		return types.QValueArrayBoolean{Val: arr}, err
	case types.QValueKindArrayTimestamp:
		arr, err := castBigQueryArray[time.Time](qfield, values)
		return types.QValueArrayTimestamp{Val: arr}, err
	case types.QValueKindArrayDate:
		dates, err := castBigQueryArray[civil.Date](qfield, values)
		if err != nil {
			return nil, err
		}
		arr := make([]time.Time, len(dates))
		for i, d := range dates {
			arr[i] = d.In(time.UTC)
		}
		return types.QValueArrayDate{Val: arr}, nil
	case types.QValueKindArrayNumeric:
		rats, err := castBigQueryArray[*big.Rat](qfield, values)
		if err != nil {
			return nil, err
		}
		arr := make([]decimal.Decimal, len(rats))
		for i, r := range rats {
			arr[i] = decimal.NewFromBigRat(r, int32(qfield.Scale))
		}
		return types.QValueArrayNumeric{Val: arr, Precision: qfield.Precision, Scale: qfield.Scale}, nil
	default:
		return nil, fmt.Errorf("unsupported repeated BigQuery column type for %s: %s", qfield.Name, qfield.Type)
	}
}

// castBigQueryArray asserts every element of a REPEATED column's values to T,
// mirroring the analogous helper in the cockroachdb connector's qvalue_convert.go.
func castBigQueryArray[T any](qfield types.QField, values []bigquery.Value) ([]T, error) {
	arr := make([]T, len(values))
	for i, v := range values {
		t, ok := v.(T)
		if !ok {
			return nil, fmt.Errorf("array column %s: element %d has unexpected type %T", qfield.Name, i, v)
		}
		arr[i] = t
	}
	return arr, nil
}

// civilTimeToDuration converts a civil.Time (BigQuery TIME) into the time.Duration
// since midnight that types.QValueTime expects.
func civilTimeToDuration(t civil.Time) time.Duration {
	return time.Duration(t.Hour)*time.Hour +
		time.Duration(t.Minute)*time.Minute +
		time.Duration(t.Second)*time.Second +
		time.Duration(t.Nanosecond)*time.Nanosecond
}
