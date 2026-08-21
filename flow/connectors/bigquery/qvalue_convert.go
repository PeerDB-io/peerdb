package connbigquery

import (
	"encoding/base64"
	"encoding/json"
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
		if fieldSchema.Repeated {
			return types.QValueKindArrayString
		}
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
		if fieldSchema.Repeated {
			return types.QValueKindArrayTimestamp
		}
		return types.QValueKindTimestamp
	case bigquery.DateFieldType:
		if fieldSchema.Repeated {
			return types.QValueKindArrayDate
		}
		return types.QValueKindDate
	case bigquery.TimeFieldType:
		if fieldSchema.Repeated {
			return types.QValueKindArrayString
		}
		return types.QValueKindTime
	case bigquery.NumericFieldType, bigquery.BigNumericFieldType:
		if fieldSchema.Repeated {
			return types.QValueKindArrayNumeric
		}
		return types.QValueKindNumeric
	case bigquery.GeographyFieldType:
		if fieldSchema.Repeated {
			return types.QValueKindArrayString
		}
		return types.QValueKindGeography
	case bigquery.JSONFieldType:
		if fieldSchema.Repeated {
			return types.QValueKindArrayJSON
		}
		return types.QValueKindJSON
	case bigquery.IntervalFieldType:
		if fieldSchema.Repeated {
			return types.QValueKindArrayInterval
		}
		return types.QValueKindInterval
	case bigquery.RangeFieldType:
		if fieldSchema.Repeated {
			return types.QValueKindArrayString
		}
		return types.QValueKindString
	case bigquery.RecordFieldType:
		// QValue has no structured-record kind. Preserve field names and values as
		// JSON text in STRING or ARRAY<STRING> values.

		if fieldSchema.Repeated {
			return types.QValueKindArrayString
		}

		return types.QValueKindString
	default:
		return types.QValueKindInvalid
	}
}

// numericRoundingScale fallback to default scale if precision and scale are not set.
func numericRoundingScale(qfield types.QField) int16 {
	if qfield.Precision == 0 && qfield.Scale == 0 {
		switch bigquery.FieldType(qfield.OriginalType) {
		case bigquery.NumericFieldType:
			return bigquery.NumericScaleDigits
		case bigquery.BigNumericFieldType:
			return bigquery.BigNumericScaleDigits
		default:
			return datatypes.PeerDBBigQueryScale
		}
	}
	return qfield.Scale
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
	precision := int16(bqField.Precision)
	scale := int16(bqField.Scale)
	if precision == 0 && scale == 0 {
		switch bqField.Type {
		case bigquery.NumericFieldType:
			precision = bigquery.NumericPrecisionDigits
			scale = bigquery.NumericScaleDigits
		case bigquery.BigNumericFieldType:
			precision = bigquery.BigNumericPrecisionDigits
			scale = bigquery.BigNumericScaleDigits
		}
	}

	return types.QField{
		Name:         bqField.Name,
		Type:         BigQueryTypeToQValueKind(bqField),
		OriginalType: string(bqField.Type),
		Precision:    precision,
		Scale:        scale,
		Nullable:     !bqField.Required,
	}
}

func qvalueFromBigQueryValue(
	qfield types.QField, value bigquery.Value, field *bigquery.FieldSchema,
) (types.QValue, error) {
	if value == nil {
		return types.QValueNull(qfield.Type), nil
	}

	if field.Repeated {
		values, ok := value.([]bigquery.Value)
		if !ok {
			return nil, fmt.Errorf("expected []bigquery.Value for repeated column %s, got %T", qfield.Name, value)
		}
		return qvalueArrayFromBigQueryValues(field, qfield, values)
	}

	switch field.Type {
	case bigquery.BooleanFieldType:
		v, ok := value.(bool)
		if !ok {
			return nil, unexpectedBigQueryValueType(qfield, value)
		}
		return types.QValueBoolean{Val: v}, nil
	case bigquery.IntegerFieldType:
		v, ok := value.(int64)
		if !ok {
			return nil, unexpectedBigQueryValueType(qfield, value)
		}
		return types.QValueInt64{Val: v}, nil
	case bigquery.FloatFieldType:
		v, ok := value.(float64)
		if !ok {
			return nil, unexpectedBigQueryValueType(qfield, value)
		}
		return types.QValueFloat64{Val: v}, nil
	case bigquery.BytesFieldType:
		v, ok := value.([]byte)
		if !ok {
			return nil, unexpectedBigQueryValueType(qfield, value)
		}
		return types.QValueBytes{Val: v}, nil
	case bigquery.StringFieldType:
		v, ok := value.(string)
		if !ok {
			return nil, unexpectedBigQueryValueType(qfield, value)
		}
		return types.QValueString{Val: v}, nil
	case bigquery.JSONFieldType:
		v, ok := value.(string)
		if !ok {
			return nil, unexpectedBigQueryValueType(qfield, value)
		}
		return types.QValueJSON{Val: v}, nil
	case bigquery.GeographyFieldType:
		v, ok := value.(string)
		if !ok {
			return nil, unexpectedBigQueryValueType(qfield, value)
		}
		return types.QValueGeography{Val: v}, nil
	case bigquery.TimestampFieldType:
		v, ok := value.(time.Time)
		if !ok {
			return nil, unexpectedBigQueryValueType(qfield, value)
		}
		return types.QValueTimestamp{Val: v}, nil
	case bigquery.DateFieldType:
		v, ok := value.(civil.Date)
		if !ok {
			return nil, unexpectedBigQueryValueType(qfield, value)
		}
		return types.QValueDate{Val: v.In(time.UTC)}, nil
	case bigquery.DateTimeFieldType:
		v, ok := value.(civil.DateTime)
		if !ok {
			return nil, unexpectedBigQueryValueType(qfield, value)
		}
		// BigQuery DATETIME is timezone-unaware; BigQueryTypeToQValueKind maps it
		// to QValueKindTimestamp (same as TIMESTAMP -- see the snapshot-export
		// path's CAST(... AS TIMESTAMP) treatment for the same reasoning), so it's
		// carried as a UTC time.Time here too.
		return types.QValueTimestamp{Val: v.In(time.UTC)}, nil
	case bigquery.TimeFieldType:
		v, ok := value.(civil.Time)
		if !ok {
			return nil, unexpectedBigQueryValueType(qfield, value)
		}
		return types.QValueTime{Val: civilTimeToDuration(v)}, nil
	case bigquery.NumericFieldType, bigquery.BigNumericFieldType:
		v, ok := value.(*big.Rat)
		if !ok {
			return nil, unexpectedBigQueryValueType(qfield, value)
		}
		return types.QValueNumeric{
			Val:       decimal.NewFromBigRat(v, int32(numericRoundingScale(qfield))),
			Precision: qfield.Precision,
			Scale:     qfield.Scale,
		}, nil
	case bigquery.IntervalFieldType:
		v, ok := value.(*bigquery.IntervalValue)
		if !ok {
			return nil, unexpectedBigQueryValueType(qfield, value)
		}
		return types.QValueInterval{Val: v.String()}, nil
	case bigquery.RangeFieldType:
		v, ok := value.(*bigquery.RangeValue)
		if !ok {
			return nil, unexpectedBigQueryValueType(qfield, value)
		}
		return types.QValueString{Val: bigQueryRangeString(v)}, nil
	case bigquery.RecordFieldType:
		v, ok := value.([]bigquery.Value)
		if !ok {
			return nil, unexpectedBigQueryValueType(qfield, value)
		}
		jsonValue, err := bigQueryRecordJSONString(field.Schema, v)
		if err != nil {
			return nil, fmt.Errorf("failed to encode record column %s: %w", qfield.Name, err)
		}
		return types.QValueString{Val: jsonValue}, nil
	default:
		return nil, fmt.Errorf("unsupported BigQuery field type %s for column %s", field.Type, qfield.Name)
	}
}

func qvalueArrayFromBigQueryValues(
	field *bigquery.FieldSchema, qfield types.QField, values []bigquery.Value,
) (types.QValue, error) {
	switch qfield.Type {
	case types.QValueKindArrayString:
		arr, err := bigQueryStringArray(field, qfield, values)
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
		if field.Type == bigquery.DateTimeFieldType {
			dateTimes, err := castBigQueryArray[civil.DateTime](qfield, values)
			if err != nil {
				return nil, err
			}
			arr := make([]time.Time, len(dateTimes))
			for i, dateTime := range dateTimes {
				arr[i] = dateTime.In(time.UTC)
			}
			return types.QValueArrayTimestamp{Val: arr}, nil
		}
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
			arr[i] = decimal.NewFromBigRat(r, int32(numericRoundingScale(qfield)))
		}
		return types.QValueArrayNumeric{Val: arr, Precision: qfield.Precision, Scale: qfield.Scale}, nil
	case types.QValueKindArrayInterval:
		intervals, err := castBigQueryArray[*bigquery.IntervalValue](qfield, values)
		if err != nil {
			return nil, err
		}
		arr := make([]string, len(intervals))
		for i, interval := range intervals {
			arr[i] = interval.String()
		}
		return types.QValueArrayInterval{Val: arr}, nil
	case types.QValueKindArrayJSON:
		jsonValues, err := castBigQueryArray[string](qfield, values)
		if err != nil {
			return nil, err
		}
		rawValues := make([]json.RawMessage, len(jsonValues))
		for i, jsonValue := range jsonValues {
			rawValues[i] = json.RawMessage(jsonValue)
		}
		encoded, err := json.Marshal(rawValues)
		if err != nil {
			return nil, fmt.Errorf("failed to encode JSON array column %s: %w", qfield.Name, err)
		}
		return types.QValueJSON{Val: string(encoded), IsArray: true}, nil
	default:
		return nil, fmt.Errorf("unsupported repeated BigQuery column type for %s: %s", qfield.Name, qfield.Type)
	}
}

func unexpectedBigQueryValueType(qfield types.QField, value bigquery.Value) error {
	return fmt.Errorf("unexpected BigQuery value type %T for column %s", value, qfield.Name)
}

func bigQueryStringArray(
	field *bigquery.FieldSchema, qfield types.QField, values []bigquery.Value,
) ([]string, error) {
	arr := make([]string, len(values))
	for i, value := range values {
		switch field.Type {
		case bigquery.StringFieldType, bigquery.GeographyFieldType:
			v, ok := value.(string)
			if !ok {
				return nil, fmt.Errorf("array column %s: element %d has unexpected type %T", qfield.Name, i, value)
			}
			arr[i] = v
		case bigquery.BytesFieldType:
			v, ok := value.([]byte)
			if !ok {
				return nil, fmt.Errorf("array column %s: element %d has unexpected type %T", qfield.Name, i, value)
			}
			arr[i] = base64.StdEncoding.EncodeToString(v)
		case bigquery.TimeFieldType:
			v, ok := value.(civil.Time)
			if !ok {
				return nil, fmt.Errorf("array column %s: element %d has unexpected type %T", qfield.Name, i, value)
			}
			arr[i] = bigquery.CivilTimeString(v)
		case bigquery.RangeFieldType:
			v, ok := value.(*bigquery.RangeValue)
			if !ok {
				return nil, fmt.Errorf("array column %s: element %d has unexpected type %T", qfield.Name, i, value)
			}
			arr[i] = bigQueryRangeString(v)
		case bigquery.RecordFieldType:
			v, ok := value.([]bigquery.Value)
			if !ok {
				return nil, fmt.Errorf("array column %s: element %d has unexpected type %T", qfield.Name, i, value)
			}
			encoded, err := bigQueryRecordJSONString(field.Schema, v)
			if err != nil {
				return nil, fmt.Errorf("array column %s: failed to encode record element %d: %w", qfield.Name, i, err)
			}
			arr[i] = encoded
		default:
			return nil, fmt.Errorf("unsupported repeated BigQuery string representation for %s: %s", qfield.Name, field.Type)
		}
	}
	return arr, nil
}

func bigQueryRecordJSONString(schema bigquery.Schema, values []bigquery.Value) (string, error) {
	if len(schema) != len(values) {
		return "", fmt.Errorf("record schema has %d fields but value has %d elements", len(schema), len(values))
	}
	record := make(map[string]any, len(schema))
	for i, field := range schema {
		value, err := bigQueryValueForJSON(field, values[i])
		if err != nil {
			return "", fmt.Errorf("field %s: %w", field.Name, err)
		}
		record[field.Name] = value
	}
	encoded, err := json.Marshal(record)
	if err != nil {
		return "", err
	}
	return string(encoded), nil
}

func bigQueryValueForJSON(field *bigquery.FieldSchema, value bigquery.Value) (any, error) {
	if value == nil {
		return nil, nil
	}
	if field.Repeated {
		values, ok := value.([]bigquery.Value)
		if !ok {
			return nil, fmt.Errorf("expected []bigquery.Value, got %T", value)
		}
		elementField := *field
		elementField.Repeated = false
		arr := make([]any, len(values))
		for i, element := range values {
			converted, err := bigQueryValueForJSON(&elementField, element)
			if err != nil {
				return nil, fmt.Errorf("element %d: %w", i, err)
			}
			arr[i] = converted
		}
		return arr, nil
	}

	switch field.Type {
	case bigquery.RecordFieldType:
		values, ok := value.([]bigquery.Value)
		if !ok {
			return nil, fmt.Errorf("expected []bigquery.Value, got %T", value)
		}
		encoded, err := bigQueryRecordJSONString(field.Schema, values)
		if err != nil {
			return nil, err
		}
		return json.RawMessage(encoded), nil
	case bigquery.BytesFieldType:
		v, ok := value.([]byte)
		if !ok {
			return nil, fmt.Errorf("expected []byte, got %T", value)
		}
		return base64.StdEncoding.EncodeToString(v), nil
	case bigquery.DateFieldType:
		v, ok := value.(civil.Date)
		if !ok {
			return nil, fmt.Errorf("expected civil.Date, got %T", value)
		}
		return v.String(), nil
	case bigquery.TimeFieldType:
		v, ok := value.(civil.Time)
		if !ok {
			return nil, fmt.Errorf("expected civil.Time, got %T", value)
		}
		return bigquery.CivilTimeString(v), nil
	case bigquery.DateTimeFieldType:
		v, ok := value.(civil.DateTime)
		if !ok {
			return nil, fmt.Errorf("expected civil.DateTime, got %T", value)
		}
		return bigquery.CivilDateTimeString(v), nil
	case bigquery.TimestampFieldType:
		v, ok := value.(time.Time)
		if !ok {
			return nil, fmt.Errorf("expected time.Time, got %T", value)
		}
		return v.UTC().Format(time.RFC3339Nano), nil
	case bigquery.NumericFieldType, bigquery.BigNumericFieldType:
		v, ok := value.(*big.Rat)
		if !ok {
			return nil, fmt.Errorf("expected *big.Rat, got %T", value)
		}
		scale := bigquery.NumericScaleDigits
		if field.Type == bigquery.BigNumericFieldType {
			scale = bigquery.BigNumericScaleDigits
		}
		return json.Number(decimal.NewFromBigRat(v, int32(scale)).String()), nil
	case bigquery.JSONFieldType:
		v, ok := value.(string)
		if !ok {
			return nil, fmt.Errorf("expected string, got %T", value)
		}
		return json.RawMessage(v), nil
	case bigquery.IntervalFieldType:
		v, ok := value.(*bigquery.IntervalValue)
		if !ok {
			return nil, fmt.Errorf("expected *bigquery.IntervalValue, got %T", value)
		}
		return v.String(), nil
	case bigquery.RangeFieldType:
		v, ok := value.(*bigquery.RangeValue)
		if !ok {
			return nil, fmt.Errorf("expected *bigquery.RangeValue, got %T", value)
		}
		return bigQueryRangeString(v), nil
	default:
		return value, nil
	}
}

func bigQueryRangeString(value *bigquery.RangeValue) string {
	return fmt.Sprintf("[%s, %s)", bigQueryRangeBoundString(value.Start), bigQueryRangeBoundString(value.End))
}

func bigQueryRangeBoundString(value bigquery.Value) string {
	switch v := value.(type) {
	case nil:
		return "UNBOUNDED"
	case civil.Date:
		return v.String()
	case civil.DateTime:
		return bigquery.CivilDateTimeString(v)
	case time.Time:
		return v.UTC().Format("2006-01-02 15:04:05.999999 UTC")
	default:
		return fmt.Sprint(v)
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
