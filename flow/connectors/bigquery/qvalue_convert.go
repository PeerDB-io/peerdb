package connbigquery

import (
	"fmt"

	"cloud.google.com/go/bigquery"

	"github.com/PeerDB-io/peerdb/flow/datatypes"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
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
		types.QValueKindUInt8, types.QValueKindUInt16, types.QValueKindUInt32, types.QValueKindUInt64:
		bqField.Type = bigquery.IntegerFieldType
	// decimal types
	case types.QValueKindFloat32, types.QValueKindFloat64:
		bqField.Type = bigquery.FloatFieldType
	case types.QValueKindNumeric:
		precision, scale := datatypes.GetNumericTypeForWarehouse(columnDescription.TypeModifier, datatypes.BigQueryNumericCompatibility{})
		bqField.Type = bigquery.BigNumericFieldType
		bqField.Precision = int64(precision)
		bqField.Scale = int64(scale)
	// string related
	case types.QValueKindString, types.QValueKindEnum:
		bqField.Type = bigquery.StringFieldType
	// json related
	case types.QValueKindJSON, types.QValueKindJSONB, types.QValueKindHStore:
		bqField.Type = bigquery.JSONFieldType
	// time related
	case types.QValueKindTimestamp, types.QValueKindTimestampTZ:
		bqField.Type = bigquery.TimestampFieldType
	// TODO: https://github.com/PeerDB-io/peerdb/issues/189 - DATE support is incomplete
	case types.QValueKindDate:
		bqField.Type = bigquery.DateFieldType
	// TODO: https://github.com/PeerDB-io/peerdb/issues/189 - TIME/TIMETZ support is incomplete
	case types.QValueKindTime, types.QValueKindTimeTZ:
		bqField.Type = bigquery.TimeFieldType
	// TODO: https://github.com/PeerDB-io/peerdb/issues/189 - handle INTERVAL types again,
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
	case types.QValueKindArrayString, types.QValueKindArrayEnum, types.QValueKindArrayNumeric:
		bqField.Type = bigquery.StringFieldType
		bqField.Repeated = true
	case types.QValueKindGeography, types.QValueKindGeometry, types.QValueKindPoint:
		bqField.Type = bigquery.GeographyFieldType
	// UUID related - stored as strings for now
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
	default:
		return types.QValueKindInvalid
	}
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
