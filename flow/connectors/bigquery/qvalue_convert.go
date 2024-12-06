package connbigquery

import (
	"fmt"

	"cloud.google.com/go/bigquery"

	"github.com/PeerDB-io/peer-flow/datatypes"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model/qvalue"
)

func qValueKindToBigQueryType(columnDescription *protos.FieldDescription, nullableEnabled bool) bigquery.FieldSchema {
	bqField := bigquery.FieldSchema{
		Name:     columnDescription.Name,
		Required: nullableEnabled && !columnDescription.Nullable,
	}
	switch qvalue.QValueKind(columnDescription.Type) {
	// boolean
	case qvalue.QValueKindBoolean:
		bqField.Type = bigquery.BooleanFieldType
	// integer types
	case qvalue.QValueKindInt16, qvalue.QValueKindInt32, qvalue.QValueKindInt64:
		bqField.Type = bigquery.IntegerFieldType
	// decimal types
	case qvalue.QValueKindFloat32, qvalue.QValueKindFloat64:
		bqField.Type = bigquery.FloatFieldType
	case qvalue.QValueKindNumeric:
		precision, scale := datatypes.GetNumericTypeForWarehouse(columnDescription.TypeModifier, datatypes.BigQueryNumericCompatibility{})
		bqField.Type = bigquery.BigNumericFieldType
		bqField.Precision = int64(precision)
		bqField.Scale = int64(scale)
	// string related
	case qvalue.QValueKindString:
		bqField.Type = bigquery.StringFieldType
	// json related
	case qvalue.QValueKindJSON, qvalue.QValueKindJSONB, qvalue.QValueKindHStore:
		bqField.Type = bigquery.JSONFieldType
	// time related
	case qvalue.QValueKindTimestamp, qvalue.QValueKindTimestampTZ:
		bqField.Type = bigquery.TimestampFieldType
	// TODO: https://github.com/PeerDB-io/peerdb/issues/189 - DATE support is incomplete
	case qvalue.QValueKindDate:
		bqField.Type = bigquery.DateFieldType
	// TODO: https://github.com/PeerDB-io/peerdb/issues/189 - TIME/TIMETZ support is incomplete
	case qvalue.QValueKindTime, qvalue.QValueKindTimeTZ:
		bqField.Type = bigquery.TimeFieldType
	// TODO: https://github.com/PeerDB-io/peerdb/issues/189 - handle INTERVAL types again,
	// bytes
	case qvalue.QValueKindBytes:
		bqField.Type = bigquery.BytesFieldType
	case qvalue.QValueKindArrayInt16, qvalue.QValueKindArrayInt32, qvalue.QValueKindArrayInt64:
		bqField.Type = bigquery.IntegerFieldType
		bqField.Repeated = true
	case qvalue.QValueKindArrayFloat32, qvalue.QValueKindArrayFloat64:
		bqField.Type = bigquery.FloatFieldType
		bqField.Repeated = true
	case qvalue.QValueKindArrayBoolean:
		bqField.Type = bigquery.BooleanFieldType
		bqField.Repeated = true
	case qvalue.QValueKindArrayTimestamp, qvalue.QValueKindArrayTimestampTZ:
		bqField.Type = bigquery.TimestampFieldType
		bqField.Repeated = true
	case qvalue.QValueKindArrayDate:
		bqField.Type = bigquery.DateFieldType
		bqField.Repeated = true
	case qvalue.QValueKindArrayString:
		bqField.Type = bigquery.StringFieldType
		bqField.Repeated = true
	case qvalue.QValueKindGeography, qvalue.QValueKindGeometry, qvalue.QValueKindPoint:
		bqField.Type = bigquery.GeographyFieldType
	// UUID related - stored as strings for now
	case qvalue.QValueKindUUID:
		bqField.Type = bigquery.StringFieldType
	case qvalue.QValueKindArrayUUID:
		bqField.Type = bigquery.StringFieldType
		bqField.Repeated = true
	// rest will be strings
	default:
		bqField.Type = bigquery.StringFieldType
	}

	return bqField
}

// BigQueryTypeToQValueKind converts a bigquery.FieldType to a QValueKind
func BigQueryTypeToQValueKind(fieldSchema *bigquery.FieldSchema) qvalue.QValueKind {
	switch fieldSchema.Type {
	case bigquery.StringFieldType:
		if fieldSchema.Repeated {
			return qvalue.QValueKindArrayString
		}
		return qvalue.QValueKindString
	case bigquery.BytesFieldType:
		return qvalue.QValueKindBytes
	case bigquery.IntegerFieldType:
		if fieldSchema.Repeated {
			return qvalue.QValueKindArrayInt64
		}
		return qvalue.QValueKindInt64
	case bigquery.FloatFieldType:
		if fieldSchema.Repeated {
			return qvalue.QValueKindArrayFloat64
		}
		return qvalue.QValueKindFloat64
	case bigquery.BooleanFieldType:
		if fieldSchema.Repeated {
			return qvalue.QValueKindArrayBoolean
		}
		return qvalue.QValueKindBoolean
	case bigquery.TimestampFieldType:
		if fieldSchema.Repeated {
			return qvalue.QValueKindArrayTimestamp
		}
		return qvalue.QValueKindTimestamp
	case bigquery.DateFieldType:
		if fieldSchema.Repeated {
			return qvalue.QValueKindArrayDate
		}
		return qvalue.QValueKindDate
	case bigquery.TimeFieldType:
		return qvalue.QValueKindTime
	case bigquery.RecordFieldType:
		return qvalue.QValueKindStruct
	case bigquery.NumericFieldType, bigquery.BigNumericFieldType:
		return qvalue.QValueKindNumeric
	case bigquery.GeographyFieldType:
		return qvalue.QValueKindGeography
	case bigquery.JSONFieldType:
		return qvalue.QValueKindJSON
	default:
		return qvalue.QValueKindInvalid
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

func BigQueryFieldToQField(bqField *bigquery.FieldSchema) qvalue.QField {
	return qvalue.QField{
		Name:      bqField.Name,
		Type:      BigQueryTypeToQValueKind(bqField),
		Precision: int16(bqField.Precision),
		Scale:     int16(bqField.Scale),
		Nullable:  !bqField.Required,
	}
}
