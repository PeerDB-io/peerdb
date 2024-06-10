package connbigquery

import (
	"cloud.google.com/go/bigquery"

	"github.com/PeerDB-io/peer-flow/model/qvalue"
)

func qValueKindToBigQueryType(colType string) bigquery.FieldSchema {
	switch qvalue.QValueKind(colType) {
	// boolean
	case qvalue.QValueKindBoolean:
		return bigquery.FieldSchema{
			Type: bigquery.BooleanFieldType,
		}
	// integer types
	case qvalue.QValueKindInt16, qvalue.QValueKindInt32, qvalue.QValueKindInt64:
		return bigquery.FieldSchema{
			Type: bigquery.IntegerFieldType,
		}
	// decimal types
	case qvalue.QValueKindFloat32, qvalue.QValueKindFloat64:
		return bigquery.FieldSchema{
			Type: bigquery.FloatFieldType,
		}
	case qvalue.QValueKindNumeric:
		return bigquery.FieldSchema{
			Type: bigquery.BigNumericFieldType,
		}
	// string related
	case qvalue.QValueKindString:
		return bigquery.FieldSchema{
			Type: bigquery.StringFieldType,
		}
	// json also is stored as string for now
	case qvalue.QValueKindJSON, qvalue.QValueKindHStore:
		return bigquery.FieldSchema{
			Type: bigquery.JSONFieldType,
		}
	// time related
	case qvalue.QValueKindTimestamp, qvalue.QValueKindTimestampTZ:
		return bigquery.FieldSchema{
			Type: bigquery.TimestampFieldType,
		}
	// TODO: https://github.com/PeerDB-io/peerdb/issues/189 - DATE support is incomplete
	case qvalue.QValueKindDate:
		return bigquery.FieldSchema{
			Type: bigquery.DateFieldType,
		}
	// TODO: https://github.com/PeerDB-io/peerdb/issues/189 - TIME/TIMETZ support is incomplete
	case qvalue.QValueKindTime, qvalue.QValueKindTimeTZ:
		return bigquery.FieldSchema{
			Type: bigquery.TimeFieldType,
		}
	// TODO: https://github.com/PeerDB-io/peerdb/issues/189 - handle INTERVAL types again,
	// bytes
	case qvalue.QValueKindBit, qvalue.QValueKindBytes:
		return bigquery.FieldSchema{
			Type: bigquery.BytesFieldType,
		}
	case qvalue.QValueKindArrayInt16, qvalue.QValueKindArrayInt32, qvalue.QValueKindArrayInt64:
		return bigquery.FieldSchema{
			Type:     bigquery.IntegerFieldType,
			Repeated: true,
		}
	case qvalue.QValueKindArrayFloat32, qvalue.QValueKindArrayFloat64:
		return bigquery.FieldSchema{
			Type:     bigquery.FloatFieldType,
			Repeated: true,
		}
	case qvalue.QValueKindArrayBoolean:
		return bigquery.FieldSchema{
			Type:     bigquery.BooleanFieldType,
			Repeated: true,
		}
	case qvalue.QValueKindArrayTimestamp, qvalue.QValueKindArrayTimestampTZ:
		return bigquery.FieldSchema{
			Type:     bigquery.TimestampFieldType,
			Repeated: true,
		}
	case qvalue.QValueKindArrayDate:
		return bigquery.FieldSchema{
			Type:     bigquery.DateFieldType,
			Repeated: true,
		}
	case qvalue.QValueKindArrayString:
		return bigquery.FieldSchema{
			Type:     bigquery.StringFieldType,
			Repeated: true,
		}
	case qvalue.QValueKindGeography, qvalue.QValueKindGeometry, qvalue.QValueKindPoint:
		return bigquery.FieldSchema{
			Type: bigquery.GeographyFieldType,
		}
	// rest will be strings
	default:
		return bigquery.FieldSchema{
			Type: bigquery.StringFieldType,
		}
	}
}

// BigQueryTypeToQValueKind converts a bigquery.FieldType to a QValueKind
func BigQueryTypeToQValueKind(fieldSchema bigquery.FieldSchema) qvalue.QValueKind {
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

func qValueKindToBigQueryTypeString(colType string) string {
	bqTypeSchema := qValueKindToBigQueryType(colType)
	bqType := string(bqTypeSchema.Type)
	if bqTypeSchema.Type == bigquery.FloatFieldType {
		bqType = "FLOAT64"
	}
	if bqTypeSchema.Type == bigquery.BooleanFieldType {
		bqType = "BOOL"
	}
	if bqTypeSchema.Repeated {
		return "ARRAY<" + bqType + ">"
	}
	return bqType
}

func BigQueryFieldToQField(bqField *bigquery.FieldSchema) qvalue.QField {
	return qvalue.QField{
		Name:      bqField.Name,
		Type:      BigQueryTypeToQValueKind(*bqField),
		Precision: int16(bqField.Precision),
		Scale:     int16(bqField.Scale),
		Nullable:  !bqField.Required,
	}
}
