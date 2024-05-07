package connbigquery

import (
	"cloud.google.com/go/bigquery"

	"github.com/PeerDB-io/peer-flow/model/qvalue"
)

func qValueKindToBigQueryType(colType string) bigquery.FieldSchema {
	switch qvalue.QValueKind(colType) {
	case qvalue.QValueKindBoolean:
		return bigquery.FieldSchema{
			Type: bigquery.BooleanFieldType,
		}
	case qvalue.QValueKindInt16, qvalue.QValueKindInt32, qvalue.QValueKindInt64:
		return bigquery.FieldSchema{
			Type: bigquery.IntegerFieldType,
		}
	case qvalue.QValueKindFloat32, qvalue.QValueKindFloat64:
		return bigquery.FieldSchema{
			Type: bigquery.FloatFieldType,
		}
	case qvalue.QValueKindNumeric:
		return bigquery.FieldSchema{
			Type: bigquery.NumericFieldType,
		}
	case qvalue.QValueKindString:
		return bigquery.FieldSchema{
			Type: bigquery.StringFieldType,
		}
	case qvalue.QValueKindJSON, qvalue.QValueKindHStore:
		return bigquery.FieldSchema{
			Type: bigquery.JSONFieldType,
		}
	case qvalue.QValueKindTimestamp, qvalue.QValueKindTimestampTZ:
		return bigquery.FieldSchema{
			Type: bigquery.TimestampFieldType,
		}
	case qvalue.QValueKindDate:
		return bigquery.FieldSchema{
			Type: bigquery.DateFieldType,
		}
	case qvalue.QValueKindTime, qvalue.QValueKindTimeTZ:
		return bigquery.FieldSchema{
			Type: bigquery.TimeFieldType,
		}
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
	case qvalue.QValueKindGeography, qvalue.QValueKindGeometry, qvalue.QValueKindPoint:
		return bigquery.FieldSchema{
			Type: bigquery.GeographyFieldType,
		}
	default:
		return bigquery.FieldSchema{
			Type: bigquery.StringFieldType,
		}
	}
}

// BigQueryTypeToQValueKind converts a bigquery.FieldType to a QValueKind
func BigQueryTypeToQValueKind(fieldType bigquery.FieldType) qvalue.QValueKind {
	switch fieldType {
	case bigquery.StringFieldType:
		return qvalue.QValueKindString
	case bigquery.BytesFieldType:
		return qvalue.QValueKindBytes
	case bigquery.IntegerFieldType:
		return qvalue.QValueKindInt64
	case bigquery.FloatFieldType:
		return qvalue.QValueKindFloat64
	case bigquery.BooleanFieldType:
		return qvalue.QValueKindBoolean
	case bigquery.TimestampFieldType:
		return qvalue.QValueKindTimestamp
	case bigquery.DateFieldType:
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
	bqType := qValueKindToBigQueryType(colType).Type
	bqTypeAsString := string(bqType)
	// string(bigquery.FloatFieldType) is "FLOAT" which is not a BigQuery type.
	if bqType == bigquery.FloatFieldType {
		bqTypeAsString = "FLOAT64"
	}
	if bqType == bigquery.BooleanFieldType {
		bqTypeAsString = "BOOL"
	}
	return bqTypeAsString
}

func BigQueryFieldToQField(bqField *bigquery.FieldSchema) qvalue.QField {
	return qvalue.QField{
		Name:      bqField.Name,
		Type:      BigQueryTypeToQValueKind(bqField.Type),
		Precision: int16(bqField.Precision),
		Scale:     int16(bqField.Scale),
		Nullable:  !bqField.Required,
	}
}
