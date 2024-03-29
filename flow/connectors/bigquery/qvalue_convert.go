package connbigquery

import (
	"cloud.google.com/go/bigquery"

	"github.com/PeerDB-io/peer-flow/model/qvalue"
)

func parseKindToBigQueryType(colType string) bigquery.FieldType {
	qt := qvalue.ParseQType(colType)

	switch qt.Kind {
	// boolean
	case qvalue.QKindBoolean:
		return bigquery.BooleanFieldType
	// integer types
	case qvalue.QKindInt16, qvalue.QKindInt32, qvalue.QKindInt64:
		return bigquery.IntegerFieldType
	// decimal types
	case qvalue.QKindFloat32, qvalue.QKindFloat64:
		return bigquery.FloatFieldType
	case qvalue.QKindNumeric:
		return bigquery.BigNumericFieldType
	// string related
	case qvalue.QKindString:
		return bigquery.StringFieldType
	// json also is stored as string for now
	case qvalue.QKindJSON, qvalue.QKindHStore:
		return bigquery.JSONFieldType
	// time related
	case qvalue.QKindTimestamp, qvalue.QKindTimestampTZ:
		return bigquery.TimestampFieldType
	// TODO: https://github.com/PeerDB-io/peerdb/issues/189 - DATE support is incomplete
	case qvalue.QKindDate:
		return bigquery.DateFieldType
	// TODO: https://github.com/PeerDB-io/peerdb/issues/189 - TIME/TIMETZ support is incomplete
	case qvalue.QKindTime, qvalue.QKindTimeTZ:
		return bigquery.TimeFieldType
	// TODO: https://github.com/PeerDB-io/peerdb/issues/189 - handle INTERVAL types again,
	// bytes
	case qvalue.QKindBit, qvalue.QKindBytes:
		return bigquery.BytesFieldType
	case qvalue.QKindGeography, qvalue.QKindGeometry, qvalue.QKindPoint:
		return bigquery.GeographyFieldType
	// rest will be strings
	default:
		return bigquery.StringFieldType
	}
}

// BigQueryTypeToQValueKind converts a bigquery.FieldType to a QValueKind
func BigQueryTypeToQKind(fieldType bigquery.FieldType) qvalue.QKind {
	switch fieldType {
	case bigquery.StringFieldType:
		return qvalue.QKindString
	case bigquery.BytesFieldType:
		return qvalue.QKindBytes
	case bigquery.IntegerFieldType:
		return qvalue.QKindInt64
	case bigquery.FloatFieldType:
		return qvalue.QKindFloat64
	case bigquery.BooleanFieldType:
		return qvalue.QKindBoolean
	case bigquery.TimestampFieldType:
		return qvalue.QKindTimestamp
	case bigquery.DateFieldType:
		return qvalue.QKindDate
	case bigquery.TimeFieldType:
		return qvalue.QKindTime
	case bigquery.RecordFieldType:
		return qvalue.QKindStruct
	case bigquery.NumericFieldType, bigquery.BigNumericFieldType:
		return qvalue.QKindNumeric
	case bigquery.GeographyFieldType:
		return qvalue.QKindGeography
	case bigquery.JSONFieldType:
		return qvalue.QKindJSON
	default:
		return qvalue.QKindInvalid
	}
}

func qValueKindToBigQueryTypeString(colType string) string {
	bqType := parseKindToBigQueryType(colType)
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
	var array uint8
	if bqField.Repeated {
		array = 1
	}
	return qvalue.QField{
		Name: bqField.Name,
		Type: qvalue.QType{
			Kind:  BigQueryTypeToQKind(bqField.Type),
			Array: array,
		},
		Precision: int16(bqField.Precision),
		Scale:     int16(bqField.Scale),
		Nullable:  !bqField.Required,
	}
}
