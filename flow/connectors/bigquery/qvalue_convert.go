package connbigquery

import (
	"cloud.google.com/go/bigquery"

	"github.com/PeerDB-io/peer-flow/model/qvalue"
)

func qValueKindToBigQueryType(colType string) bigquery.FieldType {
	switch qvalue.QValueKind(colType) {
	// boolean
	case qvalue.QValueKindBoolean:
		return bigquery.BooleanFieldType
	// integer types
	case qvalue.QValueKindInt16, qvalue.QValueKindInt32, qvalue.QValueKindInt64:
		return bigquery.IntegerFieldType
	// decimal types
	case qvalue.QValueKindFloat32, qvalue.QValueKindFloat64:
		return bigquery.FloatFieldType
	case qvalue.QValueKindNumeric:
		return bigquery.BigNumericFieldType
	// string related
	case qvalue.QValueKindString:
		return bigquery.StringFieldType
	// json also is stored as string for now
	case qvalue.QValueKindJSON, qvalue.QValueKindHStore:
		return bigquery.JSONFieldType
	// time related
	case qvalue.QValueKindTimestamp, qvalue.QValueKindTimestampTZ:
		return bigquery.TimestampFieldType
	// TODO: https://github.com/PeerDB-io/peerdb/issues/189 - DATE support is incomplete
	case qvalue.QValueKindDate:
		return bigquery.DateFieldType
	// TODO: https://github.com/PeerDB-io/peerdb/issues/189 - TIME/TIMETZ support is incomplete
	case qvalue.QValueKindTime, qvalue.QValueKindTimeTZ:
		return bigquery.TimeFieldType
	// TODO: https://github.com/PeerDB-io/peerdb/issues/189 - handle INTERVAL types again,
	// bytes
	case qvalue.QValueKindBit, qvalue.QValueKindBytes:
		return bigquery.BytesFieldType
	// For Arrays we return the types of the individual elements,
	// and wherever this function is called, the 'Repeated' attribute of
	// FieldSchema must be set to true.
	case qvalue.QValueKindArrayInt16, qvalue.QValueKindArrayInt32, qvalue.QValueKindArrayInt64:
		return bigquery.IntegerFieldType
	case qvalue.QValueKindArrayFloat32, qvalue.QValueKindArrayFloat64:
		return bigquery.FloatFieldType
	case qvalue.QValueKindArrayBoolean:
		return bigquery.BooleanFieldType
	case qvalue.QValueKindArrayTimestamp, qvalue.QValueKindArrayTimestampTZ:
		return bigquery.TimestampFieldType
	case qvalue.QValueKindArrayDate:
		return bigquery.DateFieldType
	case qvalue.QValueKindGeography, qvalue.QValueKindGeometry, qvalue.QValueKindPoint:
		return bigquery.GeographyFieldType
	// rest will be strings
	default:
		return bigquery.StringFieldType
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
	bqType := qValueKindToBigQueryType(colType)
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
