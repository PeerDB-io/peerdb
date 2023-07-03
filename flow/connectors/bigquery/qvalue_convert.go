package connbigquery

import (
	"fmt"

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
		return bigquery.NumericFieldType
	// string related
	case qvalue.QValueKindString:
		return bigquery.StringFieldType
	// json also is stored as string for now
	case qvalue.QValueKindJSON:
		return bigquery.StringFieldType
	// time related
	case qvalue.QValueKindTimestamp, qvalue.QValueKindTimestampTZ:
		return bigquery.TimestampFieldType
	case qvalue.QValueKindDate:
		return bigquery.DateFieldType
	case qvalue.QValueKindTime, qvalue.QValueKindTimeTZ:
		return bigquery.TimeFieldType
	// TODO: handle INTERVAL types again
	// bytes
	case qvalue.QValueKindBit, qvalue.QValueKindBytes:
		return bigquery.BytesFieldType
	// rest will be strings
	default:
		return bigquery.StringFieldType
	}
}

// bigqueryTypeToQValueKind converts a bigquery FieldType to a QValueKind.
func BigQueryTypeToQValueKind(fieldType bigquery.FieldType) (qvalue.QValueKind, error) {
	switch fieldType {
	case bigquery.StringFieldType:
		return qvalue.QValueKindString, nil
	case bigquery.BytesFieldType:
		return qvalue.QValueKindBytes, nil
	case bigquery.IntegerFieldType:
		return qvalue.QValueKindInt64, nil
	case bigquery.FloatFieldType:
		return qvalue.QValueKindFloat64, nil
	case bigquery.BooleanFieldType:
		return qvalue.QValueKindBoolean, nil
	case bigquery.TimestampFieldType:
		return qvalue.QValueKindTimestamp, nil
	case bigquery.DateFieldType:
		return qvalue.QValueKindDate, nil
	case bigquery.TimeFieldType:
		return qvalue.QValueKindTime, nil
	case bigquery.RecordFieldType:
		return qvalue.QValueKindStruct, nil
	case bigquery.NumericFieldType:
		return qvalue.QValueKindNumeric, nil
	case bigquery.GeographyFieldType:
		return qvalue.QValueKindString, nil
	default:
		return "", fmt.Errorf("unsupported bigquery field type: %v", fieldType)
	}
}
